// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Package publisher is the publish loop the six consuming projects would
// otherwise each write again: the hash-dedup retained publish, the retraction
// ordering Home Assistant enforces, the orphan sweep over retained discovery
// configs, and the birth/LWT availability policy.
//
// It exists because ADR 0070 counted what a types-only library would leave
// behind. Four or five near-identical copies of this loop already exist across
// the family, and they have drifted in ways an operator pays for: one bridge's
// daemon LWT is referenced by no entity at all, so a hard crash leaves a
// retained "online" standing forever; another publishes its availability topic
// inside Home Assistant's own birth tree, where it is both wrong and inert.
// Neither defect is visible from the code that contains it — only from a
// broker capture — which is why the policy belongs in one place with the
// measurement written beside it.
//
// The transport is an interface declared here rather than
// [github.com/SukramJ/go-mqtt] itself, and it speaks in plain bytes and a
// plain QoS byte. Two reasons, both measured: the consumers wrap their client
// differently — one publishes through a circuit breaker and subscribes around
// it — so a concrete client type would fit none of them; and the whole
// runtime has to be exercisable without a broker, which is what the fake in
// this package's own tests does. A go-mqtt client is adapted in one call, see
// [github.com/SukramJ/go-hamqtt/publisher/gomqtt].
package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// Handler receives one delivered message.
//
// Deliberately the flat three-argument shape rather than a message struct: the
// runtime reads a retained discovery config and needs the topic, the bytes and
// nothing else, and a struct here would have to grow a field every time a
// transport gains one.
//
// Contract, inherited from the transport and not negotiable: a handler runs
// synchronously inline in the client's read loop — the same goroutine that
// decodes PUBACK and PINGRESP — so it must return promptly. Anything that
// publishes, and therefore waits on an acknowledgement that only that same
// goroutine can deliver, self-deadlocks. [Runtime.WatchBirth] is the measured
// case: it parses inline and hands the republish to a worker.
type Handler func(topic string, payload []byte, retained bool)

// Transport is the narrow slice of an MQTT client this runtime needs.
//
// Three methods, because that is what the four jobs cost: a retained publish
// for configs and availability, a subscribe for the orphan snapshot and the
// birth topic, and the unsubscribe that takes the snapshot down again. A
// runtime that could do more than this would be able to reach past the
// consumer's own client policy — its QoS defaults, its breaker, its reconnect
// loop — which is exactly what the six copies did.
type Transport interface {
	// Publish sends payload to topic. An empty payload with retain set is a
	// retraction: it clears the broker's retained message.
	Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error
	// Subscribe registers handler for filter and blocks until the broker has
	// acknowledged it. The runtime relies on that: a snapshot window that
	// starts before the subscription exists sees none of the retained
	// messages it was opened for.
	Subscribe(ctx context.Context, filter string, qos byte, handler Handler) error
	// Unsubscribe removes the subscription for filter.
	Unsubscribe(ctx context.Context, filter string) error
}

// Config parameterises a [Runtime]. The zero value is usable except for the
// availability half, which needs a [Config.StatusTopic].
type Config struct {
	// Prefix is the discovery prefix. Empty means [discovery.DefaultPrefix].
	Prefix string

	// StatusTopic is the consumer's own availability topic — the one its
	// Last Will clears and [Runtime.AnnounceOnline] sets.
	//
	// It must be a topic the published entities actually reference in their
	// `availability` list, and it must sit in the consumer's own tree rather
	// than under the discovery prefix. Both halves are measured defects of
	// the reference implementations: one bridge puts it at
	// `<discovery_prefix>/status/lwt`, inside Home Assistant's own birth
	// tree, and two reference no entity to it at all, so a hard crash leaves
	// every entity looking available forever. [topic.Layout.Bridge] renders
	// the right one, and [discovery.StdContext] already points
	// [model.LevelBridge] at it.
	StatusTopic string

	// QoS applies to every publish and subscribe the runtime performs.
	// The zero value is QoS 1, which is what a retained config wants: at
	// most once loses the config a consumer may never publish again.
	QoS byte

	// SweepWindow is how long [Runtime.Sweep] listens before judging. Zero
	// means [DefaultSweepWindow].
	SweepWindow time.Duration

	// OnResync is called after a birth-triggered republish, with the number
	// of configs replayed and whatever error the replay produced.
	//
	// A hook rather than a log line: the runtime cannot know whether a
	// consumer counts this as a metric, logs it, or ignores it, and the one
	// measured consumer does the first two. Nil is fine.
	OnResync func(replayed int, err error)

	// Logger receives the runtime's own diagnostics. Nil means
	// [slog.Default].
	Logger *slog.Logger
}

// DefaultSweepWindow is how long a snapshot subscription listens before it
// decides what is an orphan.
//
// Two seconds, taken from the measured consumer: long enough for Mosquitto,
// EMQX and VerneMQ to flush a retained QoS 1 queue of a few thousand configs
// to a fresh subscriber, short enough to sit in a boot path. A window that
// ends early does not mis-delete — the sweep retracts strictly what it saw —
// it only leaves orphans for the next boot.
const DefaultSweepWindow = 2 * time.Second

// ErrNoStatusTopic is returned by the availability calls when
// [Config.StatusTopic] is empty. Refusing is deliberate: publishing an
// availability marker to a topic nobody named is the inert-LWT defect this
// package exists to stop reproducing.
var ErrNoStatusTopic = errors.New("publisher: no status topic configured")

// Runtime owns the retained discovery state of one consumer process: what it
// has published, what it has superseded, and what it must replay when Home
// Assistant comes back.
//
// # Locking
//
// Two locks, and the order between them is fixed:
//
//	sweepMu  →  mu
//
// sweepMu serialises snapshot windows end-to-end; there must never be two,
// because a transport keys its subscriptions by filter and the second window's
// teardown would unsubscribe the filter out from under the first. mu guards
// the three maps below and is never held across a [Transport] call — a
// publish blocks on a broker acknowledgement, and holding the claim lock
// across it would stall every other publisher and the sweep behind one slow
// PUBACK.
type Runtime struct {
	tr  Transport
	cfg Config
	log *slog.Logger

	sweepMu sync.Mutex

	mu sync.Mutex
	// declared maps a retained config topic to the exact bytes the broker
	// accepted. The bytes rather than a digest of them: the birth resync
	// replays this map, and a digest would force the consumer to re-render
	// its whole fleet to answer a question the runtime already knows the
	// answer to.
	declared map[string][]byte
	// announced marks a topic whose publish is in flight. The broker fans a
	// message out to its subscribers — the sweep's own snapshot
	// subscription included — before Publish returns, so a claim taken only
	// afterwards arrives too late to keep the sweep off a config being
	// written right now.
	announced map[string]bool
	// superseded records the topics retracted to clear the way for the
	// other discovery form. See [Runtime.PublishBundle].
	superseded map[string]bool

	birth *dispatcher
}

// New builds a runtime over tr. A nil transport is a programming error and
// panics here rather than on the first publish, where the stack no longer
// names the composition root that got it wrong.
func New(tr Transport, cfg Config) *Runtime {
	if tr == nil {
		panic("publisher: nil transport")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = discovery.DefaultPrefix
	}
	if cfg.QoS == 0 {
		cfg.QoS = 1
	}
	if cfg.SweepWindow <= 0 {
		cfg.SweepWindow = DefaultSweepWindow
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Runtime{
		tr:         tr,
		cfg:        cfg,
		log:        logger,
		declared:   map[string][]byte{},
		announced:  map[string]bool{},
		superseded: map[string]bool{},
	}
}

// Prefix is the discovery prefix this runtime publishes under, after the
// default has been applied.
func (r *Runtime) Prefix() string { return r.cfg.Prefix }

// Publish writes one retained discovery config, and reports whether it
// reached the broker.
//
// The dedup gate is the point. A consumer's boot snapshot re-renders every
// entity it drives, and on a steady-state restart every single payload is
// byte-identical to the retained one already on the broker — nine thousand
// writes that change nothing, against a Home Assistant that re-reads and
// re-validates each. Comparing the bytes turns that into zero, and the false
// return value is what lets a consumer count the difference.
//
// An empty payload is a retraction and leaves the topic out of the declared
// set rather than recording it, so that set keeps naming exactly the entities
// this process drives — which is the set the sweep and the birth resync both
// read. Prefer [Runtime.Retract], which says so at the call site.
//
// The topic is claimed before the publish, not after: see the note on
// Runtime.announced.
func (r *Runtime) Publish(ctx context.Context, topic string, payload []byte) (bool, error) {
	if topic == "" {
		return false, errors.New("publisher: empty topic")
	}
	retraction := len(payload) == 0

	r.mu.Lock()
	previous, declared := r.declared[topic]
	r.mu.Unlock()
	if declared && bytes.Equal(previous, payload) {
		return false, nil
	}
	if retraction && !declared {
		// Nothing this process declared, nothing to clear. A retraction of
		// a topic the broker may still hold is [Runtime.Retract]'s job, and
		// it says so explicitly rather than arriving here by accident.
		return false, nil
	}

	if !retraction {
		r.mu.Lock()
		r.announced[topic] = true
		r.mu.Unlock()
	}

	if err := r.tr.Publish(ctx, topic, payload, r.cfg.QoS, true); err != nil {
		// The claim is dropped again on failure. Leaving it standing would
		// keep the sweep off a topic that carries a previous build's config
		// and that this process has just failed to overwrite — the one case
		// where the orphan really is one.
		if !retraction {
			r.mu.Lock()
			delete(r.announced, topic)
			r.mu.Unlock()
		}
		return false, fmt.Errorf("publisher: publish %s: %w", topic, err)
	}

	// Record what the broker accepted, never what was merely attempted. A
	// consumer publishing through a circuit breaker fails every config of a
	// boot that hits an outage; caching the payload anyway would make the
	// next identical one hit the dedup gate and publish nothing, leaving the
	// entity absent until the operator restarts the daemon.
	r.mu.Lock()
	if retraction {
		delete(r.declared, topic)
		delete(r.announced, topic)
	} else {
		r.declared[topic] = bytes.Clone(payload)
	}
	r.mu.Unlock()
	return true, nil
}

// PublishBundle writes one device's discovery document, retracting the
// per-entity configs it supersedes first.
//
// The order is not cosmetic and it is not a preference. Measured against a
// live Home Assistant 2026.9 instance on 2026-09-10 (openccu-loom ADR 0070,
// amendment of that date): publishing a device bundle while a per-entity
// config for the same unique id is still retained is refused, and the entire
// signal is one log line on the consumer's side —
//
//	WARNING [mqtt.entity] Received a conflicting MQTT discovery message for
//	entity sensor.…; the entity was previously discovered on topic
//	homeassistant/sensor/…/config …; the conflicting discovery message was
//	received on topic homeassistant/device/…/config
//
// The bundle sits retained on the broker, the entity keeps its old config,
// and nothing anywhere reports that the migration did not happen. The refusal
// is symmetric — measured again on 2026-09-11, a per-entity config published
// while the device document is still retained is refused the same way with
// the topics named the other way round — so [Runtime.PublishComponent]
// retracts in the opposite direction for exactly the same reason.
// `migrate_discovery: true` was tried and did not lift the conflict.
//
// The two publishes belong together. Between the retraction and the bundle
// the entity does not exist — absent, not merely unavailable — which is why a
// failed retraction aborts before the bundle rather than pressing on: having
// lost the old config and then failed to write the new one is the one outcome
// worse than not having started. The registry entry itself survives the round
// trip; Home Assistant keys it on `unique_id`, not on the discovery topic,
// and the measurement kept a renamed entity_id, a custom name and the
// device_id across the move.
//
// A superseded topic is retracted once per process rather than on every
// change of the document. After the first retraction the broker holds nothing
// there, so a second is a message for nothing — and a boot that rewrites a
// device forty times would otherwise send forty rounds of them.
func (r *Runtime) PublishBundle(ctx context.Context, b *discovery.Bundle) (bool, error) {
	if b == nil {
		return false, errors.New("publisher: nil bundle")
	}
	payload, err := json.Marshal(b)
	if err != nil {
		return false, fmt.Errorf("publisher: marshal bundle %s: %w", b.NodeID, err)
	}
	// BundleConfigTopic rather than [discovery.Bundle.Topic] so a prefix
	// written with a trailing slash addresses the same tree the parser reads.
	topic := BundleConfigTopic(r.cfg.Prefix, b.NodeID)

	// Checked before the retraction, not after: a repeat publish must not
	// tear down the per-entity topics a second time, and on a steady-state
	// boot this is the common path.
	r.mu.Lock()
	previous, declared := r.declared[topic]
	r.mu.Unlock()
	if declared && bytes.Equal(previous, payload) {
		return false, nil
	}

	if err := r.supersede(ctx, SupersededTopics(r.cfg.Prefix, b)); err != nil {
		return false, err
	}
	return r.Publish(ctx, topic, payload)
}

// PublishComponent writes one entity's standalone discovery config, in the
// per-entity form, retracting the device document it supersedes first.
//
// The rollback direction of [Runtime.PublishBundle], and it needs the same
// care for the same measured reason: Home Assistant's refusal is symmetric,
// so turning device-bundle mode back off is not "stop publishing bundles".
// Without the document retracted first, every per-entity config of that first
// boot is refused and the only evidence is a log line.
//
// objectID is the topic's object-id segment, conventionally the component key
// the same entity has inside a bundle, so the two forms address the same
// entity from either side. The platform comes from the component: the
// per-entity topic carries it, which is why [discovery.Component.EntityJSON]
// then drops the key from the payload.
func (r *Runtime) PublishComponent(
	ctx context.Context,
	nodeID, objectID string,
	comp discovery.Component,
) (bool, error) {
	if nodeID == "" || objectID == "" {
		return false, errors.New("publisher: component needs a node id and an object id")
	}
	if comp.Platform == "" {
		return false, errors.New("publisher: component has no platform")
	}
	payload, err := comp.EntityJSON()
	if err != nil {
		return false, fmt.Errorf("publisher: encode component %s/%s: %w", nodeID, objectID, err)
	}
	topic := EntityConfigTopic(r.cfg.Prefix, string(comp.Platform), nodeID, objectID)

	r.mu.Lock()
	previous, declared := r.declared[topic]
	r.mu.Unlock()
	if declared && bytes.Equal(previous, payload) {
		return false, nil
	}

	if err := r.supersede(ctx, []string{BundleConfigTopic(r.cfg.Prefix, nodeID)}); err != nil {
		return false, err
	}
	return r.Publish(ctx, topic, payload)
}

// supersede clears the topics of the other discovery form, once each.
//
// A topic this process never published is retracted anyway. On the boot that
// performs the migration the declared set is empty and the retained configs
// on the broker are precisely the previous build's — skipping them because
// this process does not know them would leave every one in place, and every
// document refused.
func (r *Runtime) supersede(ctx context.Context, topics []string) error {
	for _, t := range topics {
		r.mu.Lock()
		done := r.superseded[t]
		r.mu.Unlock()
		if done {
			continue
		}
		if err := r.tr.Publish(ctx, t, nil, r.cfg.QoS, true); err != nil {
			return fmt.Errorf("publisher: retract superseded %s: %w", t, err)
		}
		r.mu.Lock()
		r.superseded[t] = true
		// An empty payload is a retraction, not a declaration. Leaving the
		// topic declared would keep a cleared entity in the set the sweep
		// treats as live, and the birth resync would replay an empty
		// payload to a topic the broker no longer retains.
		delete(r.declared, t)
		delete(r.announced, t)
		r.mu.Unlock()
	}
	return nil
}

// Retract clears retained configs and forgets them.
//
// Separate from a zero-length [Runtime.Publish] because the two answer
// different questions. Publish retracts only what this process declared,
// which is what a dedup gate must do; Retract clears a topic whatever this
// process knows about it, which is what a consumer dropping an entity — or
// migrating away from a form — actually needs.
//
// Best-effort across the list: one topic a broker refuses must not leave the
// rest of a removed device's entities standing in Home Assistant. Every
// failure is joined so the caller sees the whole picture.
func (r *Runtime) Retract(ctx context.Context, topics ...string) error {
	var errs []error
	for _, t := range topics {
		if err := ctx.Err(); err != nil {
			// A cancelled context is a shutdown, not a per-topic failure.
			errs = append(errs, err)
			break
		}
		if err := r.tr.Publish(ctx, t, nil, r.cfg.QoS, true); err != nil {
			errs = append(errs, fmt.Errorf("publisher: retract %s: %w", t, err))
			continue
		}
		r.mu.Lock()
		delete(r.declared, t)
		delete(r.announced, t)
		r.mu.Unlock()
	}
	return errors.Join(errs...)
}

// Declared lists the config topics this process currently claims, sorted.
//
// Sorted rather than map order because the two things that read it — an
// operator's diagnostic dump and a test — both compare one run against
// another, and map order makes that comparison meaningless.
func (r *Runtime) Declared() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.declared))
	for t := range r.declared {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// Republish re-sends every declared config and reports how many went out.
//
// It bypasses the dedup gate by construction — it publishes the cached bytes
// directly — because the whole point is to write payloads the broker already
// holds. Home Assistant keeps the retained configs across its own restart but
// does not reliably re-read them across every addon reload and firmware
// update, and a deterministic replay on the birth message closes that race.
//
// Best-effort per topic: a breaker open for one entity must not abort the
// replay for the fleet behind it, which would leave most of it unavailable
// after a broker restart. A cancelled context stops the walk rather than
// turning every remaining topic into an error.
func (r *Runtime) Republish(ctx context.Context) (int, error) {
	r.mu.Lock()
	snapshot := make(map[string][]byte, len(r.declared))
	for t, p := range r.declared {
		snapshot[t] = p
	}
	r.mu.Unlock()

	topics := make([]string, 0, len(snapshot))
	for t := range snapshot {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	var errs []error
	sent := 0
	for _, t := range topics {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := r.tr.Publish(ctx, t, snapshot[t], r.cfg.QoS, true); err != nil {
			errs = append(errs, fmt.Errorf("publisher: republish %s: %w", t, err))
			continue
		}
		sent++
	}
	return sent, errors.Join(errs...)
}

// SupersededTopics returns the per-entity config topics a bundle replaces, in
// stable order.
//
// Derived from the document's own components rather than read back from the
// broker, so the migration needs no snapshot: a component inside a bundle and
// the per-entity config of the same entity differ only in where they are
// addressed. Tombstoned components — the platform-only entries
// [discovery.Bundle.Remove] writes — are included, because their retained
// per-entity config is exactly what has to go.
//
// Exported because a consumer that publishes its bundles through something
// other than [Runtime.PublishBundle] still needs the list, and deriving it a
// second time is how two call sites end up disagreeing about the object-id
// segment.
func SupersededTopics(prefix string, b *discovery.Bundle) []string {
	if b == nil {
		return nil
	}
	out := make([]string, 0, len(b.Components))
	for _, key := range b.Keys() {
		platform := b.Components[key].Platform
		if platform == "" {
			continue
		}
		out = append(out, EntityConfigTopic(prefix, string(platform), b.NodeID, key))
	}
	sort.Strings(out)
	return out
}
