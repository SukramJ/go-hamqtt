// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// This file is the conformance fixture: one broker, all four planes of the
// runtime layer driven together.
//
// It exists because every defect this package's doc comments cite is a
// disagreement BETWEEN planes, and a unit test cannot see one. The discovery
// plane writes `state_topic: "bridge/AC-1/values/power"` into a config and
// the assertion passes; the state plane writes to
// `bridge/AC-1/VALUES/power` and its own assertion passes too. Both halves
// are green, the entity sits at `unknown` in Home Assistant forever, and
// nothing on the wire names the mismatch — which is the shape of four of the
// five measured defects in §2.2 of the shared-model concept: an availability
// topic no entity references, an entity pointing at a topic no producer
// feeds, a declared state topic that is never published, a `default_entity_id`
// on one platform published under another.
//
// So the fixture asserts agreement rather than behaviour. Every test below
// takes the rendered config as the specification — it is the only artefact
// Home Assistant actually reads — and checks that the plane responsible for
// each topic it names is the plane that writes it.

// planeOp is one transport call, recorded in order across every plane.
//
// Ordered, and shared across the planes rather than one recorder per plane,
// because two of the orderings this layer encodes span planes: the
// retract-then-publish rule of [Runtime.PublishBundle], and the
// config-before-availability rule of [AvailabilityPublisher.Retract]. A
// per-plane log cannot express either.
type planeOp struct {
	kind    string // "publish", "subscribe", "unsubscribe"
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

// planeBroker is a broker that remembers, shared by all four planes.
//
// Named apart from this package's other two fixtures — `fakeTransport` of
// the discovery loop and `availBroker` of the availability plane — because
// it is deliberately the only one with a retained store AND a subscription
// fan-out AND a cross-plane op log. The other two are narrower on purpose
// and must not grow into this.
type planeBroker struct {
	mu       sync.Mutex
	ops      []planeOp
	retained map[string][]byte
	subs     map[string]Handler

	// fail decides an error per topic, standing in for a breaker open on one
	// entity while the rest of the fleet publishes.
	fail func(topic string) error
}

func newPlaneBroker() *planeBroker {
	return &planeBroker{retained: map[string][]byte{}, subs: map[string]Handler{}}
}

func (b *planeBroker) Publish(_ context.Context, t string, payload []byte, qos byte, retain bool) error {
	b.mu.Lock()
	if b.fail != nil {
		if err := b.fail(t); err != nil {
			b.mu.Unlock()
			return err
		}
	}
	b.ops = append(b.ops, planeOp{
		kind: "publish", topic: t, payload: append([]byte(nil), payload...), qos: qos, retain: retain,
	})
	if retain {
		if len(payload) == 0 {
			delete(b.retained, t)
		} else {
			b.retained[t] = append([]byte(nil), payload...)
		}
	}
	handlers := make([]Handler, 0, len(b.subs))
	for filter, h := range b.subs {
		if planeMatches(filter, t) {
			handlers = append(handlers, h)
		}
	}
	b.mu.Unlock()
	// Fanned out before Publish returns, exactly as a broker does.
	for _, h := range handlers {
		h(t, payload, retain)
	}
	return nil
}

func (b *planeBroker) Subscribe(_ context.Context, filter string, qos byte, h Handler) error {
	b.mu.Lock()
	b.ops = append(b.ops, planeOp{kind: "subscribe", topic: filter, qos: qos})
	b.subs[filter] = h
	replay := map[string][]byte{}
	for t, p := range b.retained {
		if planeMatches(filter, t) {
			replay[t] = p
		}
	}
	b.mu.Unlock()
	keys := make([]string, 0, len(replay))
	for t := range replay {
		keys = append(keys, t)
	}
	sort.Strings(keys)
	for _, t := range keys {
		h(t, replay[t], true)
	}
	return nil
}

func (b *planeBroker) Unsubscribe(_ context.Context, filter string) error {
	b.mu.Lock()
	b.ops = append(b.ops, planeOp{kind: "unsubscribe", topic: filter})
	delete(b.subs, filter)
	b.mu.Unlock()
	return nil
}

// planeMatches is the sliver of MQTT filter matching the fixture needs: an
// exact topic, a trailing multi-level wildcard, or a single-level `+`.
//
// `+` is not decoration here: a consumer's command subscription is a wildcard
// in practice — one filter per device tree, not one per datapoint — and the
// state/command disjointness invariant has to be checked against the filter
// the consumer would really install.
func planeMatches(filter, t string) bool {
	fs := strings.Split(filter, "/")
	ts := strings.Split(t, "/")
	for i, seg := range fs {
		if seg == "#" {
			return true
		}
		if i >= len(ts) {
			return false
		}
		if seg != "+" && seg != ts[i] {
			return false
		}
	}
	return len(fs) == len(ts)
}

// seed puts a retained message on the broker without any plane having written
// it — a previous build's leftover, which is what the orphan paths exist for.
func (b *planeBroker) seed(t string, payload []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retained[t] = append([]byte(nil), payload...)
}

// drop clears the retained store while the connection stays up: a broker
// restarted without a persistent retained store, which is the case both
// Republish calls exist for and the one a test cannot reach any other way.
func (b *planeBroker) drop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.retained)
}

// retainedTopics lists what the broker currently holds, sorted.
func (b *planeBroker) retainedTopics() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.retained))
	for t := range b.retained {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// holds reports whether the broker retains a non-empty payload on t, which is
// the only question Home Assistant's own subscriber asks.
func (b *planeBroker) holds(t string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.retained[t]) > 0
}

func (b *planeBroker) retainedPayload(t string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.retained[t])
}

// publishOrder returns the topics published, in order, so a cross-plane
// ordering can be asserted.
func (b *planeBroker) publishOrder() []planeOp {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]planeOp, 0, len(b.ops))
	for _, o := range b.ops {
		if o.kind == "publish" {
			out = append(out, o)
		}
	}
	return out
}

const (
	// conformPrefix is Home Assistant's tree. conformRoot is the consumer's.
	// They must be different strings, and one of the measured defects is a
	// consumer that put its own availability topic inside the first one.
	conformPrefix = "homeassistant"
	conformRoot   = "bridge"
)

// fleet is the whole runtime layer of one consumer process over one broker.
type fleet struct {
	t *testing.T

	broker *planeBroker
	layout topic.Layout
	dctx   discovery.StdContext

	run   *Runtime
	avail *AvailabilityPublisher
	state *StatePublisher

	dev      *model.Device
	entities []model.Entity
	bundle   *discovery.Bundle
}

// commandFilter is the one subscription a consumer installs for every command
// its devices accept.
//
// A wildcard, because that is what a consumer can actually install: the
// alternative is one subscription per writable datapoint, re-derived on every
// discovery change. `topic.Default` renders a command as the state topic plus
// a `set` segment, so `<root>/#` is the honest shape — and it is precisely
// that breadth that makes the state/command disjointness invariant worth
// pinning, since a filter this wide matches every state topic too unless the
// last segment is checked.
func commandFilter() string { return conformRoot + "/+/+/+/set" }

// newFleet wires discovery, state, command and availability over one broker,
// exactly as a consumer's composition root does.
func newFleet(t *testing.T) *fleet {
	t.Helper()

	broker := newPlaneBroker()
	layout := topic.Default{Root: conformRoot}
	dctx := discovery.StdContext{Layout: layout, Namespace: "conform", Lang: "en"}

	dev := &model.Device{
		Identity: model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "AC-1"}}},
		Name:     model.L("Living room AC"),
		Model:    "FTXM35",
	}

	power := &model.Basic{
		EntityKey:      "power",
		EntityPlatform: hacatalog.PlatformSensor,
		Description: model.Description{
			Name:        model.L("Power"),
			DeviceClass: model.DeviceClass(hacatalog.SensorDeviceClassPower),
			StateClass:  hacatalog.StateClassMeasurement,
			Unit:        "W",
		},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S(dev.UID(), "", model.BucketValues, "power"),
			Mode: model.Read,
		}},
	}
	boost := &model.Basic{
		EntityKey:      "boost",
		EntityPlatform: hacatalog.PlatformSwitch,
		Description:    model.Description{Name: model.L("Boost")},
		Binds: []model.Binding{
			{
				Role: model.RoleState,
				Slot: model.S(dev.UID(), "", model.BucketValues, "boost"),
				Mode: model.Read,
			},
			{
				Role: model.RoleCommand,
				Slot: model.S(dev.UID(), "", model.BucketValues, "boost"),
				Mode: model.Write,
			},
		},
	}
	target := &model.Basic{
		EntityKey:      "target",
		EntityPlatform: hacatalog.PlatformNumber,
		Description: model.Description{
			Name: model.L("Target temperature"),
			Min:  model.Ptr(16.0),
			Max:  model.Ptr(30.0),
			Step: model.Ptr(0.5),
		},
		Binds: []model.Binding{
			{
				Role: model.RoleState,
				Slot: model.S(dev.UID(), "", model.BucketValues, "target"),
				Mode: model.Read,
			},
			{
				Role: model.RoleCommand,
				Slot: model.S(dev.UID(), "", model.BucketValues, "target"),
				Mode: model.Write,
			},
		},
	}
	entities := []model.Entity{power, boost, target}

	bundle, err := discovery.Render(dctx, dev, entities,
		discovery.Origin{Name: "go-conform2mqtt", SW: "0.0.0"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if err := discovery.Validate(bundle); err != nil {
		t.Fatalf("validate: %v", err)
	}
	// Guard against the way a cross-plane harness fails silently: every
	// invariant below iterates the rendered components and skips the ones
	// with no topic of the kind it checks, so a fixture whose bundle stopped
	// carrying command topics — a platform key list that moved, a binding
	// mode typo — would pass every test while checking nothing.
	assertFixtureCarriesTopics(t, bundle)

	run := New(broker, Config{
		Prefix: conformPrefix,
		// The same string the layout wrote into every config's bridge-level
		// availability entry. Passing anything else is the measured defect;
		// TestConformanceBridgeAvailabilityIsTheRuntimeStatusTopic is what
		// holds the two together.
		StatusTopic: layout.Bridge(),
	})
	t.Cleanup(run.Close)

	return &fleet{
		t:      t,
		broker: broker,
		layout: layout,
		dctx:   dctx,
		run:    run,
		avail:  NewAvailability(broker, AvailabilityConfig{Layout: layout}),
		state: StateFor(run, StateConfig{
			Encoding:       dctx.Encoding(),
			CommandFilters: []string{commandFilter()},
		}),
		dev:      dev,
		entities: entities,
		bundle:   bundle,
	}
}

// boot runs the sequence a consumer's start-up performs, in the order the
// package documents: availability first so nothing is briefly claimed
// available on stale data, then discovery, then state.
func (f *fleet) boot(ctx context.Context) {
	f.t.Helper()

	if err := f.run.AnnounceOnline(ctx); err != nil {
		f.t.Fatalf("announce online: %v", err)
	}
	if _, err := f.avail.Device(ctx, DeviceSlot(f.dev, f.entities[0]), true); err != nil {
		f.t.Fatalf("device availability: %v", err)
	}
	if _, err := f.run.PublishBundle(ctx, f.bundle); err != nil {
		f.t.Fatalf("publish bundle: %v", err)
	}
	f.publishState(ctx)
}

// publishState writes one value per readable state binding, through the state
// plane, addressed the way the layout addresses it.
func (f *fleet) publishState(ctx context.Context) {
	f.t.Helper()
	for _, e := range f.entities {
		b, ok := model.Bind(e, model.RoleState)
		if !ok || !b.Mode.CanRead() {
			continue
		}
		if _, err := f.state.PublishValue(ctx, f.layout.State(b.Slot), 1, true); err != nil {
			f.t.Fatalf("state %s: %v", e.Key(), err)
		}
	}
}

// components returns the rendered components in stable key order, which is
// the specification every invariant below reads.
func (f *fleet) components() []struct {
	key  string
	comp discovery.Component
	ent  model.Entity
} {
	out := make([]struct {
		key  string
		comp discovery.Component
		ent  model.Entity
	}, 0, len(f.bundle.Components))
	byKey := map[string]model.Entity{}
	for _, e := range f.entities {
		byKey[e.Key()] = e
	}
	for _, key := range f.bundle.Keys() {
		out = append(out, struct {
			key  string
			comp discovery.Component
			ent  model.Entity
		}{key, f.bundle.Components[key], byKey[key]})
	}
	return out
}

// configTopic is where this fleet's device document is retained.
func (f *fleet) configTopic() string { return BundleConfigTopic(conformPrefix, f.bundle.NodeID) }

// readBack decodes the document the broker actually holds, so an invariant is
// checked against the bytes Home Assistant would read rather than against the
// in-memory struct that produced them.
func (f *fleet) readBack() map[string]map[string]any {
	f.t.Helper()
	raw := f.broker.retainedPayload(f.configTopic())
	if raw == "" {
		f.t.Fatalf("no device document retained at %s", f.configTopic())
	}
	var doc struct {
		Components map[string]map[string]any `json:"components"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		f.t.Fatalf("decode device document: %v", err)
	}
	return doc.Components
}

// availabilityTopics pulls the topics one rendered component gates on.
func availabilityTopicsOf(c discovery.Component) []string {
	out := make([]string, 0, len(c.Availability))
	for _, entry := range c.Availability {
		if entry.Topic != "" {
			out = append(out, entry.Topic)
		}
	}
	return out
}

// assertFixtureCarriesTopics fails when the fixture has nothing to check.
//
// A conformance harness that iterates and skips is the one kind of test that
// can go green by having lost its subject, and this one iterates three times:
// over state topics, over command topics and over availability entries. Each
// has to be non-empty for the run to mean anything.
func assertFixtureCarriesTopics(t *testing.T, b *discovery.Bundle) {
	t.Helper()
	var states, commands, avail int
	for _, key := range b.Keys() {
		c := b.Components[key]
		if c.StateTopic != "" {
			states++
		}
		if c.CommandTopic != "" {
			commands++
		}
		avail += len(c.Availability)
	}
	if states == 0 || commands == 0 || avail == 0 {
		t.Fatalf("the fixture carries %d state topics, %d command topics and %d availability "+
			"entries; every conformance invariant would pass by iterating nothing",
			states, commands, avail)
	}
}
