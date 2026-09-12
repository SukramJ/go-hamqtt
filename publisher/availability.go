// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

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

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// ErrNoAvailabilityLayout is returned by the slot-shaped calls when no
// [topic.Layout] was configured.
//
// Refusing rather than guessing a topic: the availability topic is the one
// string that has to be identical on both sides of the discovery config, and
// a runtime that invented its own would produce an entity permanently
// unavailable — Home Assistant waits on a topic nothing publishes — with
// nothing on the wire naming the mismatch.
var ErrNoAvailabilityLayout = errors.New("publisher: no topic layout configured")

// AvailabilityConfig parameterises an [AvailabilityPublisher].
type AvailabilityConfig struct {
	// Layout renders the topics. It must be the same [topic.Layout] the
	// consumer handed [discovery.StdContext], because that is the one that
	// wrote the `availability` list into every config.
	Layout topic.Layout

	// QoS applies to every availability publish and retraction. The zero
	// value is QoS 1.
	//
	// One is not the state plane's default by accident. The measured
	// consumer pins QoS 1 on the device availability topic even where its
	// datapoint states run at the configured state QoS: a lost state
	// message is corrected by the next reading, while a lost `offline`
	// leaves every entity of a dead device showing its last value until
	// something else happens to flip the topic.
	QoS byte

	// CommandFilters are the topic filters the consumer subscribes for
	// commands, and they are checked for the same reason
	// [StateConfig.CommandFilters] are: a write that lands inside this
	// process's own subscription is echoed back into its command handler.
	//
	// It matters here and not only on the state plane because
	// [AvailabilityPublisher.Self] writes to [topic.Layout.State] — a
	// state-plane topic, which is exactly where a collision lives — and a
	// consumer whose availability datapoint sits under a wildcard command
	// filter would otherwise have the same topic refused by
	// [StatePublisher.Publish] and accepted here.
	//
	// Opt-in and off by default, identically to the state plane: the runtime
	// cannot discover the consumer's subscriptions, and an empty list must
	// not silently mean "nothing collides".
	CommandFilters []string

	// Logger receives the diagnostics. Nil means [slog.Default].
	Logger *slog.Logger
}

// AvailabilityPublisher owns the retained availability topics of one consumer
// process: which of them it has written, and at what value.
//
// It is the publishing half of the three levels [model.Availability] declares.
// v0.25.0 ships the declaring side of all three and the publishing side of
// exactly one — [model.LevelBridge], through [Runtime.Will] and
// [Runtime.AnnounceOnline]. Nothing published [model.LevelDevice], which is
// the level that makes an entity grey out when its device goes off-bus, so
// every config rendered with the default availability named a topic no
// process in the family ever wrote. Under `availability_mode: all` — the
// default — an availability source that never publishes is not neutral: the
// entity stays unavailable, and the only evidence is its absence.
//
// It is a type of its own rather than more methods on [Runtime] because the
// two own different trees and must be able to fail apart. [Runtime] writes
// under the discovery prefix, which is Home Assistant's; this writes in the
// consumer's own tree, which is also read by whatever else is on the broker.
// A consumer that publishes no discovery at all still needs this half.
//
// # Locking
//
// One mutex, and it is a leaf: mu guards the last-published map and is never
// held across a [Transport] call. Holding it across a publish would serialise
// every device's availability behind one slow PUBACK, which on a bus-wide
// outage is precisely when several hundred of them flip at once. It takes no
// lock of [Runtime]'s and [Runtime] takes none of its, so the two compose in
// either order.
//
// # Threading
//
// Every method here blocks until the broker acknowledges. go-mqtt delivers
// messages synchronously inline in its read loop, and an acknowledgement can
// only arrive on that same goroutine, so calling any of these from a
// [Handler] is a self-deadlock. Hand the work to a worker, exactly as
// [Runtime.WatchBirth] does with its replay.
type AvailabilityPublisher struct {
	tr      Transport
	layout  topic.Layout
	qos     byte
	log     *slog.Logger
	filters []string

	mu sync.Mutex
	// last is the payload the broker accepted per topic, and it is the
	// transition gate. The bytes rather than a bool because the same map
	// carries the two-token device form and the true/false self form, and
	// a bool would have to be interpreted differently per topic by
	// whichever call site read it next.
	//
	// The [cachedWrite] wrapper is what lets
	// [AvailabilityPublisher.Reset] open the gate without dropping the
	// index: the map is also [AvailabilityPublisher.Topics], the worklist
	// of [AvailabilityPublisher.Republish] and the ownership set of
	// [AvailabilityPublisher.Sweep].
	last map[string]cachedWrite
}

// NewAvailability builds a publisher over tr.
//
// A nil transport panics here rather than on the first flip, where the stack
// no longer names the composition root that got it wrong — the same trade
// [New] makes. A nil layout does not panic: it is legal for a consumer that
// only ever calls [AvailabilityPublisher.Publish] with a topic it rendered
// itself, and the slot-shaped calls report [ErrNoAvailabilityLayout].
func NewAvailability(tr Transport, cfg AvailabilityConfig) *AvailabilityPublisher {
	if tr == nil {
		panic("publisher: nil transport")
	}
	if cfg.QoS == 0 {
		cfg.QoS = 1
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &AvailabilityPublisher{
		tr:      tr,
		layout:  cfg.Layout,
		qos:     cfg.QoS,
		log:     logger,
		filters: cfg.CommandFilters,
		last:    map[string]cachedWrite{},
	}
}

// DeviceSlot is the coordinate [model.LevelDevice] resolves against: the
// device's address, in the containers its entities sit in.
//
// It delegates to [discovery.DeviceSlot] rather than deriving the coordinate
// a second time: the declaring and the publishing side addressing different
// topics is precisely the defect this function exists to prevent, and two
// copies of the derivation are how that happens.
func DeviceSlot(dev *model.Device, e model.Entity) model.Slot {
	return discovery.DeviceSlot(dev, e)
}

// DeviceTopic renders the availability topic of one device coordinate.
//
// Exported alongside the publish calls because a consumer has to be able to
// name the topic without writing to it — a diagnostic dump, a retraction list
// built before the devices are gone, and the ownership predicate of
// [AvailabilityPublisher.Sweep] all need the string and none of them wants
// the message.
func (a *AvailabilityPublisher) DeviceTopic(s model.Slot) (string, error) {
	if a.layout == nil {
		return "", ErrNoAvailabilityLayout
	}
	return a.layout.Availability(s), nil
}

// Device flips one device's retained reachability topic and reports whether
// that was a transition.
//
// The gate is the point, and it is measured. Availability is re-evaluated on
// every inbound value, so an ungated call republishes `online` once per
// datapoint event — broker spam and retained-topic churn on a bus that
// carries thousands of events a minute. With it the topic carries exactly the
// flips: offline at boot, online on the first reading, offline when the
// device stops answering, online when it comes back. The returned bool is
// what lets a consumer hang its own per-transition work — a bus announcement,
// a metric — off the same gate rather than a second one that could disagree
// with this one about the same device.
//
// The tokens are [discovery.PayloadOnline] and [discovery.PayloadOffline],
// which is what the declaring side writes into every config's
// `payload_available` — a bare word, not a JSON envelope. Home Assistant
// silently ignores an availability payload it does not recognise and leaves
// the entity unavailable, so an envelope here would look exactly like a
// device that never came back.
func (a *AvailabilityPublisher) Device(ctx context.Context, s model.Slot, online bool) (bool, error) {
	t, err := a.DeviceTopic(s)
	if err != nil {
		return false, err
	}
	return a.Publish(ctx, t, online)
}

// Publish flips any availability topic by name, for a consumer that renders
// its own — a per-interface connectivity flag, a role that owns an
// availability topic of its own.
//
// Same gate, same tokens, same retain: an availability marker that is not
// retained tells nothing to a Home Assistant that subscribes after the flip,
// which is exactly when it needs to be told.
func (a *AvailabilityPublisher) Publish(ctx context.Context, t string, online bool) (bool, error) {
	payload := discovery.PayloadOffline
	if online {
		payload = discovery.PayloadOnline
	}
	return a.write(ctx, t, []byte(payload))
}

// selfEnvelope is the two keys the [model.LevelSelf] templates read. It is
// deliberately not the consumer's full state envelope: this path publishes a
// datapoint the state plane does not drive, and inventing timestamps for it
// would put a second, disagreeing writer on a topic that already has one.
type selfEnvelope struct {
	Value     bool `json:"value"`
	Available bool `json:"available"`
}

// SelfAvailabilityPayload renders what a [model.LevelSelf] datapoint must
// carry so the config that declares it actually matches on the wire.
//
// Three measured defects live in this one payload, and all three are silent —
// Home Assistant does not report an availability payload it cannot read, it
// simply never marks the entity available:
//
//   - The explicit [model.RoleAvailability] binding is read through
//     [discovery.SelfAvailabilityTemplate], which reaches for `value`, not
//     `available`. The envelope's own flag answers a different question —
//     whether that datapoint is itself reachable — so the value is where the
//     answer goes.
//   - Under [discovery.RawEncoding] the config carries no template at all,
//     because there is nothing to reach into. The payload is therefore the
//     bare boolean; publishing an envelope there renders `value_json`
//     undefined, which matches neither payload token.
//   - The tokens are `true` and `false`, lower case, not online/offline.
//     That is what the declaring side writes into `payload_available` for
//     this level, and the templates pipe through `| lower` to meet it.
//
// Exported as a pure function because a consumer whose state plane already
// owns that datapoint's topic must be able to render the same bytes without
// a second writer — see [AvailabilityPublisher.Self].
func SelfAvailabilityPayload(enc discovery.Encoding, available bool) []byte {
	if enc == discovery.RawEncoding {
		if available {
			return []byte("true")
		}
		return []byte("false")
	}
	// Marshalling two bools cannot fail, and an error return on a function
	// whose only input is a bool would be noise at every call site.
	body, _ := json.Marshal(selfEnvelope{Value: available, Available: true})
	return body
}

// Self publishes an entity's own [model.LevelSelf] availability on the state
// topic of its [model.RoleAvailability] binding.
//
// It is for a synthesised availability datapoint — one the consumer computes
// rather than reads off a device, such as "is this control usable right now",
// which is the shape the measured consumer publishes for its execute buttons.
// An entity whose availability binding is a real datapoint needs no call
// here: its state plane already writes that topic, and a second writer would
// fight it. What both need is the same bytes, which is why
// [SelfAvailabilityPayload] is exported.
//
// An entity with no availability binding reports (false, nil) rather than an
// error. The declaring side treats that as the fallback shape — the entity's
// own state envelope carries the flag — and there is nothing for this
// publisher to write there; a description written once and reused across
// device shapes lands in that case normally.
func (a *AvailabilityPublisher) Self(
	ctx context.Context,
	e model.Entity,
	enc discovery.Encoding,
	available bool,
) (bool, error) {
	if a.layout == nil {
		return false, ErrNoAvailabilityLayout
	}
	b, ok := model.Bind(e, model.RoleAvailability)
	if !ok {
		return false, nil
	}
	return a.write(ctx, a.layout.State(b.Slot), SelfAvailabilityPayload(enc, available))
}

// guard refuses a topic that matches one of the consumer's own command
// subscriptions, exactly as [StatePublisher] does and with the same
// [ErrStateCommandCollision]. One answer to one question across the whole
// package: three planes with three different answers to "may I write a topic
// I also subscribe to" was the defect, not the check.
func (a *AvailabilityPublisher) guard(t string) error {
	for _, f := range a.filters {
		if MatchFilter(f, t) {
			return fmt.Errorf("%w: %s matches %s", ErrStateCommandCollision, t, f)
		}
	}
	return nil
}

// write is the gated retained publish every call above lands on.
func (a *AvailabilityPublisher) write(ctx context.Context, t string, payload []byte) (bool, error) {
	if t == "" {
		return false, errors.New("publisher: empty availability topic")
	}
	if err := a.guard(t); err != nil {
		return false, err
	}

	a.mu.Lock()
	previous := a.last[t]
	a.mu.Unlock()
	if previous.gated && bytes.Equal(previous.payload, payload) {
		return false, nil
	}

	if err := a.tr.Publish(ctx, t, payload, a.qos, true); err != nil {
		// Record nothing, and remove nothing either. That is the same rule
		// the package's other two dedup gates follow — [StatePublisher.Publish]
		// and [Runtime.Publish] both leave the previously accepted value
		// standing and merely decline to record — and all three say so,
		// because two planes answering one question differently is the
		// defect class itself.
		//
		// Declining to record is already enough to keep the retry alive: the
		// cached payload is still the one the broker accepted, so the failed
		// payload differs from it and the gate lets it through. Deleting the
		// entry instead buys nothing on top of that and costs index
		// membership — this map is not only the gate, it is
		// [AvailabilityPublisher.Topics], the worklist of
		// [AvailabilityPublisher.Republish] and the ownership set of
		// [AvailabilityPublisher.Sweep]. An `offline` that fails during a
		// broker outage, which is exactly when it fails, would drop the
		// topic out of all three while the broker still retains `online`: no
		// sweep clears it, no republish re-sends it, and the device becomes
		// the permanently available ghost [AvailabilityPublisher.Retract]
		// exists to prevent.
		return false, fmt.Errorf("publisher: availability %s: %w", t, err)
	}

	a.mu.Lock()
	a.last[t] = cachedWrite{payload: bytes.Clone(payload), gated: true}
	a.mu.Unlock()
	return true, nil
}

// Online reports the last value this process published for a topic, and
// whether it published one at all.
//
// The second bool is not a convenience. "Never published" and "published
// offline" look the same to a caller that only gets a bool, and they are
// opposite faults: the first means this process has not reached the topic
// yet, the second that it has and the device is down.
func (a *AvailabilityPublisher) Online(t string) (online, known bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.last[t]
	if !ok {
		return false, false
	}
	return string(entry.payload) == discovery.PayloadOnline, true
}

// Topics lists the availability topics this process has written, sorted.
//
// Sorted rather than map order for the same reason [Runtime.Declared] is: the
// two things that read it — an operator's diagnostic dump and a test — both
// compare one run against another.
func (a *AvailabilityPublisher) Topics() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.last))
	for t := range a.last {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// Forget drops the gate's memory of topics without touching the broker.
//
// It is the call that keeps the gate honest whenever something else clears a
// retained availability topic — [AvailabilityPublisher.Retract] does it
// itself, a consumer's own wildcard cleanup has to say so. A stale cached
// `online` behind a cleared topic classifies the next flip as "no
// transition", and a device readopted under the same address then has every
// one of its entities unavailable for the life of the daemon. That is a
// measured defect, not a hypothetical one.
func (a *AvailabilityPublisher) Forget(topics ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, t := range topics {
		delete(a.last, t)
	}
}

// Reset opens the gate for every remembered topic, so the next flip of each
// publishes unconditionally — and keeps what they carry, so the index
// survives.
//
// This is the reconnect call, and the reasoning is worth stating because the
// obvious answer is the wrong one. A broker replays its retained availability
// topics to Home Assistant on its own, so a reconnect appears to need
// nothing. But the replay is of what the broker still holds, and a broker
// that came back without a persistent retained store holds nothing — while
// this process's gate still believes every device is online and suppresses
// the republish. Every entity then sits unavailable until the daemon
// restarts. The measured consumer clears its gate at the head of every
// reconnect snapshot for exactly that reason.
//
// Home Assistant's own restart is the opposite case and needs no reset: the
// broker's retained state is intact, Home Assistant re-subscribes, and the
// replay is complete. [Runtime.WatchBirth] replays discovery configs there
// because Home Assistant does not re-read those reliably; availability
// topics it does.
//
// Pair it with the consumer's own snapshot pass, which re-asserts every
// device's current reachability. [AvailabilityPublisher.Republish] is the
// same idea for a consumer that has no such pass — and the two compose in
// either order, which they did not when this call emptied the map: a reader
// pairing the documented reconnect calls as Reset-then-Republish sent nothing
// at all, because the republish had no worklist left. Opening the gate is
// what a reconnect needs; forgetting the fleet is not, and
// [AvailabilityPublisher.Forget] is where that is said on purpose.
//
// [StatePublisher.Reset] is the same call on the state plane, deliberately
// the same name and the same semantics, so one reconnect handler needs one
// idiom rather than two.
func (a *AvailabilityPublisher) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for t, e := range a.last {
		e.gated = false
		a.last[t] = e
	}
}

// Republish re-sends every remembered availability topic at its last value
// and reports how many went out.
//
// For a consumer whose reconnect path has no device snapshot to re-walk. It
// bypasses the gate by construction — it writes payloads the broker may
// already hold — because the case it exists for is precisely a broker that
// does not.
//
// It bypasses the gate but not [AvailabilityConfig.CommandFilters]: a topic
// the consumer now subscribes to is reported as [ErrStateCommandCollision]
// and skipped rather than echoed to its own handler.
//
// Best-effort per topic: a breaker open for one device must not abort the
// replay for the fleet behind it, which would leave most of it unavailable
// after a broker restart. A cancelled context stops the walk rather than
// turning every remaining topic into an error.
func (a *AvailabilityPublisher) Republish(ctx context.Context) (int, error) {
	a.mu.Lock()
	snapshot := make(map[string][]byte, len(a.last))
	for t, e := range a.last {
		snapshot[t] = e.payload
	}
	a.mu.Unlock()

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
		if err := a.guard(t); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := a.tr.Publish(ctx, t, snapshot[t], a.qos, true); err != nil {
			errs = append(errs, fmt.Errorf("publisher: republish availability %s: %w", t, err))
			continue
		}
		sent++
	}
	return sent, errors.Join(errs...)
}

// Retract clears retained availability topics and forgets them.
//
// This is the removal path, and it is not optional. A retained `online` that
// outlives the device it describes is a ghost: Home Assistant reads it on
// every restart and the entity behind it stays permanently available, showing
// the last value it ever saw. Retracting the discovery config alone does not
// clear it — the two live in different trees, and the broker keeps handing
// the orphan out to anything that subscribes.
//
// Order it after the config retraction, the way the measured consumer does.
// The config retraction is what removes the entity; clearing the availability
// topic first only makes it unavailable for the moment in between, which an
// operator watching the restart reads as a fault.
//
// A topic inside [AvailabilityConfig.CommandFilters] is refused with
// [ErrStateCommandCollision] and left standing, as it is on every other write
// here: an empty retained payload on a topic this process subscribes to is an
// empty command delivered to its own handler.
//
// Best-effort across the list: one topic a broker refuses must not leave the
// rest of a removed device's markers standing. Every failure is joined so the
// caller sees the whole picture.
func (a *AvailabilityPublisher) Retract(ctx context.Context, topics ...string) error {
	var errs []error
	for _, t := range topics {
		if err := ctx.Err(); err != nil {
			// A cancelled context is a shutdown, not a per-topic failure.
			errs = append(errs, err)
			break
		}
		if err := a.guard(t); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := a.tr.Publish(ctx, t, nil, a.qos, true); err != nil {
			errs = append(errs, fmt.Errorf("publisher: retract availability %s: %w", t, err))
			continue
		}
		a.mu.Lock()
		delete(a.last, t)
		a.mu.Unlock()
	}
	return errors.Join(errs...)
}

// RetractDevice clears one device's reachability topic, naming it from the
// same coordinate [AvailabilityPublisher.Device] published it under.
func (a *AvailabilityPublisher) RetractDevice(ctx context.Context, s model.Slot) error {
	t, err := a.DeviceTopic(s)
	if err != nil {
		return err
	}
	return a.Retract(ctx, t)
}

// Sweep retracts every availability topic this process has written that live
// no longer claims, and returns what it cleared.
//
// It is the availability half of [Runtime.Sweep] and works the other way
// round on purpose. The discovery sweep reads the broker: retained configs
// live under one known prefix, so a wildcard snapshot can find leftovers no
// running process remembers. Availability topics do not — they sit in the
// consumer's own tree, whose shape is [topic.Layout]'s secret and which holds
// state, commands and everything else the consumer publishes. A wildcard
// pass there would have to decide what an availability topic even looks like,
// and would retract another writer's on a layout it guessed wrong.
//
// So this pass judges only what this process wrote, which makes it the answer
// to a device disappearing mid-run rather than to a leftover from a previous
// build. A leftover from a previous build is cleared by a consumer walking
// its removals through [AvailabilityPublisher.RetractDevice], and a
// consumer's boot pass that flips every live device to its current state is
// what makes this pass see the right set at all — run it after that pass, for
// the same reason [Runtime.Sweep] runs after the discovery snapshot.
//
// live is required: sweeping against a predicate nobody supplied would clear
// every device the process has ever announced, which is the whole fleet.
// [ErrSweepUnscoped] says so.
func (a *AvailabilityPublisher) Sweep(
	ctx context.Context,
	live func(topic string) bool,
) ([]string, error) {
	if live == nil {
		return nil, ErrSweepUnscoped
	}
	var stale []string
	for _, t := range a.Topics() {
		if !live(t) {
			stale = append(stale, t)
		}
	}
	if len(stale) == 0 {
		return nil, nil
	}
	if err := a.Retract(ctx, stale...); err != nil {
		// Report what actually went, not what was attempted: the retraction
		// is best-effort per topic, and a caller that logged the attempt
		// list would name topics still standing on the broker.
		cleared := make([]string, 0, len(stale))
		for _, t := range stale {
			if _, known := a.Online(t); !known {
				cleared = append(cleared, t)
			}
		}
		a.log.Warn("publisher.availability.sweep",
			slog.Int("stale", len(stale)),
			slog.Int("retracted", len(cleared)),
			slog.String("err", err.Error()))
		return cleared, err
	}
	a.log.Debug("publisher.availability.sweep", slog.Int("retracted", len(stale)))
	return stale, nil
}
