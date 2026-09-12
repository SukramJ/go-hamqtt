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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// Envelope is the state payload [discovery.EnvelopeEncoding] points Home
// Assistant at, and the only shape [discovery.ValueTemplate] and
// [discovery.AvailabilityTemplate] can read.
//
// Those two templates already ship in this module; nothing here produced the
// payload they parse, so every consumer wrote the two-key struct again. It is
// two keys because that is what the templates address — `value_json.value`
// and `value_json.available` — and a third would be read by nothing.
//
// Deliberately without a timestamp, which is where this departs from the
// measured consumer. Its canonical per-datapoint envelope carries
// `modified_at` and `refreshed_at` as epoch seconds off the event, and its
// legacy mirror carries `modified_at` as an RFC3339 wall clock read at
// publish time; both change on every emission. That consumer can afford them
// because it does not dedup state at all — every value it receives is
// written. Here either field would make every payload unique and turn
// [StatePublisher]'s dedup gate into a no-op: a sensor re-reporting 21.5 °C
// every ten seconds would write to the broker every ten seconds forever. A
// consumer that needs the timestamp on the wire should marshal its own struct
// and hand the bytes to [StatePublisher.Publish], having chosen to opt out of
// dedup by doing so.
type Envelope struct {
	// Value is the datapoint's value. Present even when nil: the
	// [discovery.ValueTemplate] guard reads `value_json.value is not none`,
	// so a null here renders as the empty state rather than a template
	// error, which is how an observed-but-unset datapoint is expressed.
	Value any `json:"value"`
	// Available reports whether this datapoint itself is reachable. It is
	// what [model.LevelSelf] availability resolves against, and the reason
	// the envelope exists rather than a bare value: the alternative is a
	// second topic per datapoint.
	Available bool `json:"available"`
}

// JSON renders the envelope. A method rather than leaving every call site to
// reach for [encoding/json], because the bytes are compared against the dedup
// cache and two call sites marshalling the same value differently would each
// think the other's payload was a change.
func (e Envelope) JSON() ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("publisher: marshal state envelope: %w", err)
	}
	return b, nil
}

// DefaultLatencyWindow is how many recent acknowledged state publishes
// [StatePublisher.Latency] summarises.
//
// 128, taken from the measured consumer's probe. The window bounds memory on
// a bridge that publishes continuously and keeps the summary describing the
// recent past: a median over the whole uptime of a daemon running for weeks
// takes days to react to a broker that went slow an hour ago.
const DefaultLatencyWindow = 128

var (
	// ErrEmptyStatePayload is returned when a retained state publish is
	// handed no bytes.
	//
	// An empty retained payload is MQTT's retraction, not a state: it
	// deletes the value instead of writing one, and the entity goes absent.
	// The measured consumer reaches that outcome by accident — its raw value
	// renderer maps a nil value to zero bytes and publishes it retained — so
	// arriving there has to be explicit. [StatePublisher.Evict] is the way
	// to say it on purpose.
	ErrEmptyStatePayload = errors.New("publisher: empty state payload is a retraction, use Evict")

	// ErrRawNilValue is returned when a nil value is published under
	// [discovery.RawEncoding], for the same reason: the bare rendering of
	// nil is zero bytes, which retracts. Under [discovery.EnvelopeEncoding]
	// a nil value is normal and renders as `{"value":null,…}`.
	ErrRawNilValue = errors.New("publisher: nil value has no raw rendering, use Evict or the envelope encoding")
)

// StateConfig parameterises a [StatePublisher]. The zero value is usable and
// gives QoS 1 retained state in the envelope encoding, which is what a
// consumer with no opinion wants.
type StateConfig struct {
	// QoS applies to every retained state publish and to eviction. The zero
	// value is QoS 1, matching [Config.QoS]: at most once loses a value, and
	// the broker then retains the previous one until the datapoint next
	// changes, which on a sensor that reports on change alone is forever.
	//
	// The reference implementation defaults its state plane to QoS 0
	// instead. That default is not a measurement: its own configuration
	// never populates the field, so the operator cannot reach it and it has
	// never been weighed against a lossy broker. It also makes
	// [StatePublisher.Latency] permanently blind, since only an acknowledged
	// publish can be timed. Both reasons point the same way, so this default
	// follows the rest of the package rather than that consumer.
	QoS byte

	// PulseQoS applies to [StatePublisher.Pulse]. The zero value is QoS 0,
	// which is what the measured consumer hardcodes for every pulse topic —
	// a keypress that arrives late is worse than one that does not arrive,
	// and the acknowledgement round trip is on the event path.
	//
	// A separate knob rather than reusing QoS because the two answer
	// different questions, and the reference implementation having only one
	// is why its pulses ignore the operator's configured state QoS
	// entirely.
	PulseQoS byte

	// Encoding selects the payload shape [StatePublisher.PublishValue]
	// renders. The zero value is [discovery.EnvelopeEncoding], the same
	// default [discovery.Context] carries, so a consumer that never sets
	// either cannot end up with templates reading a shape it does not
	// publish.
	Encoding discovery.Encoding

	// CommandFilters are the topic filters the consumer subscribes for
	// commands. When non-empty, every publish is checked against them and a
	// match is refused with [ErrStateCommandCollision] rather than echoed
	// back into the process's own command handler.
	//
	// Opt-in because the runtime cannot discover them — the consumer owns
	// its subscriptions — and off by default because an empty list must not
	// silently mean "nothing collides".
	CommandFilters []string

	// LatencyWindow is how many recent samples [StatePublisher.Latency]
	// keeps. Zero means [DefaultLatencyWindow]; negative disables the
	// measurement entirely for a consumer that does not read it.
	LatencyWindow int

	// Logger receives the publisher's own diagnostics. Nil means
	// [slog.Default].
	Logger *slog.Logger
}

// StatePublisher writes entity state, as distinct from the retained discovery
// configs [Runtime] owns.
//
// The two are separate types because they have opposite economics. A config
// is written once per boot and must survive on the broker; a state topic is
// written at device speed, and the measured consumer's own probe exists
// precisely because that rate is what makes a slow broker visible. Folding
// state into [Runtime.Publish] would put the fleet's discovery bookkeeping —
// the claim map the orphan sweep reads — on the hot path of every temperature
// reading.
//
// What it adds over a bare [Transport.Publish] is the four things every
// consumer in the family wrote again: the dedup gate, the index that lets a
// removed device's retained values be cleared without its datapoint list
// still existing, the distinction between a retained state and a pulse, and
// the acknowledgement-latency summary.
//
// # Concurrency
//
// Two locks, and the order between them is fixed:
//
//	mu  →  latMu
//
// mu guards the published index; latMu guards the latency window. Neither is
// ever held across a [Transport] call — a publish blocks on a broker
// acknowledgement, and holding the index lock across it would stall every
// other datapoint behind one slow PUBACK.
//
// One further contract, and it is the caller's: a given topic must be
// published by one goroutine at a time. Two concurrent publishes of different
// payloads to one retained topic already leave the broker's retained value
// undefined, and the dedup cache cannot be more definite than the wire — it
// may end up holding the payload the broker did not keep, and then skip that
// payload the next time it is offered. Every consumer in the family drives a
// topic from one source, which is why this costs nothing to honour.
//
// Note also that a [Transport] delivers inbound messages synchronously inline
// in its read loop. A handler that publishes state waits on an
// acknowledgement only that same goroutine can deliver, and deadlocks; hand
// the publish to a worker, the way [Runtime.WatchBirth] does.
type StatePublisher struct {
	tr  Transport
	cfg StateConfig
	log *slog.Logger

	mu sync.Mutex
	// published maps a retained state topic to the exact bytes the broker
	// accepted. It is both halves of the job: the dedup comparison, and the
	// index [StatePublisher.EvictPrefix] walks to clear a removed device
	// whose datapoints no longer exist anywhere to be enumerated from.
	published map[string][]byte

	latMu   sync.Mutex
	samples []time.Duration
	total   uint64
}

// NewStatePublisher builds a state publisher over tr. A nil transport is a
// programming error and panics here rather than on the first publish, where
// the stack no longer names the composition root that got it wrong.
func NewStatePublisher(tr Transport, cfg StateConfig) *StatePublisher {
	if tr == nil {
		panic("publisher: nil transport")
	}
	if cfg.QoS == 0 {
		cfg.QoS = 1
	}
	if cfg.LatencyWindow == 0 {
		cfg.LatencyWindow = DefaultLatencyWindow
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &StatePublisher{
		tr:        tr,
		cfg:       cfg,
		log:       logger,
		published: map[string][]byte{},
	}
}

// StateFor builds a state publisher sharing r's transport, QoS and logger.
//
// The composition root that already has a [Runtime] has also already decided
// which client, which QoS and which logger this process publishes with, and
// a second set of answers is how the discovery plane and the state plane end
// up on different brokers. Whatever cfg sets explicitly still wins; the zero
// fields inherit.
func StateFor(r *Runtime, cfg StateConfig) *StatePublisher {
	if r == nil {
		panic("publisher: nil runtime")
	}
	if cfg.QoS == 0 {
		cfg.QoS = r.cfg.QoS
	}
	if cfg.Logger == nil {
		cfg.Logger = r.log
	}
	return NewStatePublisher(r.tr, cfg)
}

// Publish writes one retained state payload and reports whether it reached
// the broker.
//
// Retained, because that is what makes an entity show a value to a Home
// Assistant that subscribes after the fact — on its own restart, on a reload,
// on first pairing. A non-retained state leaves the entity blank until the
// device next reports, which on a door sensor is whenever somebody next opens
// the door. Use [StatePublisher.Pulse] for the topics that genuinely must not
// be replayed.
//
// The dedup gate is the difference from a bare transport call. A device that
// re-reports an unchanged value — every CCU datapoint does, on every poll —
// costs one broker write and one Home Assistant state evaluation per
// emission, forever, and the reference implementation does not filter them at
// all. Comparing the bytes turns that into nothing, and the false return
// value is what lets a consumer count the difference.
//
// What the gate must not swallow is the case where the broker has forgotten.
// A broker restarted without persistence drops every retained state while
// this process stays connected or reconnects underneath; the cache would then
// answer "already published" for values the broker no longer holds, and every
// entity would sit blank until each device happened to change. That is what
// [StatePublisher.Republish] is for, and why it does not consult the gate.
//
// An empty payload is refused with [ErrEmptyStatePayload] rather than quietly
// retracting.
func (p *StatePublisher) Publish(ctx context.Context, topic string, payload []byte) (bool, error) {
	if topic == "" {
		return false, errors.New("publisher: empty state topic")
	}
	if len(payload) == 0 {
		return false, ErrEmptyStatePayload
	}
	if err := p.guard(topic); err != nil {
		return false, err
	}

	p.mu.Lock()
	previous, known := p.published[topic]
	p.mu.Unlock()
	if known && bytes.Equal(previous, payload) {
		return false, nil
	}

	if err := p.send(ctx, topic, payload, p.cfg.QoS, true); err != nil {
		return false, fmt.Errorf("publisher: publish state %s: %w", topic, err)
	}

	// Recorded only once the broker accepted it, never when it was merely
	// attempted. A consumer publishing through a circuit breaker fails every
	// value of an outage; caching them anyway would make the next identical
	// one hit the gate and publish nothing, leaving the entity blank until
	// the value changes again.
	//
	// The failure path above removes nothing either, and that is a rule all
	// three dedup gates in this package follow: this one,
	// [AvailabilityPublisher.Publish] and [Runtime.Publish] each keep the
	// last payload the broker accepted and only decline to record the one it
	// refused. Declining is already what lets the retry through, because the
	// refused payload still differs from the cached one; deleting the entry
	// on top of that costs index membership, and each of these maps is also
	// its plane's topic list, republish worklist and ownership set.
	p.mu.Lock()
	p.published[topic] = bytes.Clone(payload)
	p.mu.Unlock()
	return true, nil
}

// PublishValue renders value in the configured [StateConfig.Encoding] and
// publishes it retained.
//
// The rendering belongs here rather than at the call site because the
// encoding is a property of the discovery config already on the broker: an
// entity rendered with [discovery.EnvelopeEncoding] carries
// [discovery.ValueTemplate], and a bare value published to its state topic
// reads as a template error on every message. Taking the encoding from one
// [StateConfig] is what keeps the two ends agreeing.
//
// available fills [Envelope.Available] and is ignored under
// [discovery.RawEncoding], which has no room for it — that is the cost of the
// bare shape, and the reason the envelope is the default.
func (p *StatePublisher) PublishValue(ctx context.Context, topic string, value any, available bool) (bool, error) {
	payload, err := p.render(value, available)
	if err != nil {
		return false, err
	}
	return p.Publish(ctx, topic, payload)
}

// Pulse writes one non-retained state payload.
//
// The distinction is Home Assistant's, not a preference: an event entity
// advances on each delivery, so a retained pulse re-fires on every reconnect
// and every restart — a doorbell that rings whenever Home Assistant reloads.
// The measured consumer hardcodes QoS 0 and retain=false on every one of its
// pulse topics for exactly that reason.
//
// No dedup and no memory. Two identical keypresses are two events, not one,
// so the gate that is right for a state would drop the second; and a pulse
// leaves nothing retained on the broker, so there is nothing for
// [StatePublisher.Evict] to clear and nothing for
// [StatePublisher.Republish] to replay.
func (p *StatePublisher) Pulse(ctx context.Context, topic string, payload []byte) error {
	if topic == "" {
		return errors.New("publisher: empty state topic")
	}
	if err := p.guard(topic); err != nil {
		return err
	}
	if err := p.send(ctx, topic, payload, p.cfg.PulseQoS, false); err != nil {
		return fmt.Errorf("publisher: pulse %s: %w", topic, err)
	}
	return nil
}

// Evict clears retained state topics and forgets them.
//
// An empty payload with retain set is MQTT's mechanism for deleting a
// retained message, and Home Assistant's documented way of clearing a stale
// entity state. It is what a datapoint that has gone away needs: without it
// the broker keeps replaying the last value the device ever reported, and an
// entity that no longer exists anywhere keeps showing 21.5 °C to everyone who
// subscribes.
//
// A topic this process never published is cleared anyway. On the boot after
// an upgrade that renamed a topic, the retained values on the broker are the
// previous build's and this process knows none of them — skipping them
// because they are unfamiliar is how they become permanent.
//
// Best-effort across the list: one topic a broker refuses must not leave the
// rest of a removed device's datapoints standing. Every failure is joined so
// the caller sees the whole picture, and a cancelled context stops the walk
// rather than turning every remaining topic into its own error.
func (p *StatePublisher) Evict(ctx context.Context, topics ...string) error {
	var errs []error
	for _, t := range topics {
		if t == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := p.tr.Publish(ctx, t, nil, p.cfg.QoS, true); err != nil {
			errs = append(errs, fmt.Errorf("publisher: evict %s: %w", t, err))
			continue
		}
		p.mu.Lock()
		delete(p.published, t)
		p.mu.Unlock()
	}
	return errors.Join(errs...)
}

// EvictPrefix clears every remembered state topic under prefix and reports
// how many went out.
//
// This is why the index exists. A device removed from its controller takes
// its channel and parameter list with it, so at the moment its retained state
// has to be cleared there is nothing left to enumerate the topics from — the
// reference implementation keeps the same index for the same reason, and
// names it the only way a removed device's raw plane can be found.
//
// Matching is on segment boundaries: prefix matches a topic equal to it, or
// one continuing after a `/`. The reference implementation instead looks for
// the lower-cased address anywhere in the topic, which matches any topic that
// happens to contain those characters between slashes at any depth — a device
// whose address is a prefix of another's is not the failure, a device whose
// address appears as some other device's channel name is.
//
// Best-effort like [StatePublisher.Evict]: a topic the broker refuses stays
// in the index, so a later call retries it rather than declaring it gone.
func (p *StatePublisher) EvictPrefix(ctx context.Context, prefix string) (int, error) {
	if prefix == "" {
		return 0, errors.New("publisher: empty state topic prefix")
	}
	bounded := strings.TrimSuffix(prefix, "/") + "/"

	p.mu.Lock()
	matched := make([]string, 0, len(p.published))
	for t := range p.published {
		if t == prefix || strings.HasPrefix(t, bounded) {
			matched = append(matched, t)
		}
	}
	p.mu.Unlock()
	sort.Strings(matched)

	var errs []error
	cleared := 0
	for _, t := range matched {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := p.tr.Publish(ctx, t, nil, p.cfg.QoS, true); err != nil {
			errs = append(errs, fmt.Errorf("publisher: evict %s: %w", t, err))
			continue
		}
		p.mu.Lock()
		delete(p.published, t)
		p.mu.Unlock()
		cleared++
	}
	return cleared, errors.Join(errs...)
}

// Republish re-sends every remembered state value and reports how many went
// out.
//
// It bypasses the dedup gate by construction — it publishes the cached bytes
// directly — because the whole point is to write values the gate believes the
// broker already holds. The case is a broker restarted without persistence:
// every retained state is gone, this process reconnects, and nothing it
// publishes afterwards differs from what it last sent, so the entities stay
// blank until each device next changes. On a temperature sensor that is
// minutes; on a door contact it is however long until somebody opens the
// door.
//
// The counterpart of [Runtime.Republish], and deliberately not wired to the
// same trigger. Home Assistant's birth message says Home Assistant restarted,
// which the retained configs already survive; this one belongs on the
// consumer's own reconnect, where the thing that may have been lost is the
// broker's retained tree. A consumer that wants both calls both.
//
// Best-effort per topic: a breaker open for one datapoint must not abort the
// replay for the fleet behind it. A cancelled context stops the walk rather
// than turning every remaining topic into an error.
func (p *StatePublisher) Republish(ctx context.Context) (int, error) {
	p.mu.Lock()
	topics := make([]string, 0, len(p.published))
	snapshot := make(map[string][]byte, len(p.published))
	for t, v := range p.published {
		topics = append(topics, t)
		snapshot[t] = v
	}
	p.mu.Unlock()
	sort.Strings(topics)

	var errs []error
	sent := 0
	for _, t := range topics {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := p.send(ctx, t, snapshot[t], p.cfg.QoS, true); err != nil {
			errs = append(errs, fmt.Errorf("publisher: republish state %s: %w", t, err))
			continue
		}
		sent++
	}
	return sent, errors.Join(errs...)
}

// Forget drops topics from the dedup index without publishing anything.
//
// The cheap half of [StatePublisher.Republish], for a consumer that would
// rather let the next natural value through than write the whole fleet at
// once: after forgetting, the next publish of an unchanged value is no longer
// deduped. It is also what a consumer needs when something other than this
// publisher has written the topic — a manual `mosquitto_pub`, a second
// process — and the cache therefore no longer describes the broker.
//
// The topic keeps whatever the broker retains. Forgetting is not evicting,
// and a caller that means "clear it" wants [StatePublisher.Evict].
func (p *StatePublisher) Forget(topics ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range topics {
		delete(p.published, t)
	}
}

// Published lists the retained state topics this process currently holds a
// value for, sorted.
//
// Sorted rather than map order because the two things that read it — an
// operator's diagnostic dump and a test — both compare one run against
// another, and map order makes that comparison meaningless.
func (p *StatePublisher) Published() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.published))
	for t := range p.published {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// StateLatency summarises the recent acknowledged state publishes.
//
// Its job is to tell a slow broker apart from a slow device, which is the
// question an operator actually asks when a dashboard lags and which nothing
// else in the stack can answer. Window occupancy and the single most recent
// sample were measured and dropped: occupancy saturates within seconds of
// bring-up and then cannot distinguish a live median from a stale one, and
// one sample is noise next to the median beside it.
type StateLatency struct {
	// Total counts every timed publish since start. A total that advances
	// says the median is current, one that stops says it is stale, and zero
	// says nothing has ever been measured — which on a QoS 0 deployment is
	// the permanent and correct answer.
	Total uint64
	// MedianMs is the middle of the window: the latency a typical publish
	// sees, unmoved by a single stalled one.
	MedianMs float64
	// MaxMs is the worst in the window. It is reported beside the median
	// because the two disagreeing is the signal — a broker that is fine on
	// average but occasionally stalls looks healthy on the median alone.
	MaxMs float64
}

// Latency summarises the current window.
//
// Only QoS 1 and 2 publishes are timed. A QoS 0 publish returns as soon as
// the packet reaches the socket — the broker never answers it — so timing one
// measures this process's own buffer and reports near-zero however sick the
// broker is: a reading with no negative control, identical whether the broker
// is healthy or gone. On a QoS 0 deployment this reports Total 0 rather than
// inventing a number.
//
// A failed publish is not timed either. Its duration is the time to a refused
// connection or a tripped breaker, which describes the failure, not the
// distance to a working broker.
//
// What is measured is the full acknowledged publish: the network both ways,
// the broker's own processing, and any time the client's in-flight window
// held the packet back. That last part is the backpressure reading — a
// saturated send quota shows up here as latency, which is what an operator
// wants to see, and why this is acknowledgement time rather than a round
// trip.
func (p *StatePublisher) Latency() StateLatency {
	p.latMu.Lock()
	defer p.latMu.Unlock()
	if len(p.samples) == 0 {
		return StateLatency{Total: p.total}
	}
	sorted := make([]time.Duration, len(p.samples))
	copy(sorted, p.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	ms := func(d time.Duration) float64 {
		return float64(d.Nanoseconds()) / float64(time.Millisecond)
	}
	mid := len(sorted) / 2
	median := sorted[mid]
	if len(sorted)%2 == 0 {
		median = (sorted[mid-1] + sorted[mid]) / 2
	}
	return StateLatency{Total: p.total, MedianMs: ms(median), MaxMs: ms(sorted[len(sorted)-1])}
}

// send is the one place a state payload reaches the transport, so the timing
// and the QoS 0 exclusion cannot be forgotten by a new call site.
func (p *StatePublisher) send(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	if qos == 0 || p.cfg.LatencyWindow < 0 {
		return p.tr.Publish(ctx, topic, payload, qos, retain)
	}
	started := time.Now()
	err := p.tr.Publish(ctx, topic, payload, qos, retain)
	if err == nil {
		p.record(time.Since(started))
	}
	return err
}

// record files one acknowledgement duration, evicting the oldest sample once
// the window is full.
func (p *StatePublisher) record(d time.Duration) {
	p.latMu.Lock()
	defer p.latMu.Unlock()
	p.total++
	if len(p.samples) < p.cfg.LatencyWindow {
		p.samples = append(p.samples, d)
		return
	}
	copy(p.samples, p.samples[1:])
	p.samples[len(p.samples)-1] = d
}

// guard refuses a topic that matches one of the consumer's own command
// subscriptions. See [ErrStateCommandCollision].
func (p *StatePublisher) guard(topic string) error {
	for _, f := range p.cfg.CommandFilters {
		if MatchFilter(f, topic) {
			return fmt.Errorf("%w: %s matches %s", ErrStateCommandCollision, topic, f)
		}
	}
	return nil
}

// render turns a value into the bytes the configured encoding calls for.
func (p *StatePublisher) render(value any, available bool) ([]byte, error) {
	if p.cfg.Encoding == discovery.RawEncoding {
		if value == nil {
			return nil, ErrRawNilValue
		}
		return RenderRawValue(value)
	}
	return Envelope{Value: value, Available: available}.JSON()
}

// RenderRawValue renders a value for [discovery.RawEncoding]: the bare bytes
// Home Assistant reads when no value template stands between it and the
// topic.
//
// Exported because a consumer publishing a raw-encoded topic through
// something other than [StatePublisher.PublishValue] — its own batching, its
// own breaker — still needs the identical rendering, and a second one is how
// a bool starts arriving as "1" on one topic and "true" on the next.
//
// Strings pass through unquoted and bools render as `true`/`false`, matching
// the measured consumer. Floats differ from it deliberately: it formats with
// `%f` and trims, which is six decimal places and no more, so 0.0000001
// reaches the broker as `0` and a power meter reporting in kilowatts loses
// its last digits. This renders the shortest decimal that round-trips
// instead. Anything not a Go scalar falls through to JSON.
func RenderRawValue(v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return nil, ErrRawNilValue
	case string:
		return []byte(x), nil
	case []byte:
		return bytes.Clone(x), nil
	case bool:
		return []byte(strconv.FormatBool(x)), nil
	case int:
		return []byte(strconv.FormatInt(int64(x), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(x), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(x, 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(x), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(x), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(x, 10)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(x), 'f', -1, 32)), nil
	case float64:
		return []byte(strconv.FormatFloat(x, 'f', -1, 64)), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("publisher: render raw value: %w", err)
	}
	return b, nil
}
