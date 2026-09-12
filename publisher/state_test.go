// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// slowTransport delays every publish by a fixed amount, which is the only way
// to give the latency window a value the assertion can bound from below
// without depending on how fast the machine running the test is.
type slowTransport struct {
	mu    sync.Mutex
	delay time.Duration
	calls int
	// failPublish, failSubscribe and failUnsubscribe make the slow path
	// fail too.
	//
	// All three used to return nil unconditionally, which put the latency
	// window's own exclusion rule out of reach: a failed publish must not be
	// timed, and the only way to assert that on a transport that takes
	// measurable time is one that can both take time and fail.
	failPublish     error
	failSubscribe   error
	failUnsubscribe error
}

func (s *slowTransport) Publish(_ context.Context, _ string, _ []byte, _ byte, _ bool) error {
	s.mu.Lock()
	d := s.delay
	fail := s.failPublish
	s.calls++
	s.mu.Unlock()
	time.Sleep(d)
	return fail
}

func (s *slowTransport) Subscribe(context.Context, string, byte, Handler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failSubscribe
}

func (s *slowTransport) Unsubscribe(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failUnsubscribe
}

// stateOps snapshots the fake's recorded operations.
func stateOps(f *fakeTransport) []op {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]op(nil), f.ops...)
}

func TestEnvelopeJSONCarriesBothTemplateKeys(t *testing.T) {
	t.Parallel()

	b, err := Envelope{Value: 21.5, Available: true}.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if got, want := string(b), `{"value":21.5,"available":true}`; got != want {
		t.Fatalf("envelope = %s, want %s", got, want)
	}

	// A nil value must still emit the key: discovery.ValueTemplate guards on
	// `value_json.value is not none`, and a missing key is a template error
	// rather than an empty state.
	b, err = Envelope{Available: false}.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if got, want := string(b), `{"value":null,"available":false}`; got != want {
		t.Fatalf("envelope = %s, want %s", got, want)
	}
}

func TestEnvelopeJSONReportsAnUnmarshallableValue(t *testing.T) {
	t.Parallel()

	if _, err := (Envelope{Value: make(chan int)}).JSON(); err == nil {
		t.Fatal("expected an error for a value json cannot encode")
	}
}

func TestPublishStateIsRetainedAndDeduped(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()

	sent, err := p.Publish(ctx, "b/1/state", []byte("21.5"))
	if err != nil || !sent {
		t.Fatalf("first publish: sent=%v err=%v", sent, err)
	}
	ops := stateOps(f)
	if len(ops) != 1 {
		t.Fatalf("ops = %d, want 1", len(ops))
	}
	if !ops[0].retain {
		t.Fatal("state must be retained: a Home Assistant that subscribes later sees nothing otherwise")
	}
	if ops[0].qos != 1 {
		t.Fatalf("qos = %d, want the zero-value default 1", ops[0].qos)
	}

	// The same value again is the common case on a polling device, and it
	// must not reach the broker.
	sent, err = p.Publish(ctx, "b/1/state", []byte("21.5"))
	if err != nil {
		t.Fatalf("repeat publish: %v", err)
	}
	if sent {
		t.Fatal("an unchanged value must be deduped")
	}
	if n := len(stateOps(f)); n != 1 {
		t.Fatalf("ops = %d after a repeat, want 1", n)
	}

	sent, err = p.Publish(ctx, "b/1/state", []byte("22.0"))
	if err != nil || !sent {
		t.Fatalf("changed publish: sent=%v err=%v", sent, err)
	}
	if n := len(stateOps(f)); n != 2 {
		t.Fatalf("ops = %d after a change, want 2", n)
	}
}

func TestPublishStateRejectsAnEmptyPayloadAndAnEmptyTopic(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})

	if _, err := p.Publish(context.Background(), "b/1/state", nil); !errors.Is(err, ErrEmptyStatePayload) {
		t.Fatalf("err = %v, want ErrEmptyStatePayload — an empty retained payload retracts", err)
	}
	if _, err := p.Publish(context.Background(), "", []byte("x")); err == nil {
		t.Fatal("expected an error for an empty topic")
	}
	if n := len(stateOps(f)); n != 0 {
		t.Fatalf("ops = %d, want 0", n)
	}
}

func TestPublishStateCachesOnlyWhatTheBrokerAccepted(t *testing.T) {
	t.Parallel()

	f := newFake()
	boom := errors.New("breaker open")
	f.failPublish = func(string) error { return boom }
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()

	if _, err := p.Publish(ctx, "b/1/state", []byte("21.5")); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the transport error", err)
	}
	if got := p.Published(); len(got) != 0 {
		t.Fatalf("published = %v, want nothing cached after a failure", got)
	}

	// The identical value must go out once the broker is back; caching a
	// failed attempt would leave the entity blank until the value changed.
	f.mu.Lock()
	f.failPublish = nil
	f.mu.Unlock()
	sent, err := p.Publish(ctx, "b/1/state", []byte("21.5"))
	if err != nil || !sent {
		t.Fatalf("retry: sent=%v err=%v", sent, err)
	}
}

func TestPublishValueRendersTheConfiguredEncoding(t *testing.T) {
	t.Parallel()

	f := newFake()
	env := NewStatePublisher(f, StateConfig{})
	if _, err := env.PublishValue(context.Background(), "b/1/state", 21.5, true); err != nil {
		t.Fatalf("envelope publish: %v", err)
	}
	ops := stateOps(f)
	if got, want := string(ops[0].payload), `{"value":21.5,"available":true}`; got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}

	g := newFake()
	raw := NewStatePublisher(g, StateConfig{Encoding: discovery.RawEncoding})
	if _, err := raw.PublishValue(context.Background(), "b/1/state", 21.5, true); err != nil {
		t.Fatalf("raw publish: %v", err)
	}
	if got, want := string(stateOps(g)[0].payload), "21.5"; got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}

	// A nil raw value renders as zero bytes, which retracts. It has to be
	// refused rather than published.
	if _, err := raw.PublishValue(context.Background(), "b/1/state", nil, true); !errors.Is(err, ErrRawNilValue) {
		t.Fatalf("err = %v, want ErrRawNilValue", err)
	}
	// Under the envelope encoding the same nil is ordinary.
	if _, err := env.PublishValue(context.Background(), "b/2/state", nil, false); err != nil {
		t.Fatalf("envelope nil: %v", err)
	}
}

func TestRenderRawValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   any
		want string
	}{
		{"string passes through unquoted", "auto", "auto"},
		{"bytes pass through", []byte("raw"), "raw"},
		{"bool", true, "true"},
		{"bool false", false, "false"},
		{"int", 42, "42"},
		{"int32", int32(-7), "-7"},
		{"int64", int64(1 << 40), "1099511627776"},
		{"uint", uint(9), "9"},
		{"uint32", uint32(9), "9"},
		{"uint64", uint64(9), "9"},
		{"float32", float32(1.5), "1.5"},
		{"float64", 21.5, "21.5"},
		// The reference renderer formats with %f and trims, so six decimal
		// places is all it has: this value reaches its broker as "0".
		{"small float keeps its digits", 0.0000001, "0.0000001"},
		{"integral float has no trailing dot", float64(3), "3"},
		{"anything else is json", map[string]int{"a": 1}, `{"a":1}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, err := RenderRawValue(c.in)
			if err != nil {
				t.Fatalf("RenderRawValue(%v): %v", c.in, err)
			}
			if string(got) != c.want {
				t.Fatalf("RenderRawValue(%v) = %q, want %q", c.in, got, c.want)
			}
		})
	}

	if _, err := RenderRawValue(nil); !errors.Is(err, ErrRawNilValue) {
		t.Fatalf("nil: err = %v, want ErrRawNilValue", err)
	}
	if _, err := RenderRawValue(make(chan int)); err == nil {
		t.Fatal("expected an error for a value json cannot encode")
	}
}

func TestPulseIsNeitherRetainedNorDeduped(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()

	for range 2 {
		if err := p.Pulse(ctx, "b/1/event", []byte(`{"event_type":"press_short"}`)); err != nil {
			t.Fatalf("pulse: %v", err)
		}
	}
	ops := stateOps(f)
	if len(ops) != 2 {
		t.Fatalf("ops = %d, want 2 — two identical keypresses are two events", len(ops))
	}
	for _, o := range ops {
		if o.retain {
			t.Fatal("a retained pulse re-fires on every reconnect")
		}
		if o.qos != 0 {
			t.Fatalf("qos = %d, want the zero-value default 0", o.qos)
		}
	}
	if got := p.Published(); len(got) != 0 {
		t.Fatalf("published = %v, want nothing: a pulse leaves nothing retained", got)
	}
	if err := p.Pulse(ctx, "", nil); err == nil {
		t.Fatal("expected an error for an empty topic")
	}
}

func TestPulseHonoursItsOwnQoS(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{QoS: 2, PulseQoS: 1})
	if err := p.Pulse(context.Background(), "b/1/event", []byte("x")); err != nil {
		t.Fatalf("pulse: %v", err)
	}
	if got := stateOps(f)[0].qos; got != 1 {
		t.Fatalf("pulse qos = %d, want 1 — PulseQoS must not inherit QoS", got)
	}
}

func TestPulseReportsATransportFailure(t *testing.T) {
	t.Parallel()

	f := newFake()
	boom := errors.New("nope")
	f.failPublish = func(string) error { return boom }
	p := NewStatePublisher(f, StateConfig{})
	if err := p.Pulse(context.Background(), "b/1/event", []byte("x")); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the transport error", err)
	}
}

func TestEvictClearsTheRetainedValueAndForgetsIt(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()

	if _, err := p.Publish(ctx, "b/1/state", []byte("21.5")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := p.Evict(ctx, "b/1/state", ""); err != nil {
		t.Fatalf("evict: %v", err)
	}

	ops := stateOps(f)
	last := ops[len(ops)-1]
	if last.topic != "b/1/state" || len(last.payload) != 0 || !last.retain {
		t.Fatalf("eviction = %+v, want an empty retained payload", last)
	}
	f.mu.Lock()
	_, stillRetained := f.retained["b/1/state"]
	f.mu.Unlock()
	if stillRetained {
		t.Fatal("the broker still holds the retained value")
	}
	if got := p.Published(); len(got) != 0 {
		t.Fatalf("published = %v, want the topic forgotten", got)
	}

	// The value may be re-published afterwards: eviction must not leave a
	// dedup entry claiming the broker still has it.
	sent, err := p.Publish(ctx, "b/1/state", []byte("21.5"))
	if err != nil || !sent {
		t.Fatalf("re-publish after eviction: sent=%v err=%v", sent, err)
	}
}

func TestEvictClearsATopicThisProcessNeverPublished(t *testing.T) {
	t.Parallel()

	f := newFake()
	f.seed("b/old/state", []byte("stale"))
	p := NewStatePublisher(f, StateConfig{})

	if err := p.Evict(context.Background(), "b/old/state"); err != nil {
		t.Fatalf("evict: %v", err)
	}
	f.mu.Lock()
	_, stillRetained := f.retained["b/old/state"]
	f.mu.Unlock()
	if stillRetained {
		t.Fatal("a previous build's retained value must be clearable")
	}
}

func TestEvictIsBestEffortAndStopsOnCancellation(t *testing.T) {
	t.Parallel()

	f := newFake()
	boom := errors.New("refused")
	f.failPublish = func(topic string) error {
		if topic == "b/2/state" {
			return boom
		}
		return nil
	}
	p := NewStatePublisher(f, StateConfig{})

	err := p.Evict(context.Background(), "b/1/state", "b/2/state", "b/3/state")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the refused topic reported", err)
	}
	if n := len(stateOps(f)); n != 2 {
		t.Fatalf("ops = %d, want 2 — the walk must continue past a refusal", n)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Evict(cancelled, "b/4/state"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestEvictPrefixMatchesOnSegmentBoundaries(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()

	// "b/dev1" and "b/dev10" share a character prefix but are different
	// devices; a substring match would clear the second one's live state.
	for _, topic := range []string{"b/dev1/1/state", "b/dev1/2/state", "b/dev10/1/state", "b/dev1"} {
		if _, err := p.Publish(ctx, topic, []byte("v")); err != nil {
			t.Fatalf("publish %s: %v", topic, err)
		}
	}

	n, err := p.EvictPrefix(ctx, "b/dev1")
	if err != nil {
		t.Fatalf("evict prefix: %v", err)
	}
	if n != 3 {
		t.Fatalf("cleared = %d, want 3 (the two channels and the exact topic)", n)
	}
	got := p.Published()
	if len(got) != 1 || got[0] != "b/dev10/1/state" {
		t.Fatalf("published = %v, want only the other device's topic", got)
	}

	if _, err := p.EvictPrefix(ctx, ""); err == nil {
		t.Fatal("expected an error for an empty prefix")
	}
}

func TestEvictPrefixKeepsARefusedTopicInTheIndex(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()
	for _, topic := range []string{"b/dev1/1/state", "b/dev1/2/state"} {
		if _, err := p.Publish(ctx, topic, []byte("v")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	boom := errors.New("refused")
	f.mu.Lock()
	f.failPublish = func(topic string) error {
		if topic == "b/dev1/2/state" {
			return boom
		}
		return nil
	}
	f.mu.Unlock()

	n, err := p.EvictPrefix(ctx, "b/dev1")
	if n != 1 || !errors.Is(err, boom) {
		t.Fatalf("cleared = %d err = %v, want 1 and the refusal", n, err)
	}
	got := p.Published()
	if len(got) != 1 || got[0] != "b/dev1/2/state" {
		t.Fatalf("published = %v, want the refused topic still indexed for a retry", got)
	}
}

func TestEvictPrefixStopsOnCancellation(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	if _, err := p.Publish(context.Background(), "b/dev1/1/state", []byte("v")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.EvictPrefix(cancelled, "b/dev1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestRepublishBypassesTheDedupGate(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()
	for _, topic := range []string{"b/2/state", "b/1/state"} {
		if _, err := p.Publish(ctx, topic, []byte("v")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	// The broker forgot everything — restarted without persistence.
	f.mu.Lock()
	f.retained = map[string][]byte{}
	f.mu.Unlock()

	n, err := p.Republish(ctx)
	if err != nil || n != 2 {
		t.Fatalf("republish = %d, %v; want 2 and no error", n, err)
	}
	f.mu.Lock()
	restored := len(f.retained)
	f.mu.Unlock()
	if restored != 2 {
		t.Fatalf("broker holds %d retained values, want 2", restored)
	}

	ops := stateOps(f)
	replay := ops[len(ops)-2:]
	if replay[0].topic != "b/1/state" || replay[1].topic != "b/2/state" {
		t.Fatalf("replay order = %s, %s; want sorted", replay[0].topic, replay[1].topic)
	}
}

func TestRepublishContinuesPastAFailedTopicAndStopsOnCancellation(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()
	for _, topic := range []string{"b/1/state", "b/2/state", "b/3/state"} {
		if _, err := p.Publish(ctx, topic, []byte("v")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	boom := errors.New("breaker open")
	f.mu.Lock()
	f.failPublish = func(topic string) error {
		if topic == "b/2/state" {
			return boom
		}
		return nil
	}
	f.mu.Unlock()

	n, err := p.Republish(ctx)
	if n != 2 || !errors.Is(err, boom) {
		t.Fatalf("republish = %d, %v; want 2 and the failure reported", n, err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Republish(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestForgetLetsTheNextIdenticalValueThrough(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()

	if _, err := p.Publish(ctx, "b/1/state", []byte("21.5")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	p.Forget("b/1/state")
	if got := p.Published(); len(got) != 0 {
		t.Fatalf("published = %v, want the topic forgotten", got)
	}
	sent, err := p.Publish(ctx, "b/1/state", []byte("21.5"))
	if err != nil || !sent {
		t.Fatalf("after Forget: sent=%v err=%v", sent, err)
	}
	// Forgetting is not evicting: the broker keeps what it had.
	f.mu.Lock()
	held := string(f.retained["b/1/state"])
	f.mu.Unlock()
	if held != "21.5" {
		t.Fatalf("broker holds %q, want the value still retained", held)
	}
}

func TestPublishedIsSorted(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	for _, topic := range []string{"b/c", "b/a", "b/b"} {
		if _, err := p.Publish(context.Background(), topic, []byte("v")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	got := p.Published()
	want := []string{"b/a", "b/b", "b/c"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("published = %v, want %v", got, want)
	}
}

func TestLatencyIgnoresQoS0AndFailures(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{PulseQoS: 0})
	ctx := context.Background()
	if err := p.Pulse(ctx, "b/1/event", []byte("x")); err != nil {
		t.Fatalf("pulse: %v", err)
	}
	if got := p.Latency(); got.Total != 0 {
		t.Fatalf("latency = %+v, want Total 0 — a QoS 0 publish is never acknowledged", got)
	}

	g := newFake()
	boom := errors.New("nope")
	g.failPublish = func(string) error { return boom }
	q := NewStatePublisher(g, StateConfig{})
	if _, err := q.Publish(ctx, "b/1/state", []byte("v")); err == nil {
		t.Fatal("expected the publish to fail")
	}
	if got := q.Latency(); got.Total != 0 {
		t.Fatalf("latency = %+v, want Total 0 — a failure measures the failure", got)
	}
}

func TestLatencySummarisesAcknowledgedPublishes(t *testing.T) {
	t.Parallel()

	s := &slowTransport{delay: 2 * time.Millisecond}
	p := NewStatePublisher(s, StateConfig{})
	for i := range 3 {
		if _, err := p.Publish(context.Background(), "b/"+string(rune('a'+i)), []byte("v")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	got := p.Latency()
	if got.Total != 3 {
		t.Fatalf("Total = %d, want 3", got.Total)
	}
	if got.MedianMs <= 0 {
		t.Fatalf("MedianMs = %v, want a positive reading", got.MedianMs)
	}
	if got.MaxMs < got.MedianMs {
		t.Fatalf("MaxMs %v < MedianMs %v", got.MaxMs, got.MedianMs)
	}

	// An even-length window averages the two middle samples, so the median
	// of a two-sample window sits between them.
	q := NewStatePublisher(&slowTransport{delay: time.Millisecond}, StateConfig{LatencyWindow: 2})
	for i := range 5 {
		if _, err := q.Publish(context.Background(), "b/"+string(rune('a'+i)), []byte("v")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	stats := q.Latency()
	if stats.Total != 5 {
		t.Fatalf("Total = %d, want 5 — the total counts past the window", stats.Total)
	}
	q.latMu.Lock()
	kept := len(q.samples)
	q.latMu.Unlock()
	if kept != 2 {
		t.Fatalf("window holds %d samples, want 2", kept)
	}
}

func TestLatencyCanBeDisabled(t *testing.T) {
	t.Parallel()

	p := NewStatePublisher(newFake(), StateConfig{LatencyWindow: -1})
	if _, err := p.Publish(context.Background(), "b/1/state", []byte("v")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := p.Latency(); got.Total != 0 {
		t.Fatalf("latency = %+v, want nothing measured", got)
	}
}

func TestStateTopicsAreRefusedInsideACommandSubscription(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{
		CommandFilters: []string{"b/+/+/set", "b/hub/programs/+/trigger"},
	})
	ctx := context.Background()

	// The measured regression: a program's state mirrored onto its own
	// trigger topic, echoed back by the broker, ran the program on every
	// boot.
	_, err := p.Publish(ctx, "b/hub/programs/12459/trigger", []byte("true"))
	if !errors.Is(err, ErrStateCommandCollision) {
		t.Fatalf("err = %v, want ErrStateCommandCollision", err)
	}
	if err := p.Pulse(ctx, "b/1/2/set", []byte("x")); !errors.Is(err, ErrStateCommandCollision) {
		t.Fatalf("pulse err = %v, want ErrStateCommandCollision", err)
	}
	if n := len(stateOps(f)); n != 0 {
		t.Fatalf("ops = %d, want nothing on the wire", n)
	}

	if _, err := p.Publish(ctx, "b/hub/programs/12459/state", []byte("true")); err != nil {
		t.Fatalf("a disjoint state topic must pass: %v", err)
	}
}

func TestStatePlaneUsesTheSharedFilterMatcher(t *testing.T) {
	t.Parallel()

	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/+/c", "a/b/c", true},
		{"a/+/c", "a/b/d", false},
		{"a/+/c", "a/b/c/d", false},
		{"a/+", "a/b/c", false},
		{"a/#", "a/b/c", true},
		{"a/#", "a", true},
		{"a/#", "b/c", false},
		{"#", "a/b", true},
		{"a/#/b", "a/x/b", false},
		{"+/b", "a/b", true},
		{"a/b", "a/b/c", false},
		{"a/b/c", "a/b", false},
		// MQTT 5 §4.7.2: a leading wildcard must not reach the broker's own
		// tree.
		{"#", "$SYS/broker/uptime", false},
		{"+/broker/uptime", "$SYS/broker/uptime", false},
		{"$SYS/#", "$SYS/broker/uptime", true},
		{"", "a/b", false},
		{"a/b", "", false},
	}
	for _, c := range cases {
		if got := MatchFilter(c.filter, c.topic); got != c.want {
			t.Errorf("MatchFilter(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
}

func TestStateForInheritsTheRuntimesTransportAndQoS(t *testing.T) {
	t.Parallel()

	f := newFake()
	r := New(f, Config{QoS: 2, StatusTopic: "b/bridge/status"})
	p := StateFor(r, StateConfig{})
	if p.cfg.QoS != 2 {
		t.Fatalf("qos = %d, want the runtime's 2", p.cfg.QoS)
	}
	if p.log != r.log {
		t.Fatal("the state publisher must inherit the runtime's logger")
	}
	if _, err := p.Publish(context.Background(), "b/1/state", []byte("v")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := stateOps(f)[0].qos; got != 2 {
		t.Fatalf("published qos = %d, want 2", got)
	}

	// An explicit setting still wins over the inherited one.
	q := StateFor(r, StateConfig{QoS: 1})
	if q.cfg.QoS != 1 {
		t.Fatalf("qos = %d, want the explicit 1", q.cfg.QoS)
	}
}

func TestNilTransportAndNilRuntimePanic(t *testing.T) {
	t.Parallel()

	for name, fn := range map[string]func(){
		"nil transport": func() { NewStatePublisher(nil, StateConfig{}) },
		"nil runtime":   func() { StateFor(nil, StateConfig{}) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Fatal("expected a panic naming the composition root")
				}
			}()
			fn()
		})
	}
}

// TestEveryWriteHonoursTheCollisionGuard is the consistency regression.
//
// The guard used to cover [StatePublisher.Publish] and [StatePublisher.Pulse]
// and nothing else, so [StatePublisher.Evict] — the one call a consumer
// reaches for the moment a device is removed — published an empty retained
// payload straight into this process's own command subscription, where it
// arrives as an empty command. Evict legitimately reaches topics this process
// never published; that is a question about the index, and the guard asks a
// different one: does this process subscribe to the topic.
func TestEveryWriteHonoursTheCollisionGuard(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{CommandFilters: []string{"b/+/+/set"}})
	ctx := context.Background()
	const colliding = "b/1/2/set"

	if err := p.Evict(ctx, colliding); !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("Evict err = %v, want ErrStateCommandCollision", err)
	}
	if n := len(stateOps(f)); n != 0 {
		t.Fatalf("the eviction reached the wire: %+v", stateOps(f))
	}

	// Best-effort across the list: the disjoint topic still goes out.
	if err := p.Evict(ctx, colliding, "b/1/2/state"); !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("Evict err = %v, want the collision reported", err)
	}
	if got := f.retractions(); len(got) != 1 || got[0] != "b/1/2/state" {
		t.Errorf("retractions = %v, want only the disjoint topic", got)
	}

	// A consumer that widens its filters after publishing must not have the
	// index walks echo to itself either.
	wide := newFake()
	q := NewStatePublisher(wide, StateConfig{})
	if _, err := q.Publish(ctx, colliding, []byte("21.5")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	q.cfg.CommandFilters = []string{"b/+/+/set"}
	if n, err := q.EvictPrefix(ctx, "b/1"); n != 0 || !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("EvictPrefix = %d, %v; want the collision reported", n, err)
	}
	if n, err := q.Republish(ctx); n != 0 || !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("Republish = %d, %v; want the collision reported", n, err)
	}
	if got := len(stateOps(wide)); got != 1 {
		t.Errorf("ops = %d, want only the original publish", got)
	}
}

// TestEvictPrefixFoldsCase is the porting trap the segment-boundary fix left
// open: the reference implementation lower-cases the address it searches for,
// and dropping that along with its substring search turned a mis-cased device
// address into a silent no-op. (0, nil) is indistinguishable from "nothing to
// clear", so a removed device's whole retained state stayed standing.
func TestEvictPrefixFoldsCase(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()

	const live = "ccu/ABC0001/1/values/TEMP"
	if _, err := p.Publish(ctx, live, []byte("21.5")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := p.Publish(ctx, "ccu/ABC0001", []byte("x")); err != nil {
		t.Fatalf("publish the bare address: %v", err)
	}

	n, err := p.EvictPrefix(ctx, "ccu/abc0001")
	if err != nil {
		t.Fatalf("evict prefix: %v", err)
	}
	if n != 2 {
		t.Fatalf("cleared = %d, want both topics of the mis-cased address", n)
	}
	if got := p.Published(); len(got) != 0 {
		t.Fatalf("published = %v, want the device gone", got)
	}

	// Folding widens the match on case alone, never across the segment
	// boundary: a sibling address that only shares a character prefix stays.
	if _, err := p.Publish(ctx, "ccu/ABC00010/1/values/TEMP", []byte("9")); err != nil {
		t.Fatalf("publish the sibling: %v", err)
	}
	if n, err = p.EvictPrefix(ctx, "ccu/abc0001"); err != nil || n != 0 {
		t.Fatalf("EvictPrefix = %d, %v; want the sibling spared", n, err)
	}
}

// TestStateResetIsTheSameIdiomAsTheAvailabilityPlanes closes the second half
// of the two-idioms defect: the availability plane had a Reset and the state
// plane had only Forget plus Republish, for the identical "the broker came
// back without its retained store" problem.
func TestStateResetIsTheSameIdiomAsTheAvailabilityPlanes(t *testing.T) {
	t.Parallel()

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()
	const name = "b/dev/1/state"

	if _, err := p.Publish(ctx, name, []byte("21.5")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if sent, _ := p.Publish(ctx, name, []byte("21.5")); sent {
		t.Fatal("the gate did not hold before the reset")
	}

	p.Reset()
	if got := p.Published(); len(got) != 1 || got[0] != name {
		t.Errorf("Reset dropped the index: Published() = %v", got)
	}
	if n, err := p.Republish(ctx); err != nil || n != 1 {
		t.Errorf("Reset then Republish sent %d, %v; want the value re-sent", n, err)
	}
	sent, err := p.Publish(ctx, name, []byte("21.5"))
	if err != nil || !sent {
		t.Fatalf("the post-reconnect publish was suppressed: %v, %v", sent, err)
	}
	if sent, err = p.Publish(ctx, name, []byte("21.5")); err != nil || sent {
		t.Errorf("the gate stayed open after the reset's one write: %v, %v", sent, err)
	}
}

// TestComponentStateTopicIsTheOnlyProvablyEqualRoute measures the asymmetry
// that made a derived state topic a silent defect.
//
// The renderer projects `state_topic` only on the platforms whose catalog
// schema accepts the key. A [topic.Layout] has no such notion and returns a
// topic for every platform, so on the ten that name a topic per role or are
// write-only, the derived topic and the config's are different strings and
// the publish succeeds into nothing. The entity stays "unknown" forever and
// nothing logs anything, which is why this has to be caught by a type, not by
// an operator.
func TestComponentStateTopicIsTheOnlyProvablyEqualRoute(t *testing.T) {
	t.Parallel()

	layout := topic.Default{Root: "b"}
	dctx := discovery.StdContext{Layout: layout, Namespace: "b", Lang: "en"}
	dev := &model.Device{
		Identity: model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "VEQ0001"}}},
		Name:     model.L("Thermostat"),
	}
	slot := model.S(dev.UID(), "1", model.BucketValues, "TEMPERATURE")
	entity := func(p hacatalog.Platform) *model.Basic {
		return &model.Basic{
			EntityKey:      "temperature",
			EntityPlatform: p,
			Description:    model.Description{Name: model.L("Temperature")},
			Binds:          []model.Binding{{Role: model.RoleState, Slot: slot, Mode: model.Read}},
		}
	}

	// The platform that does carry the key: both routes agree, which is what
	// makes the disagreement below meaningful rather than a rendering quirk.
	got, err := StateTopicFor(dctx, dev, entity(hacatalog.PlatformSensor))
	if err != nil {
		t.Fatalf("StateTopicFor(sensor): %v", err)
	}
	if want := layout.State(slot); got != want {
		t.Fatalf("sensor state topic = %q, want %q", got, want)
	}

	// The platform that does not. The layout still answers, which is the
	// trap; the component-shaped route refuses.
	if derived := layout.State(slot); derived == "" {
		t.Fatal("the layout declined to derive a topic; this test no longer describes the trap")
	}
	climate := entity(hacatalog.PlatformClimate)
	if _, err = StateTopicFor(dctx, dev, climate); !errors.Is(err, ErrNoComponentStateTopic) {
		t.Errorf("StateTopicFor(climate) err = %v, want ErrNoComponentStateTopic", err)
	}

	comp, err := discovery.RenderComponent(dctx, dev, climate, discovery.Origin{})
	if err != nil {
		t.Fatalf("RenderComponent: %v", err)
	}
	if comp.StateTopic != "" {
		t.Fatalf("climate rendered state_topic = %q; the catalog gate moved", comp.StateTopic)
	}

	f := newFake()
	p := NewStatePublisher(f, StateConfig{})
	ctx := context.Background()
	if _, err = p.PublishComponent(ctx, comp, []byte("21.5")); !errors.Is(err, ErrNoComponentStateTopic) {
		t.Errorf("PublishComponent err = %v, want ErrNoComponentStateTopic", err)
	}
	if _, err = p.PublishComponentValue(ctx, comp, 21.5, true); !errors.Is(err, ErrNoComponentStateTopic) {
		t.Errorf("PublishComponentValue err = %v, want ErrNoComponentStateTopic", err)
	}
	if n := len(stateOps(f)); n != 0 {
		t.Fatalf("a topic no config names reached the wire: %+v", stateOps(f))
	}

	// And the accepted shape publishes to exactly the string the config
	// carries.
	sensorComp, err := discovery.RenderComponent(dctx, dev, entity(hacatalog.PlatformSensor), discovery.Origin{})
	if err != nil {
		t.Fatalf("RenderComponent(sensor): %v", err)
	}
	sent, err := p.PublishComponentValue(ctx, sensorComp, 21.5, true)
	if err != nil || !sent {
		t.Fatalf("PublishComponentValue = %v, %v", sent, err)
	}
	ops := stateOps(f)
	if len(ops) != 1 || ops[0].topic != sensorComp.StateTopic {
		t.Errorf("published to %+v, want %q", ops, sensorComp.StateTopic)
	}
	if sent, err = p.PublishComponent(ctx, sensorComp, []byte("22.0")); err != nil || !sent {
		t.Errorf("PublishComponent = %v, %v", sent, err)
	}

	// A device the renderer refuses is reported as such rather than as a
	// missing state topic: the two are different faults and a consumer
	// wiring its fleet needs to know which it has.
	if _, err = StateTopicFor(dctx, nil, climate); err == nil ||
		errors.Is(err, ErrNoComponentStateTopic) {
		t.Errorf("StateTopicFor(nil device) err = %v, want the render failure", err)
	}
}

// TestLatencyIgnoresASlowFailure is the assertion the fixtures previously
// made unreachable: only a transport that both takes measurable time and
// fails can show that a failed publish is not timed.
//
// It matters because the duration of a failure is the distance to a refused
// connection or a tripped breaker, not to a working broker. Timing it would
// make [StatePublisher.Latency] report a healthy median during an outage, or
// a catastrophic one, depending on how the failure happens to be produced —
// either way a number describing something other than the broker.
func TestLatencyIgnoresASlowFailure(t *testing.T) {
	t.Parallel()

	boom := errors.New("breaker open")
	s := &slowTransport{delay: 2 * time.Millisecond, failPublish: boom}
	p := NewStatePublisher(s, StateConfig{})
	ctx := context.Background()

	if _, err := p.Publish(ctx, "b/1/state", []byte("v")); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the refusal", err)
	}
	if got := p.Latency(); got.Total != 0 || got.MedianMs != 0 {
		t.Errorf("Latency = %+v, want nothing timed", got)
	}

	// The same transport, no longer failing, does get timed — so the
	// exclusion above is the failure and not the fixture.
	s.mu.Lock()
	s.failPublish = nil
	s.mu.Unlock()
	if _, err := p.Publish(ctx, "b/1/state", []byte("v")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := p.Latency(); got.Total != 1 || got.MedianMs <= 0 {
		t.Errorf("Latency = %+v, want the accepted publish timed", got)
	}
}
