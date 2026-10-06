// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// manualClock is a clock a test advances by hand.
type manualClock struct{ now time.Time }

func (c *manualClock) Now() time.Time { return c.now }

func (c *manualClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// t0 is ADR 0083's own example timestamp, 1730385720123 ms.
var t0 = time.UnixMilli(1730385720123)

func statusPublisher(tr Transport, ext string) (*StatePublisher, *manualClock) {
	clock := &manualClock{now: t0}
	return NewStatePublisher(tr, StateConfig{
		Encoding:     discovery.StatusObjectEncoding,
		ExtensionKey: ext,
		Clock:        clock.Now,
		Logger:       discardLogger(),
	}), clock
}

func lastPublish(t *testing.T, f *fakeTransport) op {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.ops) - 1; i >= 0; i-- {
		if f.ops[i].kind == "publish" {
			return f.ops[i]
		}
	}
	t.Fatal("nothing published")
	return op{}
}

// TestStatusObjectGolden pins the bytes, key order included: val, ts, lc,
// then the one extension key. Integer milliseconds, as spec §5.2 requires.
func TestStatusObjectGolden(t *testing.T) {
	t.Parallel()

	f := newFake()
	p, _ := statusPublisher(f, "hm")
	ctx := context.Background()
	if _, err := p.PublishStatus(ctx, "n/status/a", Observation{Value: 21.6, Ext: map[string]any{"available": true}}); err != nil {
		t.Fatal(err)
	}
	got := lastPublish(t, f)
	if want := `{"val":21.6,"ts":1730385720123,"lc":1730385720123,"hm":{"available":true}}`; string(got.payload) != want {
		t.Errorf("payload = %s, want %s", got.payload, want)
	}
	if !got.retain || got.qos != 0 {
		t.Errorf("retain/qos = %v/%d, want retained at QoS 0 (spec §4)", got.retain, got.qos)
	}

	if _, err := p.PublishStatus(ctx, "n/status/b", Observation{Value: true}); err != nil {
		t.Fatal(err)
	}
	if got, want := string(lastPublish(t, f).payload), `{"val":true,"ts":1730385720123,"lc":1730385720123}`; got != want {
		t.Errorf("no extension: %s, want %s", got, want)
	}

	body, err := StatusObject{Val: "eco", TS: 2, LC: 1}.JSON()
	if err != nil || string(body) != `{"val":"eco","ts":2,"lc":1}` {
		t.Errorf("StatusObject.JSON = %s, %v", body, err)
	}
}

// TestStatusDedupComparesValNotTs is spec §3.2's "MUST NOT republish
// unchanged state in a tight loop": a re-observation of the same value is
// nothing on the wire, a changed value moves lc, and lc never moves on its
// own.
func TestStatusDedupComparesValNotTs(t *testing.T) {
	t.Parallel()

	f := newFake()
	p, clock := statusPublisher(f, "hm")
	ctx := context.Background()
	publish := func(v, ext any) bool {
		t.Helper()
		sent, err := p.PublishStatus(ctx, "n/status/x", Observation{Value: v, Ext: ext})
		if err != nil {
			t.Fatal(err)
		}
		return sent
	}

	publish(1, nil)
	clock.advance(10 * time.Second)
	if publish(1, nil) {
		t.Error("an unchanged value was republished because its ts moved")
	}
	if n := f.count("publish"); n != 1 {
		t.Errorf("publishes = %d, want 1", n)
	}

	clock.advance(10 * time.Second)
	if !publish(2, nil) {
		t.Fatal("a changed value was swallowed")
	}
	if got, want := string(lastPublish(t, f).payload), `{"val":2,"ts":1730385740123,"lc":1730385740123}`; got != want {
		t.Errorf("changed value: %s, want %s", got, want)
	}

	// The extension carries state of its own; a flip there must go out,
	// and since val did not change, lc stays where it was.
	clock.advance(5 * time.Second)
	if !publish(2, map[string]bool{"available": false}) {
		t.Fatal("an extension change was swallowed")
	}
	if got, want := string(lastPublish(t, f).payload), `{"val":2,"ts":1730385745123,"lc":1730385740123,"hm":{"available":false}}`; got != want {
		t.Errorf("extension change: %s, want %s", got, want)
	}
}

// TestStatusReconnect: Republish re-sends the cached object unchanged —
// the same observation re-delivered keeps its ts — and Reset lets the next
// observation through once with a new ts and the remembered lc.
func TestStatusReconnect(t *testing.T) {
	t.Parallel()

	f := newFake()
	p, clock := statusPublisher(f, "")
	ctx := context.Background()
	if _, err := p.PublishStatus(ctx, "n/status/x", Observation{Value: "on"}); err != nil {
		t.Fatal(err)
	}
	original := string(lastPublish(t, f).payload)

	clock.advance(time.Minute)
	if n, err := p.Republish(ctx); err != nil || n != 1 {
		t.Fatalf("Republish = %d, %v", n, err)
	}
	if got := string(lastPublish(t, f).payload); got != original {
		t.Errorf("replay = %s, want the cached %s", got, original)
	}

	p.Reset()
	sent, err := p.PublishStatus(ctx, "n/status/x", Observation{Value: "on"})
	if err != nil || !sent {
		t.Fatalf("after Reset: sent %v, %v", sent, err)
	}
	if got, want := string(lastPublish(t, f).payload), `{"val":"on","ts":1730385780123,"lc":1730385720123}`; got != want {
		t.Errorf("after Reset: %s, want %s", got, want)
	}
	if sent, _ := p.PublishStatus(ctx, "n/status/x", Observation{Value: "on"}); sent {
		t.Error("the gate stayed open after the one post-Reset publish")
	}
}

// TestStatusObservationTime: a device's own timestamp is ts (spec §5.2), and
// an out-of-order earlier observation of the same value cannot leave lc after
// ts.
func TestStatusObservationTime(t *testing.T) {
	t.Parallel()

	f := newFake()
	p, _ := statusPublisher(f, "")
	ctx := context.Background()
	at := t0.Add(-time.Hour)
	if _, err := p.PublishStatus(ctx, "n/status/x", Observation{Value: 5, At: at}); err != nil {
		t.Fatal(err)
	}
	if got, want := string(lastPublish(t, f).payload), `{"val":5,"ts":1730382120123,"lc":1730382120123}`; got != want {
		t.Errorf("payload = %s, want %s", got, want)
	}

	p.Reset()
	if _, err := p.PublishStatus(ctx, "n/status/x", Observation{Value: 5, At: at.Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if got, want := string(lastPublish(t, f).payload), `{"val":5,"ts":1730382060123,"lc":1730382060123}`; got != want {
		t.Errorf("earlier observation: %s, want lc <= ts: %s", got, want)
	}
}

// TestStatusMemoryFollowsTheIndex: whatever drops a topic from the index
// drops its value memory, so a later observation starts a fresh lc instead
// of being gated against a value the broker no longer holds.
func TestStatusMemoryFollowsTheIndex(t *testing.T) {
	t.Parallel()

	f := newFake()
	p, clock := statusPublisher(f, "")
	ctx := context.Background()
	obs := Observation{Value: 1}
	for name, drop := range map[string]func(){
		"Evict":       func() { _ = p.Evict(ctx, "n/status/x") },
		"EvictPrefix": func() { _, _ = p.EvictPrefix(ctx, "n/status") },
		"Forget":      func() { p.Forget("n/status/x") },
		"raw Publish": func() { _, _ = p.Publish(ctx, "n/status/x", []byte("raw")) },
	} {
		if _, err := p.PublishStatus(ctx, "n/status/x", obs); err != nil {
			t.Fatal(err)
		}
		drop()
		clock.advance(time.Second)
		sent, err := p.PublishStatus(ctx, "n/status/x", obs)
		if err != nil || !sent {
			t.Errorf("%s: sent %v, %v — want a fresh publish", name, sent, err)
		}
		want := clock.now.UnixMilli()
		if got := string(lastPublish(t, f).payload); got != `{"val":1,"ts":`+strconv.FormatInt(want, 10)+`,"lc":`+strconv.FormatInt(want, 10)+`}` {
			t.Errorf("%s: %s, want a fresh lc", name, got)
		}
	}
}

// TestStatusPulse: an occurrence is a status object that is not retained,
// at the pulse QoS, with ts = lc, and every one goes out.
func TestStatusPulse(t *testing.T) {
	t.Parallel()

	f := newFake()
	p, _ := statusPublisher(f, "hm")
	ctx := context.Background()
	for range 2 {
		if err := p.PulseStatus(ctx, "n/status/a/1/event", Observation{Value: "PRESS_SHORT", Ext: map[string]int{"channel": 1}}); err != nil {
			t.Fatal(err)
		}
	}
	if n := f.count("publish"); n != 2 {
		t.Errorf("publishes = %d, want both keypresses", n)
	}
	got := lastPublish(t, f)
	if want := `{"val":"PRESS_SHORT","ts":1730385720123,"lc":1730385720123,"hm":{"channel":1}}`; string(got.payload) != want {
		t.Errorf("pulse = %s, want %s", got.payload, want)
	}
	if got.retain || got.qos != 0 {
		t.Errorf("retain/qos = %v/%d, want not retained at QoS 0", got.retain, got.qos)
	}
	if err := p.PulseStatus(ctx, "n/status/a/1/event", Observation{}); !errors.Is(err, ErrNilStatusValue) {
		t.Errorf("nil pulse: %v", err)
	}
}

// TestStatusEncodingDefaultsAndRefusals: QoS 0 is the default only under the
// status-object encoding, a stated level wins, PublishValue routes through
// the object form, and an extension key that redefines a spec field is a
// composition-root mistake.
func TestStatusEncodingDefaultsAndRefusals(t *testing.T) {
	t.Parallel()

	f := newFake()
	if p := NewStatePublisher(f, StateConfig{Logger: discardLogger()}); p.qos != 1 {
		t.Errorf("envelope default QoS = %d, want 1 unchanged", p.qos)
	}
	stated := NewStatePublisher(f, StateConfig{Encoding: discovery.StatusObjectEncoding, QoS: QoSAtLeastOnce, Logger: discardLogger()})
	if stated.qos != 1 {
		t.Errorf("stated QoS = %d, want 1", stated.qos)
	}

	rt := New(f, Config{StatusTopic: "x/bridge/status", QoS: QoSAtLeastOnce, Logger: discardLogger()})
	if p := StateFor(rt, StateConfig{Encoding: discovery.StatusObjectEncoding}); p.qos != 0 {
		t.Errorf("StateFor status QoS = %d, want 0, not the runtime's 1", p.qos)
	}
	if p := StateFor(rt, StateConfig{}); p.qos != 1 {
		t.Errorf("StateFor envelope QoS = %d, want the runtime's 1", p.qos)
	}

	p, _ := statusPublisher(f, "")
	if _, err := p.PublishValue(context.Background(), "n/status/v", 3, false); err != nil {
		t.Fatal(err)
	}
	if got := string(lastPublish(t, f).payload); got != `{"val":3,"ts":1730385720123,"lc":1730385720123}` {
		t.Errorf("PublishValue = %s", got)
	}
	if _, err := p.PublishStatus(context.Background(), "n/status/v", Observation{}); !errors.Is(err, ErrNilStatusValue) {
		t.Errorf("nil value: %v", err)
	}

	for _, key := range []string{"val", "ts", "lc"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("ExtensionKey %q accepted", key)
				}
			}()
			NewStatePublisher(f, StateConfig{ExtensionKey: key})
		}()
	}
}

// TestStatusFailureRecordsNothing: like every gate here, only what the
// broker accepted is remembered, so the retry goes out.
func TestStatusFailureRecordsNothing(t *testing.T) {
	t.Parallel()

	f := newFake()
	p, _ := statusPublisher(f, "")
	f.failPublish = func(string) error { return errors.New("down") }
	if _, err := p.PublishStatus(context.Background(), "n/status/x", Observation{Value: 1}); err == nil {
		t.Fatal("want the transport error")
	}
	f.failPublish = nil
	if sent, err := p.PublishStatus(context.Background(), "n/status/x", Observation{Value: 1}); err != nil || !sent {
		t.Errorf("retry: sent %v, %v", sent, err)
	}
}
