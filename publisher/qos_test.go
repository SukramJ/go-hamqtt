// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// TestQoSAtMostOnceReachesTheWireAsZeroEverywhere is the measured need of
// [QoS] itself: go-zendure2mqtt publishes every message at QoS 0 and, before
// this type, could not say so — all four runtime types read the zero value as
// unset and coerced it to 1, so adoption changed the delivery guarantees of
// an installed base on the wire with a broker capture as the only evidence.
//
// One test across all four types on purpose. The failure this guards against
// is not "one constructor is wrong", it is "the four disagree", which is only
// visible when they are asserted side by side.
func TestQoSAtMostOnceReachesTheWireAsZeroEverywhere(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("runtime", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		r := New(f, Config{QoS: QoSAtMostOnce, StatusTopic: "b/bridge/status"})
		if _, err := r.Publish(ctx, "homeassistant/sensor/n/o/config", []byte("{}")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if got := lastPublishQoS(t, f); got != 0 {
			t.Fatalf("config published at QoS %d, want 0 — QoSAtMostOnce was not honoured", got)
		}
	})

	t.Run("state", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		p := NewStatePublisher(f, StateConfig{QoS: QoSAtMostOnce})
		if _, err := p.Publish(ctx, "b/1/state", []byte("v")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if got := lastPublishQoS(t, f); got != 0 {
			t.Fatalf("state published at QoS %d, want 0 — QoSAtMostOnce was not honoured", got)
		}
	})

	t.Run("availability", func(t *testing.T) {
		t.Parallel()
		b := &availBroker{}
		a := NewAvailability(b, AvailabilityConfig{QoS: QoSAtMostOnce, Logger: discardLogger()})
		if _, err := a.Publish(ctx, "b/dev/availability", true); err != nil {
			t.Fatalf("publish: %v", err)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if len(b.writes) != 1 || b.writes[0].qos != 0 {
			t.Fatalf("availability writes = %+v, want one at QoS 0 — QoSAtMostOnce was not honoured", b.writes)
		}
	})

	t.Run("command", func(t *testing.T) {
		t.Parallel()
		f := newFake()
		r := NewCommandRouter(f, CommandConfig{QoS: QoSAtMostOnce})
		if err := r.Handle("b/+/set", func(context.Context, Command) {}); err != nil {
			t.Fatalf("handle: %v", err)
		}
		if err := r.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		defer func() {
			if err := r.Stop(ctx); err != nil {
				t.Fatalf("stop: %v", err)
			}
		}()
		f.mu.Lock()
		defer f.mu.Unlock()
		var subs []op
		for _, o := range f.ops {
			if o.kind == "subscribe" {
				subs = append(subs, o)
			}
		}
		if len(subs) != 1 || subs[0].qos != 0 {
			t.Fatalf("subscriptions = %+v, want one at QoS 0 — QoSAtMostOnce was not honoured", subs)
		}
	})
}

// TestQoSUnsetStillMeansAtLeastOnce is the other half of the same contract,
// and the reason the numeric values are what they are: a consumer that says
// nothing must behave exactly as it did in v0.26.0, and a consumer that wrote
// the literal 1 must still mean QoS 1.
func TestQoSUnsetStillMeansAtLeastOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	f := newFake()
	r := New(f, Config{})
	if _, err := r.Publish(ctx, "homeassistant/sensor/n/o/config", []byte("{}")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := lastPublishQoS(t, f); got != 1 {
		t.Fatalf("runtime default QoS %d, want 1", got)
	}

	fs := newFake()
	ps := NewStatePublisher(fs, StateConfig{})
	if _, err := ps.Publish(ctx, "b/1/state", []byte("v")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := lastPublishQoS(t, fs); got != 1 {
		t.Fatalf("state default QoS %d, want 1", got)
	}

	b := &availBroker{}
	a := NewAvailability(b, AvailabilityConfig{})
	if _, err := a.Publish(ctx, "b/dev/availability", true); err != nil {
		t.Fatalf("publish: %v", err)
	}
	b.mu.Lock()
	gotAvail := b.writes[0].qos
	b.mu.Unlock()
	if gotAvail != 1 {
		t.Fatalf("availability default QoS %d, want 1", gotAvail)
	}

	fc := newFake()
	rc := NewCommandRouter(fc, CommandConfig{})
	if err := rc.Handle("b/+/set", func(context.Context, Command) {}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := rc.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = rc.Stop(ctx) }()
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, o := range fc.ops {
		if o.kind == "subscribe" && o.qos != 1 {
			t.Fatalf("command default subscribe QoS %d, want 1", o.qos)
		}
	}
}

// TestPulseQoSDefaultsToAtMostOnce pins the one field whose default differs,
// and pins that stating QoSAtMostOnce there is not mistaken for a raise.
func TestPulseQoSDefaultsToAtMostOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, cfg := range map[string]StateConfig{
		"unset":       {},
		"stated zero": {PulseQoS: QoSAtMostOnce},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFake()
			p := NewStatePublisher(f, cfg)
			if err := p.Pulse(ctx, "b/1/press", []byte("x")); err != nil {
				t.Fatalf("pulse: %v", err)
			}
			if got := lastPublishQoS(t, f); got != 0 {
				t.Fatalf("pulse QoS %d, want 0", got)
			}
		})
	}
}

// TestInvalidQoSPanicsAtConstruction covers the third state the old `byte`
// field had no answer for: a value that is neither unset nor a level. The
// panic names the field, because the mistake is in a struct literal at a
// composition root and nowhere else.
func TestInvalidQoSPanicsAtConstruction(t *testing.T) {
	t.Parallel()

	cases := map[string]func(){
		"runtime":      func() { New(newFake(), Config{QoS: 7}) },
		"state":        func() { NewStatePublisher(newFake(), StateConfig{QoS: 7}) },
		"pulse":        func() { NewStatePublisher(newFake(), StateConfig{PulseQoS: 9}) },
		"availability": func() { NewAvailability(&availBroker{}, AvailabilityConfig{QoS: 7}) },
		"command":      func() { NewCommandRouter(newFake(), CommandConfig{QoS: 7}) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				got, _ := recover().(string)
				if !strings.Contains(got, "not a QoS level") {
					t.Fatalf("recover() = %v, want a panic naming the field and the levels", got)
				}
			}()
			fn()
			t.Fatal("an unrecognised QoS must not be silently coerced")
		})
	}
}

// TestAvailabilityQoSZeroIsHonouredAndLogged pins the availability decision:
// a default rather than a floor, and loud rather than silent. The warning is
// the whole difference between this and a refusal — see
// [AvailabilityConfig.QoS].
func TestAvailabilityQoSZeroIsHonouredAndLogged(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	NewAvailability(&availBroker{}, AvailabilityConfig{QoS: QoSAtMostOnce, Logger: logger})
	if !strings.Contains(buf.String(), "publisher.availability.at_most_once") {
		t.Fatalf("log = %q, want a warning naming the consequence of QoS 0 availability", buf.String())
	}

	buf.Reset()
	NewAvailability(&availBroker{}, AvailabilityConfig{Logger: logger})
	if buf.Len() != 0 {
		t.Fatalf("log = %q, want silence for the default level", buf.String())
	}
}

// TestQoSResolutionAndWire covers the type on its own, including the two
// values that have no wire form.
func TestQoSResolutionAndWire(t *testing.T) {
	t.Parallel()

	if got := QoSUnset.Or(QoSExactlyOnce); got != QoSExactlyOnce {
		t.Fatalf("unset.Or(2) = %v, want 2", got)
	}
	if got := QoSAtMostOnce.Or(QoSAtLeastOnce); got != QoSAtMostOnce {
		t.Fatalf("QoSAtMostOnce.Or(1) = %v — a stated level must survive resolution", got)
	}
	for q, want := range map[QoS]byte{QoSAtMostOnce: 0, QoSAtLeastOnce: 1, QoSExactlyOnce: 2} {
		got, ok := q.Wire()
		if !ok || got != want {
			t.Fatalf("%v.Wire() = %d,%v, want %d,true", q, got, ok, want)
		}
	}
	for _, q := range []QoS{QoSUnset, QoS(7)} {
		if got, ok := q.Wire(); ok {
			t.Fatalf("%v.Wire() = %d,true, want no wire form", q, got)
		}
	}
	for q, want := range map[QoS]string{
		QoSUnset: "unset", QoSAtMostOnce: "0", QoSAtLeastOnce: "1",
		QoSExactlyOnce: "2", QoS(7): "invalid",
	} {
		if got := q.String(); got != want {
			t.Fatalf("String() = %q, want %q", got, want)
		}
	}
}

// lastPublishQoS is the QoS of the most recent publish the fake saw.
func lastPublishQoS(t *testing.T, f *fakeTransport) byte {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.ops) - 1; i >= 0; i-- {
		if f.ops[i].kind == "publish" {
			return f.ops[i].qos
		}
	}
	t.Fatal("no publish was recorded")
	return 0
}

// discardLogger keeps the deliberate QoS-0 availability warning out of the
// test output where the test is not about it.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), &slog.HandlerOptions{Level: slog.LevelError}))
}
