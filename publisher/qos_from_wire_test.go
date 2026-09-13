// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// TestQoSFromWireKeepsAConfiguredZeroAtZero is the whole point of the
// conversion: an operator who configured `MQTT_QOS: 0` must reach the broker
// at QoS 0, and `QoS(0)` is the spelling that silently does not.
//
// Verified off the transport call rather than off the constant, which is what
// both measured consumers do — the level a stub was actually handed is the
// only thing a promise in a configuration template can be checked against.
func TestQoSFromWireKeepsAConfiguredZeroAtZero(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   byte
		want QoS
		wire byte
	}{
		{0, QoSAtMostOnce, 0},
		{1, QoSAtLeastOnce, 1},
		{2, QoSExactlyOnce, 2},
	} {
		got, err := QoSFromWire(tc.in)
		if err != nil {
			t.Fatalf("QoSFromWire(%d): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("QoSFromWire(%d) = %v, want %v", tc.in, got, tc.want)
		}
		if got == QoSUnset {
			t.Fatalf("QoSFromWire(%d) produced QoSUnset, which no configured value may become", tc.in)
		}

		f := newFake()
		r := New(f, Config{QoS: got})
		if _, err := r.Publish(context.Background(), "homeassistant/sensor/n/o/config", []byte(`{"a":1}`)); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		sent := f.ops[len(f.ops)-1].qos
		f.mu.Unlock()
		if sent != tc.wire {
			t.Fatalf("MQTT_QOS: %d reached the transport as QoS %d", tc.in, sent)
		}
	}
}

// TestQoSFromWireRefusesAnythingElse pins that the conversion has no silent
// fallback — a configuration value out of range is the operator's mistake and
// has to be reported as one.
func TestQoSFromWireRefusesAnythingElse(t *testing.T) {
	t.Parallel()
	for _, b := range []byte{3, 0x80, 255} {
		got, err := QoSFromWire(b)
		if !errors.Is(err, ErrQoSOutOfRange) {
			t.Fatalf("QoSFromWire(%d) = %v, %v; want ErrQoSOutOfRange", b, got, err)
		}
		if got != QoSUnset {
			t.Fatalf("a refused conversion must not hand back a usable level, got %v", got)
		}
	}
}

// TestStatingOneStateQoSAndNotTheOtherWarns pins go-homeconnect2mqtt's
// load-bearing catch: PulseQoS is the only field in the package whose default
// is QoS 0, so a plane that states its state QoS and forgets this one
// publishes pulses at a level nobody chose — and only when the two differ,
// which is why a single-configuration test never sees it.
func TestStatingOneStateQoSAndNotTheOtherWarns(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	NewStatePublisher(newFake(), StateConfig{QoS: QoSAtLeastOnce, Logger: logger})
	if !strings.Contains(buf.String(), "publisher.state.pulse_qos_unstated") {
		t.Fatalf("a stated QoS beside an unstated PulseQoS must warn, log was %q", buf.String())
	}

	// Both stated: nothing to say. Both unstated: the consumer has no
	// opinion at all and the package's defaults are the answer, which is
	// not the asymmetry this warns about.
	for _, cfg := range []StateConfig{
		{QoS: QoSAtLeastOnce, PulseQoS: QoSAtMostOnce},
		{},
	} {
		buf.Reset()
		cfg.Logger = logger
		NewStatePublisher(newFake(), cfg)
		if strings.Contains(buf.String(), "publisher.state.pulse_qos_unstated") {
			t.Fatalf("%+v must not warn, log was %q", cfg, buf.String())
		}
	}
}
