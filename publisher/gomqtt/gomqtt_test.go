// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package gomqtt

import (
	"context"
	"errors"
	"testing"

	mqtt "github.com/SukramJ/go-mqtt"
)

// fakeClient is a go-mqtt client that records rather than connects. It
// implements both halves so the same value can be handed to Transport and,
// separately, to Split.
type fakeClient struct {
	topic   string
	payload []byte
	qos     mqtt.QoS
	retain  bool

	filter  string
	handler mqtt.MessageHandler

	unsubscribed string
	err          error
}

func (f *fakeClient) Publish(_ context.Context, topic string, payload []byte, qos mqtt.QoS, retain bool, _ ...mqtt.PublishOption) error {
	f.topic, f.payload, f.qos, f.retain = topic, payload, qos, retain
	return f.err
}

func (f *fakeClient) Subscribe(_ context.Context, filter string, qos mqtt.QoS, h mqtt.MessageHandler, _ ...mqtt.SubscribeOption) (mqtt.SubscribeResult, error) {
	f.filter, f.qos, f.handler = filter, qos, h
	return mqtt.SubscribeResult{}, f.err
}

func (f *fakeClient) Unsubscribe(_ context.Context, filter string) error {
	f.unsubscribed = filter
	return f.err
}

// TestAdapterPassesEverythingThrough pins that the adapter is a translation
// and nothing else — in particular that the QoS the runtime chose for a
// retained discovery config reaches the client unchanged, which is the value
// the four hand-written copies of this glue disagreed about.
func TestAdapterPassesEverythingThrough(t *testing.T) {
	t.Parallel()
	c := &fakeClient{}
	tr := Transport(c)
	ctx := context.Background()

	if err := tr.Publish(ctx, "t", []byte("p"), 2, true); err != nil {
		t.Fatal(err)
	}
	if c.topic != "t" || string(c.payload) != "p" || c.qos != mqtt.QoS2 || !c.retain {
		t.Fatalf("publish arrived as %q %q qos=%d retain=%v", c.topic, c.payload, c.qos, c.retain)
	}

	var got [3]any
	if err := tr.Subscribe(ctx, "f/#", 1, func(topic string, payload []byte, retained bool) {
		got = [3]any{topic, string(payload), retained}
	}); err != nil {
		t.Fatal(err)
	}
	if c.filter != "f/#" || c.qos != mqtt.QoS1 {
		t.Fatalf("subscribe arrived as %q qos=%d", c.filter, c.qos)
	}
	// A delivery is flattened to the three fields the runtime reads, the
	// retain bit included — the sweep needs it to tell a retained config
	// from a live one.
	c.handler(&mqtt.Message{Topic: "f/x", Payload: []byte("v"), Retain: true})
	if got != [3]any{"f/x", "v", true} {
		t.Fatalf("delivery arrived as %v", got)
	}

	if err := tr.Unsubscribe(ctx, "f/#"); err != nil {
		t.Fatal(err)
	}
	if c.unsubscribed != "f/#" {
		t.Fatalf("unsubscribed %q", c.unsubscribed)
	}
}

// TestAdapterReportsErrors pins that nothing is swallowed: a consumer
// publishing through a circuit breaker must see the breaker's error, or the
// runtime caches a config the broker never accepted.
func TestAdapterReportsErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("open")
	c := &fakeClient{err: boom}
	tr := Split(c, c)
	ctx := context.Background()

	if err := tr.Publish(ctx, "t", nil, 1, true); !errors.Is(err, boom) {
		t.Fatalf("publish: %v", err)
	}
	if err := tr.Subscribe(ctx, "f", 1, func(string, []byte, bool) {}); !errors.Is(err, boom) {
		t.Fatalf("subscribe: %v", err)
	}
	if err := tr.Unsubscribe(ctx, "f"); !errors.Is(err, boom) {
		t.Fatalf("unsubscribe: %v", err)
	}
}
