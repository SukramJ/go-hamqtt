// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package gomqtt

import (
	"context"
	"errors"
	"testing"

	mqtt "github.com/SukramJ/go-mqtt"

	"github.com/SukramJ/go-hamqtt/publisher"
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

	// opts counts the SubscribeOption values the adapter passed. Their
	// values are opaque outside go-mqtt — the option type is a func over
	// an unexported struct — so the count is all a fake can see, and it is
	// enough to catch an option dropped from a call that must carry two.
	opts int
}

func (f *fakeClient) Publish(_ context.Context, topic string, payload []byte, qos mqtt.QoS, retain bool, _ ...mqtt.PublishOption) error {
	f.topic, f.payload, f.qos, f.retain = topic, payload, qos, retain
	return f.err
}

func (f *fakeClient) Subscribe(_ context.Context, filter string, qos mqtt.QoS, h mqtt.MessageHandler, opts ...mqtt.SubscribeOption) (mqtt.SubscribeResult, error) {
	f.filter, f.qos, f.handler, f.opts = filter, qos, h, len(opts)
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

// TestAdapterSubscribeFormsCarryTheirOptions pins the three subscribe forms
// apart. The plain one carries nothing, the No-Local one carries one
// option, and the attributed one carries two — the identifier AND No Local,
// because a Subscription Identifier is MQTT 5.0 only, so a call that
// reaches that method is on a link where No Local is available too and the
// router calls it INSTEAD of SubscribeNoLocal rather than as well.
//
// Counted rather than inspected: mqtt.SubscribeOption is a func over an
// unexported struct, so no fake can read the values back. What the count
// does catch is an option dropped, which for the attributed form would
// reopen the echo class silently.
func TestAdapterSubscribeFormsCarryTheirOptions(t *testing.T) {
	t.Parallel()
	c := &fakeClient{}
	tr := Transport(c)
	ctx := context.Background()
	h := func(string, []byte, bool) {}

	if err := tr.Subscribe(ctx, "f", 1, h); err != nil {
		t.Fatal(err)
	}
	if c.opts != 0 {
		t.Errorf("plain Subscribe carried %d options, want 0", c.opts)
	}

	nl, ok := tr.(publisher.NoLocalSubscriber)
	if !ok {
		t.Fatal("the adapter must offer No Local; the command router falls back silently without it")
	}
	if err := nl.SubscribeNoLocal(ctx, "f", 1, h); err != nil {
		t.Fatal(err)
	}
	if c.opts != 1 {
		t.Errorf("SubscribeNoLocal carried %d options, want 1", c.opts)
	}

	as, ok := tr.(publisher.AttributingSubscriber)
	if !ok {
		t.Fatal("the adapter must offer attribution; the command router refuses every overlap without it")
	}
	if err := as.SubscribeAttributed(ctx, "f", 1, 7, h); err != nil {
		t.Fatal(err)
	}
	if c.opts != 2 {
		t.Errorf("SubscribeAttributed carried %d options, want 2 (identifier + No Local)", c.opts)
	}
	if c.filter != "f" || c.qos != mqtt.QoS1 {
		t.Errorf("attributed subscribe arrived as %q qos=%d", c.filter, c.qos)
	}
	// The delivery still flattens to the three fields the router reads.
	var got [3]any
	if err := as.SubscribeAttributed(ctx, "f/#", 1, 8, func(topic string, payload []byte, retained bool) {
		got = [3]any{topic, string(payload), retained}
	}); err != nil {
		t.Fatal(err)
	}
	c.handler(&mqtt.Message{Topic: "f/x", Payload: []byte("v"), Retain: true})
	if got != [3]any{"f/x", "v", true} {
		t.Fatalf("attributed delivery arrived as %v", got)
	}
}

// TestAdapterAttributedSubscribeReportsErrors pins the error path that
// makes the capability honest rather than merely claimed: go-mqtt refuses a
// Subscription Identifier on a v3.1.1 link instead of dropping it, and the
// router turns that refusal into a failed Start.
func TestAdapterAttributedSubscribeReportsErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("subscription identifiers require MQTT 5.0")
	c := &fakeClient{err: boom}
	as, ok := Split(c, c).(publisher.AttributingSubscriber)
	if !ok {
		t.Fatal("the adapter must offer attribution")
	}
	err := as.SubscribeAttributed(context.Background(), "f", 1, 3, func(string, []byte, bool) {})
	if !errors.Is(err, boom) {
		t.Fatalf("attributed subscribe: %v", err)
	}
}

// TestV311AdapterWithholdsTheV5Capabilities is the composition-root half of a
// contradiction the v0.27.0–v0.29.0 review measured: the README said an
// overlapping pair is "refused at registration with ErrAmbiguousRoutes" on
// MQTT 3.1.1, while the shipped adapter statically implements
// AttributingSubscriber whatever the wrapped client is talking — so the
// overlap was accepted on both dialects and only Start failed.
//
// A v3.1.1 link has no property block to carry a Subscription Identifier and
// no No Local, and both facts are known before the first SUBSCRIBE. This
// adapter therefore claims neither, which moves the failure from boot to the
// composition root.
func TestV311AdapterWithholdsTheV5Capabilities(t *testing.T) {
	t.Parallel()
	c := &fakeClient{}
	tr := TransportV311(c)

	if _, ok := tr.(publisher.AttributingSubscriber); ok {
		t.Error("the v3.1.1 adapter claims attribution; the overlap is then accepted and Start fails at boot")
	}
	if _, ok := tr.(publisher.NoLocalSubscriber); ok {
		t.Error("the v3.1.1 adapter claims No Local, which a v3.1.1 broker ignores")
	}

	// And a router over it refuses the overlapping pair where a consumer
	// can act on it.
	r := publisher.NewCommandRouter(tr, publisher.CommandConfig{})
	if err := r.Handle("ccu/+/+/set", func(context.Context, publisher.Command) {}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	err := r.Handle("ccu/+/PRESS_SHORT/set", func(context.Context, publisher.Command) {})
	if !errors.Is(err, publisher.ErrAmbiguousRoutes) {
		t.Fatalf("Handle = %v, want ErrAmbiguousRoutes at registration", err)
	}
	if !errors.Is(err, publisher.ErrAttributionUnavailable) {
		t.Errorf("the refusal must name the missing capability, got %v", err)
	}

	// It is still a whole transport: the three methods reach the client.
	ctx := context.Background()
	if err := tr.Publish(ctx, "t", []byte("p"), 2, true); err != nil {
		t.Fatal(err)
	}
	if c.topic != "t" || c.qos != mqtt.QoS2 || !c.retain {
		t.Fatalf("publish arrived as %q qos=%d retain=%v", c.topic, c.qos, c.retain)
	}
	if err := tr.Subscribe(ctx, "f/#", 1, func(string, []byte, bool) {}); err != nil {
		t.Fatal(err)
	}
	if c.filter != "f/#" || c.opts != 0 {
		t.Fatalf("subscribe arrived as %q with %d options, want no v5 options", c.filter, c.opts)
	}
	if err := tr.Unsubscribe(ctx, "f/#"); err != nil {
		t.Fatal(err)
	}
	if c.unsubscribed != "f/#" {
		t.Fatalf("unsubscribed %q", c.unsubscribed)
	}
}

// TestSplitV311IsSplitWithoutTheClaims pins the two-value constructor, the
// shape a consumer publishing through a circuit breaker actually has.
func TestSplitV311IsSplitWithoutTheClaims(t *testing.T) {
	t.Parallel()
	c := &fakeClient{}
	tr := SplitV311(c, c)
	if _, ok := tr.(publisher.AttributingSubscriber); ok {
		t.Error("SplitV311 claims attribution")
	}
	if err := tr.Publish(context.Background(), "t", nil, 1, true); err != nil {
		t.Fatal(err)
	}
	if c.topic != "t" {
		t.Fatalf("publish arrived as %q", c.topic)
	}
}
