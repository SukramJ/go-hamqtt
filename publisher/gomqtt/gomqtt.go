// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Package gomqtt adapts a [github.com/SukramJ/go-mqtt] client to
// [publisher.Transport].
//
// It is its own package so the runtime stays testable without a transport
// dependency and a consumer on some other client owes nothing to this one.
// It exists at all — rather than being left to each consumer — because the
// same fifteen lines were already written four times in the family, and the
// copies drifted: the interesting one is the QoS a retained discovery config
// is published at, which is not the QoS the state plane uses.
package gomqtt

import (
	"context"

	mqtt "github.com/SukramJ/go-mqtt"

	"github.com/SukramJ/go-hamqtt/publisher"
)

// Transport wraps a whole client.
func Transport(c mqtt.Client) publisher.Transport { return Split(c, c) }

// Split wraps a publisher and a subscriber separately.
//
// The measured shape, and the reason [Transport] is not the only
// constructor: a consumer publishes through a circuit breaker so a broker
// outage fails fast, but subscribes around it, since breaking the subscribe
// path would only delay resubscription after a reconnect without preventing
// anything. That leaves two values where one interface is wanted, and
// [mqtt.SplitClient] joining them again would hide which half is decorated.
func Split(p mqtt.Publisher, s mqtt.Subscriber) publisher.Transport {
	return adapter{pub: p, sub: s}
}

// Compile-time assertions that the adapter carries both optional
// capabilities, so the command router takes its safe paths rather than
// falling back silently.
var (
	_ publisher.NoLocalSubscriber     = adapter{}
	_ publisher.AttributingSubscriber = adapter{}
)

// adapter is a value type: it holds two interfaces and no mutable state of
// its own, so whatever concurrency guarantees the wrapped client makes are
// exactly the ones it makes.
type adapter struct {
	pub mqtt.Publisher
	sub mqtt.Subscriber
}

// Publish implements [publisher.Transport].
func (a adapter) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	return a.pub.Publish(ctx, topic, payload, mqtt.QoS(qos), retain)
}

// Subscribe implements [publisher.Transport].
//
// The granted QoS is dropped rather than reported. A broker that grants less
// than requested still delivers, and the runtime's two subscriptions — a
// retained snapshot and the birth topic — both work at any QoS; surfacing it
// would put a decision in the caller's hands that it has no action for.
func (a adapter) Subscribe(ctx context.Context, filter string, qos byte, h publisher.Handler) error {
	_, err := a.sub.Subscribe(ctx, filter, mqtt.QoS(qos), func(msg *mqtt.Message) {
		h(msg.Topic, msg.Payload, msg.Retain)
	})
	return err
}

// SubscribeNoLocal implements [publisher.NoLocalSubscriber] by asking the
// broker not to deliver this client's own publishes back to it.
//
// It exists because the collision it closes is the measured one: a consumer
// mirrored a program's state onto that program's own trigger topic, and the
// echo ran the program on every boot, every republish and once per freshly
// discovered program, with nothing in the logs saying so. No Local removes
// the whole class for the process itself.
//
// It does NOT remove the need for [publisher.CommandRouter.CheckDisjoint]:
// No Local is MQTT 5 only, a v3.1.1 link silently ignores the option, and it
// says nothing about a second process publishing into the same tree.
func (a adapter) SubscribeNoLocal(ctx context.Context, filter string, qos byte, h publisher.Handler) error {
	_, err := a.sub.Subscribe(ctx, filter, mqtt.QoS(qos), func(msg *mqtt.Message) {
		h(msg.Topic, msg.Payload, msg.Retain)
	}, mqtt.WithNoLocal())
	return err
}

// SubscribeAttributed implements [publisher.AttributingSubscriber] by
// setting the MQTT 5.0 Subscription Identifier (§3.8.2.1.2) id on the
// subscription, so go-mqtt delivers a stamped message to this subscription
// alone instead of re-matching its topic against every filter it holds.
//
// It exists because the alternative was measured and refused: a broker
// sends one copy of a PUBLISH per matching subscription, and re-matching
// each copy ran one command's handler twice against Mosquitto 2.1.2 on both
// dialects — a toggle toggling twice, with nothing in any log. The router
// therefore refused overlapping command filters outright, which cost a
// consumer the ability to special-case one parameter name; this is what
// buys that back. go-mqtt v1.5.0 is the first release that lets a caller
// set an identifier.
//
// No Local is set as well, and unconditionally. A Subscription Identifier
// is MQTT 5.0 only, so a call that reaches here at all is on a v5 link
// where No Local is available too, and the router calls this method
// INSTEAD of [publisher.NoLocalSubscriber.SubscribeNoLocal] rather than as
// well as it — dropping it here would silently reopen the echo class that
// method exists to close.
//
// The error is what makes the capability honest rather than merely claimed.
// go-mqtt refuses WithSubscriptionID on a v3.1.1 link instead of ignoring
// it, and a broker that answers with "Subscription Identifiers not
// supported" surfaces as a SUBACK failure; either way the error reaches the
// router, which fails Start with
// [publisher.ErrAttributionUnavailable] rather than subscribing without the
// identifier. That is the v5-capable-adapter-on-a-v3.1.1-broker case, and a
// silent downgrade there would leave the consumer with accepted overlapping
// routes and a handler running twice.
func (a adapter) SubscribeAttributed(
	ctx context.Context, filter string, qos byte, id uint32, h publisher.Handler,
) error {
	_, err := a.sub.Subscribe(ctx, filter, mqtt.QoS(qos), func(msg *mqtt.Message) {
		h(msg.Topic, msg.Payload, msg.Retain)
	}, mqtt.WithNoLocal(), mqtt.WithSubscriptionID(id))
	return err
}

// Unsubscribe implements [publisher.Transport].
func (a adapter) Unsubscribe(ctx context.Context, filter string) error {
	return a.sub.Unsubscribe(ctx, filter)
}
