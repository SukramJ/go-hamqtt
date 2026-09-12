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

// Unsubscribe implements [publisher.Transport].
func (a adapter) Unsubscribe(ctx context.Context, filter string) error {
	return a.sub.Unsubscribe(ctx, filter)
}
