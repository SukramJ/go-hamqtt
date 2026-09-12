// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package topic

import "github.com/SukramJ/go-hamqtt/model"

// PulseKind names a topic that carries an occurrence rather than a value.
//
// [Layout] names the four topics a state machine needs — state, command,
// availability, bridge — and a consumer that publishes pulses has four more.
// The measured need is openccu-loom's: a keypress, a per-channel event
// aggregate, an impulse and a device error, each a non-retained payload on a
// topic of its own. With no vocabulary for them, a consumer reaching
// `publisher.StatePublisher.Pulse` — which takes a plain topic string — had to
// format those topics itself, outside the one package that is supposed to know
// its topic tree, and beside a Layout that already knew every other segment of
// it.
//
// The kinds are named after the coordinates they address rather than after one
// device family's wire protocol: a pulse is either per datapoint or per
// channel, and the three channel-level ones differ in what the occurrence
// means, which is what makes them separate topics instead of one topic with a
// type field.
type PulseKind uint8

const (
	// PulseDataPointEvent is one named event type of one datapoint — the leaf
	// below the channel's event aggregate. The event type is the slot's
	// [model.Slot.Path], so a keypress is addressed by coordinate like
	// everything else rather than by a second string argument.
	PulseDataPointEvent PulseKind = iota + 1
	// PulseChannelEvent is the channel's event aggregate: the topic every
	// event of that channel is reported on, carrying which one it was.
	PulseChannelEvent
	// PulseChannelImpulse is a channel's impulse — an occurrence with no
	// value at all, of which there is nothing to report but that it happened.
	PulseChannelImpulse
	// PulseChannelDeviceError is a channel's error report. Separate from the
	// event aggregate because a subscriber that wants events does not want to
	// be woken by faults, and one that watches for faults must not have to
	// filter every keypress of the fleet.
	PulseChannelDeviceError
)

// Leaf is the topic segment this kind conventionally renders as, and the
// spelling [Default] uses.
//
// [PulseDataPointEvent] and [PulseChannelEvent] share the `event` segment on
// purpose: the datapoint form is the leaf below the aggregate, which is how a
// subscription to the aggregate topic stays free of the per-event-type
// traffic. The other two are siblings of it rather than children for the same
// reason — nesting them under `event` would deliver every impulse to every
// event subscriber.
func (k PulseKind) Leaf() string {
	switch k {
	case PulseDataPointEvent, PulseChannelEvent:
		return "event"
	case PulseChannelImpulse:
		return "impulse"
	case PulseChannelDeviceError:
		return "device_error"
	default:
		return ""
	}
}

// String returns a debug spelling. It is not a topic segment — [PulseKind.Leaf]
// is.
func (k PulseKind) String() string {
	switch k {
	case PulseDataPointEvent:
		return "datapoint_event"
	case PulseChannelEvent:
		return "channel_event"
	case PulseChannelImpulse:
		return "channel_impulse"
	case PulseChannelDeviceError:
		return "channel_device_error"
	default:
		return "unknown"
	}
}

// Valid reports whether k is a declared kind. The zero value is not one: a
// pulse with no kind names no topic, unlike [model.BucketUnset], which is a
// real coordinate with one segment fewer.
func (k PulseKind) Valid() bool {
	return k >= PulseDataPointEvent && k <= PulseChannelDeviceError
}

// PulseLayout is a [Layout] that also names the topics occurrences are
// published on.
//
// It is an optional capability interface rather than a fifth method on
// [Layout], which is the one design decision this addition had to make. Both
// forms work; this one is the module's own extension mechanism — "not
// implementing one is the opt-out, so a new capability is additive by
// construction" — and it is the only form under which a consumer that
// publishes no pulses needs no change at all. A fifth method on Layout would
// break every one of the six consumers' layouts at once, including the five
// that have nothing to return, and an embedded default would answer for them
// with a topic nobody publishes to: worse than a compile error, because it
// looks like an answer.
//
// Consumers reach it through [PulseTopic] rather than by asserting, so the
// declining case has one spelling instead of one per call site.
type PulseLayout interface {
	Layout
	// Pulse is where an occurrence of this kind on this coordinate is
	// published. An empty string means this layout publishes no such topic —
	// see [PulseTopic].
	Pulse(kind PulseKind, s model.Slot) string
}

// PulseTopic renders the pulse topic for kind on s, or "" when the layout
// names none.
//
// A [Layout] that does not implement [PulseLayout] declines every kind, and so
// may a layout that implements it and has no topic for one particular kind.
// Nothing is invented on its behalf: the obvious fallback — a segment appended
// to the state topic — would produce a plausible, deterministic topic that no
// consumer subscribes to and no operator can find, which is the failure mode
// that costs an afternoon rather than a compile.
//
// An empty result must therefore not be published. That is the one thing a
// caller has to check, and it is why this returns the empty string rather than
// an error: a pulse whose topic a layout declines is not a fault, it is a
// consumer that does not publish that kind.
func PulseTopic(l Layout, kind PulseKind, s model.Slot) string {
	pulses, ok := l.(PulseLayout)
	if !ok {
		return ""
	}
	return pulses.Pulse(kind, s)
}

var _ PulseLayout = Default{}

// Pulse implements [PulseLayout]:
//
//	<root>[/<scope...>]/<uid>[/<channel>]/event/<path...>  datapoint event
//	<root>[/<scope...>]/<uid>[/<channel>]/event            channel event
//	<root>[/<scope...>]/<uid>[/<channel>]/impulse          channel impulse
//	<root>[/<scope...>]/<uid>[/<channel>]/device_error     channel device error
//
// The bucket is absent from all four, unlike the state and command topics: a
// paramset classifies a value that persists, and an occurrence is not one. The
// three channel-level kinds ignore [model.Slot.Path] for the same reason —
// they address the channel, not a datapoint on it.
//
// An unknown kind, a slot with no address, or a datapoint event with no path
// to name the event type all render "", which [PulseTopic] documents as "this
// layout names no such topic". A datapoint event with no path segment would
// otherwise render the channel aggregate's own topic and publish one kind's
// payload onto another's subscribers.
func (d Default) Pulse(kind PulseKind, s model.Slot) string {
	if !kind.Valid() || s.Address == "" {
		return ""
	}
	if kind == PulseDataPointEvent && len(s.Path) == 0 {
		return ""
	}

	parts := make([]string, 0, len(s.Scope)+len(s.Path)+4)
	parts = append(parts, d.Root)
	parts = append(parts, s.Scope...)
	parts = append(parts, s.Address)
	if s.Channel != "" {
		parts = append(parts, s.Channel)
	}
	parts = append(parts, kind.Leaf())
	if kind == PulseDataPointEvent {
		parts = append(parts, s.Path...)
	}
	return Join(parts...)
}
