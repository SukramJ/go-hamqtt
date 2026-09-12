// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package topic_test

import (
	"testing"

	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// pulseSlot is one channel's coordinate inside a central and a wire
// interface, as the measured consumer addresses it.
func pulseSlot(path ...string) model.Slot {
	return model.S("0001ABCD", "1", model.BucketValues, path...).In("ccu-01", "HmIP-RF")
}

// TestDefaultNamesTheFourPulseTopics. The measured need: a consumer using
// publisher.StatePublisher.Pulse — which takes a plain topic string — had to
// render these four itself, outside the one package that knows its topic tree.
// The shapes follow the reference consumer's: the bucket is absent because an
// occurrence belongs to no paramset, and the three channel kinds are siblings
// of `event` rather than children, so an event subscriber is not woken by
// every impulse in the fleet.
func TestDefaultNamesTheFourPulseTopics(t *testing.T) {
	t.Parallel()

	layout := topic.Default{Root: "gh"}
	cases := []struct {
		kind topic.PulseKind
		slot model.Slot
		want string
	}{
		{topic.PulseDataPointEvent, pulseSlot("press_short"), "gh/ccu-01/HmIP-RF/0001ABCD/1/event/press_short"},
		{topic.PulseChannelEvent, pulseSlot("press_short"), "gh/ccu-01/HmIP-RF/0001ABCD/1/event"},
		{topic.PulseChannelImpulse, pulseSlot(), "gh/ccu-01/HmIP-RF/0001ABCD/1/impulse"},
		{topic.PulseChannelDeviceError, pulseSlot(), "gh/ccu-01/HmIP-RF/0001ABCD/1/device_error"},
	}
	for _, tc := range cases {
		if got := topic.PulseTopic(layout, tc.kind, tc.slot); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.kind, got, tc.want)
		}
	}
}

// TestAChannelPulseIgnoresTheDatapointPath. The three channel-level kinds
// address the channel, not a datapoint on it. A path leaking into one of them
// would publish a channel aggregate onto a per-event-type topic, where the
// subscriber for the aggregate never looks.
func TestAChannelPulseIgnoresTheDatapointPath(t *testing.T) {
	t.Parallel()

	layout := topic.Default{Root: "gh"}
	bare := topic.PulseTopic(layout, topic.PulseChannelImpulse, pulseSlot())
	withPath := topic.PulseTopic(layout, topic.PulseChannelImpulse, pulseSlot("press_short"))
	if bare != withPath {
		t.Errorf("the path changed a channel-level topic: %q vs %q", bare, withPath)
	}
}

// TestADatapointEventWithNoEventTypeNamesNoTopic. Without the guard the
// rendered topic is the channel aggregate's own, which is a different kind's
// topic with a different payload shape — a defect that publishes successfully
// and is only visible to whoever is subscribed to the aggregate.
func TestADatapointEventWithNoEventTypeNamesNoTopic(t *testing.T) {
	t.Parallel()

	layout := topic.Default{Root: "gh"}
	if got := topic.PulseTopic(layout, topic.PulseDataPointEvent, pulseSlot()); got != "" {
		t.Errorf("got %q, want no topic at all", got)
	}
	if got := topic.PulseTopic(layout, topic.PulseKind(99), pulseSlot("x")); got != "" {
		t.Errorf("an unknown kind rendered %q", got)
	}
	if got := layout.Pulse(topic.PulseChannelEvent, model.Slot{}); got != "" {
		t.Errorf("a slot with no address rendered %q", got)
	}
}

// TestALayoutThatPublishesNoPulsesNeedsNoChange. This is the whole reason
// [topic.PulseLayout] is a capability interface and not a fifth method on
// [topic.Layout]: the five consumers that publish no pulses keep compiling,
// and nothing is invented on their behalf — an appended segment would be a
// plausible topic nobody subscribes to, which is worse than none.
func TestALayoutThatPublishesNoPulsesNeedsNoChange(t *testing.T) {
	t.Parallel()

	var plain topic.Layout = quietLayout{}
	if got := topic.PulseTopic(plain, topic.PulseChannelEvent, pulseSlot()); got != "" {
		t.Errorf("a layout that names no pulse topic answered %q", got)
	}
	if _, is := plain.(topic.PulseLayout); is {
		t.Error("quietLayout must not satisfy PulseLayout, or the test proves nothing")
	}
}

// quietLayout implements only the four original [topic.Layout] methods, as a
// consumer that publishes no pulses does.
type quietLayout struct{}

func (quietLayout) State(model.Slot) string        { return "s" }
func (quietLayout) Command(model.Slot) string      { return "c" }
func (quietLayout) Availability(model.Slot) string { return "a" }
func (quietLayout) Bridge() string                 { return "b" }

// TestAConsumerCanNameItsOwnPulseTopics. The capability is reached through
// [topic.PulseTopic] so the declining case has one spelling; a layout that
// implements it must be the one that answers.
func TestAConsumerCanNameItsOwnPulseTopics(t *testing.T) {
	t.Parallel()

	if got := topic.PulseTopic(loudLayout{}, topic.PulseChannelImpulse, pulseSlot()); got != "own/impulse" {
		t.Errorf("got %q, want the layout's own answer", got)
	}
}

// loudLayout is a consumer's own layout, with a pulse tree that is not the
// default's.
type loudLayout struct {
	quietLayout
}

func (loudLayout) Pulse(kind topic.PulseKind, _ model.Slot) string {
	return "own/" + kind.Leaf()
}
