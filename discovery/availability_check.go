// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrAvailabilityUnpublished reports an entity whose availability list names a
// topic the consumer says it does not publish. See [CheckAvailability].
var ErrAvailabilityUnpublished = errors.New("discovery: availability topic is not published")

// AvailabilityTopics lists every availability topic the given components
// reference, deduplicated and sorted.
//
// Both spellings are read: the `availability` list and the singular
// `availability_topic` a consumer sets when it collapses a one-entry list into
// Home Assistant's older keys.
//
// It exists to be asserted on. A fleet's availability topics are a very small
// set — usually one string, occasionally one per device — and the whole of the
// failure this function serves is a set with one member too many. The measured
// assertion is go-mtec2mqtt's: "distinct `availability` values across all 200
// payloads | exactly one".
func AvailabilityTopics(comps ...Component) []string {
	seen := map[string]bool{}
	// Indexed rather than ranged by value: a Component is a large struct and
	// this walks a whole fleet of them.
	for i := range comps {
		comp := &comps[i]
		for _, t := range componentAvailabilityTopics(comp) {
			seen[t] = true
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// BundleAvailabilityTopics is [AvailabilityTopics] over a whole device
// document.
func BundleAvailabilityTopics(b *Bundle) []string {
	if b == nil {
		return nil
	}
	return AvailabilityTopics(bundleComponents(b)...)
}

// CheckAvailability refuses components whose availability list names a topic
// this consumer does not publish.
//
// # What it is for
//
// The zero [model.Availability] resolves to `{LevelBridge, LevelDevice}` under
// mode `all`, and `LevelDevice` names a per-device availability topic. Two
// consumers in the ADR 0070 fan-out publish no such topic: adopting the
// default would have left every entity permanently unavailable — 100 for one
// bridge, 264 for another — because under `all` Home Assistant requires every
// listed source to say `online`, and a source nobody publishes is not
// neutral. There is nothing on the wire and nothing in any log; both consumers
// caught it only because their migration had a measurement step. A third is
// the exact inverse — it publishes the device topic, and the default is the
// right answer there — which is why the default cannot simply be changed.
//
// So the library cannot know which kind of consumer it is talking to, and this
// is the call that makes the consumer say. The statement is a topic predicate
// rather than a list of levels on purpose: a level has to be resolved through
// a [topic.Layout] to become a string, and it is the string a config carries
// and an operator greps for. A bridge-only consumer states one topic:
//
//	err := discovery.CheckAvailability(
//		func(t string) bool { return t == layout.Bridge() },
//		comps...)
//
// and the library's default then fails the render instead of greying out the
// fleet. That is generalised from go-daikin2mqtt's own entity builder, which
// "refuses anything that is not one plain bridge-level source ... so dropping
// [model.BridgeOnly] produces two entries and fails the render instead of
// silently publishing a second, never-written topic that would grey out all
// 264 entities."
//
// A consumer whose device-level availability points at a state topic plus a
// template — one measured plane does exactly that — accepts it here the same
// way, because that topic is one it publishes.
//
// # What it cannot do
//
// It cannot decide the question on its own. `publishes` is the consumer's
// assertion, and a predicate wide enough to accept everything asserts
// nothing. What it converts is the failure mode: from a silent fleet-wide
// outage discoverable only from a broker capture, into an error at the
// composition root naming the entity and the topic.
//
// A nil predicate is a programming error rather than "accept everything", and
// is refused.
func CheckAvailability(publishes func(topic string) bool, comps ...Component) error {
	if publishes == nil {
		return errors.New("discovery: CheckAvailability needs a predicate saying which topics this consumer publishes")
	}
	var bad []string
	for i := range comps {
		comp := &comps[i]
		for _, t := range componentAvailabilityTopics(comp) {
			if publishes(t) {
				continue
			}
			bad = append(bad, fmt.Sprintf("%s references %q", componentLabel(comp), t))
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Strings(bad)
	return fmt.Errorf("%w: %s", ErrAvailabilityUnpublished, strings.Join(bad, "; "))
}

// CheckBundleAvailability is [CheckAvailability] over a whole device document,
// reporting components by their bundle key.
func CheckBundleAvailability(b *Bundle, publishes func(topic string) bool) error {
	if b == nil {
		return errors.New("discovery: nil bundle")
	}
	if publishes == nil {
		return errors.New("discovery: CheckAvailability needs a predicate saying which topics this consumer publishes")
	}
	var bad []string
	for _, key := range b.Keys() {
		comp := b.Components[key]
		for _, t := range componentAvailabilityTopics(&comp) {
			if publishes(t) {
				continue
			}
			bad = append(bad, fmt.Sprintf("%s references %q", key, t))
		}
	}
	if len(bad) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrAvailabilityUnpublished, strings.Join(bad, "; "))
}

// componentAvailabilityTopics is both spellings of one component's
// availability topics, in payload order, empty ones skipped — an empty topic
// is [Validate]'s finding, not this one's.
func componentAvailabilityTopics(comp *Component) []string {
	out := make([]string, 0, len(comp.Availability)+1)
	for _, a := range comp.Availability {
		if a.Topic != "" {
			out = append(out, a.Topic)
		}
	}
	if comp.AvailabilityTopic != "" {
		out = append(out, comp.AvailabilityTopic)
	}
	return out
}

// componentLabel names a loose component in an error, preferring the string
// an operator can grep the broker for.
func componentLabel(comp *Component) string {
	switch {
	case comp.UniqueID != "":
		return comp.UniqueID
	case comp.Platform != "":
		return string(comp.Platform)
	default:
		return "component"
	}
}

// bundleComponents is the document's components in key order.
func bundleComponents(b *Bundle) []Component {
	keys := b.Keys()
	out := make([]Component, 0, len(keys))
	for _, k := range keys {
		out = append(out, b.Components[k])
	}
	return out
}
