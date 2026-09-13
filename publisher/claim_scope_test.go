// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// TestSelfClaimedSparesASiblingInstancesConfigs is the trap driven rather
// than asked.
//
// Two instances of the same bridge under one MQTT root publish byte-identical
// topics, unique ids, availability topics and state topics — one measured
// consumer's second console does exactly that — so no predicate over the
// topic or the payload can tell them apart. The claim list can: this process
// published one of these configs and did not publish the other.
//
// The sibling's config here is the shape that breaks the usual predicate as
// well: a `button`, which carries no `state_topic` at all. 24 of 264 configs
// for one consumer, 20 of 687 for another, and where the missing field let
// the rule collapse to a shared prefix, a sibling's entities were deleted
// from a live Home Assistant.
func TestSelfClaimedSparesASiblingInstancesConfigs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ours := "homeassistant/sensor/ccu_abc/temperature/config"
	orphan := "homeassistant/sensor/ccu_abc/humidity/config"
	sibling := "homeassistant/button/ccu_abc/restart/config"

	f := newFake()
	// The sibling's button: same root, same node id, no state_topic.
	f.seed(sibling, []byte(`{"command_topic":"x/restart","unique_id":"u9"}`))
	f.seed(orphan, []byte(`{"state_topic":"x/h","unique_id":"u3"}`))

	r := New(f, Config{})
	if _, err := r.Publish(ctx, ours, []byte(`{"state_topic":"x/t"}`)); err != nil {
		t.Fatal(err)
	}
	// An entity this process published and has since dropped: the only kind
	// of orphan a claim list may clear.
	if _, err := r.Publish(ctx, orphan, []byte(`{"state_topic":"x/h","unique_id":"u3"}`)); err != nil {
		t.Fatal(err)
	}
	if err := r.Retract(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	f.seed(orphan, []byte(`{"state_topic":"x/h","unique_id":"u3"}`)) // the retraction did not stick

	res, err := r.Sweep(ctx, SweepRequest{SelfClaimed: true, Window: testWindow})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if !slices.Equal(res.Retracted, []string{orphan}) {
		t.Fatalf("retracted %v, want exactly %q", res.Retracted, orphan)
	}
	if !f.holds(sibling) {
		t.Fatal("the sibling instance's button was deleted from Home Assistant")
	}
	if !f.holds(ours) {
		t.Fatal("a live config of this process was retracted")
	}
	if slices.Contains(res.Owned, sibling) {
		t.Fatalf("a topic this process never published must not be owned: %v", res.Owned)
	}
}

// TestSelfClaimedSatisfiesTheUnscopedRefusal pins that the claim list is a
// first-class answer to [ErrSweepUnscoped] and not an addition to a predicate
// a consumer must still write.
func TestSelfClaimedSatisfiesTheUnscopedRefusal(t *testing.T) {
	t.Parallel()
	r := New(newFake(), Config{})
	if _, err := r.Sweep(context.Background(), SweepRequest{Window: testWindow}); !errors.Is(err, ErrSweepUnscoped) {
		t.Fatalf("a pass with neither rule must refuse, got %v", err)
	}
	if _, err := r.Sweep(context.Background(), SweepRequest{SelfClaimed: true, Window: testWindow}); err != nil {
		t.Fatalf("SelfClaimed alone must scope a pass: %v", err)
	}
}

// TestSelfClaimedAndOwnsBothHaveToAgree pins the conjunction: stating both can
// only narrow a pass, never widen one.
func TestSelfClaimedAndOwnsBothHaveToAgree(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mine := "homeassistant/sensor/ccu_abc/temperature/config"
	theirs := "homeassistant/sensor/zigbee_thing/x/config"

	f := newFake()
	f.seed(theirs, []byte(`{"state_topic":"z/x"}`))
	r := New(f, Config{})
	if _, err := r.Publish(ctx, mine, []byte(`{"state_topic":"x/t"}`)); err != nil {
		t.Fatal(err)
	}
	if err := r.Retract(ctx, mine); err != nil {
		t.Fatal(err)
	}
	f.seed(mine, []byte(`{"state_topic":"x/t"}`))

	// A predicate that declines our own node id leaves the claim list with
	// nothing to clear, even though the claim list alone would have cleared
	// it.
	res, err := r.Sweep(ctx, SweepRequest{
		SelfClaimed: true,
		Owns:        ownsNode("zigbee_"),
		Window:      testWindow,
	})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(res.Retracted) != 0 {
		t.Fatalf("the two rules must both agree, retracted %v", res.Retracted)
	}
	if !f.holds(theirs) {
		t.Fatal("another integration's config was cleared")
	}
}

// TestClaimedOnlyGrows pins the one fact the claim list rests on: having
// published a topic is not undone by retracting it, which is what lets a
// retraction the broker never applied be retried.
func TestClaimedOnlyGrows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	topic := "homeassistant/sensor/ccu_abc/temperature/config"

	f := newFake()
	r := New(f, Config{})
	if _, err := r.Publish(ctx, topic, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := r.Retract(ctx, topic); err != nil {
		t.Fatal(err)
	}
	if got := r.Declared(); len(got) != 0 {
		t.Fatalf("declared %v after a retraction, want none", got)
	}
	if got := r.Claimed(); !slices.Equal(got, []string{topic}) {
		t.Fatalf("claimed %v, want %q to survive the retraction", got, topic)
	}
	// A reconnect is about a broker, not about this process: what this
	// process has published stays true.
	r.Reset()
	if got := r.Claimed(); !slices.Equal(got, []string{topic}) {
		t.Fatalf("claimed %v after Reset, want it kept", got)
	}
}
