// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

const testWindow = 20 * time.Millisecond

func ownsNode(prefix string) func(ConfigTopic) bool {
	return func(t ConfigTopic) bool { return strings.HasPrefix(t.NodeID, prefix) }
}

// TestSweepRecognisesBothFormsAndSparesWhatWasPublished is the sweep's whole
// contract in one pass: a leftover per-entity config and a leftover device
// document both go, an entity this process just published stays, and another
// integration's tree is never touched.
//
// Matching only the per-entity form is the measured defect this pins against:
// it made every device document invisible — never inspected, never cleared,
// retained forever by a broker no consumer would ever claim it from again.
func TestSweepRecognisesBothFormsAndSparesWhatWasPublished(t *testing.T) {
	t.Parallel()
	f := newFake()
	// The broker's retained tree at boot: two leftovers of ours in the two
	// forms, one config we are about to re-publish, and one belonging to a
	// parallel deployment.
	f.seed("homeassistant/sensor/ccu_old/temperature/config", []byte(`{"old":1}`))
	f.seed("homeassistant/device/ccu_gone/config", []byte(`{"old":2}`))
	f.seed("homeassistant/sensor/ccu_live/temperature/config", []byte(`{"live":0}`))
	f.seed("homeassistant/sensor/zigbee_thing/x/config", []byte(`{"theirs":1}`))
	// An already-cleared topic: retracting it again would be a message for
	// nothing.
	f.seed("homeassistant/sensor/ccu_empty/x/config", nil)

	r := New(f, Config{})
	ctx := context.Background()
	if _, err := r.Publish(ctx, "homeassistant/sensor/ccu_live/temperature/config", []byte(`{"live":1}`)); err != nil {
		t.Fatal(err)
	}

	res, err := r.Sweep(ctx, SweepRequest{Owns: ownsNode("ccu_"), Window: testWindow})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	slices.Sort(res.Retracted)
	want := []string{
		"homeassistant/device/ccu_gone/config",
		"homeassistant/sensor/ccu_old/temperature/config",
	}
	if !reflect.DeepEqual(res.Retracted, want) {
		t.Fatalf("retracted %v want %v", res.Retracted, want)
	}
	// Inspected counts every owned config the window delivered, including
	// the live one. The pair is what tells "the window saw nothing" apart
	// from "the window saw everything and nothing was orphaned".
	if res.Inspected != 3 {
		t.Fatalf("inspected %d want 3", res.Inspected)
	}
	if !slices.Contains(r.Declared(), "homeassistant/sensor/ccu_live/temperature/config") {
		t.Fatal("the live config must survive the sweep")
	}
	f.mu.Lock()
	_, theirs := f.retained["homeassistant/sensor/zigbee_thing/x/config"]
	f.mu.Unlock()
	if !theirs {
		t.Fatal("another integration's retained config must not be touched")
	}
}

// TestSweepRefusesWithoutAnOwnershipPredicate pins the safety property: a
// discovery prefix is shared, and a sweep that guessed at ownership would
// clear every other integration's entities on the broker.
func TestSweepRefusesWithoutAnOwnershipPredicate(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	if _, err := r.Sweep(context.Background(), SweepRequest{}); !errors.Is(err, ErrSweepUnscoped) {
		t.Fatalf("want ErrSweepUnscoped, got %v", err)
	}
	if f.count("subscribe") != 0 {
		t.Fatal("an unscoped sweep must not even subscribe")
	}
}

// TestSweepTakesItsSubscriptionDown pins the teardown. A one-shot boot pass
// over `<prefix>/#` whose handler closes over a worklist keeps running on
// the consumer's own publishes for the rest of the process if the
// subscription is left installed — and a client that replays subscriptions
// on reconnect carries it across the very broker restart that stranded it.
func TestSweepTakesItsSubscriptionDown(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	if _, err := r.Sweep(context.Background(), SweepRequest{Owns: func(ConfigTopic) bool { return true }, Window: testWindow}); err != nil {
		t.Fatal(err)
	}
	if f.count("unsubscribe") != 1 {
		t.Fatalf("want exactly one unsubscribe, got %d", f.count("unsubscribe"))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.subs) != 0 {
		t.Fatalf("subscription left installed: %v", f.subs)
	}
}

// TestSweepReportsASubscribeFailure pins that a sweep which could not look
// says so, rather than reporting an empty result that reads like a clean
// broker.
func TestSweepReportsASubscribeFailure(t *testing.T) {
	t.Parallel()
	f := newFake()
	boom := errors.New("refused")
	f.failSubscribe = boom
	r := New(f, Config{})
	if _, err := r.Sweep(context.Background(), SweepRequest{Owns: func(ConfigTopic) bool { return true }, Window: testWindow}); !errors.Is(err, boom) {
		t.Fatalf("want the subscribe error, got %v", err)
	}
}

// TestSweepHonoursACancelledContext pins that a shutdown during a boot pass
// stops it instead of running out the window.
func TestSweepHonoursACancelledContext(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Sweep(ctx, SweepRequest{Owns: func(ConfigTopic) bool { return true }, Window: time.Hour}); err == nil {
		t.Fatal("want an error for a cancelled sweep")
	}
}

// TestSweepTrimsTheWindowToTheBudget pins that a caller whose deadline is
// the window plus a small margin gets a shorter window rather than a pass
// that ends in DeadlineExceeded having cleared nothing.
func TestSweepTrimsTheWindowToTheBudget(t *testing.T) {
	t.Parallel()
	f := newFake()
	f.seed("homeassistant/device/ccu_gone/config", []byte(`{"old":1}`))
	r := New(f, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	res, err := r.Sweep(ctx, SweepRequest{Owns: ownsNode("ccu_"), Window: time.Hour})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(res.Retracted) != 1 {
		t.Fatalf("want the orphan cleared inside the budget, got %v", res.Retracted)
	}
}

// TestSweepSerialisesWindows pins the lock the snapshot slot exists for: two
// windows on the same filter leave the second handler installed over the
// first and the first teardown unsubscribes for both, after which neither
// sees anything.
func TestSweepSerialisesWindows(t *testing.T) {
	t.Parallel()
	f := newFake()
	f.seed("homeassistant/device/ccu_gone/config", []byte(`{"old":1}`))
	r := New(f, Config{})

	type outcome struct {
		res SweepResult
		err error
	}
	results := make(chan outcome, 2)
	for range 2 {
		go func() {
			res, err := r.Sweep(context.Background(), SweepRequest{Owns: ownsNode("ccu_"), Window: testWindow})
			results <- outcome{res, err}
		}()
	}
	cleared := 0
	for range 2 {
		got := <-results
		if got.err != nil {
			t.Errorf("sweep: %v", got.err)
		}
		cleared += len(got.res.Retracted)
	}
	// Exactly one of the two clears it: the second window finds it already
	// gone, because the fake honours the retraction.
	if cleared != 1 {
		t.Fatalf("want the orphan cleared once, got %d", cleared)
	}
	if f.count("subscribe") != 2 || f.count("unsubscribe") != 2 {
		t.Fatalf("want two complete windows, got %d/%d", f.count("subscribe"), f.count("unsubscribe"))
	}
}

// TestSweepRecheckesClaimsBeforeRetracting pins the second claim check.
// Clearing thousands of topics takes seconds, and a publisher that claimed
// one of them in the meantime has made it live again — retracting it would
// delete an entity that exists. The claim is injected at the teardown, which
// is exactly the moment between the window closing and the retractions
// starting.
func TestSweepRecheckesClaimsBeforeRetracting(t *testing.T) {
	t.Parallel()
	f := newFake()
	orphan := "homeassistant/sensor/ccu_x/late/config"
	f.seed(orphan, []byte(`{"old":1}`))
	r := New(f, Config{})
	f.onUnsubscribe = func() {
		if _, err := r.Publish(context.Background(), orphan, []byte(`{"new":1}`)); err != nil {
			t.Errorf("late publish: %v", err)
		}
	}

	res, err := r.Sweep(context.Background(), SweepRequest{Owns: ownsNode("ccu_"), Window: testWindow})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Retracted) != 0 {
		t.Fatalf("a topic claimed mid-sweep must be spared, got %v", res.Retracted)
	}
	if !slices.Contains(r.Declared(), orphan) {
		t.Fatal("the late publish must still be declared after the sweep")
	}
}

// TestSweepReportsOnlyWhatItCleared covers the branch a review found wholly
// untested: a retraction the broker refuses.
//
// The distinction matters because the result is what a consumer acts on. A
// sweep that reports a topic as retracted when the broker still holds it
// tells the consumer the ghost is gone, so nothing retries and nothing
// escalates — and the entity stays in Home Assistant, permanently available,
// showing the last value it ever saw. The pass must therefore continue past
// the refusal (one unreachable topic must not abandon the rest) while
// reporting only the topics it actually cleared, and must keep claiming the
// refused one so a later publish is not dedup-suppressed against a payload
// that was never written.
func TestSweepReportsOnlyWhatItCleared(t *testing.T) {
	t.Parallel()
	f := newFake()
	stubborn := "homeassistant/sensor/ccu_x/stubborn/config"
	willing := "homeassistant/sensor/ccu_x/willing/config"
	f.seed(stubborn, []byte(`{"old":1}`))
	f.seed(willing, []byte(`{"old":2}`))
	f.failPublish = func(topic string) error {
		if topic == stubborn {
			return errors.New("broker refused")
		}
		return nil
	}
	r := New(f, Config{})

	res, err := r.Sweep(context.Background(), SweepRequest{
		Owns: ownsNode("ccu_"), Window: testWindow,
	})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Inspected != 2 {
		t.Errorf("inspected = %d, want both configs seen", res.Inspected)
	}
	if slices.Contains(res.Retracted, stubborn) {
		t.Error("the sweep reported a topic the broker refused to clear")
	}
	if !slices.Contains(res.Retracted, willing) {
		t.Error("one refusal abandoned the rest of the pass")
	}
	if !f.holds(stubborn) {
		t.Error("the refused topic is no longer retained, so the refusal was not real")
	}
}

// TestSweepReportOnlyLooksWithoutTouching is the measured need of
// [SweepRequest.ReportOnly]: openccu-loom PR #797 had to give up on this pass
// because looking and clearing were one act. Its one-off scrub must run
// before the first snapshot, when the claim set is still empty — so the
// retracting pass would have deleted the entire retained discovery fleet.
//
// The assertion that matters is the negative one: not a single message leaves
// the process, and the broker still holds every topic afterwards.
func TestSweepReportOnlyLooksWithoutTouching(t *testing.T) {
	t.Parallel()
	f := newFake()
	f.seed("homeassistant/sensor/ccu_old/temperature/config", []byte(`{"old":1}`))
	f.seed("homeassistant/device/ccu_gone/config", []byte(`{"old":2}`))
	f.seed("homeassistant/sensor/zigbee_thing/x/config", []byte(`{"theirs":1}`))

	// The claim set is empty, exactly as it is before the first snapshot —
	// which is what makes the retracting pass unusable there.
	r := New(f, Config{})
	res, err := r.Sweep(context.Background(), SweepRequest{
		Owns:       ownsNode("ccu_"),
		Window:     testWindow,
		ReportOnly: true,
	})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(res.Retracted) != 0 {
		t.Fatalf("retracted %v — a report-only pass must clear nothing", res.Retracted)
	}
	if n := f.count("publish"); n != 0 {
		t.Fatalf("%d publishes — a report-only pass must not write at all", n)
	}
	for _, topic := range []string{
		"homeassistant/sensor/ccu_old/temperature/config",
		"homeassistant/device/ccu_gone/config",
		"homeassistant/sensor/zigbee_thing/x/config",
	} {
		if !f.holds(topic) {
			t.Fatalf("%s was cleared — the whole point is that it survives", topic)
		}
	}

	// And it reports what it judged, which is what lets the caller retract
	// a list of its own: the two owned topics, not the third writer's.
	owned := append([]string(nil), res.Owned...)
	slices.Sort(owned)
	want := []string{
		"homeassistant/device/ccu_gone/config",
		"homeassistant/sensor/ccu_old/temperature/config",
	}
	if !reflect.DeepEqual(owned, want) {
		t.Fatalf("owned %v want %v", owned, want)
	}
	if res.Inspected != 2 {
		t.Fatalf("inspected %d want 2", res.Inspected)
	}
}

// TestSweepReportsOwnedOnARetractingPassToo pins that Owned is not a
// report-only field: on an ordinary pass, Owned minus Retracted is what this
// process still claims, and a result that reported only the retractions could
// not tell a window that saw nothing from one that found nothing to do.
func TestSweepReportsOwnedOnARetractingPassToo(t *testing.T) {
	t.Parallel()
	f := newFake()
	f.seed("homeassistant/sensor/ccu_old/temperature/config", []byte(`{"old":1}`))
	f.seed("homeassistant/sensor/ccu_live/temperature/config", []byte(`{"live":0}`))

	r := New(f, Config{})
	ctx := context.Background()
	if _, err := r.Publish(ctx, "homeassistant/sensor/ccu_live/temperature/config", []byte(`{"live":1}`)); err != nil {
		t.Fatal(err)
	}
	res, err := r.Sweep(ctx, SweepRequest{Owns: ownsNode("ccu_"), Window: testWindow})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	owned := append([]string(nil), res.Owned...)
	slices.Sort(owned)
	want := []string{
		"homeassistant/sensor/ccu_live/temperature/config",
		"homeassistant/sensor/ccu_old/temperature/config",
	}
	if !reflect.DeepEqual(owned, want) {
		t.Fatalf("owned %v want %v — the claimed config must be reported as seen, not only spared", owned, want)
	}
	if len(res.Retracted) != 1 {
		t.Fatalf("retracted %v want just the orphan", res.Retracted)
	}
}
