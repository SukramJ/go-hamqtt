// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

func testBundle() *discovery.Bundle {
	return &discovery.Bundle{
		NodeID: "ccu_abc",
		Device: discovery.DeviceInfo{Identifiers: []string{"ccu_abc"}, Name: "Thermostat"},
		Origin: discovery.Origin{Name: "go-hamqtt"},
		Components: map[string]discovery.Component{
			"temperature": {Platform: hacatalog.Platform("sensor"), UniqueID: "u1", StateTopic: "x/t"},
			"valve":       {Platform: hacatalog.Platform("number"), UniqueID: "u2", StateTopic: "x/v"},
		},
	}
}

// TestPublishDedup pins the gate the boot snapshot depends on: a
// byte-identical republish must not reach the broker at all, because a
// steady-state restart re-renders every entity and would otherwise write
// thousands of configs that change nothing.
func TestPublishDedup(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})

	sent, err := r.Publish(context.Background(), "homeassistant/sensor/n/o/config", []byte(`{"a":1}`))
	if err != nil || !sent {
		t.Fatalf("first publish: sent=%v err=%v", sent, err)
	}
	sent, err = r.Publish(context.Background(), "homeassistant/sensor/n/o/config", []byte(`{"a":1}`))
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if sent {
		t.Fatal("an unchanged payload must not be published again")
	}
	if n := f.count("publish"); n != 1 {
		t.Fatalf("want 1 broker write, got %d", n)
	}

	// A changed payload goes out. The dedup key is the topic, so the same
	// topic with different bytes is a new config and not a repeat.
	sent, err = r.Publish(context.Background(), "homeassistant/sensor/n/o/config", []byte(`{"a":2}`))
	if err != nil || !sent {
		t.Fatalf("changed publish: sent=%v err=%v", sent, err)
	}
	if n := f.count("publish"); n != 2 {
		t.Fatalf("want 2 broker writes, got %d", n)
	}
}

// TestPublishCachesOnlyWhatTheBrokerAccepted pins that a failed publish is
// not remembered. Caching an attempt would make the next identical payload
// hit the dedup gate, leaving the entity absent from Home Assistant until the
// operator restarts the consumer.
func TestPublishCachesOnlyWhatTheBrokerAccepted(t *testing.T) {
	t.Parallel()
	f := newFake()
	boom := errors.New("broker down")
	f.failPublish = func(string) error { return boom }
	r := New(f, Config{})

	if _, err := r.Publish(context.Background(), "t", []byte("p")); !errors.Is(err, boom) {
		t.Fatalf("want the broker error, got %v", err)
	}
	if got := r.Declared(); len(got) != 0 {
		t.Fatalf("a failed publish must not be declared, got %v", got)
	}
	// The in-flight claim is dropped too, or the sweep would spare a topic
	// carrying a previous build's config that this process failed to
	// overwrite.
	if r.claims("t") {
		t.Fatal("a failed publish must not leave a claim standing")
	}
}

// TestPublishBundleRetractsBeforePublishing is the measurement of
// 2026-09-10: Home Assistant refuses a device bundle while a per-entity
// config for the same unique id is still retained, with nothing but a log
// line to show for it. The retraction must therefore be on the wire before
// the bundle is.
func TestPublishBundleRetractsBeforePublishing(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	b := testBundle()

	if _, err := r.PublishBundle(context.Background(), b); err != nil {
		t.Fatalf("publish bundle: %v", err)
	}

	want := []string{
		"homeassistant/number/ccu_abc/valve/config",
		"homeassistant/sensor/ccu_abc/temperature/config",
		"homeassistant/device/ccu_abc/config",
	}
	if got := f.publishes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("wrong order\n got %v\nwant %v", got, want)
	}
	if got := f.retractions(); len(got) != 2 {
		t.Fatalf("want the two per-entity topics retracted, got %v", got)
	}
}

// TestPublishBundleRetractsSupersededOnce pins the one deliberate deviation
// from the reference implementation: after the first retraction the broker
// holds nothing at those topics, so retracting again on every change of the
// document is a message for nothing.
func TestPublishBundleRetractsSupersededOnce(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})

	b := testBundle()
	if _, err := r.PublishBundle(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	// Change the document so the dedup gate does not swallow the call.
	b.Components["valve"] = discovery.Component{Platform: hacatalog.Platform("number"), UniqueID: "u2", StateTopic: "x/v2"}
	sent, err := r.PublishBundle(context.Background(), b)
	if err != nil || !sent {
		t.Fatalf("changed bundle: sent=%v err=%v", sent, err)
	}
	if got := len(f.retractions()); got != 2 {
		t.Fatalf("want the 2 first-time retractions only, got %d", got)
	}

	// And an unchanged document does nothing at all.
	sent, err = r.PublishBundle(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if sent {
		t.Fatal("an unchanged bundle must not be republished")
	}
}

// TestPublishBundleAbortsOnFailedRetraction pins the ordering's failure
// mode. Between the retraction and the bundle the entity does not exist, so
// pressing on after a failed retraction risks the one outcome worse than not
// having started: the old config gone and the new one refused.
func TestPublishBundleAbortsOnFailedRetraction(t *testing.T) {
	t.Parallel()
	f := newFake()
	boom := errors.New("nope")
	f.failPublish = func(topic string) error {
		if topic == "homeassistant/sensor/ccu_abc/temperature/config" {
			return boom
		}
		return nil
	}
	r := New(f, Config{})

	if _, err := r.PublishBundle(context.Background(), testBundle()); !errors.Is(err, boom) {
		t.Fatalf("want the retraction error, got %v", err)
	}
	if slices.Contains(f.publishes(), "homeassistant/device/ccu_abc/config") {
		t.Fatal("the bundle must not be published after a failed retraction")
	}
}

// TestPublishComponentRetractsTheDocument is the symmetric measurement of
// 2026-09-11: a per-entity config published while the device document is
// still retained is refused the same way, so the rollback direction needs
// the same ordering.
func TestPublishComponentRetractsTheDocument(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})

	comp := discovery.Component{Platform: hacatalog.Platform("sensor"), UniqueID: "u1", StateTopic: "x/t"}
	if _, err := r.PublishComponent(context.Background(), "ccu_abc", "temperature", comp); err != nil {
		t.Fatalf("publish component: %v", err)
	}
	want := []string{
		"homeassistant/device/ccu_abc/config",
		"homeassistant/sensor/ccu_abc/temperature/config",
	}
	if got := f.publishes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("wrong order\n got %v\nwant %v", got, want)
	}

	// A second entity of the same device does not retract the document
	// again — it is already gone.
	if _, err := r.PublishComponent(context.Background(), "ccu_abc", "valve", comp); err != nil {
		t.Fatal(err)
	}
	if got := len(f.retractions()); got != 1 {
		t.Fatalf("want 1 document retraction, got %d", got)
	}
}

// TestPublishComponentRefusesIncompleteInput pins that the per-entity form
// cannot be addressed without its three segments and its platform: each
// missing one produces a topic Home Assistant silently ignores.
func TestPublishComponentRefusesIncompleteInput(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	comp := discovery.Component{Platform: hacatalog.Platform("sensor")}

	for _, tc := range []struct{ name, node, object string }{
		{"no node", "", "o"},
		{"no object", "n", ""},
	} {
		if _, err := r.PublishComponent(context.Background(), tc.node, tc.object, comp); err == nil {
			t.Fatalf("%s: want an error", tc.name)
		}
	}
	if _, err := r.PublishComponent(context.Background(), "n", "o", discovery.Component{}); err == nil {
		t.Fatal("want an error for a component with no platform")
	}
}

// TestRetractForgetsAndClears pins the difference between Retract and a
// zero-length Publish: Retract clears a topic whatever this process knows
// about it, which is what a consumer dropping an entity needs.
func TestRetractForgetsAndClears(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	ctx := context.Background()

	if _, err := r.Publish(ctx, "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := r.Retract(ctx, "a", "never-published"); err != nil {
		t.Fatal(err)
	}
	if got := r.Declared(); len(got) != 0 {
		t.Fatalf("want nothing declared, got %v", got)
	}
	if got := f.retractions(); !reflect.DeepEqual(got, []string{"a", "never-published"}) {
		t.Fatalf("both topics must be cleared, got %v", got)
	}

	// A zero-length Publish of an unknown topic is a no-op instead.
	sent, err := r.Publish(ctx, "unknown", nil)
	if err != nil || sent {
		t.Fatalf("want a silent no-op, got sent=%v err=%v", sent, err)
	}
}

// TestRetractIsBestEffort pins that one refused topic does not leave the
// rest of a removed device's entities standing in Home Assistant.
func TestRetractIsBestEffort(t *testing.T) {
	t.Parallel()
	f := newFake()
	boom := errors.New("refused")
	f.failPublish = func(topic string) error {
		if topic == "b" {
			return boom
		}
		return nil
	}
	r := New(f, Config{})

	err := r.Retract(context.Background(), "a", "b", "c")
	if !errors.Is(err, boom) {
		t.Fatalf("want the failure reported, got %v", err)
	}
	if got := f.retractions(); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("want the other two cleared anyway, got %v", got)
	}
}

// TestRepublishReplaysTheCachedBytes pins that the replay bypasses the dedup
// gate by construction — writing payloads the broker already holds is the
// entire point.
func TestRepublishReplaysTheCachedBytes(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	ctx := context.Background()

	for _, topic := range []string{"b", "a"} {
		if _, err := r.Publish(ctx, topic, []byte(topic)); err != nil {
			t.Fatal(err)
		}
	}
	n, err := r.Republish(ctx)
	if err != nil || n != 2 {
		t.Fatalf("republish: n=%d err=%v", n, err)
	}
	if got := f.publishes(); !reflect.DeepEqual(got, []string{"b", "a", "a", "b"}) {
		t.Fatalf("want the two originals then a sorted replay, got %v", got)
	}
}

// TestRepublishStopsOnCancellation pins that a shutdown mid-replay is a
// shutdown, not a per-topic failure for every remaining entity.
func TestRepublishStopsOnCancellation(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	if _, err := r.Publish(context.Background(), "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := r.Republish(ctx)
	if n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("want a cancelled replay, got n=%d err=%v", n, err)
	}
}

// TestSupersededTopicsIncludesTombstones pins that a component present only
// as a platform-only removal marker still contributes its per-entity topic:
// that retained config is exactly what has to go.
func TestSupersededTopicsIncludesTombstones(t *testing.T) {
	t.Parallel()
	b := testBundle()
	b.Remove(map[string]hacatalog.Platform{"gone": hacatalog.Platform("switch")}, "gone")
	b.Components["unknown"] = discovery.Component{} // no platform, not addressable

	want := []string{
		"homeassistant/number/ccu_abc/valve/config",
		"homeassistant/sensor/ccu_abc/temperature/config",
		"homeassistant/switch/ccu_abc/gone/config",
	}
	if got := SupersededTopics("", b); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if got := SupersededTopics("", nil); got != nil {
		t.Fatalf("a nil bundle supersedes nothing, got %v", got)
	}
}

// TestNewAppliesDefaults pins the zero Config: the Home Assistant prefix,
// QoS 1 (a retained config lost at QoS 0 may never be published again) and
// the two-second window.
func TestNewAppliesDefaults(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	if r.Prefix() != discovery.DefaultPrefix {
		t.Fatalf("prefix %q", r.Prefix())
	}
	if r.cfg.QoS != 1 || r.cfg.SweepWindow != DefaultSweepWindow {
		t.Fatalf("qos %d window %v", r.cfg.QoS, r.cfg.SweepWindow)
	}
	if _, err := r.Publish(context.Background(), "t", []byte("p")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ops[0].retain || f.ops[0].qos != 1 {
		t.Fatalf("a discovery config must go out retained at QoS 1, got %+v", f.ops[0])
	}
}

func TestNewPanicsWithoutTransport(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("want a panic naming the composition root")
		}
	}()
	New(nil, Config{})
}

func TestPublishRejectsEmptyTopicAndNilBundle(t *testing.T) {
	t.Parallel()
	r := New(newFake(), Config{})
	if _, err := r.Publish(context.Background(), "", []byte("p")); err == nil {
		t.Fatal("want an error for an empty topic")
	}
	if _, err := r.PublishBundle(context.Background(), nil); err == nil {
		t.Fatal("want an error for a nil bundle")
	}
}
