// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"reflect"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// TestSupersededTopicsHonoursAStatedLegacyForm is the measured need of
// [LegacyTopicFunc]: go-zendure2mqtt's 29 retained configs are keyed
// `<prefix>/<platform>/<unique_id>/config`, with no node-id level, so the
// five-segment default retracted none of them — the device bundle would have
// been published while every per-entity config was still retained, which Home
// Assistant refuses with one `WARNING` line and no entities.
func TestSupersededTopicsHonoursAStatedLegacyForm(t *testing.T) {
	t.Parallel()

	b := testBundle()
	b.Remove(map[string]hacatalog.Platform{"gone": hacatalog.Platform("switch")}, "gone")

	t.Run("by unique id", func(t *testing.T) {
		t.Parallel()
		want := []string{
			"homeassistant/number/u2/config",
			"homeassistant/sensor/u1/config",
		}
		got := SupersededTopics("", b, LegacyTopicByUniqueID)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v — the node-id-less fleet is not retracted", got, want)
		}
	})

	t.Run("by object id", func(t *testing.T) {
		t.Parallel()
		want := []string{
			"homeassistant/number/valve/config",
			"homeassistant/sensor/temperature/config",
			"homeassistant/switch/gone/config",
		}
		got := SupersededTopics("", b, LegacyTopicByObjectID)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})

	t.Run("a fleet on two forms names both, de-duplicated", func(t *testing.T) {
		t.Parallel()
		want := []string{
			"homeassistant/number/ccu_abc/valve/config",
			"homeassistant/number/u2/config",
			"homeassistant/sensor/ccu_abc/temperature/config",
			"homeassistant/sensor/u1/config",
			"homeassistant/switch/ccu_abc/gone/config",
		}
		got := SupersededTopics("", b,
			LegacyTopicWithNodeID, LegacyTopicByUniqueID, LegacyTopicWithNodeID)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})

	t.Run("saying nothing is exactly today", func(t *testing.T) {
		t.Parallel()
		if got, want := SupersededTopics("", b), SupersededTopics("", b, LegacyTopicWithNodeID); !reflect.DeepEqual(got, want) {
			t.Fatalf("default %v differs from the stated five-segment form %v", got, want)
		}
	})
}

// TestLegacyFormsSkipWhatTheyCannotKey pins that a component with nothing to
// key on yields no topic rather than one with a blank segment — a topic built
// from a blank segment belongs to nobody and retracting it is a write into
// somebody else's tree.
func TestLegacyFormsSkipWhatTheyCannotKey(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		form LegacyTopicFunc
		e    LegacyEntity
	}{
		"unique id form, no unique id": {LegacyTopicByUniqueID, LegacyEntity{Platform: "sensor"}},
		"unique id form, no platform":  {LegacyTopicByUniqueID, LegacyEntity{UniqueID: "u"}},
		"object id form, no object id": {LegacyTopicByObjectID, LegacyEntity{Platform: "sensor"}},
		"node id form, no node id":     {LegacyTopicWithNodeID, LegacyEntity{Platform: "sensor", ObjectID: "o"}},
		"node id form, no object id":   {LegacyTopicWithNodeID, LegacyEntity{Platform: "sensor", NodeID: "n"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := tc.form(tc.e); got != "" {
				t.Fatalf("got %q, want no topic at all", got)
			}
		})
	}

	// And a bundle whose components cannot be keyed contributes nothing,
	// rather than a list of topics ending in `//config`.
	b := &discovery.Bundle{
		NodeID:     "n",
		Components: map[string]discovery.Component{"a": {Platform: hacatalog.Platform("sensor")}},
	}
	if got := SupersededTopics("", b, LegacyTopicByUniqueID); len(got) != 0 {
		t.Fatalf("got %v, want nothing — no unique id is nothing to retract", got)
	}
	if got := SupersededTopics("", b, nil); len(got) != 0 {
		t.Fatalf("got %v, want nothing — a nil form renders nothing", got)
	}
}

// TestPublishBundleRetractsTheStatedLegacyForm is the whole point reaching
// the broker: the retraction of the *consumer's* topics has to happen, and it
// has to happen before the bundle. Home Assistant's refusal is silent in the
// other order — measured on a live 2026.9 instance on 2026-09-10.
func TestPublishBundleRetractsTheStatedLegacyForm(t *testing.T) {
	t.Parallel()

	f := newFake()
	r := New(f, Config{
		StatusTopic:        "b/bridge/status",
		LegacyEntityTopics: []LegacyTopicFunc{LegacyTopicByUniqueID},
	})
	// The fleet as the broker actually holds it: four segments, no node id.
	f.seed("homeassistant/sensor/u1/config", []byte(`{"unique_id":"u1"}`))
	f.seed("homeassistant/number/u2/config", []byte(`{"unique_id":"u2"}`))

	if _, err := r.PublishBundle(context.Background(), testBundle()); err != nil {
		t.Fatalf("publish bundle: %v", err)
	}

	for _, legacy := range []string{
		"homeassistant/sensor/u1/config",
		"homeassistant/number/u2/config",
	} {
		if f.holds(legacy) {
			t.Fatalf("%s is still retained — Home Assistant refuses the bundle and logs one WARNING", legacy)
		}
	}

	publishes := f.publishes()
	bundleAt := -1
	for i, topic := range publishes {
		if topic == "homeassistant/device/ccu_abc/config" {
			bundleAt = i
		}
	}
	if bundleAt < 2 {
		t.Fatalf("publishes %v — the two legacy retractions must both precede the bundle", publishes)
	}
}
