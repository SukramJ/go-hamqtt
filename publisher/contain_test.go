// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// containBundle is a device of two entities, one of which Home Assistant
// refuses: a sensor with `entity_category: config` cannot be added
// (sensor/__init__.py:305-313 in core 2026.10).
func containBundle() *discovery.Bundle {
	return &discovery.Bundle{
		NodeID: "ccu_dev1",
		Device: discovery.DeviceInfo{Identifiers: []string{"ccu:dev1"}},
		Origin: discovery.Origin{Name: "test"},
		Components: map[string]discovery.Component{
			"temp": {
				Platform: hacatalog.PlatformSensor, UniqueID: "ccu_dev1_temp", StateTopic: "ccu/dev1/temp",
			},
			"bad": {
				Platform: hacatalog.PlatformSensor, UniqueID: "ccu_dev1_bad", StateTopic: "ccu/dev1/bad",
				EntityCategory: hacatalog.EntityCategoryConfig,
			},
		},
	}
}

func publishedComponents(t *testing.T, f *fakeTransport, topic string) map[string]any {
	t.Helper()
	f.mu.Lock()
	raw := f.retained[topic]
	f.mu.Unlock()
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("retained %s: %v", topic, err)
	}
	return doc["components"].(map[string]any)
}

// TestPublishBundleContainsOnlyWhenAsked: the default publishes the bundle's
// bytes exactly as before; Config.Contain publishes the contained document
// and reports what it withheld.
func TestPublishBundleContainsOnlyWhenAsked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	topic := BundleConfigTopic("", "ccu_dev1")

	off := newFake()
	if _, err := New(off, Config{}).PublishBundle(ctx, containBundle()); err != nil {
		t.Fatal(err)
	}
	if _, present := publishedComponents(t, off, topic)["bad"]; !present {
		t.Fatal("containment ran without Config.Contain")
	}

	on := newFake()
	var reported *discovery.Containment
	in := containBundle()
	r := New(on, Config{
		Contain:     &discovery.ContainOptions{},
		OnContained: func(nodeID string, c *discovery.Containment) { reported = c },
	})
	if _, err := r.PublishBundle(ctx, in); err != nil {
		t.Fatal(err)
	}
	comps := publishedComponents(t, on, topic)
	if _, present := comps["bad"]; present {
		t.Fatalf("bad was published: %v", comps["bad"])
	}
	if _, present := comps["temp"]; !present {
		t.Fatal("temp was withheld with it")
	}
	if reported == nil || len(reported.Withheld) != 1 {
		t.Fatalf("OnContained = %+v", reported)
	}
	if _, still := in.Components["bad"]; !still {
		t.Error("PublishBundle modified the caller's bundle")
	}
}

// TestPublishBundleRefusesAnUnpublishableDocument: a document error no
// component can cure publishes nothing — not even the supersede
// retractions, which would otherwise strand the entities they clear.
func TestPublishBundleRefusesAnUnpublishableDocument(t *testing.T) {
	t.Parallel()
	f := newFake()
	b := containBundle()
	b.Device.Identifiers = nil
	_, err := New(f, Config{Contain: &discovery.ContainOptions{}}).PublishBundle(context.Background(), b)
	if !errors.Is(err, discovery.ErrUnpublishable) {
		t.Fatalf("err = %v", err)
	}
	if n := f.count("publish"); n != 0 {
		t.Fatalf("%d publishes for an unpublishable document: %v", n, f.publishes())
	}
}

// TestTheSweepSparesAWithheldComponentsOldConfig is the orphan-sweep half of
// the tombstone trap. During a migration from the per-entity form, the
// withheld component's old per-entity config is what still describes its
// entity; PublishBundle does not retract it (the document leaves the
// component out), and a sweep that judged it an orphan would delete the
// entity with its registry entry. Once the component is published again, the
// supersede step retracts it as usual and the runtime stops keeping it.
func TestTheSweepSparesAWithheldComponentsOldConfig(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFake()
	oldBad := EntityConfigTopic("", "sensor", "ccu_dev1", "bad")
	oldTemp := EntityConfigTopic("", "sensor", "ccu_dev1", "temp")
	f.seed(oldBad, []byte(`{"old":"bad"}`))
	f.seed(oldTemp, []byte(`{"old":"temp"}`))

	r := New(f, Config{Contain: &discovery.ContainOptions{}})
	if _, err := r.PublishBundle(ctx, containBundle()); err != nil {
		t.Fatal(err)
	}
	if f.holds(oldTemp) {
		t.Fatal("the published component's per-entity config was not superseded")
	}
	if !f.holds(oldBad) {
		t.Fatal("the withheld component's per-entity config was retracted by the publish")
	}
	if got := r.Kept(); !slices.Equal(got, []string{oldBad}) {
		t.Fatalf("Kept = %v, want [%s]", got, oldBad)
	}

	res, err := r.Sweep(ctx, SweepRequest{Owns: ownsNode("ccu_"), Window: testWindow})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(res.Retracted, oldBad) || !f.holds(oldBad) {
		t.Fatalf("the sweep retracted the withheld component's config: %v", res.Retracted)
	}

	// Fixed: published again, its old config is superseded and no longer kept.
	fixed := containBundle()
	setComp(fixed, "bad", func(c *discovery.Component) { c.EntityCategory = "" })
	if _, err := r.PublishBundle(ctx, fixed); err != nil {
		t.Fatal(err)
	}
	if f.holds(oldBad) {
		t.Fatal("the repaired component's per-entity config was not superseded")
	}
	if got := r.Kept(); len(got) != 0 {
		t.Fatalf("Kept = %v after the component was published", got)
	}
}

// TestWithheldTopicsOfAnUncontainedBundle: nothing withheld, nothing kept.
func TestWithheldTopicsOfAnUncontainedBundle(t *testing.T) {
	t.Parallel()
	if got := WithheldTopics("", containBundle()); got != nil {
		t.Fatalf("WithheldTopics = %v", got)
	}
	if got := WithheldTopics("", nil); got != nil {
		t.Fatalf("WithheldTopics(nil) = %v", got)
	}
	c := discovery.Contain(containBundle(), discovery.ContainOptions{})
	got := WithheldTopics("", c.Bundle, LegacyTopicWithNodeID, LegacyTopicByUniqueID)
	want := []string{"homeassistant/sensor/ccu_dev1/bad/config", "homeassistant/sensor/ccu_dev1_bad/config"}
	if !slices.Equal(got, want) {
		t.Fatalf("WithheldTopics = %v, want %v", got, want)
	}
}

func setComp(b *discovery.Bundle, key string, edit func(c *discovery.Component)) {
	c := b.Components[key]
	edit(&c)
	b.Components[key] = c
}
