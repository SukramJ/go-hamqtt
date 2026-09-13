// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// What availability can express, pinned.
//
// One measured consumer publishes a two-level, list-form, `availability_mode:
// all` availability where the split is *per entity* — 128 of its configs gated
// on the bridge alone, 187 on the bridge and the object — and where the second
// level is the object's own state topic read through a `value_template`,
// rather than a dedicated availability topic. Both halves of that were open
// questions. These tests answer them against the model as it stands, so a
// design step has measured ground rather than an assumption.
//
// The answers: availability is per-entity vocabulary, not per-device, so the
// split needs no new type; `value_template` is on [AvailabilityEntry] and
// [StdContext] already emits one for [model.LevelSelf]; and a consumer whose
// entries must be spelled some other way overrides [Context.Availability],
// which is an interface method for exactly that reason.

func availEntity(key, addr string, av model.Availability) *model.Basic {
	return &model.Basic{
		EntityKey:      key,
		EntityPlatform: hacatalog.PlatformSensor,
		Description:    model.Description{Name: model.L(key), Availability: av},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S(addr, "", model.BucketValues, key),
			Mode: model.Read,
		}},
	}
}

// TestAvailabilityIsPerComponentNotPerDevice. Two entities of one device, one
// gated on the bridge alone and one on bridge-and-device, render two different
// `availability` lists in the same bundle — so a fleet whose entities disagree
// needs no per-entity escape hatch and no second bundle.
func TestAvailabilityIsPerComponentNotPerDevice(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	addr := dev.UID()
	bundle, err := discovery.Render(testContext(), dev, []model.Entity{
		availEntity("bridge_only", addr, model.BridgeOnly()),
		// The zero Availability resolves to {LevelBridge, LevelDevice}.
		availEntity("bridge_and_device", addr, model.Availability{}),
	}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	one := bundle.Components["bridge_only"].Availability
	two := bundle.Components["bridge_and_device"].Availability
	if len(one) != 1 {
		t.Errorf("bridge-only entity got %d availability entries, want 1: %+v", len(one), one)
	}
	if len(two) != 2 {
		t.Errorf("bridge+device entity got %d availability entries, want 2: %+v", len(two), two)
	}
	if len(one) > 0 && len(two) > 1 && one[0].Topic != two[0].Topic {
		t.Errorf("the shared bridge level rendered two different topics: %q vs %q", one[0].Topic, two[0].Topic)
	}
	// The mode is per component too, and `all` is the default both carry.
	for key, comp := range bundle.Components {
		if comp.AvailabilityMode != string(model.AvailabilityAll) {
			t.Errorf("%s: availability_mode = %q, want %q", key, comp.AvailabilityMode, model.AvailabilityAll)
		}
	}
}

// avTemplateContext is the override a consumer needs when its availability
// entries are spelled in its own vocabulary rather than [StdContext]'s: the
// second level points at the object's *state* topic and maps that object's
// own value words onto Home Assistant's with a template.
//
// [Context.Availability] is an interface method, so this is the sanctioned
// route and it needs nothing added to the model.
type avTemplateContext struct {
	discovery.StdContext
}

func (c avTemplateContext) Availability(dev *model.Device, e model.Entity) []discovery.AvailabilityEntry {
	entries := c.StdContext.Availability(dev, e)
	if len(entries) < 2 {
		return entries
	}
	b, ok := model.Bind(e, model.RoleState)
	if !ok {
		return entries
	}
	entries[1] = discovery.AvailabilityEntry{
		Topic:         c.StateTopic(b.Slot),
		ValueTemplate: "{{ 'online' if value == 'ONLINE' else 'offline' }}",
	}
	return entries
}

// TestAvailabilityEntryCarriesAValueTemplate pins that a `value_template` on
// an availability entry reaches the wire, both from [StdContext] itself (for
// [model.LevelSelf]) and from a consumer's own [Context.Availability].
func TestAvailabilityEntryCarriesAValueTemplate(t *testing.T) {
	t.Parallel()

	t.Run("StdContext emits one for LevelSelf", func(t *testing.T) {
		t.Parallel()
		ctx := selfContext(discovery.EnvelopeEncoding)
		entry := onlyEntry(t, ctx.Availability(testDevice(), selfEntity(testDevice().UID(), true)))
		if entry.ValueTemplate != discovery.SelfAvailabilityTemplate {
			t.Errorf("value_template = %q, want %q", entry.ValueTemplate, discovery.SelfAvailabilityTemplate)
		}
	})

	t.Run("a consumer's override reaches the payload", func(t *testing.T) {
		t.Parallel()
		dev := testDevice()
		ctx := avTemplateContext{StdContext: testContext().(discovery.StdContext)}
		comp, err := discovery.RenderComponent(ctx, dev, availEntity("state", dev.UID(), model.Availability{}),
			discovery.Origin{Name: "x"})
		if err != nil {
			t.Fatalf("RenderComponent: %v", err)
		}
		if len(comp.Availability) != 2 {
			t.Fatalf("want two availability entries, got %+v", comp.Availability)
		}
		got := comp.Availability[1]
		if got.ValueTemplate == "" {
			t.Fatal("the overridden entry lost its value_template")
		}
		raw, err := comp.EntityJSON()
		if err != nil {
			t.Fatalf("EntityJSON: %v", err)
		}
		if !strings.Contains(string(raw), `"value_template":"{{ 'online' if value == 'ONLINE' else 'offline' }}"`) {
			t.Errorf("the availability template is not in the bytes: %s", raw)
		}
	})
}
