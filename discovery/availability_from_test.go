// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"reflect"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// TestAvailabilityFromResolvesTheDeviceLevelFromTheBinding is the case two
// consumers overrode Context.Availability for: their item tree keys the
// device's `online` item on the bare identifier the binding carries
// ("uuid-1"), not on the device UID StdContext derives from the identity.
// Everything else — the bridge entry, the vocabulary — stays StdContext's.
func TestAvailabilityFromResolvesTheDeviceLevelFromTheBinding(t *testing.T) {
	t.Parallel()
	ctx := smartHomeContext(t)
	dev := testDevice()
	e := shEntity(hacatalog.PlatformSensor, "temp", model.Read, nil)

	std := ctx.Availability(dev, e)
	got := ctx.AvailabilityFrom(dev, e, discovery.BindingSlot)
	if len(std) != 2 || len(got) != 2 {
		t.Fatalf("entries: std %v, from %v", std, got)
	}
	if got[0] != std[0] {
		t.Errorf("bridge entry changed: %+v, want %+v", got[0], std[0])
	}
	want := discovery.OnlineAvailability(ctx.Layout.Availability(e.Bindings()[0].Slot), discovery.StatusObjectEncoding)
	if got[1] != want {
		t.Errorf("device entry = %+v, want %+v", got[1], want)
	}
	if got[1].Topic == std[1].Topic {
		t.Errorf("the device entry still addresses the device UID: %s", got[1].Topic)
	}

	if nilResolver := ctx.AvailabilityFrom(dev, e, nil); !reflect.DeepEqual(nilResolver, std) {
		t.Errorf("a nil resolver = %v, want Availability's %v", nilResolver, std)
	}
}

// TestAvailabilityFromSkipsWhatItCannotResolve: no binding, no device level;
// a slot the layout renders as no topic is skipped rather than emitted with
// an empty topic Validate would then refuse.
func TestAvailabilityFromSkipsWhatItCannotResolve(t *testing.T) {
	t.Parallel()
	ctx := smartHomeContext(t)
	dev := testDevice()
	dev.Via = &model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "GW"}}}
	e := shEntity(hacatalog.PlatformSensor, "temp", model.Read, nil)
	e.Description.Availability = model.Availability{Levels: []model.AvailabilityLevel{model.LevelBridge, model.LevelDevice, model.LevelParent}}

	unbound := &model.Basic{EntityKey: "x", EntityPlatform: hacatalog.PlatformSensor, Description: e.Description}
	if got := ctx.AvailabilityFrom(dev, unbound, discovery.BindingSlot); len(got) != 1 {
		t.Errorf("an unbound entity rendered %v, want the bridge entry alone", got)
	}

	empty := func(*model.Device, model.Entity) (model.Slot, bool) { return model.Slot{}, true }
	parent := ctx.Availability(dev, e)
	if len(parent) != 3 {
		t.Fatalf("Availability rendered %d entries, want bridge, device and parent", len(parent))
	}
	got := ctx.AvailabilityFrom(dev, e, empty)
	if len(got) != 2 || got[1].Topic == "" {
		t.Errorf("AvailabilityFrom with an address-less slot = %v, want bridge and parent", got)
	}
}

// TestBindingSlotOfNothing: a nil entity has no binding.
func TestBindingSlotOfNothing(t *testing.T) {
	t.Parallel()
	if _, ok := discovery.BindingSlot(nil, nil); ok {
		t.Error("BindingSlot(nil) resolved")
	}
}

// TestStatusAttributesTemplate pins the spelling two consumers each defined
// locally before it was exported.
func TestStatusAttributesTemplate(t *testing.T) {
	t.Parallel()
	if discovery.StatusAttributesTemplate != "{{ value_json.val | tojson }}" {
		t.Errorf("StatusAttributesTemplate = %q", discovery.StatusAttributesTemplate)
	}
}
