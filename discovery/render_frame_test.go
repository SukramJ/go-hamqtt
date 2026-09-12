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

// frameEntity is the entity the combined-projection plane renders its frame
// from: it describes nothing at all, because every display key of a combined
// entity is the projection's.
func frameEntity() *model.Basic {
	e := phase3Entity()
	e.Description = model.Description{}
	return e
}

// TestRenderFrameFillsOnlyWhatTheComponentLeavesUnset. The measured need: the
// combined-projection plane receives a finished component built outside the
// model and needs the model to supply only the frame — device, origin, state
// topic, availability, unique id. It rendered that frame and gap-filled by
// hand, six `if comp.X == ""` lines, with nothing to catch the seventh key.
func TestRenderFrameFillsOnlyWhatTheComponentLeavesUnset(t *testing.T) {
	t.Parallel()

	projection := discovery.Component{
		Platform:      hacatalog.PlatformSensor,
		Name:          "Assembled",
		StateTopic:    "gh/own/document",
		ValueTemplate: `{{ value_json.assembled }}`,
	}
	comp, err := discovery.RenderFrame(phase3Context(), phase3Device(), frameEntity(),
		discovery.Origin{Name: "openccu-loom"}, projection)
	if err != nil {
		t.Fatalf("RenderFrame: %v", err)
	}

	// The projection's keys survive verbatim.
	if comp.StateTopic != "gh/own/document" {
		t.Errorf("state_topic = %q; the frame overwrote the projection's own", comp.StateTopic)
	}
	if comp.Name != "Assembled" || comp.ValueTemplate != `{{ value_json.assembled }}` {
		t.Errorf("the projection's display keys were overwritten: %+v", comp)
	}

	// The frame filled the gaps.
	if comp.UniqueID == "" {
		t.Error("unique_id was not filled from the frame")
	}
	if len(comp.Availability) == 0 || comp.AvailabilityMode == "" {
		t.Error("the availability list and mode were not filled from the frame")
	}
	if comp.Device == nil || comp.Origin == nil {
		t.Error("the device and origin blocks were not filled from the frame")
	}
}

// TestRenderFrameCoversEveryKeyOfComponent. The reason the fill is reflective:
// a hand-written gap-fill is a list to forget a key from, and this is the test
// that would have caught the forgotten one. Every settable key of a rendered
// frame reaches an empty component.
func TestRenderFrameCoversEveryKeyOfComponent(t *testing.T) {
	t.Parallel()

	ctx := phase3Context()
	dev := phase3Device()
	origin := discovery.Origin{Name: "openccu-loom"}

	frame, err := discovery.RenderComponent(ctx, dev, frameEntity(), origin)
	if err != nil {
		t.Fatalf("RenderComponent: %v", err)
	}
	filled, err := discovery.RenderFrame(ctx, dev, frameEntity(), origin, discovery.Component{})
	if err != nil {
		t.Fatalf("RenderFrame: %v", err)
	}
	if !reflect.DeepEqual(frame, filled) {
		t.Errorf("an empty component did not receive the whole frame:\nframe  %+v\nfilled %+v", frame, filled)
	}
}

// TestRenderFrameKeepsAnExplicitlyEmptyList. A nil slice is "no opinion"; an
// empty one is the statement that this entity has no availability, the same
// distinction [model.NoAvailability] draws in a description. Filling it from
// the frame would re-gate an entity whose whole job is to report that the
// thing an availability topic points at is gone.
func TestRenderFrameKeepsAnExplicitlyEmptyList(t *testing.T) {
	t.Parallel()

	projection := discovery.Component{
		Platform:     hacatalog.PlatformSensor,
		Availability: []discovery.AvailabilityEntry{},
	}
	comp, err := discovery.RenderFrame(phase3Context(), phase3Device(), frameEntity(),
		discovery.Origin{Name: "openccu-loom"}, projection)
	if err != nil {
		t.Fatalf("RenderFrame: %v", err)
	}
	if len(comp.Availability) != 0 {
		t.Errorf("an explicitly empty availability list was filled in: %+v", comp.Availability)
	}
}

// TestRenderFrameMergesExtraKeyByKey. Extra is the one field that is not
// all-or-nothing: a single key set by the projection must not lose the frame's
// other keys, and the projection's own key must still win.
func TestRenderFrameMergesExtraKeyByKey(t *testing.T) {
	t.Parallel()

	e := frameEntity()
	e.Description.Extra = map[string]any{"frame_only": 1, "shared": "frame"}
	own := map[string]any{"shared": "projection"}
	projection := discovery.Component{Platform: hacatalog.PlatformSensor, Extra: own}

	comp, err := discovery.RenderFrame(phase3Context(), phase3Device(), e,
		discovery.Origin{Name: "openccu-loom"}, projection)
	if err != nil {
		t.Fatalf("RenderFrame: %v", err)
	}
	if comp.Extra["shared"] != "projection" {
		t.Errorf("shared = %#v, want the projection's value", comp.Extra["shared"])
	}
	if comp.Extra["frame_only"] != 1 {
		t.Errorf("the frame's own Extra key was lost: %#v", comp.Extra)
	}
	// The caller's map is untouched: it is shared, and a consumer reuses it
	// for the next entity.
	if len(own) != 1 {
		t.Errorf("the caller's Extra map was written into: %#v", own)
	}
}

// TestRenderFrameRefusesADeviceWithNoIdentity. Same contract as
// [discovery.RenderComponent], since it is what renders the frame: a device
// Home Assistant would refuse must not reach the broker as a payload that
// looks complete.
func TestRenderFrameRefusesADeviceWithNoIdentity(t *testing.T) {
	t.Parallel()

	if _, err := discovery.RenderFrame(phase3Context(), &model.Device{}, frameEntity(),
		discovery.Origin{Name: "x"}, discovery.Component{}); err == nil {
		t.Error("a device with no identity was accepted")
	}
}
