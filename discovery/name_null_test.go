// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// nameNullEntity is one entity on the given platform, with the description the
// test wants to see projected.
func nameNullEntity(platform hacatalog.Platform, desc model.Description) *model.Basic {
	e := phase3Entity()
	e.EntityPlatform = platform
	e.Description = desc
	return e
}

// nameNullBody renders one entity and returns the JSON object Home Assistant
// would receive.
func nameNullBody(t *testing.T, e model.Entity) map[string]any {
	t.Helper()
	comp, err := discovery.RenderComponent(phase3Context(), phase3Device(), e,
		discovery.Origin{Name: "openccu-loom"})
	if err != nil {
		t.Fatalf("RenderComponent: %v", err)
	}
	return phase3Body(t, comp)
}

// TestNameNullPublishesTheNullAndNotAnEmptyName. The measured need: three
// discovery planes set [discovery.Component.NameNull] after rendering, two of
// them by re-deriving it from the component they had just been handed, because
// the description could not say it. `name: null` tells Home Assistant to show
// the device's name alone; an absent key makes it derive one from the platform.
// This pins that a description can now state the first.
func TestNameNullPublishesTheNullAndNotAnEmptyName(t *testing.T) {
	t.Parallel()

	body := nameNullBody(t, nameNullEntity(hacatalog.PlatformSwitch,
		model.Description{NameNull: true}))

	value, present := body["name"]
	if !present {
		t.Fatal("the name key is absent, so Home Assistant derives a name from the platform")
	}
	if value != nil {
		t.Errorf("name is %#v, want JSON null", value)
	}
}

// TestNameNullWinsOverALiteralName. The precedence is stated on the field and
// has to hold, because the case that needs it is an operator rule saying "show
// the device name alone" running after a catalogue default has already filled
// in a name. The other precedence would make the statement unreachable from an
// enricher.
func TestNameNullWinsOverALiteralName(t *testing.T) {
	t.Parallel()

	body := nameNullBody(t, nameNullEntity(hacatalog.PlatformSwitch,
		model.Description{Name: model.L("Boost"), NameKey: "entity.boost", NameNull: true}))

	if value, present := body["name"]; !present || value != nil {
		t.Errorf("name is %#v (present=%v), want JSON null — the literal survived", value, present)
	}
}

// TestTheZeroDescriptionStillHasNoOpinionOnTheName. The zero value carries
// every entity in every consumer that never asks for the null, so it must go
// on meaning "say nothing" — an empty name reaching the payload as `""` or as
// `null` would re-seed the entity id of a whole published fleet.
func TestTheZeroDescriptionStillHasNoOpinionOnTheName(t *testing.T) {
	t.Parallel()

	body := nameNullBody(t, nameNullEntity(hacatalog.PlatformSwitch, model.Description{}))

	if value, present := body["name"]; present {
		t.Errorf("the zero description published name=%#v; it must publish no name key at all", value)
	}
}

// TestNameNullIsNotProjectedOntoAPlatformWithoutTheKey. Home Assistant's
// discovery schemas are extra=REMOVE_EXTRA: an undeclared key is dropped with
// no error on the wire and no log line. `name` is declared by 30 of the 32
// platforms — device_automation and tag are the exceptions — so projecting the
// null there would be invisible noise, and this is the same gate every other
// conditional projection in the pipeline goes through.
func TestNameNullIsNotProjectedOntoAPlatformWithoutTheKey(t *testing.T) {
	t.Parallel()

	for _, platform := range []hacatalog.Platform{"device_automation", "tag"} {
		body := nameNullBody(t, nameNullEntity(platform, model.Description{NameNull: true}))
		if value, present := body["name"]; present {
			t.Errorf("%s: published name=%#v although its schema declares no `name`",
				platform, value)
		}
	}
}
