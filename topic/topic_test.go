// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package topic_test

import (
	"testing"

	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// TestSlugTransliteratesUmlauts is the defect one reference bridge shipped by
// omitting this step: "Größe" slugged to "gr_e", diverging from Home
// Assistant's own slugify and from its five sibling projects.
func TestSlugTransliteratesUmlauts(t *testing.T) {
	t.Parallel()

	for in, want := range map[string]string{
		"Größe":           "groesse",
		"Außentemperatur": "aussentemperatur",
		"Wärmepumpe":      "waermepumpe",
		"Öl-Füllstand":    "oel_fuellstand",
		"Übertemperatur":  "uebertemperatur",
	} {
		if got := topic.Slug(in); got != want {
			t.Errorf("Slug(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSlugProducesLegalSegments pins the shape Home Assistant needs and the
// collapse rules that keep it readable.
func TestSlugProducesLegalSegments(t *testing.T) {
	t.Parallel()

	for in, want := range map[string]string{
		"Living Room AC": "living_room_ac",
		"serial:AC-1":    "serial_ac_1",
		"  padded  ":     "padded",
		"a///b":          "a_b",
		"UPPER":          "upper",
		"mixed42":        "mixed42",
	} {
		if got := topic.Slug(in); got != want {
			t.Errorf("Slug(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSlugNeverReturnsEmpty guards the discovery topic: an empty segment
// produces a path Home Assistant ignores without a word.
func TestSlugNeverReturnsEmpty(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "///", "   ", "!!!"} {
		if got := topic.Slug(in); got == "" {
			t.Errorf("Slug(%q) returned empty", in)
		}
	}
}

// TestSafeIsWeakOnPurpose: a topic segment may hold almost anything, and
// mangling a device name beyond recognition helps nobody. Only the characters
// that would change the topic's shape are replaced.
func TestSafeIsWeakOnPurpose(t *testing.T) {
	t.Parallel()

	if got, want := topic.Safe("Wohnzimmer-Änderung"), "Wohnzimmer-Änderung"; got != want {
		t.Errorf("Safe kept too little: %q, want %q", got, want)
	}
	for in, want := range map[string]string{
		"a/b": "a_b",
		"a+b": "a_b",
		"a#b": "a_b",
		"a b": "a_b",
	} {
		if got := topic.Safe(in); got != want {
			t.Errorf("Safe(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestJoinDropsEmptySegments is what makes an optional channel expressible
// without every caller branching on it.
func TestJoinDropsEmptySegments(t *testing.T) {
	t.Parallel()

	if got, want := topic.Join("root", "", "leaf"), "root/leaf"; got != want {
		t.Errorf("Join = %q, want %q", got, want)
	}
}

// TestDefaultLayoutRendersEveryShape covers the arities the consumers need,
// and pins that a value cannot inject a topic level.
func TestDefaultLayoutRendersEveryShape(t *testing.T) {
	t.Parallel()

	l := topic.Default{Root: "bridge"}

	flat := model.S("serial:A", "", model.BucketValues, "power")
	if got, want := l.State(flat), "bridge/serial:A/values/power"; got != want {
		t.Errorf("State = %q, want %q", got, want)
	}
	if got, want := l.Command(flat), "bridge/serial:A/values/power/set"; got != want {
		t.Errorf("Command = %q, want %q", got, want)
	}

	deep := model.S("serial:A", "3", model.BucketMaster, "BSH", "Common", "PowerState")
	if got, want := l.State(deep), "bridge/serial:A/3/master/BSH/Common/PowerState"; got != want {
		t.Errorf("State(deep) = %q, want %q", got, want)
	}

	// A slash inside an address must not create a level.
	injected := model.S("evil/../root", "", model.BucketValues, "x")
	if got, want := l.State(injected), "bridge/evil_.._root/values/x"; got != want {
		t.Errorf("State(injected) = %q, want %q", got, want)
	}
}

// TestBridgeAndAvailabilityTopics pins the two fixed topics the availability
// list points at.
func TestBridgeAndAvailabilityTopics(t *testing.T) {
	t.Parallel()

	l := topic.Default{Root: "bridge"}
	id := model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "A"}}}

	if got, want := l.Bridge(), "bridge/bridge/status"; got != want {
		t.Errorf("Bridge = %q, want %q", got, want)
	}
	if got, want := l.Availability(id), "bridge/serial:A/availability"; got != want {
		t.Errorf("Availability = %q, want %q", got, want)
	}
}
