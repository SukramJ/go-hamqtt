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
		"Öl-Füllstand":    "oel-fuellstand",
		"Übertemperatur":  "uebertemperatur",
	} {
		if got := topic.Slug(in); got != want {
			t.Errorf("Slug(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSlugProducesLegalSegments pins the shape Home Assistant needs and the
// collapse rules that keep it readable. The hyphen survives: it is legal in a
// node id, and a consumer that builds a sub-device id as "<parent>-<group>"
// needs it to stay distinct from an underscore.
func TestSlugProducesLegalSegments(t *testing.T) {
	t.Parallel()

	for in, want := range map[string]string{
		"Living Room AC": "living_room_ac",
		"serial:AC-1":    "serial_ac-1",
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

// TestUnsetBucketDropsTheSegment is the other half of BucketUnset: an empty
// bucket must vanish from the topic rather than render a placeholder, which is
// what lets a hub datapoint sit one level shallower than a channel one.
func TestUnsetBucketDropsTheSegment(t *testing.T) {
	t.Parallel()

	l := topic.Default{Root: "loom"}
	slot := model.S("hub:ccu1", "", model.BucketUnset, "sysvar", "Anwesenheit")
	if got, want := l.State(slot), "loom/hub:ccu1/sysvar/Anwesenheit"; got != want {
		t.Errorf("State = %q, want %q", got, want)
	}
}

// TestScopeRendersAboveTheDevice covers the segments a consumer puts between
// the root and the device: openccu-loom has two (a CCU name and a wire
// interface), go-unifi2mqtt one (a site), and the other four have none.
func TestScopeRendersAboveTheDevice(t *testing.T) {
	t.Parallel()

	l := topic.Default{Root: "loom"}
	slot := model.S("VCU1234", "3", model.BucketValues, "TEMPERATURE").In("ccu-1", "HmIP-RF")

	if got, want := l.State(slot), "loom/ccu-1/HmIP-RF/VCU1234/3/values/TEMPERATURE"; got != want {
		t.Errorf("State = %q, want %q", got, want)
	}
	if got, want := l.Command(slot), "loom/ccu-1/HmIP-RF/VCU1234/3/values/TEMPERATURE/set"; got != want {
		t.Errorf("Command = %q, want %q", got, want)
	}

	// One segment, and none, are the other two shapes in the family.
	one := model.S("aabbcc", "", model.BucketValues, "poe").In("default")
	if got, want := one.String(), "default|aabbcc||values|poe"; got != want {
		t.Errorf("Key = %q, want %q", got, want)
	}
	if got, want := l.State(model.S("d", "", model.BucketValues, "x")), "loom/d/values/x"; got != want {
		t.Errorf("unscoped State = %q, want %q", got, want)
	}
}

// TestScopeDistinguishesSlots guards the map key: two devices with the same
// address under different controllers are different datapoints, and collapsing
// them would merge one CCU's state into another's.
func TestScopeDistinguishesSlots(t *testing.T) {
	t.Parallel()

	a := model.S("VCU1234", "3", model.BucketValues, "STATE").In("ccu-1", "HmIP-RF")
	b := model.S("VCU1234", "3", model.BucketValues, "STATE").In("ccu-2", "HmIP-RF")
	if a.Equal(b) {
		t.Errorf("slots under different scopes compared equal (%q)", a.Key())
	}
}

// TestEmptyScopeSegmentIsInvalid: an empty segment would vanish from the
// rendered topic and move the datapoint one level up, into another device's
// tree — silently.
func TestEmptyScopeSegmentIsInvalid(t *testing.T) {
	t.Parallel()

	if model.S("d", "", model.BucketValues, "x").In("ccu-1", "").Valid() {
		t.Error("a slot with an empty scope segment reported valid")
	}
}
