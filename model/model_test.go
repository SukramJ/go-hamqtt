// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package model_test

import (
	"slices"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/model"
)

func ident(ns, v string) model.Identifier { return model.Identifier{Namespace: ns, Value: v} }

// TestIdentityEqualMergesSharedHardware is the case the whole Identity design
// exists for: two indoor units each report the outdoor unit they share, and
// without this the registry holds it twice.
func TestIdentityEqualMergesSharedHardware(t *testing.T) {
	t.Parallel()

	fromUnitA := model.Identity{IDs: []model.Identifier{ident("serial", "OUT-9")}}
	fromUnitB := model.Identity{IDs: []model.Identifier{
		ident("daikin:gateway", "GW-2"),
		ident("serial", "OUT-9"),
	}}

	if !fromUnitA.Equal(fromUnitB) {
		t.Fatal("identities sharing a serial were not recognised as the same device")
	}
	if fromUnitA.Equal(model.Identity{IDs: []model.Identifier{ident("serial", "OUT-8")}}) {
		t.Error("different serials were treated as the same device")
	}
}

// TestIdentityEqualIgnoresConnections pins the deliberate exclusion: a MAC can
// be reassigned, and matching on one would merge two genuinely different
// devices after a DHCP reservation moved.
func TestIdentityEqualIgnoresConnections(t *testing.T) {
	t.Parallel()

	mac := model.Connection{Type: "mac", Value: "aa:bb:cc:dd:ee:ff"}
	a := model.Identity{IDs: []model.Identifier{ident("serial", "A")}, Connections: []model.Connection{mac}}
	b := model.Identity{IDs: []model.Identifier{ident("serial", "B")}, Connections: []model.Connection{mac}}

	if a.Equal(b) {
		t.Error("a shared MAC merged two devices with different serials")
	}
}

// TestIdentityMergeKeepsThePrimary guards the published identity: the UID is
// already in every entity's unique id, and recomputing it would orphan them.
func TestIdentityMergeKeepsThePrimary(t *testing.T) {
	t.Parallel()

	a := model.Identity{IDs: []model.Identifier{ident("serial", "OUT-9")}}
	b := model.Identity{IDs: []model.Identifier{
		ident("daikin:gateway", "GW-2"),
		ident("serial", "OUT-9"),
	}}

	merged := a.Merge(b)
	if got, want := merged.UID(), "serial:OUT-9"; got != want {
		t.Errorf("UID = %q, want %q — the primary must not move", got, want)
	}
	if len(merged.IDs) != 2 {
		t.Errorf("IDs = %v, want the alternate unioned in exactly once", merged.IDs)
	}

	// Merging twice must not grow the list.
	if again := merged.Merge(b); len(again.IDs) != 2 {
		t.Errorf("merging twice produced %v", again.IDs)
	}
}

// TestSlotHandlesEveryConsumerShape is why Path is a slice and Channel a
// string: the six consumers address datapoints at wildly different arities.
func TestSlotHandlesEveryConsumerShape(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		slot model.Slot
		leaf string
	}{
		{"flat", model.S("d", "", model.BucketValues, "power"), "power"},
		{"channelled", model.S("d", "3", model.BucketMaster, "TEMPERATURE"), "TEMPERATURE"},
		{"dotted", model.S("d", "", model.BucketValues, "BSH", "Common", "Setting", "PowerState"), "PowerState"},
		{"named port", model.S("d", "port-1", model.BucketValues, "poe"), "poe"},
	} {
		if !tc.slot.Valid() {
			t.Errorf("%s: slot reported invalid", tc.name)
		}
		if got := tc.slot.Leaf(); got != tc.leaf {
			t.Errorf("%s: Leaf = %q, want %q", tc.name, got, tc.leaf)
		}
	}
}

// TestSlotRejectsUnaddressable keeps a malformed coordinate from reaching the
// topic layer, where it would produce a topic with an empty level.
func TestSlotRejectsUnaddressable(t *testing.T) {
	t.Parallel()

	for name, slot := range map[string]model.Slot{
		"no address": model.S("", "", model.BucketValues, "x"),
		"no path":    model.S("d", "", model.BucketValues),
		"empty seg":  model.S("d", "", model.BucketValues, "a", "", "b"),
		"bad bucket": {Address: "d", Bucket: model.Bucket(99), Path: []string{"x"}},
	} {
		if slot.Valid() {
			t.Errorf("%s: reported valid", name)
		}
	}
}

// TestUnsetBucketAddressesAHubDatapoint pins the zero value as a legitimate
// coordinate: a system variable or a program is not on a channel and has no
// paramset, so refusing it would leave half a consumer's tree unaddressable.
func TestUnsetBucketAddressesAHubDatapoint(t *testing.T) {
	t.Parallel()

	slot := model.S("hub:ccu1", "", model.BucketUnset, "sysvar", "Anwesenheit")
	if !slot.Valid() {
		t.Fatal("a hub datapoint with no paramset reported invalid")
	}
	if got := slot.Bucket.String(); got != "" {
		t.Errorf("Bucket.String() = %q, want empty so the segment disappears", got)
	}
}

// TestEnumCodeMatchesEveryLanguage pins the reverse lookup's breadth. Home
// Assistant echoes back whatever label it was given, and which language that
// was depends on when the entity was discovered — not on today's config.
func TestEnumCodeMatchesEveryLanguage(t *testing.T) {
	t.Parallel()

	e := &model.Enum{
		Codes: []string{"eco", "comfort"},
		Labels: map[string]model.Localized{
			"eco":     {Default: "Eco", Lang: map[string]string{"de": "Sparen"}},
			"comfort": {Default: "Comfort", Lang: map[string]string{"de": "Komfort"}},
		},
	}

	for label, want := range map[string]string{
		"eco":     "eco", // the code itself
		"Eco":     "eco", // the default label
		"Sparen":  "eco", // a translation
		"Komfort": "comfort",
	} {
		got, ok := e.Code(label)
		if !ok || got != want {
			t.Errorf("Code(%q) = %q, %v; want %q, true", label, got, ok, want)
		}
	}
	if _, ok := e.Code("nonsense"); ok {
		t.Error("Code accepted a label that is in no language")
	}
}

// TestEnumOptionsKeepDeclarationOrder pins that options are not sorted: a mode
// list reads "off, heat, cool", not alphabetically.
func TestEnumOptionsKeepDeclarationOrder(t *testing.T) {
	t.Parallel()

	e := &model.Enum{Codes: []string{"off", "heat", "cool"}}
	got := e.Options("")
	if !slices.Equal(got, []string{"off", "heat", "cool"}) {
		t.Errorf("Options = %v, want declaration order", got)
	}
}

// TestPrecedenceKeepsLocalAheadOfCloud is the multi-source case: a stale cloud
// poll must not overwrite a live local reading.
func TestPrecedenceKeepsLocalAheadOfCloud(t *testing.T) {
	t.Parallel()

	p := model.Precedence("local", "cloud")
	local := model.State{Value: 21.5, Available: true, Origin: "local"}
	cloud := model.State{Value: 19.0, Available: true, Origin: "cloud"}

	if p.Accept(local, cloud) {
		t.Error("a cloud reading overwrote a live local one")
	}
	if !p.Accept(cloud, local) {
		t.Error("a local reading did not win over a cloud one")
	}
	if !p.Accept(local, local) {
		t.Error("an equal-origin update was rejected — the source could never refresh itself")
	}
}

// TestPrecedenceYieldsWhenTheWinnerIsGone stops a higher-ranked source that
// went away from freezing the entity forever.
func TestPrecedenceYieldsWhenTheWinnerIsGone(t *testing.T) {
	t.Parallel()

	p := model.Precedence("local", "cloud")
	deadLocal := model.State{Available: false, Origin: "local"}
	cloud := model.State{Value: 19.0, Available: true, Origin: "cloud"}

	if !p.Accept(deadLocal, cloud) {
		t.Error("an unavailable local reading blocked a live cloud one")
	}
}

// TestNilPolicyIsLastWriteWins covers the majority of consumers, which have one
// source and say nothing about origins.
func TestNilPolicyIsLastWriteWins(t *testing.T) {
	t.Parallel()

	if !model.Accept(nil, model.State{Value: 1, Available: true}, model.State{Value: 2}) {
		t.Error("the nil policy rejected an update")
	}
}

// TestAvailabilityDefaults pins the zero value, which is what an entity that
// says nothing about availability gets.
func TestAvailabilityDefaults(t *testing.T) {
	t.Parallel()

	levels, mode := model.Availability{}.Resolved()
	if !slices.Equal(levels, []model.AvailabilityLevel{model.LevelBridge, model.LevelDevice}) {
		t.Errorf("levels = %v, want bridge+device", levels)
	}
	if mode != model.AvailabilityAll {
		t.Errorf("mode = %q, want %q", mode, model.AvailabilityAll)
	}
}

// TestBridgeOnlyExcludesTheDevice is the subtractive case that motivated
// modelling availability as a list: a connectivity sensor must survive its own
// device going offline, or it can never report that it did.
func TestBridgeOnlyExcludesTheDevice(t *testing.T) {
	t.Parallel()

	a := model.BridgeOnly()
	if !a.Has(model.LevelBridge) {
		t.Error("BridgeOnly dropped the bridge level")
	}
	if a.Has(model.LevelDevice) {
		t.Error("BridgeOnly still includes the device level — the sensor would go down with it")
	}
}

// TestSuppressionIgnoresSelfReference stops a mistake from deleting the very
// entity that made it.
func TestSuppressionIgnoresSelfReference(t *testing.T) {
	t.Parallel()

	e := &selfSuppressor{}
	got := model.ApplySuppression([]model.Entity{e})
	if len(got) != 1 {
		t.Fatalf("an entity suppressing itself disappeared: %v", got)
	}
}

type selfSuppressor struct{ model.Basic }

func (s *selfSuppressor) Key() string                  { return "loop" }
func (s *selfSuppressor) Platform() hacatalog.Platform { return hacatalog.PlatformSensor }
func (s *selfSuppressor) Desc() *model.Description     { return &s.Description }
func (s *selfSuppressor) Bindings() []model.Binding    { return nil }
func (s *selfSuppressor) Suppresses() []string         { return []string{"loop"} }

// TestEnrichChainStopsAtTheFirstError keeps a half-applied chain from
// producing an entity nobody declared.
func TestEnrichChainStopsAtTheFirstError(t *testing.T) {
	t.Parallel()

	var ran []string
	boom := model.EnricherFunc(func(*model.Device, model.Entity) error {
		ran = append(ran, "boom")
		return errUnderTest
	})
	after := model.EnricherFunc(func(*model.Device, model.Entity) error {
		ran = append(ran, "after")
		return nil
	})

	err := model.Enrich(nil, &model.Basic{}, boom, after)
	if err == nil {
		t.Fatal("Enrich swallowed the error")
	}
	if slices.Contains(ran, "after") {
		t.Error("the chain continued past a failing enricher")
	}
}

var errUnderTest = errTest("enricher failed")

type errTest string

func (e errTest) Error() string { return string(e) }
