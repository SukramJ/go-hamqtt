// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package model_test

import (
	"context"
	"slices"
	"testing"
	"time"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/model"
)

// TestBucketSpellings pins the strings that become topic segments: changing
// one silently moves every topic of that kind.
func TestBucketSpellings(t *testing.T) {
	t.Parallel()

	for b, want := range map[model.Bucket]string{
		model.BucketValues:     "values",
		model.BucketMaster:     "master",
		model.BucketCalculated: "calculated",
		model.BucketCustom:     "custom",
		model.Bucket(0):        "unknown",
	} {
		if got := b.String(); got != want {
			t.Errorf("Bucket(%d).String() = %q, want %q", b, got, want)
		}
	}
	if model.Bucket(0).Valid() || model.Bucket(99).Valid() {
		t.Error("an undeclared bucket reported valid")
	}
	if !model.BucketValues.Valid() {
		t.Error("a declared bucket reported invalid")
	}
}

// TestSlotKeyDistinguishesCoordinates guards the map key: two different
// datapoints collapsing to one key would silently merge their state.
func TestSlotKeyDistinguishesCoordinates(t *testing.T) {
	t.Parallel()

	base := model.S("d", "1", model.BucketValues, "a")
	for name, other := range map[string]model.Slot{
		"address": model.S("e", "1", model.BucketValues, "a"),
		"channel": model.S("d", "2", model.BucketValues, "a"),
		"bucket":  model.S("d", "1", model.BucketMaster, "a"),
		"path":    model.S("d", "1", model.BucketValues, "b"),
		"depth":   model.S("d", "1", model.BucketValues, "a", "b"),
	} {
		if base.Equal(other) {
			t.Errorf("%s: two different slots compared equal (%q)", name, base.Key())
		}
	}
	if !base.Equal(model.S("d", "1", model.BucketValues, "a")) {
		t.Error("the same coordinate compared unequal")
	}
	if base.String() != base.Key() {
		t.Error("String and Key disagree")
	}
}

// TestBindModeDirections pins the read/write split the runtime routes on.
func TestBindModeDirections(t *testing.T) {
	t.Parallel()

	for mode, want := range map[model.BindMode][2]bool{
		model.Read:        {true, false},
		model.Write:       {false, true},
		model.ReadWrite:   {true, true},
		model.BindMode(0): {false, false},
	} {
		if got := [2]bool{mode.CanRead(), mode.CanWrite()}; got != want {
			t.Errorf("%s: read/write = %v, want %v", mode, got, want)
		}
	}
	if model.BindMode(0).String() != "unknown" {
		t.Error("an undeclared mode did not render as unknown")
	}
}

// TestBindingValidity keeps an unaddressable binding out of the render.
func TestBindingValidity(t *testing.T) {
	t.Parallel()

	good := model.Binding{Role: model.RoleState, Mode: model.Read, Slot: model.S("d", "", model.BucketValues, "x")}
	if !good.Valid() {
		t.Error("a complete binding reported invalid")
	}
	for name, b := range map[string]model.Binding{
		"no role":  {Mode: model.Read, Slot: good.Slot},
		"no mode":  {Role: model.RoleState, Slot: good.Slot},
		"bad slot": {Role: model.RoleState, Mode: model.Read},
	} {
		if b.Valid() {
			t.Errorf("%s: reported valid", name)
		}
	}
}

// TestBindFindsARole covers the lookup the render pipeline and every Commander
// use to route.
func TestBindFindsARole(t *testing.T) {
	t.Parallel()

	e := &model.Basic{Binds: []model.Binding{
		{Role: model.RoleState, Mode: model.Read},
		{Role: model.RoleCommand, Mode: model.Write},
	}}
	if _, ok := model.Bind(e, model.RoleCommand); !ok {
		t.Error("Bind did not find a declared role")
	}
	if _, ok := model.Bind(e, "nonsense"); ok {
		t.Error("Bind invented a role")
	}
}

// TestBasicSatisfiesEntity pins the struct form catalogs and codegen produce.
func TestBasicSatisfiesEntity(t *testing.T) {
	t.Parallel()

	var e model.Entity = &model.Basic{
		EntityKey:      "power",
		EntityPlatform: hacatalog.PlatformSensor,
		Description:    model.Description{Icon: "mdi:flash"},
	}
	if e.Key() != "power" || e.Platform() != hacatalog.PlatformSensor {
		t.Errorf("Key/Platform = %q/%q", e.Key(), e.Platform())
	}
	if e.Desc().Icon != "mdi:flash" {
		t.Error("Desc did not return the embedded description")
	}
	// Desc must return a pointer into the entity, or enrichers write to a copy.
	e.Desc().Icon = "mdi:changed"
	if e.Desc().Icon != "mdi:changed" {
		t.Error("Desc returned a copy — enrichers would have no effect")
	}
}

// TestIdentityValidity mirrors Home Assistant's own refusal to register a
// device it cannot key.
func TestIdentityValidity(t *testing.T) {
	t.Parallel()

	if (model.Identity{}).Valid() {
		t.Error("an empty identity reported valid")
	}
	if (model.Identity{}).UID() != "" {
		t.Error("an empty identity produced a uid")
	}
	byConn := model.Identity{Connections: []model.Connection{{Type: "mac", Value: "aa"}}}
	if !byConn.Valid() {
		t.Error("a connection-only identity reported invalid — Home Assistant accepts one")
	}
	if (*model.Device)(nil).UID() != "" {
		t.Error("a nil device panicked or produced a uid")
	}
}

// TestLocalizedFallbacks covers the path a consumer that does not localise
// takes, and the tag normalisation.
func TestLocalizedFallbacks(t *testing.T) {
	t.Parallel()

	l := model.Localized{Default: "Power", Lang: map[string]string{"de": "Leistung"}}
	// A slice of pairs, not a map: the padded tag is the point of the case,
	// and a map literal makes deliberate whitespace look like a typo.
	for _, tc := range []struct{ lang, want string }{
		{"", "Power"},
		{"de", "Leistung"},
		{"DE", "Leistung"},
		{" de ", "Leistung"}, // a config file will do this eventually
		{"fr", "Power"},
	} {
		if got := l.In(tc.lang); got != tc.want {
			t.Errorf("In(%q) = %q, want %q", tc.lang, got, tc.want)
		}
	}
	if !(model.Localized{}).IsZero() || model.L("x").IsZero() {
		t.Error("IsZero disagrees with its inputs")
	}
}

// TestEnumLanguagesReportsCoverage is how a consumer tells which locales its
// catalog actually covers.
func TestEnumLanguagesReportsCoverage(t *testing.T) {
	t.Parallel()

	e := &model.Enum{
		Codes: []string{"a", "b"},
		Labels: map[string]model.Localized{
			"a": {Lang: map[string]string{"de": "A", "fr": "A"}},
			"b": {Lang: map[string]string{"de": "B"}},
		},
	}
	if got := e.Languages(); !slices.Equal(got, []string{"de", "fr"}) {
		t.Errorf("Languages = %v, want [de fr]", got)
	}
	var nilEnum *model.Enum
	if nilEnum.Languages() != nil || nilEnum.Options("") != nil {
		t.Error("a nil Enum did not degrade quietly")
	}
	if got := nilEnum.Label("code", ""); got != "code" {
		t.Errorf("nil Enum Label = %q, want the code", got)
	}
}

// TestStateIsZero covers the emptiness check the runtime uses to tell "never
// seen" from "seen and false".
func TestStateIsZero(t *testing.T) {
	t.Parallel()

	if !(model.State{}).IsZero() {
		t.Error("the zero state did not report zero")
	}
	for name, s := range map[string]model.State{
		"value":     {Value: false},
		"available": {Available: true},
		"origin":    {Origin: "local"},
		"time":      {RefreshedAt: time.Now()},
		"extra":     {Extra: map[string]any{"a": 1}},
	} {
		if s.IsZero() {
			t.Errorf("%s: a populated state reported zero", name)
		}
	}
}

// TestDescriptionCloneIsolatesMutableFields guards the Static source's promise
// that one device's enrichment cannot reach another's description.
func TestDescriptionCloneIsolatesMutableFields(t *testing.T) {
	t.Parallel()

	orig := &model.Description{
		Name:         model.Localized{Default: "A", Lang: map[string]string{"de": "A"}},
		Availability: model.Availability{Levels: []model.AvailabilityLevel{model.LevelBridge}},
		Extra:        map[string]any{"k": "v"},
		Options:      &model.Enum{Codes: []string{"a"}, Labels: map[string]model.Localized{"a": model.L("A")}},
	}
	clone := orig.Clone()

	clone.Name.Lang["de"] = "changed"
	clone.Extra["k"] = "changed"
	clone.Availability.Levels[0] = model.LevelDevice
	clone.Options.Codes[0] = "changed"
	clone.Options.Labels["a"] = model.L("changed")

	if orig.Name.Lang["de"] != "A" {
		t.Error("Name.Lang was shared")
	}
	if orig.Extra["k"] != "v" {
		t.Error("Extra was shared")
	}
	if orig.Availability.Levels[0] != model.LevelBridge {
		t.Error("Availability.Levels was shared")
	}
	if orig.Options.Codes[0] != "a" {
		t.Error("Options.Codes was shared")
	}
	if orig.Options.Labels["a"].Default != "A" {
		t.Error("Options.Labels was shared")
	}
	if (*model.Description)(nil).Clone() != nil {
		t.Error("cloning nil did not yield nil")
	}
}

// TestPtrRoundTrip covers the helper that exists because a struct literal
// cannot take the address of a constant.
func TestPtrRoundTrip(t *testing.T) {
	t.Parallel()

	if got := model.Ptr(42); *got != 42 {
		t.Errorf("Ptr = %v", *got)
	}
}

// TestEntitySourceFuncAdapts covers the adapter every consumer that generates
// entities in code will use.
func TestEntitySourceFuncAdapts(t *testing.T) {
	t.Parallel()

	var src model.EntitySource = model.EntitySourceFunc(
		func(context.Context, *model.Device) ([]model.Entity, error) {
			return []model.Entity{&model.Basic{EntityKey: "x"}}, nil
		})
	got, err := src.Entities(context.Background(), nil)
	if err != nil || len(got) != 1 {
		t.Fatalf("Entities = %v, %v", got, err)
	}
}

// TestEnrichSkipsNilMembers keeps an optional enricher from needing a branch
// at every call site.
func TestEnrichSkipsNilMembers(t *testing.T) {
	t.Parallel()

	if err := model.Enrich(nil, &model.Basic{}, nil, nil); err != nil {
		t.Fatalf("Enrich with nil members: %v", err)
	}
}

// TestAvailabilityHasResolvesDefaults: Has must see the defaults, or an entity
// that declared nothing would appear to have no availability at all.
func TestAvailabilityHasResolvesDefaults(t *testing.T) {
	t.Parallel()

	var a model.Availability
	if !a.Has(model.LevelBridge) || !a.Has(model.LevelDevice) {
		t.Error("the zero Availability did not resolve its defaults")
	}
	if a.Has(model.LevelSelf) {
		t.Error("the zero Availability claimed a level it does not have")
	}
}
