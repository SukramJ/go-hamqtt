// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package catalog_test

import (
	"context"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/catalog"
	"github.com/SukramJ/go-hamqtt/model"
)

func entity(key, leaf string) *model.Basic {
	return &model.Basic{
		EntityKey:      key,
		EntityPlatform: hacatalog.PlatformSensor,
		Description:    model.Description{Name: model.L(key)},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S("serial:A", "", model.BucketValues, leaf),
			Mode: model.Read,
		}},
	}
}

func device(mdl string) *model.Device {
	return &model.Device{
		Identity: model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "A"}}},
		Model:    mdl,
	}
}

// TestPriorityDecidesTheWinner is the format's core promise: a specific rule
// overrides a general one, and the order in the slice does not matter.
func TestPriorityDecidesTheWinner(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{
		{
			Priority: 100,
			Match:    catalog.Match{Models: []string{"FTXM"}},
			Set:      catalog.Overlay{Icon: model.Ptr("mdi:specific")},
		},
		{
			Priority: 10,
			Match:    catalog.Match{Leaves: []string{"power"}},
			Set:      catalog.Overlay{Icon: model.Ptr("mdi:general"), DeviceClass: model.Ptr(model.DeviceClass("power"))},
		},
	}

	e := entity("power", "power")
	if err := rules.Enrich(device("FTXM35"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if got := e.Description.Icon; got != "mdi:specific" {
		t.Errorf("Icon = %q, want the higher-priority rule to win", got)
	}
	// The lower-priority rule still contributed what the higher one said
	// nothing about.
	if got := e.Description.DeviceClass; got != "power" {
		t.Errorf("DeviceClass = %q, want the general rule to still apply", got)
	}
}

// TestUnsetIsNotZero pins why Overlay uses pointers: a rule that clears an
// icon and a rule that says nothing about icons are different things.
func TestUnsetIsNotZero(t *testing.T) {
	t.Parallel()

	e := entity("power", "power")
	e.Description.Icon = "mdi:preexisting"

	silent := catalog.Rules{{Set: catalog.Overlay{DeviceClass: model.Ptr(model.DeviceClass("power"))}}}
	if err := silent.Enrich(device("x"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.Icon != "mdi:preexisting" {
		t.Errorf("Icon = %q — a rule that never mentioned icons cleared one", e.Description.Icon)
	}

	clearing := catalog.Rules{{Set: catalog.Overlay{Icon: model.Ptr("")}}}
	if err := clearing.Enrich(device("x"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.Icon != "" {
		t.Errorf("Icon = %q — an explicit empty override did not clear it", e.Description.Icon)
	}
}

// TestMatchCriteriaAreANDed pins that a rule with several criteria needs all
// of them, which is what keeps a device-specific rule from leaking.
func TestMatchCriteriaAreANDed(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{Models: []string{"FTXM"}, Leaves: []string{"power"}},
		Set:   catalog.Overlay{Icon: model.Ptr("mdi:hit")},
	}}

	hit := entity("power", "power")
	if err := rules.Enrich(device("FTXM35"), hit); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if hit.Description.Icon != "mdi:hit" {
		t.Error("a rule whose criteria all matched did not apply")
	}

	wrongModel := entity("power", "power")
	if err := rules.Enrich(device("RXM35"), wrongModel); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if wrongModel.Description.Icon != "" {
		t.Error("a rule applied although the model did not match")
	}
}

// TestModelMatchIsCaseInsensitivePrefix: a device family shares a prefix far
// more often than an exact name.
func TestModelMatchIsCaseInsensitivePrefix(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{Models: []string{"hmip-"}},
		Set:   catalog.Overlay{Icon: model.Ptr("mdi:hit")},
	}}
	e := entity("x", "x")
	if err := rules.Enrich(device("HmIP-BWTH"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.Icon != "mdi:hit" {
		t.Error("a case-insensitive model prefix did not match")
	}
}

// TestSuppressionIsTheSameMechanism pins the design decision: an operator
// removing an entity uses what a composite entity uses, not a second system.
func TestSuppressionIsTheSameMechanism(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{Keys: []string{"noisy"}},
		Set:   catalog.Overlay{Suppress: model.Ptr(true)},
	}}

	noisy := entity("noisy", "noisy")
	keep := entity("keep", "keep")
	for _, e := range []model.Entity{noisy, keep} {
		if err := rules.Enrich(device("x"), e); err != nil {
			t.Fatalf("Enrich: %v", err)
		}
	}

	got := catalog.Filter([]model.Entity{noisy, keep})
	if len(got) != 1 || got[0].Key() != "keep" {
		t.Fatalf("Filter kept %v, want only \"keep\"", keys(got))
	}
	// The marker must not survive into a payload.
	if keep.Description.Extra != nil {
		t.Errorf("Extra = %v, want the marker stripped", keep.Description.Extra)
	}
}

// TestStaticClonesTemplates guards against the bug a shared template would
// cause: an enricher run for one device mutating the description another
// device is still using.
func TestStaticClonesTemplates(t *testing.T) {
	t.Parallel()

	static := &catalog.Static{ByModel: map[string][]model.Basic{
		"": {{
			EntityKey:      "power",
			EntityPlatform: hacatalog.PlatformSensor,
			Description:    model.Description{Name: model.L("Power")},
			Binds: []model.Binding{{
				Role: model.RoleState,
				Slot: model.S("", "", model.BucketValues, "power"),
				Mode: model.Read,
			}},
		}},
	}}

	first, err := static.Entities(context.Background(), device("A"))
	if err != nil {
		t.Fatalf("Entities: %v", err)
	}
	first[0].Desc().Icon = "mdi:mutated"

	second, err := static.Entities(context.Background(), device("A"))
	if err != nil {
		t.Fatalf("Entities: %v", err)
	}
	if got := second[0].Desc().Icon; got != "" {
		t.Errorf("Icon = %q — the template was shared, not cloned", got)
	}
}

// TestStaticBindsTemplatesToTheDevice: a table describes a model, not an
// instance, so its slots carry no address until they are handed out.
func TestStaticBindsTemplatesToTheDevice(t *testing.T) {
	t.Parallel()

	static := &catalog.Static{ByModel: map[string][]model.Basic{
		"": {{
			EntityKey:      "power",
			EntityPlatform: hacatalog.PlatformSensor,
			Binds: []model.Binding{{
				Role: model.RoleState,
				Slot: model.S("", "", model.BucketValues, "power"),
				Mode: model.Read,
			}},
		}},
	}}

	got, err := static.Entities(context.Background(), device("A"))
	if err != nil {
		t.Fatalf("Entities: %v", err)
	}
	if addr := got[0].Bindings()[0].Slot.Address; addr != "serial:A" {
		t.Errorf("Slot.Address = %q, want the device's uid", addr)
	}
}

// TestStaticFallsBackToTheEmptyModel covers the catch-all row, which is how a
// bridge describes a family it has no specific table for.
func TestStaticFallsBackToTheEmptyModel(t *testing.T) {
	t.Parallel()

	static := &catalog.Static{ByModel: map[string][]model.Basic{
		"":       {{EntityKey: "generic", EntityPlatform: hacatalog.PlatformSensor}},
		"FTXM35": {{EntityKey: "specific", EntityPlatform: hacatalog.PlatformSensor}},
	}}

	specific, _ := static.Entities(context.Background(), device("FTXM35"))
	if len(specific) != 1 || specific[0].Key() != "specific" {
		t.Errorf("known model got %v", keys(specific))
	}
	generic, _ := static.Entities(context.Background(), device("unknown"))
	if len(generic) != 1 || generic[0].Key() != "generic" {
		t.Errorf("unknown model got %v", keys(generic))
	}
}

func keys(entities []model.Entity) []string {
	out := make([]string, 0, len(entities))
	for _, e := range entities {
		out = append(out, e.Key())
	}
	return out
}

// TestPlatformAndBucketCriteria cover the two match dimensions the other tests
// do not reach: they are how a rule set says "only configuration parameters"
// or "only selects", which is most of what a real catalog does.
func TestPlatformAndBucketCriteria(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{
		{
			Match: catalog.Match{Platforms: []hacatalog.Platform{hacatalog.PlatformSelect}},
			Set:   catalog.Overlay{Icon: model.Ptr("mdi:select")},
		},
		{
			Match: catalog.Match{Buckets: []model.Bucket{model.BucketMaster}},
			Set:   catalog.Overlay{Category: model.Ptr(hacatalog.EntityCategoryConfig)},
		},
	}

	sensorEntity := entity("power", "power")
	if err := rules.Enrich(device("x"), sensorEntity); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if sensorEntity.Description.Icon != "" {
		t.Error("a select-only rule applied to a sensor")
	}
	if sensorEntity.Description.Category != "" {
		t.Error("a master-bucket rule applied to a values datapoint")
	}

	selectEntity := entity("mode", "mode")
	selectEntity.EntityPlatform = hacatalog.PlatformSelect
	selectEntity.Binds[0].Slot = model.S("serial:A", "", model.BucketMaster, "mode")
	if err := rules.Enrich(device("x"), selectEntity); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if selectEntity.Description.Icon != "mdi:select" {
		t.Error("the platform criterion did not match a select")
	}
	if selectEntity.Description.Category != hacatalog.EntityCategoryConfig {
		t.Error("the bucket criterion did not match a master datapoint")
	}
}

// TestOverlayAppliesEveryField guards against a field being added to Overlay
// and forgotten in applyTo — a silent no-op that a catalog author would chase
// for a long time.
func TestOverlayAppliesEveryField(t *testing.T) {
	t.Parallel()

	opts := &model.Enum{Codes: []string{"a"}}
	rules := catalog.Rules{{Set: catalog.Overlay{
		Name:        model.Ptr(model.L("Renamed")),
		DeviceClass: model.Ptr(model.DeviceClass("energy")),
		StateClass:  model.Ptr(hacatalog.StateClassTotalIncreasing),
		Unit:        model.Ptr(model.Unit("kWh")),
		Icon:        model.Ptr("mdi:flash"),
		Category:    model.Ptr(hacatalog.EntityCategoryDiagnostic),
		Enabled:     model.Ptr(false),
		Precision:   model.Ptr(2),
		Min:         model.Ptr(0.0),
		Max:         model.Ptr(100.0),
		Step:        model.Ptr(0.5),
		Options:     opts,
		Extra:       map[string]any{"custom": "value"},
	}}}

	e := entity("power", "power")
	if err := rules.Enrich(device("x"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	d := e.Description
	switch {
	case d.Name.Default != "Renamed":
		t.Error("Name not applied")
	case d.DeviceClass != "energy":
		t.Error("DeviceClass not applied")
	case d.StateClass != hacatalog.StateClassTotalIncreasing:
		t.Error("StateClass not applied")
	case d.Unit != "kWh":
		t.Error("Unit not applied")
	case d.Icon != "mdi:flash":
		t.Error("Icon not applied")
	case d.Category != hacatalog.EntityCategoryDiagnostic:
		t.Error("Category not applied")
	case d.Enabled == nil || *d.Enabled:
		t.Error("Enabled not applied")
	case d.Precision == nil || *d.Precision != 2:
		t.Error("Precision not applied")
	case d.Min == nil || *d.Min != 0:
		t.Error("Min not applied")
	case d.Max == nil || *d.Max != 100:
		t.Error("Max not applied")
	case d.Step == nil || *d.Step != 0.5:
		t.Error("Step not applied")
	case d.Options != opts:
		t.Error("Options not applied")
	case d.Extra["custom"] != "value":
		t.Error("Extra not merged")
	}
}

// TestMatchOnAnEntityWithoutBindings covers the composite case: an entity with
// no state role still has to be matchable, or a rule set could never touch one.
func TestMatchOnAnEntityWithoutBindings(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{Leaves: []string{"mode"}},
		Set:   catalog.Overlay{Icon: model.Ptr("mdi:hit")},
	}}

	// No state role, but a first binding the rule can fall back to.
	composite := &model.Basic{
		EntityKey:      "climate",
		EntityPlatform: hacatalog.PlatformClimate,
		Binds: []model.Binding{{
			Role: "mode",
			Slot: model.S("serial:A", "", model.BucketValues, "mode"),
			Mode: model.ReadWrite,
		}},
	}
	if err := rules.Enrich(device("x"), composite); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if composite.Description.Icon != "mdi:hit" {
		t.Error("a rule could not match a composite entity's first binding")
	}

	// No bindings at all: a leaf criterion cannot match, and must not panic.
	bare := &model.Basic{EntityKey: "bare", EntityPlatform: hacatalog.PlatformButton}
	if err := rules.Enrich(device("x"), bare); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if bare.Description.Icon != "" {
		t.Error("a leaf criterion matched an entity with no datapoint")
	}
}

// TestKeyContainsCoversTheLongTail: no exact list covers every vendor
// parameter, and a substring rule is what a real catalog falls back to.
func TestKeyContainsCoversTheLongTail(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{KeyContains: model.Ptr("temperature")},
		Set:   catalog.Overlay{DeviceClass: model.Ptr(model.DeviceClass("temperature"))},
	}}
	hit := entity("outdoor_temperature", "x")
	miss := entity("power", "x")
	for _, e := range []model.Entity{hit, miss} {
		if err := rules.Enrich(device("x"), e); err != nil {
			t.Fatalf("Enrich: %v", err)
		}
	}
	if hit.Description.DeviceClass != "temperature" {
		t.Error("the substring rule did not match")
	}
	if miss.Description.DeviceClass != "" {
		t.Error("the substring rule matched an unrelated key")
	}
}

// TestUnitCriterionRefinesAnEarlierRule covers the two-stage pattern: one rule
// establishes a unit, a later one keys off it.
func TestUnitCriterionRefinesAnEarlierRule(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{
		{
			Priority: 10,
			Match:    catalog.Match{Keys: []string{"consumption"}},
			Set:      catalog.Overlay{Unit: model.Ptr(model.Unit("kWh"))},
		},
		{
			Priority: 20,
			Match:    catalog.Match{Unit: model.Ptr(model.Unit("kWh"))},
			Set:      catalog.Overlay{StateClass: model.Ptr(hacatalog.StateClassTotalIncreasing)},
		},
	}

	e := entity("consumption", "consumption")
	e.Description.Unit = "kWh" // as an earlier pass would have left it
	if err := rules.Enrich(device("x"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.StateClass != hacatalog.StateClassTotalIncreasing {
		t.Error("the unit criterion did not match")
	}
}
