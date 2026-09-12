// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package catalog_test

import (
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/catalog"
	"github.com/SukramJ/go-hamqtt/model"
)

// categorised is an entity carrying the consumer's own classification, which
// is not always a Home Assistant platform.
type categorised struct {
	model.Basic
	category string
}

func (c *categorised) Category() string { return c.category }

func hubEntity(key, leaf, category string) *categorised {
	return &categorised{
		Basic: model.Basic{
			EntityKey:      key,
			EntityPlatform: hacatalog.PlatformSensor,
			Description:    model.Description{Name: model.L(key)},
			Binds: []model.Binding{{
				Role: model.RoleState,
				Slot: model.S("serial:A", "", model.BucketValues, leaf),
				Mode: model.Read,
			}},
		},
		category: category,
	}
}

// TestCategoryMatchesTheConsumersOwnClassification. Match.Platforms is typed
// to what an entity renders as, and a reference table does not always key on
// that: 20 of one measured consumer's 147 rules are keyed on `hub_sensor`,
// `hub_button`, `hub_binary_sensor` and `schedule_switch` — classifications
// of what a datapoint IS, several of which render as the same platform.
func TestCategoryMatchesTheConsumersOwnClassification(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{Categories: []string{"hub_sensor"}},
		Set:   catalog.Overlay{Icon: model.Ptr("mdi:hub")},
	}}

	hub := hubEntity("service_messages", "SERVICE_MESSAGES", "hub_sensor")
	if err := rules.Enrich(device("CCU"), hub); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if hub.Description.Icon != "mdi:hub" {
		t.Error("a rule keyed on the consumer's category did not apply")
	}

	// The same platform, a different category: collapsing the two to
	// `sensor` first is what this exists to avoid.
	plain := hubEntity("temperature", "TEMPERATURE", "device_sensor")
	if err := rules.Enrich(device("CCU"), plain); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if plain.Description.Icon != "" {
		t.Error("the rule reached an entity of another category")
	}

	// An entity with no notion of a category matches no category rule
	// rather than matching every one.
	none := entity("temperature", "TEMPERATURE")
	if err := rules.Enrich(device("CCU"), none); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if none.Description.Icon != "" {
		t.Error("an entity without a category matched a category rule")
	}
}

// TestPostfixDistinguishesNumberedParameters. A vendor that numbers repeated
// parameters gives a rule no other way to say "the second one": the leaf
// differs per instance, so Leaves cannot list them, and KeyContains("_2")
// would also match "_20".
func TestPostfixDistinguishesNumberedParameters(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{Postfix: model.Ptr("_2")},
		Set:   catalog.Overlay{Icon: model.Ptr("mdi:two")},
	}}

	for _, tc := range []struct {
		leaf string
		want string
	}{
		{"LEVEL_2", "mdi:two"},
		{"LEVEL_20", ""},
		{"LEVEL", ""},
		{"LEVEL_3", ""},
	} {
		e := entity("k", tc.leaf)
		if err := rules.Enrich(device("X"), e); err != nil {
			t.Fatalf("%s: %v", tc.leaf, err)
		}
		if e.Description.Icon != tc.want {
			t.Errorf("%s: icon = %q, want %q", tc.leaf, e.Description.Icon, tc.want)
		}
	}

	// The leading underscore is optional in the rule; a table author writes
	// it both ways and neither reading is surprising.
	bare := catalog.Rules{{
		Match: catalog.Match{Postfix: model.Ptr("2")},
		Set:   catalog.Overlay{Icon: model.Ptr("mdi:two")},
	}}
	e := entity("k", "LEVEL_2")
	if err := bare.Enrich(device("X"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.Icon != "mdi:two" {
		t.Error("a postfix written without its underscore did not match")
	}
}

// TestNameContainsIsNotKeyContains: the two are different strings and a
// reference table uses both — a key is the consumer's identifier, a name is
// what an operator sees.
func TestNameContainsIsNotKeyContains(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{NameContains: model.Ptr("counter")},
		Set:   catalog.Overlay{Icon: model.Ptr("mdi:counter")},
	}}

	e := entity("svhmipraincounter", "RAIN")
	e.Description.Name = model.L("Rain Counter Today")
	if err := rules.Enrich(device("X"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.Icon != "mdi:counter" {
		t.Error("a rule keyed on the display name did not match it")
	}

	other := entity("counter_key", "X")
	other.Description.Name = model.L("Something else")
	if err := rules.Enrich(device("X"), other); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if other.Description.Icon != "" {
		t.Error("NameContains matched the key instead of the name")
	}
}

// TestMultiplierRidesOnTheDescription. The scale belongs to the entity, not
// to the datapoint: the same raw level is a fraction to one entity and a
// percentage to another, and the rule table is where that is written down.
func TestMultiplierRidesOnTheDescription(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{{
		Match: catalog.Match{Leaves: []string{"LEVEL"}},
		Set:   catalog.Overlay{Multiplier: model.Ptr(100.0)},
	}}
	e := entity("level", "LEVEL")
	if err := rules.Enrich(device("X"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.Multiplier == nil || *e.Description.Multiplier != 100.0 {
		t.Errorf("multiplier = %v, want 100", e.Description.Multiplier)
	}

	// nil is not 1.0: a rule that says nothing about scale and one that
	// pins it to unity are different statements where a later rule can
	// refine an earlier one.
	quiet := entity("other", "OTHER")
	if err := rules.Enrich(device("X"), quiet); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if quiet.Description.Multiplier != nil {
		t.Errorf("multiplier = %v on an entity no rule scaled", quiet.Description.Multiplier)
	}
}

// TestAFullySpecifiedOverlayReplacesTheWholeRecord is condition 1 of porting
// a first-match-wins table, and the one that decides byte-identity.
//
// A literal transcription leaves untouched fields nil, so a lower-priority
// rule's values survive into the higher-priority rule's result — the table
// being ported means the opposite, that the winning rule decides the whole
// record. Measured on one consumer: a device-specific FREQUENCY rule
// deliberately drops the device class its generic sibling sets, and a nil
// field hands it straight back.
func TestAFullySpecifiedOverlayReplacesTheWholeRecord(t *testing.T) {
	t.Parallel()

	generic := catalog.Rule{
		Priority: 0,
		Match:    catalog.Match{Leaves: []string{"FREQUENCY"}},
		Set: catalog.Overlay{
			DeviceClass: model.Ptr(model.DeviceClass("frequency")),
			Unit:        model.Ptr(model.Unit("Hz")),
		},
	}
	// The literal transcription: says nothing about the device class.
	naive := catalog.Rules{generic, {
		Priority: 10,
		Match:    catalog.Match{Leaves: []string{"FREQUENCY"}, Models: []string{"HMW-IO"}},
		Set:      catalog.Overlay{Unit: model.Ptr(model.Unit("mHz"))},
	}}
	e := entity("frequency", "FREQUENCY")
	if err := naive.Enrich(device("HMW-IO-12-Sw14-DR"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.DeviceClass != "frequency" {
		t.Fatal("the fixture no longer demonstrates the hazard")
	}

	// The faithful port: the winning rule states the empty device class.
	faithful := catalog.Rules{generic, {
		Priority: 10,
		Match:    catalog.Match{Leaves: []string{"FREQUENCY"}, Models: []string{"HMW-IO"}},
		Set: catalog.Overlay{
			DeviceClass: model.Ptr(model.DeviceClass("")),
			Unit:        model.Ptr(model.Unit("mHz")),
		},
	}}
	f := entity("frequency", "FREQUENCY")
	if err := faithful.Enrich(device("HMW-IO-12-Sw14-DR"), f); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if f.Description.DeviceClass != "" {
		t.Errorf("device_class = %q, want the winning rule's explicit empty",
			f.Description.DeviceClass)
	}
	if f.Description.Unit != "mHz" {
		t.Errorf("unit = %q", f.Description.Unit)
	}
}

// TestEqualPriorityAppliesInSliceOrderSoTheLastWins is condition 3, and the
// opposite of a table scanned top-down for the first hit.
func TestEqualPriorityAppliesInSliceOrderSoTheLastWins(t *testing.T) {
	t.Parallel()

	rules := catalog.Rules{
		{Match: catalog.Match{Leaves: []string{"X"}}, Set: catalog.Overlay{Icon: model.Ptr("mdi:first")}},
		{Match: catalog.Match{Leaves: []string{"X"}}, Set: catalog.Overlay{Icon: model.Ptr("mdi:last")}},
	}
	e := entity("x", "X")
	if err := rules.Enrich(device("D"), e); err != nil {
		t.Fatalf("Enrich: %v", err)
	}
	if e.Description.Icon != "mdi:last" {
		t.Errorf("icon = %q, want the LAST rule at equal priority", e.Description.Icon)
	}
}
