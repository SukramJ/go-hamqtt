// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// validBundle is a minimal bundle that passes, so each test can break exactly
// one thing and see only that failure.
func validBundle() *discovery.Bundle {
	return &discovery.Bundle{
		NodeID: "serial_ac_1",
		Device: discovery.DeviceInfo{Identifiers: []string{"serial:AC-1"}, Name: "AC"},
		Origin: discovery.Origin{Name: "go-daikin2mqtt"},
		Components: map[string]discovery.Component{
			"power": {
				Platform:    hacatalog.PlatformSensor,
				UniqueID:    "daikin_serial_ac_1_power",
				Name:        "Power",
				StateTopic:  "daikin/serial:AC-1/values/power",
				DeviceClass: "power",
				StateClass:  hacatalog.StateClassMeasurement,
			},
		},
	}
}

func requireIssue(t *testing.T, b *discovery.Bundle, want string) {
	t.Helper()
	err := discovery.Validate(b)
	if err == nil {
		t.Fatalf("Validate accepted the bundle, want an issue containing %q", want)
	}
	if !errors.Is(err, discovery.ErrInvalidBundle) {
		t.Errorf("err does not match ErrInvalidBundle: %v", err)
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("err = %v\nwant it to mention %q", err, want)
	}
}

// TestValidateAcceptsAWellFormedBundle is the control: without it, a validator
// that rejected everything would pass every other test here.
func TestValidateAcceptsAWellFormedBundle(t *testing.T) {
	t.Parallel()
	if err := discovery.Validate(validBundle()); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// TestValidateRequiresOrigin pins the one field that separates a device bundle
// from the per-entity form, where origin is optional.
func TestValidateRequiresOrigin(t *testing.T) {
	t.Parallel()
	b := validBundle()
	b.Origin.Name = ""
	requireIssue(t, b, "origin.name is required")
}

// TestValidateRequiresADeviceIdentity mirrors Home Assistant's own refusal to
// register a device it cannot key.
func TestValidateRequiresADeviceIdentity(t *testing.T) {
	t.Parallel()
	b := validBundle()
	b.Device.Identifiers = nil
	requireIssue(t, b, "at least one identifier")
}

// TestValidateRequiresUniqueIDs guards the field every user customisation in
// Home Assistant hangs off.
func TestValidateRequiresUniqueIDs(t *testing.T) {
	t.Parallel()
	b := validBundle()
	comp := b.Components["power"]
	comp.UniqueID = ""
	b.Components["power"] = comp
	requireIssue(t, b, "unique_id is required")
}

// TestValidateCatchesDuplicateUniqueIDs is the collision Home Assistant
// resolves by dropping one entity without saying which.
func TestValidateCatchesDuplicateUniqueIDs(t *testing.T) {
	t.Parallel()
	b := validBundle()
	dup := b.Components["power"]
	dup.Name = "Power again"
	b.Components["power_2"] = dup
	requireIssue(t, b, "already used by")
}

// TestValidateRejectsAnUnknownPlatform stops a typo that would otherwise
// produce a bundle Home Assistant refuses whole.
func TestValidateRejectsAnUnknownPlatform(t *testing.T) {
	t.Parallel()
	b := validBundle()
	comp := b.Components["power"]
	comp.Platform = "sensr"
	b.Components["power"] = comp
	requireIssue(t, b, "not an MQTT-capable platform")
}

// TestValidateRejectsAKeyHomeAssistantWouldDrop is the check that has no other
// way of surfacing: Home Assistant's discovery schema removes unknown keys
// instead of complaining, so a typo silently costs a feature.
func TestValidateRejectsAKeyHomeAssistantWouldDrop(t *testing.T) {
	t.Parallel()
	b := validBundle()
	comp := b.Components["power"]
	comp.Extra = map[string]any{"state_topik": "typo/here"}
	b.Components["power"] = comp
	requireIssue(t, b, "not a valid key")
}

// TestValidateRejectsAnIllegalDeviceClass catches a device class borrowed from
// the wrong platform — the failure mode a single typed DeviceClass field would
// have made impossible to express in the first place.
func TestValidateRejectsAnIllegalDeviceClass(t *testing.T) {
	t.Parallel()
	b := validBundle()
	comp := b.Components["power"]
	comp.DeviceClass = "garage" // a cover class on a sensor
	comp.StateClass = ""
	b.Components["power"] = comp
	requireIssue(t, b, "is not valid for platform")
}

// TestValidateRejectsAnIllegalStateClass is the check that protects data
// rather than configuration: an energy sensor published as a measurement
// corrupts long-term statistics irreversibly.
func TestValidateRejectsAnIllegalStateClass(t *testing.T) {
	t.Parallel()
	b := validBundle()
	comp := b.Components["power"]
	comp.DeviceClass = "energy"
	comp.StateClass = hacatalog.StateClassMeasurement
	b.Components["power"] = comp
	requireIssue(t, b, "state_class")
}

// TestValidateEnforcesTheEnumRules mirrors Home Assistant's own sensor
// validator, whose logic cannot be extracted and has to be re-stated here.
func TestValidateEnforcesTheEnumRules(t *testing.T) {
	t.Parallel()

	t.Run("options need device_class enum", func(t *testing.T) {
		t.Parallel()
		b := validBundle()
		comp := b.Components["power"]
		comp.Options = []string{"a", "b"}
		b.Components["power"] = comp
		requireIssue(t, b, `options require device_class "enum"`)
	})

	t.Run("options exclude state_class", func(t *testing.T) {
		t.Parallel()
		b := validBundle()
		comp := b.Components["power"]
		comp.DeviceClass = "enum"
		comp.Options = []string{"a", "b"}
		b.Components["power"] = comp
		requireIssue(t, b, "cannot be combined with state_class")
	})
}

// TestValidateWarnsAboutTheMicroSign pins a deliberate non-failure.
//
// Home Assistant maps the legacy micro sign U+00B5 through AMBIGUOUS_UNITS in
// sensor/__init__.py's _native_unit_of_measurement_compat, and that mapping is
// a `.get(unit, unit)`: the old spelling is accepted and rewritten, not
// rejected. Reporting it as an error would fail a consumer's CI over twelve
// entities that work — the false alarm that gets a validator muted, and with
// it the real findings it was built for.
func TestValidateWarnsAboutTheMicroSign(t *testing.T) {
	t.Parallel()

	relations, err := hacatalog.LoadRelations()
	if err != nil {
		t.Fatalf("LoadRelations: %v", err)
	}
	var legacy, canonical string
	for from, to := range relations.AmbiguousUnits {
		if strings.ContainsRune(from, 'µ') && from != to {
			legacy, canonical = from, to
			break
		}
	}
	if legacy == "" {
		t.Skip("this Home Assistant snapshot normalises no micro-sign unit")
	}

	b := validBundle()
	comp := b.Components["power"]
	comp.DeviceClass = ""
	comp.StateClass = ""
	comp.UnitOfMeasure = legacy
	b.Components["power"] = comp

	err = discovery.Validate(b)
	if err == nil {
		t.Fatalf("the legacy spelling was not even reported")
	}
	if errors.Is(err, discovery.ErrInvalidBundle) {
		t.Errorf("the legacy spelling blocks the publish, but Home Assistant accepts it: %v", err)
	}
	if !errors.Is(err, discovery.ErrAdvisory) {
		t.Errorf("err does not match ErrAdvisory: %v", err)
	}
	if !strings.Contains(err.Error(), canonical) {
		t.Errorf("err = %v\nwant it to name %q", err, canonical)
	}
}

// TestValidateReportsEveryProblem pins the batching: fixing a catalog one
// error per test run is not a workflow anyone sustains.
func TestValidateReportsEveryProblem(t *testing.T) {
	t.Parallel()

	b := validBundle()
	b.Origin.Name = ""
	b.Device.Identifiers = nil
	comp := b.Components["power"]
	comp.DeviceClass = "energy"
	comp.StateClass = hacatalog.StateClassMeasurement
	b.Components["power"] = comp

	err := discovery.Validate(b)
	if err == nil {
		t.Fatal("Validate accepted a bundle with three problems")
	}
	var verr *discovery.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("err = %T, want *ValidationError", err)
	}
	if len(verr.Issues) < 3 {
		t.Errorf("Issues = %v, want all three problems reported at once", verr.Issues)
	}
}

// TestValidateAllowsARemovalMarker keeps the deletion form legal: a component
// carrying only a platform is how a bundle says "this entity is gone".
func TestValidateAllowsARemovalMarker(t *testing.T) {
	t.Parallel()

	b := validBundle()
	b.Remove(map[string]hacatalog.Platform{"old_sensor": hacatalog.PlatformSensor}, "old_sensor")
	if err := discovery.Validate(b); err != nil {
		t.Fatalf("Validate rejected a removal marker: %v", err)
	}
}

// TestRemovalRemembersTheEntityItDeletes pins the half of a removal that is
// not in the payload and must not be: the deleted entity's `unique_id`.
//
// A tombstone carries a platform and nothing else, so a consumer retracting
// the removed entity's old per-entity config by `unique_id` — the
// node-id-less legacy form measured across go-zendure2mqtt's fleet on
// 2026-09-12 — had nothing to key on and left the stale config retained,
// which re-creates the deleted entity as a permanently unavailable phantom on
// every MQTT-integration restart. The identity is remembered in
// Bundle.Tombstones instead, where it cannot un-remove anything.
func TestRemovalRemembersTheEntityItDeletes(t *testing.T) {
	t.Parallel()

	was := map[string]discovery.Component{
		"old_sensor": {Platform: hacatalog.PlatformSensor, UniqueID: "daikin_old_sensor"},
	}

	t.Run("RemoveComponents, for a key the new document no longer renders", func(t *testing.T) {
		t.Parallel()
		b := validBundle()
		b.RemoveComponents(was, "old_sensor")

		if got := b.Components["old_sensor"]; got.Platform != hacatalog.PlatformSensor || got.UniqueID != "" {
			t.Fatalf("payload entry = %+v, want a platform and nothing else", got)
		}
		if got := b.Tombstones["old_sensor"].UniqueID; got != "daikin_old_sensor" {
			t.Fatalf("remembered unique id = %q, want the removed entity's", got)
		}
		if err := discovery.Validate(b); err != nil {
			t.Fatalf("Validate rejected a removal marker: %v", err)
		}
	})

	t.Run("Remove, for a key that was still declared", func(t *testing.T) {
		t.Parallel()
		b := validBundle()
		b.Components["old_sensor"] = was["old_sensor"]
		b.Remove(map[string]hacatalog.Platform{"old_sensor": hacatalog.PlatformSensor}, "old_sensor")

		if got := b.Components["old_sensor"].UniqueID; got != "" {
			t.Fatalf("payload entry kept unique_id %q — that un-removes the entity", got)
		}
		if got := b.Tombstones["old_sensor"].UniqueID; got != "daikin_old_sensor" {
			t.Fatalf("remembered unique id = %q, want the overwritten entry's", got)
		}
	})

	t.Run("a component with no platform is skipped", func(t *testing.T) {
		t.Parallel()
		b := validBundle()
		b.RemoveComponents(map[string]discovery.Component{"ghost": {UniqueID: "u"}}, "ghost", "absent")
		if _, ok := b.Components["ghost"]; ok {
			t.Fatal("wrote a component with no platform; it marshals to {} and Home Assistant ignores it")
		}
		if _, ok := b.Tombstones["absent"]; ok {
			t.Fatal("remembered a key that was never handed over")
		}
	})

	t.Run("the remembered identity stays out of the payload", func(t *testing.T) {
		t.Parallel()
		b := validBundle()
		b.RemoveComponents(was, "old_sensor")
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if bytes.Contains(raw, []byte("daikin_old_sensor")) {
			t.Fatalf("payload %s carries the removed unique_id — the entity is not removed", raw)
		}
	})
}

// TestBundleTopicUsesTheConfiguredPrefix covers the option that exists because
// Home Assistant allows the prefix to be changed and every reference
// implementation hardcoded it.
func TestBundleTopicUsesTheConfiguredPrefix(t *testing.T) {
	t.Parallel()

	b := &discovery.Bundle{NodeID: "dev1"}
	if got, want := b.Topic(""), "homeassistant/device/dev1/config"; got != want {
		t.Errorf("Topic(\"\") = %q, want %q", got, want)
	}
	if got, want := b.Topic("ha"), "ha/device/dev1/config"; got != want {
		t.Errorf("Topic(\"ha\") = %q, want %q", got, want)
	}
}

// TestEnvelopeEncodingAddsAValueTemplate pins the decision that the envelope
// is the default, and that raw encoding omits the template rather than
// pointing it at nothing.
func TestEnvelopeEncodingAddsAValueTemplate(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := sensor("power", "power", dev.UID())

	envelope, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got := envelope.Components["power"].ValueTemplate; got != discovery.ValueTemplate {
		t.Errorf("envelope value_template = %q, want the guarded template", got)
	}

	rawCtx := testContext().(discovery.StdContext)
	rawCtx.Enc = discovery.RawEncoding
	bare, err := discovery.Render(rawCtx, dev, []model.Entity{sensor("power", "power", dev.UID())},
		discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got := bare.Components["power"].ValueTemplate; got != "" {
		t.Errorf("raw value_template = %q, want none", got)
	}
}
