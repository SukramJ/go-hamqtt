// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"encoding/json"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// The three defects below were found by mapping openccu-loom onto this module
// before migrating it — the reason ADR 0070 sequences the hardest consumer
// first. Each would have shipped and then failed on loom's first day.

// TestSelectIsValid is defect (a). `select` requires `options` and declares no
// `device_class` at all, so the sensor rule "options require device_class enum"
// rejected every select ever built. Because an invalid bundle publishes nothing
// for the whole device, one enum parameter would have silenced all ~40 entities
// of a thermostat.
func TestSelectIsValid(t *testing.T) {
	t.Parallel()

	b := validBundle()
	b.Components["mode"] = discovery.Component{
		Platform:     hacatalog.PlatformSelect,
		UniqueID:     "daikin_serial_ac_1_mode",
		Name:         "Mode",
		StateTopic:   "daikin/serial:AC-1/values/mode",
		CommandTopic: "daikin/serial:AC-1/values/mode/set",
		Options:      []string{"off", "heat", "cool"},
	}
	if err := discovery.Validate(b); err != nil {
		t.Fatalf("Validate rejected a legal select: %v", err)
	}
}

// TestSensorOptionsStillNeedEnum keeps the rule where it belongs: Home
// Assistant's own sensor validator does enforce it, and losing that would trade
// one false positive for a real one.
func TestSensorOptionsStillNeedEnum(t *testing.T) {
	t.Parallel()

	b := validBundle()
	comp := b.Components["power"]
	comp.DeviceClass = "power"
	comp.StateClass = ""
	comp.Options = []string{"a", "b"}
	b.Components["power"] = comp
	requireIssue(t, b, `options require device_class "enum"`)
}

// builderExtra writes a platform key that has no typed field — the case Extra
// exists for, and the only route available to the 16 platforms that have no
// Fields struct yet.
type builderExtra struct{ model.Basic }

func (b *builderExtra) BuildDiscovery(ctx discovery.Context, comp *discovery.Component) error {
	if comp.Extra == nil {
		comp.Extra = map[string]any{}
	}
	comp.Extra["payload_on"] = "ON"
	return nil
}

// TestBuilderExtraSurvives is defect (b). renderComponent assigned
// `comp.Extra = desc.Extra` *after* running the Builder, so anything a Builder
// put there was discarded — closing the escape hatch exactly where the
// platforms without a typed Fields struct need it.
func TestBuilderExtraSurvives(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := &builderExtra{}
	e.EntityKey = "contact"
	e.EntityPlatform = hacatalog.PlatformBinarySensor
	e.Description = model.Description{
		Name:  model.L("Contact"),
		Extra: map[string]any{"off_delay": 30},
	}
	e.Binds = []model.Binding{{
		Role: model.RoleState,
		Slot: model.S(dev.UID(), "", model.BucketValues, "contact"),
		Mode: model.Read,
	}}

	bundle, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	raw, err := json.Marshal(bundle.Components["contact"])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["payload_on"] != "ON" {
		t.Errorf("payload_on = %v — the Builder's Extra was discarded", got["payload_on"])
	}
	if got["off_delay"] == nil {
		t.Errorf("off_delay missing — the Description's Extra was lost")
	}
}

// TestDescriptionExtraOutranksTheBuilder pins the documented precedence:
// Description.Extra is the last word, so the two Extras must merge with the
// description winning, not one replacing the other.
func TestDescriptionExtraOutranksTheBuilder(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := &builderExtra{}
	e.EntityKey = "contact"
	e.EntityPlatform = hacatalog.PlatformBinarySensor
	e.Description = model.Description{
		Name:  model.L("Contact"),
		Extra: map[string]any{"payload_on": "OPEN"}, // same key as the Builder
	}

	bundle, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got := bundle.Components["contact"].Extra["payload_on"]; got != "OPEN" {
		t.Errorf("payload_on = %v, want the Description to win", got)
	}
}

// TestBoundsLandOnLegalKeys is defect (c). Min/Max/Step were projected onto
// `min`/`max`/`step` for every platform, but those keys exist only on `number`
// (and min/max on `text`). A climate entity with bounds produced three keys
// Home Assistant would drop, and there was no way to carry bounds on a climate
// Description at all.
func TestBoundsLandOnLegalKeys(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	desc := model.Description{
		Name: model.L("Setpoint"),
		Min:  model.Ptr(5.0),
		Max:  model.Ptr(30.0),
		Step: model.Ptr(0.5),
	}

	slot := model.S(dev.UID(), "", model.BucketValues, "setpoint")
	rw := []model.Binding{
		{Role: model.RoleState, Slot: slot, Mode: model.Read},
		{Role: model.RoleCommand, Slot: slot, Mode: model.Write},
	}
	ro := []model.Binding{{Role: model.RoleState, Slot: slot, Mode: model.Read}}

	t.Run("number keeps them", func(t *testing.T) {
		t.Parallel()
		e := &model.Basic{EntityKey: "setpoint", EntityPlatform: hacatalog.PlatformNumber, Description: desc, Binds: rw}
		b, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		comp := b.Components["setpoint"]
		if comp.Min == nil || comp.Max == nil || comp.Step == nil {
			t.Error("number lost its bounds")
		}
		if err := discovery.Validate(b); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})

	t.Run("climate does not emit illegal keys", func(t *testing.T) {
		t.Parallel()
		e := &model.Basic{EntityKey: "thermostat", EntityPlatform: hacatalog.PlatformClimate, Description: desc, Binds: ro}
		b, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if err := discovery.Validate(b); err != nil {
			t.Fatalf("Validate rejected a climate carrying bounds: %v", err)
		}
	})

	t.Run("sensor does not emit illegal keys", func(t *testing.T) {
		t.Parallel()
		e := &model.Basic{EntityKey: "reading", EntityPlatform: hacatalog.PlatformSensor, Description: desc, Binds: ro}
		b, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
		if err != nil {
			t.Fatalf("Render: %v", err)
		}
		if err := discovery.Validate(b); err != nil {
			t.Fatalf("Validate rejected a sensor carrying bounds: %v", err)
		}
	})
}

// TestPlatformsWithoutPlainTopicsStayClean is the fourth finding, and it came
// out of fixing the third: ten platforms declare no `state_topic` and eleven no
// `command_topic` — climate, water_heater and lawn_mower name a topic per role;
// button, scene and notify are write-only. The default projection emitted them
// regardless, so every such entity carried keys Home Assistant drops in
// silence.
func TestPlatformsWithoutPlainTopicsStayClean(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	slot := model.S(dev.UID(), "", model.BucketValues, "x")
	binds := []model.Binding{
		{Role: model.RoleState, Slot: slot, Mode: model.Read},
		{Role: model.RoleCommand, Slot: slot, Mode: model.Write},
	}

	for _, platform := range []hacatalog.Platform{
		hacatalog.PlatformClimate,
		hacatalog.PlatformWaterHeater,
		hacatalog.PlatformButton,
		hacatalog.PlatformScene,
	} {
		e := &model.Basic{
			EntityKey:      "e",
			EntityPlatform: platform,
			Description:    model.Description{Name: model.L("E")},
			Binds:          binds,
		}
		b, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
		if err != nil {
			t.Fatalf("%s: Render: %v", platform, err)
		}
		comp := b.Components["e"]
		if comp.StateTopic != "" && !schemaHas(t, platform, "state_topic") {
			t.Errorf("%s: emitted state_topic, which the platform does not declare", platform)
		}
		if comp.CommandTopic != "" && !schemaHas(t, platform, "command_topic") {
			t.Errorf("%s: emitted command_topic, which the platform does not declare", platform)
		}
	}
}

// TestPlatformsWithPlainTopicsKeepThem is the control: guarding the projection
// must not silence the platforms that do declare the keys.
func TestPlatformsWithPlainTopicsKeepThem(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	slot := model.S(dev.UID(), "", model.BucketValues, "x")
	e := &model.Basic{
		EntityKey:      "e",
		EntityPlatform: hacatalog.PlatformSwitch,
		Description:    model.Description{Name: model.L("E")},
		Binds: []model.Binding{
			{Role: model.RoleState, Slot: slot, Mode: model.Read},
			{Role: model.RoleCommand, Slot: slot, Mode: model.Write},
		},
	}
	b, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	comp := b.Components["e"]
	if comp.StateTopic == "" || comp.CommandTopic == "" || comp.ValueTemplate == "" {
		t.Errorf("switch lost a topic it declares: state=%q command=%q template=%q",
			comp.StateTopic, comp.CommandTopic, comp.ValueTemplate)
	}
	if err := discovery.Validate(b); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func schemaHas(t *testing.T, platform hacatalog.Platform, key string) bool {
	t.Helper()
	mqtt, err := hacatalog.LoadMQTT()
	if err != nil {
		t.Fatalf("LoadMQTT: %v", err)
	}
	_, ok := mqtt.Platforms[string(platform)].Keys[key]
	return ok
}
