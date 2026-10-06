// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"encoding/json"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

func smartHomeContext(t *testing.T) discovery.StdContext {
	t.Helper()
	l, err := topic.NewSmartHome("daikin")
	if err != nil {
		t.Fatal(err)
	}
	return discovery.StdContext{Layout: l, Namespace: "daikin", Enc: discovery.StatusObjectEncoding}
}

func shEntity(platform hacatalog.Platform, key string, mode model.BindMode, opts *model.Enum) *model.Basic {
	e := &model.Basic{
		EntityKey:      key,
		EntityPlatform: platform,
		Description:    model.Description{Name: model.L(key), Options: opts},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S("uuid-1", "climateControl", model.BucketUnset, key),
			Mode: model.Read,
		}},
	}
	if mode.CanWrite() {
		e.Binds = append(e.Binds, model.Binding{
			Role: model.RoleCommand,
			Slot: model.S("uuid-1", "climateControl", model.BucketUnset, key),
			Mode: model.Write,
		})
	}
	return e
}

func renderOne(t *testing.T, ctx discovery.Context, e model.Entity) discovery.Component {
	t.Helper()
	comp, err := discovery.RenderComponent(ctx, testDevice(), e, discovery.Origin{})
	if err != nil {
		t.Fatal(err)
	}
	return comp
}

// TestStatusObjectValueTemplates: a status object is read at `val`, and a
// boolean one through `| lower`, because Jinja renders a JSON true as `True`
// and that matches no `payload_on` the convention publishes.
func TestStatusObjectValueTemplates(t *testing.T) {
	t.Parallel()

	ctx := smartHomeContext(t)
	cases := []struct {
		platform hacatalog.Platform
		want     string
	}{
		{hacatalog.PlatformSensor, `{{ value_json.val }}`},
		{hacatalog.Platform("binary_sensor"), `{{ value_json.val | lower }}`},
		{hacatalog.Platform("switch"), `{{ value_json.val | lower }}`},
		{hacatalog.Platform("number"), `{{ value_json.val }}`},
	}
	for _, c := range cases {
		comp := renderOne(t, ctx, shEntity(c.platform, "power", model.ReadWrite, nil))
		if comp.ValueTemplate != c.want {
			t.Errorf("%s: value_template = %q, want %q", c.platform, comp.ValueTemplate, c.want)
		}
		if comp.StateTopic != "daikin/status/uuid-1/climateControl/power" {
			t.Errorf("%s: state_topic = %q", c.platform, comp.StateTopic)
		}
	}
	comp := renderOne(t, ctx, shEntity(hacatalog.Platform("switch"), "power", model.ReadWrite, nil))
	if comp.CommandTopic != "daikin/set/uuid-1/climateControl/power" {
		t.Errorf("command_topic = %q, want the status item path under set", comp.CommandTopic)
	}
}

// TestStatusObjectEnumMapsTokensToLabels: the token is on the wire and the
// label in `options`, so both templates are needed and both read `val`. They
// are exactly what EnumTemplates renders for the `val` field, so the two
// directions cannot disagree.
func TestStatusObjectEnumMapsTokensToLabels(t *testing.T) {
	t.Parallel()

	modes := &model.Enum{
		Codes: []string{"heating", "cooling"},
		Labels: map[string]model.Localized{
			"heating": {Default: "Heizen"},
			"cooling": {Default: "Kühlen"},
		},
	}
	comp := renderOne(t, smartHomeContext(t), shEntity(hacatalog.PlatformSelect, "operation_mode", model.ReadWrite, modes))

	wantValue := `{% set m = {'heating': 'Heizen', 'cooling': 'Kühlen'} %}` +
		`{% if value_json is defined and value_json.val is not none %}{{ m.get(value_json.val, value_json.val) }}{% endif %}`
	wantCommand := `{% set m = {'Heizen': 'heating', 'Kühlen': 'cooling'} %}{{ m.get(value, value) }}`
	if comp.ValueTemplate != wantValue {
		t.Errorf("value_template =\n%s\nwant\n%s", comp.ValueTemplate, wantValue)
	}
	if comp.CommandTemplate != wantCommand {
		t.Errorf("command_template =\n%s\nwant\n%s", comp.CommandTemplate, wantCommand)
	}
	if v, c := discovery.EnumTemplates(modes, "", discovery.StatusValueField); v != wantValue || c != wantCommand {
		t.Error("the projection disagrees with EnumTemplates for the val field")
	}
	if strings.Join(comp.Options, ",") != "Heizen,Kühlen" {
		t.Errorf("options = %v", comp.Options)
	}

	// A sensor has no command template to carry.
	sensor := renderOne(t, smartHomeContext(t), shEntity(hacatalog.PlatformSensor, "operation_mode", model.Read, modes))
	if sensor.ValueTemplate != wantValue || sensor.CommandTemplate != "" {
		t.Errorf("enum sensor: %q / %q", sensor.ValueTemplate, sensor.CommandTemplate)
	}

	// Unlabelled options are their own labels: no mapping to do.
	plain := renderOne(t, smartHomeContext(t), shEntity(hacatalog.PlatformSelect, "fan",
		model.ReadWrite, &model.Enum{Codes: []string{"auto", "quiet"}}))
	if plain.ValueTemplate != discovery.StatusValueTemplate || plain.CommandTemplate != "" {
		t.Errorf("unlabelled select: %q / %q", plain.ValueTemplate, plain.CommandTemplate)
	}
}

// TestDescriptionTemplatesStillWin: the encoding is a default, and the one
// precedence rule holds under it too.
func TestDescriptionTemplatesStillWin(t *testing.T) {
	t.Parallel()

	e := shEntity(hacatalog.PlatformSelect, "mode", model.ReadWrite,
		&model.Enum{Codes: []string{"a"}, Labels: map[string]model.Localized{"a": {Default: "A"}}})
	e.Description.ValueTemplate = "{{ value_json.val.x }}"
	e.Description.CommandTemplate = "{{ value }}"
	comp := renderOne(t, smartHomeContext(t), e)
	if comp.ValueTemplate != "{{ value_json.val.x }}" || comp.CommandTemplate != "{{ value }}" {
		t.Errorf("got %q / %q", comp.ValueTemplate, comp.CommandTemplate)
	}
}

// TestSmartHomeAvailabilityEntries is spec §8 to the byte: the bridge entry
// reads `<name>/connected` and is available at 2, the device entry reads the
// device's `online` status item, mode `all`, and no entry carries a key
// beyond the four a list entry may have — Home Assistant rejects a whole
// device document over one.
func TestSmartHomeAvailabilityEntries(t *testing.T) {
	t.Parallel()

	comp := renderOne(t, smartHomeContext(t), shEntity(hacatalog.PlatformSensor, "temp", model.Read, nil))
	body, err := json.Marshal(struct {
		A []discovery.AvailabilityEntry `json:"availability"`
		M string                        `json:"availability_mode"`
	}{comp.Availability, comp.AvailabilityMode})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"availability":[` +
		`{"topic":"daikin/connected","value_template":"{{ 'online' if value | int(0) \u003e= 2 else 'offline' }}","payload_available":"online","payload_not_available":"offline"},` +
		`{"topic":"daikin/status/` + testDevice().UID() + `/online","value_template":"{{ value_json.val | lower }}","payload_available":"true","payload_not_available":"false"}` +
		`],"availability_mode":"all"}`
	if string(body) != want {
		t.Errorf("availability =\n%s\nwant\n%s", body, want)
	}

	full, err := json.Marshal(comp)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(full), "availability_template") || strings.Contains(string(full), "avty_tpl") {
		t.Errorf("availability_template present: %s", full)
	}
}

// TestConnectedAvailabilityThreshold: spec §8 lets an entity that works
// without the device be available at 1.
func TestConnectedAvailabilityThreshold(t *testing.T) {
	t.Parallel()

	e := discovery.ConnectedAvailability("x/connected", discovery.ConnectedBroker)
	if e.ValueTemplate != "{{ 'online' if value | int(0) >= 1 else 'offline' }}" {
		t.Errorf("template = %q", e.ValueTemplate)
	}
	if e.PayloadAvailable != discovery.PayloadOnline || e.PayloadNotAvailable != discovery.PayloadOffline {
		t.Errorf("payloads = %q/%q", e.PayloadAvailable, e.PayloadNotAvailable)
	}
	raw := discovery.OnlineAvailability("x/status/d/online", discovery.RawEncoding)
	if raw.ValueTemplate != "" || raw.PayloadAvailable != "true" {
		t.Errorf("plain online entry = %+v", raw)
	}
}

// TestSmartHomeSelfAvailabilityReadsVal: a RoleAvailability datapoint under
// the status-object encoding is a boolean status item, read like one; the
// envelope-flag fallback has no flag to read and is dropped.
func TestSmartHomeSelfAvailabilityReadsVal(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	ctx := discovery.StdContext{Layout: topic.Default{Root: "loom"}, Enc: discovery.StatusObjectEncoding}
	entry := onlyEntry(t, ctx.Availability(dev, selfEntity(dev.UID(), true)))
	if entry.ValueTemplate != discovery.StatusBoolValueTemplate {
		t.Errorf("value_template = %q", entry.ValueTemplate)
	}
	if got := ctx.Availability(dev, selfEntity(dev.UID(), false)); len(got) != 0 {
		t.Errorf("fallback rendered %+v, want nothing", got)
	}
}

// TestPlainLayoutIgnoresTheConventionVocabulary: the switch is the layout.
// The status-object encoding under topic.Default changes templates, not the
// availability vocabulary, so an encoding flip alone cannot repoint a fleet
// at `connected`.
func TestPlainLayoutIgnoresTheConventionVocabulary(t *testing.T) {
	t.Parallel()

	ctx := discovery.StdContext{Layout: topic.Default{Root: "daikin"}, Enc: discovery.StatusObjectEncoding}
	comp := renderOne(t, ctx, shEntity(hacatalog.PlatformSensor, "temp", model.Read, nil))
	if comp.Availability[0].Topic != "daikin/bridge/status" || comp.Availability[0].ValueTemplate != "" ||
		comp.Availability[0].PayloadAvailable != discovery.PayloadOnline {
		t.Errorf("bridge entry = %+v", comp.Availability[0])
	}
}

// TestSmartHomeBundleValidates: everything the convention projects is a key
// Home Assistant declares, so a bundle of it passes the module's own
// validator unchanged.
func TestSmartHomeBundleValidates(t *testing.T) {
	t.Parallel()

	modes := &model.Enum{Codes: []string{"a", "b"}, Labels: map[string]model.Localized{"a": {Default: "A"}}}
	entities := []model.Entity{
		shEntity(hacatalog.PlatformSensor, "temp", model.Read, nil),
		shEntity(hacatalog.Platform("switch"), "power", model.ReadWrite, nil),
		shEntity(hacatalog.Platform("binary_sensor"), "door", model.Read, nil),
		shEntity(hacatalog.PlatformSelect, "mode", model.ReadWrite, modes),
	}
	b, err := discovery.Render(smartHomeContext(t), testDevice(), entities, discovery.Origin{Name: "go-daikin2mqtt"})
	if err != nil {
		t.Fatal(err)
	}
	if err := discovery.Validate(b); err != nil {
		t.Errorf("Validate: %v", err)
	}
}
