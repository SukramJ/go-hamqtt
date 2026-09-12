// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// The two strings the consumer's climate plane has pinned. Copied verbatim
// from the `climate/thermostat` fixture of openccu-loom's
// internal/north/mqtt/testdata/discovery_golden_aggregate.json, which is a
// byte-for-byte pin of a payload already retained on brokers: a divergence
// here moves a published payload, which is the one thing the extraction
// promised not to do.
const (
	pinnedPresetModeValueTemplate = `{% set m = {'boost': 'boost', 'week_program_1': 'Week program 1', ` +
		`'week_program_2': 'Week program 2', 'week_program_3': 'Week program 3', ` +
		`'week_program_4': 'Week program 4', 'week_program_5': 'Week program 5', ` +
		`'week_program_6': 'Week program 6'} %}` +
		`{% if value_json is defined and value_json.preset_mode is not none %}` +
		`{{ m.get(value_json.preset_mode, value_json.preset_mode) }}{% endif %}`

	pinnedPresetModeCommandTemplate = `{% set m = {'boost': 'boost', 'Week program 1': 'week_program_1', ` +
		`'Week program 2': 'week_program_2', 'Week program 3': 'week_program_3', ` +
		`'Week program 4': 'week_program_4', 'Week program 5': 'week_program_5', ` +
		`'Week program 6': 'week_program_6'} %}{{ m.get(value, value) }}`
)

// thermostatPresets is the vocabulary that fixture's thermostat carries: the
// `boost` preset Home Assistant translates itself, left unlabelled on purpose,
// and six week programs it has never heard of.
func thermostatPresets() *model.Enum {
	vocab := &model.Enum{
		Codes:  []string{"boost", "week_program_1", "week_program_2", "week_program_3", "week_program_4", "week_program_5", "week_program_6"},
		Labels: map[string]model.Localized{},
	}
	for i, code := range vocab.Codes[1:] {
		vocab.Labels[code] = model.L("Week program " + string(rune('1'+i)))
	}
	return vocab
}

// TestPresetModeTemplatesMatchTheConsumersPinnedPayload. The measured need:
// the climate plane hand-rolls both round-trip dictionaries although
// [model.Enum] already pairs the codes with the labels and
// [model.Enum.Code] is already the reverse lookup the command template
// re-implements as string formatting.
//
// The bytes are the test. That plane's payload is pinned in the consumer, so
// this helper is only usable if it reproduces the pin exactly — key order,
// separators, quoting, the `is not none` guard and both fallbacks included.
func TestPresetModeTemplatesMatchTheConsumersPinnedPayload(t *testing.T) {
	t.Parallel()

	value, command := discovery.PresetModeTemplates(thermostatPresets(), "")

	if value != pinnedPresetModeValueTemplate {
		t.Errorf("preset_mode_value_template diverges from the pinned payload:\n got %q\nwant %q",
			value, pinnedPresetModeValueTemplate)
	}
	if command != pinnedPresetModeCommandTemplate {
		t.Errorf("preset_mode_command_template diverges from the pinned payload:\n got %q\nwant %q",
			command, pinnedPresetModeCommandTemplate)
	}
}

// TestThePresetListAndTheTemplatesAgree. The list Home Assistant shows comes
// from [model.Enum.Options] and the templates from the same enum, so the
// labels in the payload's `preset_modes` and the labels the dictionaries map
// to are the same strings by construction. A state not in its own option list
// is logged as invalid and the entity shows nothing.
func TestThePresetListAndTheTemplatesAgree(t *testing.T) {
	t.Parallel()

	vocab := thermostatPresets()
	_, command := discovery.PresetModeTemplates(vocab, "")
	for _, label := range vocab.Options("") {
		if !strings.Contains(command, discovery.JinjaQuote(label)+": ") {
			t.Errorf("option %q is offered to Home Assistant but the command template cannot map it back", label)
		}
	}
	// And the reverse lookup the template replaces still agrees with it.
	if code, ok := vocab.Code("Week program 3"); !ok || code != "week_program_3" {
		t.Errorf("Enum.Code disagrees with the template: %q, %v", code, ok)
	}
}

// TestALabelWithAnApostropheIsEscaped. Labels are catalogue- or
// operator-authored text. An unescaped apostrophe closes the Jinja literal,
// and the whole diagnostic is a template error logged against a retained
// payload nobody is watching.
func TestALabelWithAnApostropheIsEscaped(t *testing.T) {
	t.Parallel()

	vocab := &model.Enum{
		Codes:  []string{"night"},
		Labels: map[string]model.Localized{"night": model.L(`Nacht'modus\x`)},
	}
	value, command := discovery.PresetModeTemplates(vocab, "")
	for _, tmpl := range []string{value, command} {
		if !strings.Contains(tmpl, `'Nacht\'modus\\x'`) {
			t.Errorf("the label is not escaped: %q", tmpl)
		}
	}
}

// TestAnEmptyEnumYieldsNoTemplates. An empty dictionary maps every state to
// nothing at all, which is a worse answer than the platform's own default —
// and a consumer whose whole option list is Home Assistant's own vocabulary
// must be able to publish no templates, or its translations are replaced by
// one frozen language.
func TestAnEmptyEnumYieldsNoTemplates(t *testing.T) {
	t.Parallel()

	for _, vocab := range []*model.Enum{nil, {}, {Codes: []string{}}} {
		value, command := discovery.PresetModeTemplates(vocab, "")
		if value != "" || command != "" {
			t.Errorf("%#v yielded templates: %q / %q", vocab, value, command)
		}
	}
}

// TestEnumTemplatesReadTheNamedFieldOrTheBareValue. The same round trip is
// needed for every platform role key, and a consumer publishing bare values
// has no document to read a field out of — where the `value_json is defined`
// guard is not merely unnecessary but wrong, since there is no value_json.
func TestEnumTemplatesReadTheNamedFieldOrTheBareValue(t *testing.T) {
	t.Parallel()

	vocab := &model.Enum{
		Codes:  []string{"auto"},
		Labels: map[string]model.Localized{"auto": model.L("Automatik")},
	}

	value, _ := discovery.EnumTemplates(vocab, "", "fan_mode")
	want := `{% set m = {'auto': 'Automatik'} %}` +
		`{% if value_json is defined and value_json.fan_mode is not none %}` +
		`{{ m.get(value_json.fan_mode, value_json.fan_mode) }}{% endif %}`
	if value != want {
		t.Errorf("fan_mode value template:\n got %q\nwant %q", value, want)
	}

	raw, rawCommand := discovery.EnumTemplates(vocab, "", "")
	wantRaw := `{% set m = {'auto': 'Automatik'} %}{{ m.get(value, value) }}`
	if raw != wantRaw {
		t.Errorf("raw value template:\n got %q\nwant %q", raw, wantRaw)
	}
	if rawCommand != `{% set m = {'Automatik': 'auto'} %}{{ m.get(value, value) }}` {
		t.Errorf("raw command template: %q", rawCommand)
	}
}

// TestTheTemplatesFollowTheLanguage. The pair must speak the language the
// option list was rendered in, or Home Assistant is handed a label the
// dictionaries do not carry.
func TestTheTemplatesFollowTheLanguage(t *testing.T) {
	t.Parallel()

	vocab := &model.Enum{
		Codes: []string{"eco"},
		Labels: map[string]model.Localized{
			"eco": {Default: "Eco", Lang: map[string]string{"de": "Sparmodus"}},
		},
	}
	value, command := discovery.PresetModeTemplates(vocab, "de")
	if !strings.Contains(value, `'eco': 'Sparmodus'`) || !strings.Contains(command, `'Sparmodus': 'eco'`) {
		t.Errorf("the templates ignored the language:\n%q\n%q", value, command)
	}
	if !reflect.DeepEqual(vocab.Options("de"), []string{"Sparmodus"}) {
		t.Errorf("Options disagrees: %v", vocab.Options("de"))
	}
}
