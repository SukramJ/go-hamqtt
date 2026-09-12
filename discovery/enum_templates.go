// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"strings"

	"github.com/SukramJ/go-hamqtt/model"
)

// PresetModeField is the field a climate entity's curated state document
// carries its preset in, and the one [PresetModeTemplates] reads.
const PresetModeField = "preset_mode"

// JinjaQuote renders s as a single-quoted Jinja string literal, escaping the
// backslash and the quote.
//
// Exported because every consumer that builds a template by hand needs it and
// the measured one had it as a private helper, four call sites from the
// templates it produced. A label is operator- or catalogue-authored text: an
// apostrophe in it ("Nacht'modus") closes the literal and Home Assistant logs
// a template error against a retained payload nobody is looking at.
func JinjaQuote(s string) string {
	escaped := strings.ReplaceAll(s, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return `'` + escaped + `'`
}

// PresetModeTemplates renders the `preset_mode_value_template` and
// `preset_mode_command_template` pair for a climate entity whose presets carry
// display labels.
//
// The measured need: the climate plane of the first full consumer hand-rolls
// both dictionaries in one function, having lifted its preset list into a
// [model.Enum] precisely because that is the one model type pairing a code the
// device speaks with a label a person reads. [model.Enum.Options] already
// emits the outbound half of that pair — the `preset_modes` list — and
// [model.Enum.Code] is already the reverse lookup, so the command template was
// a second, string-formatted implementation of a method the model ships.
//
// It is the pair or nothing. A localised list needs both: without the value
// template the entity reports a state that is not in its own option list and
// Home Assistant logs it as invalid, and without the command template the
// label travels back to a device that knows only the code.
//
// Both dictionaries are built from the same [model.Enum] in one pass, which is
// what makes them unable to disagree about which label belongs to which code.
// The reference implementation's earlier shape — two index-aligned slices —
// could, and a reorder of one of them renamed a preset in one direction only.
//
// Both fall back to the incoming value on a miss (`m.get(value, value)`), so a
// preset that appears after this payload was retained passes through instead
// of resolving to nothing.
//
// An enum with no codes yields no templates: an empty dictionary would map
// every state to nothing at all, which is a worse answer than the platform's
// own default.
func PresetModeTemplates(e *model.Enum, lang string) (valueTemplate, commandTemplate string) {
	return EnumTemplates(e, lang, PresetModeField)
}

// EnumTemplates is [PresetModeTemplates] for any round-trip key: the value
// template maps the codes a device publishes onto the labels Home Assistant
// shows, and the command template maps them back.
//
// field names the key inside the entity's own state document, the way
// `preset_mode` names it for a climate — the platform's role keys
// (`fan_mode`, `swing_mode`, `mode`) are the same shape and the same problem.
// An EMPTY field is the raw-encoding case, where the payload is the value
// itself rather than a document to read a field out of; the guard
// [ValueTemplate] carries is then pointless, since there is no `value_json` to
// be undefined.
//
// The labels come from lang, so the templates and the `options`/`preset_modes`
// list [model.Enum.Options] emits are the same strings by construction. A code
// with no label maps to itself, which is what leaves Home Assistant's own
// translations in play for the presets it defines — replacing those with a
// label from a consumer's catalogue freezes one language into a retained
// payload.
func EnumTemplates(e *model.Enum, lang, field string) (valueTemplate, commandTemplate string) {
	if e == nil || len(e.Codes) == 0 {
		return "", ""
	}

	var state, command strings.Builder
	state.WriteString(`{% set m = {`)
	command.WriteString(`{% set m = {`)
	for i, code := range e.Codes {
		if i > 0 {
			state.WriteString(", ")
			command.WriteString(", ")
		}
		// One lookup feeds both directions, so the two dictionaries cannot
		// disagree about which label belongs to which code.
		label := e.Label(code, lang)
		state.WriteString(JinjaQuote(code) + ": " + JinjaQuote(label))
		command.WriteString(JinjaQuote(label) + ": " + JinjaQuote(code))
	}

	if field == "" {
		state.WriteString(`} %}{{ m.get(value, value) }}`)
	} else {
		read := "value_json." + field
		state.WriteString(`} %}{% if value_json is defined and ` + read + ` is not none %}` +
			`{{ m.get(` + read + `, ` + read + `) }}{% endif %}`)
	}
	command.WriteString(`} %}{{ m.get(value, value) }}`)
	return state.String(), command.String()
}
