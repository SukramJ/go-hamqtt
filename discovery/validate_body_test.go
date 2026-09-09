// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"errors"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// The cases below are not invented. Every one of them was found by running
// this validator over one consumer's 9,996 live discovery payloads, where they
// accounted for 195 broken entities that nothing in that project's test suite
// had ever reported — which is the whole argument for validating the
// per-entity form rather than only the bundle.
func TestValidateBodyFindsTheDefectsFoundInTheField(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		platform hacatalog.Platform
		body     map[string]any
		want     string
	}{
		"siren takes state_value_template, not value_template": {
			platform: hacatalog.PlatformSiren,
			body: map[string]any{
				"unique_id":      "u",
				"command_topic":  "x/set",
				"state_topic":    "x/state",
				"value_template": "{{ value_json.value }}",
			},
			want: `"value_template" is not a valid key`,
		},
		"button is stateless": {
			platform: hacatalog.PlatformButton,
			body: map[string]any{
				"unique_id":     "u",
				"command_topic": "x/set",
				"state_topic":   "x/state",
			},
			want: `"state_topic" is not a valid key`,
		},
		"translation_key is not an MQTT discovery key": {
			platform: hacatalog.PlatformSensor,
			body: map[string]any{
				"unique_id":       "u",
				"state_topic":     "x/state",
				"translation_key": "frequency",
			},
			want: `"translation_key" is not a valid key`,
		},
	} {
		err := discovery.ValidateBody(tc.platform, tc.body)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if !errors.Is(err, discovery.ErrInvalidBundle) {
			t.Errorf("%s: err does not match ErrInvalidBundle: %v", name, err)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to mention %q", name, err, tc.want)
		}
	}
}

// TestValidateBodyAcceptsTheCorrectedForms is the other half: the fix for each
// defect above must pass, or the validator is just noise.
func TestValidateBodyAcceptsTheCorrectedForms(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		platform hacatalog.Platform
		body     map[string]any
	}{
		"siren with the right template key": {
			platform: hacatalog.PlatformSiren,
			body: map[string]any{
				"unique_id":            "u",
				"command_topic":        "x/set",
				"state_topic":          "x/state",
				"state_value_template": "{{ value_json.value }}",
			},
		},
		"button without state": {
			platform: hacatalog.PlatformButton,
			body:     map[string]any{"unique_id": "u", "command_topic": "x/set"},
		},
		"sensor with the canonical micro sign": {
			platform: hacatalog.PlatformSensor,
			body: map[string]any{
				"unique_id":           "u",
				"state_topic":         "x/state",
				"unit_of_measurement": "μg/m³", // U+03BC GREEK SMALL LETTER MU
			},
		},
	} {
		if err := discovery.ValidateBody(tc.platform, tc.body); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestValidateBodyRejectsAnUnknownPlatform keeps a typo in the component
// segment from reading as "no problems found".
func TestValidateBodyRejectsAnUnknownPlatform(t *testing.T) {
	t.Parallel()

	if err := discovery.ValidateBody("sensr", map[string]any{"unique_id": "u"}); err == nil {
		t.Fatal("a misspelled platform validated")
	}
	if err := discovery.ValidateBody("", map[string]any{"unique_id": "u"}); err == nil {
		t.Fatal("an empty platform validated")
	}
}

// TestValidateBodyAcceptsARemoval pins the deletion marker: a body carrying
// nothing but its platform is how a consumer retires an entity, and demanding
// a unique_id there would make retirement impossible.
func TestValidateBodyAcceptsARemoval(t *testing.T) {
	t.Parallel()

	if err := discovery.ValidateBody(hacatalog.PlatformSensor, map[string]any{}); err != nil {
		t.Errorf("a removal marker was rejected: %v", err)
	}
}

// TestValidateBodyAndValidateAgree is the property that makes one set of rules
// credible: the same entity checked through a bundle and through its raw body
// must produce the same verdict — including the severity.
func TestValidateBodyAndValidateAgree(t *testing.T) {
	t.Parallel()

	comp := discovery.Component{
		Platform:      hacatalog.PlatformSensor,
		UniqueID:      "u",
		StateTopic:    "x/state",
		UnitOfMeasure: "µg/m³",
	}
	bundle := &discovery.Bundle{
		NodeID:     "n",
		Device:     discovery.DeviceInfo{Identifiers: []string{"n"}},
		Origin:     discovery.Origin{Name: "test"},
		Components: map[string]discovery.Component{"s": comp},
	}
	viaBundle := discovery.Validate(bundle)
	viaBody := discovery.ValidateBody(comp.Platform, map[string]any{
		"unique_id":           comp.UniqueID,
		"state_topic":         comp.StateTopic,
		"unit_of_measurement": comp.UnitOfMeasure,
	})
	if viaBundle == nil || viaBody == nil {
		t.Fatalf("one path stayed silent about the micro sign: bundle=%v body=%v", viaBundle, viaBody)
	}
	if errors.Is(viaBundle, discovery.ErrInvalidBundle) != errors.Is(viaBody, discovery.ErrInvalidBundle) {
		t.Errorf("the two paths disagree on severity:\n bundle: %v\n body:   %v", viaBundle, viaBody)
	}
	if !strings.Contains(viaBundle.Error(), "rewrites it") ||
		!strings.Contains(viaBody.Error(), "rewrites it") {
		t.Errorf("the two paths disagree:\n bundle: %v\n body:   %v", viaBundle, viaBody)
	}
}

// TestDispatchingPlatformPicksItsVariant is the case that made this necessary.
// `light` has no flat key set — three sub-schemas selected by the body's own
// `schema` key — and checking a body against the union of all three demands
// the template schema's command_on_template of a json-schema light. That is a
// false alarm on 62 real entities in one consumer's corpus, and a validator
// that cries wolf is one nobody reads.
func TestDispatchingPlatformPicksItsVariant(t *testing.T) {
	t.Parallel()

	jsonLight := map[string]any{
		"unique_id":     "u",
		"schema":        "json",
		"command_topic": "x/set",
		"brightness":    true,
	}
	if err := discovery.ValidateBody(hacatalog.PlatformLight, jsonLight); err != nil {
		t.Errorf("a json-schema light was rejected: %v", err)
	}

	// The variant is picked, not merely tolerated: a key that belongs to a
	// different sub-schema must still be reported.
	jsonLight["command_on_template"] = "{{ 1 }}"
	err := discovery.ValidateBody(hacatalog.PlatformLight, jsonLight)
	if err == nil {
		t.Fatal("a template-schema key on a json light was accepted")
	}
	if !strings.Contains(err.Error(), "command_on_template") {
		t.Errorf("err = %v, want it to name command_on_template", err)
	}

	// A body that names no variant cannot be held to any variant's required
	// keys, but its keys are still checked against their union.
	noSchema := map[string]any{"unique_id": "u", "command_topic": "x/set", "brightness": true}
	if err := discovery.ValidateBody(hacatalog.PlatformLight, noSchema); err != nil {
		t.Errorf("a light naming no schema was rejected: %v", err)
	}
	if err := discovery.ValidateBody(hacatalog.PlatformLight,
		map[string]any{"unique_id": "u", "command_topic": "x/set", "nonsense": 1}); err == nil {
		t.Error("an invented key on a variant-less light was accepted")
	}
}
