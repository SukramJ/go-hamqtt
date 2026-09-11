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

func parityBundle() *discovery.Bundle {
	return &discovery.Bundle{
		NodeID: "loom_0001abc",
		Device: discovery.DeviceInfo{Identifiers: []string{"openccu-loom_0001abc"}},
		Origin: discovery.Origin{Name: "openccu-loom"},
		Components: map[string]discovery.Component{
			"temperature": {
				Platform:   hacatalog.PlatformSensor,
				UniqueID:   "loom_0001abc_temperature",
				StateTopic: "loom/0001abc/1/values/ACTUAL_TEMPERATURE",
				// Published on purpose: the consumer's cross-stack parity
				// tooling compares against the Python integration it
				// mirrors. Home Assistant declares this key on no platform
				// and discards it.
				Extra: map[string]any{"translation_key": "temperature"},
			},
		},
	}
}

// TestADeliberateKeyStillFailsValidateByDefault. The validator is right about
// `translation_key` — Home Assistant drops it — and a tool that did not make
// the consumer's judgement must still say so. Ignoring has to be asked for.
func TestADeliberateKeyStillFailsValidateByDefault(t *testing.T) {
	t.Parallel()

	err := discovery.Validate(parityBundle())
	if err == nil {
		t.Fatal("Validate stopped reporting a key Home Assistant drops")
	}
	if !errors.Is(err, discovery.ErrInvalidBundle) {
		t.Errorf("err = %v, want it blocking", err)
	}
	if !strings.Contains(err.Error(), "translation_key") {
		t.Errorf("err does not name the key: %v", err)
	}
}

// TestIgnoringADeclaredKeyClearsTheBundle is the case this exists for. On a
// real fleet that one key was every blocking finding on 164 of 9,996
// entities and turned 64 of 398 bundles Blocking() — and an invalid bundle
// publishes NOTHING, so a consumer gating its publish on the validator would
// have withheld a sixth of its devices.
func TestIgnoringADeclaredKeyClearsTheBundle(t *testing.T) {
	t.Parallel()

	if err := discovery.ValidateIgnoring(parityBundle(),
		map[string]bool{"translation_key": true}); err != nil {
		t.Errorf("ValidateIgnoring: %v", err)
	}
}

// TestIgnoringOneKeyDoesNotSilenceTheRest: the set is an exception list, not
// an off switch. A second undeclared key must still be reported.
func TestIgnoringOneKeyDoesNotSilenceTheRest(t *testing.T) {
	t.Parallel()

	b := parityBundle()
	comp := b.Components["temperature"]
	comp.Extra["object_id"] = "sensor_temperature"
	b.Components["temperature"] = comp

	err := discovery.ValidateIgnoring(b, map[string]bool{"translation_key": true})
	if err == nil {
		t.Fatal("a second undeclared key was swallowed with the declared one")
	}
	if strings.Contains(err.Error(), "translation_key") {
		t.Errorf("the ignored key was reported after all: %v", err)
	}
	if !strings.Contains(err.Error(), "object_id") {
		t.Errorf("err does not name the key that is not excused: %v", err)
	}
}

// TestIgnoringDoesNotReachOtherChecks. The set excuses a key from the
// unknown-key check and nothing else — a missing required key is still a
// missing required key, and an entity without it does not work.
func TestIgnoringDoesNotReachOtherChecks(t *testing.T) {
	t.Parallel()

	b := parityBundle()
	b.Components["broken"] = discovery.Component{
		Platform: hacatalog.PlatformSwitch,
		UniqueID: "loom_0001abc_broken",
	}
	err := discovery.ValidateIgnoring(b, map[string]bool{"translation_key": true, "command_topic": true})
	if err == nil || !strings.Contains(err.Error(), "command_topic") {
		t.Errorf("a required key was excused by the ignore set: %v", err)
	}
}

// TestValidateBodyIgnoringIsTheSameRuleForThePerEntityForm, which is the form
// the consumer with this key actually publishes.
func TestValidateBodyIgnoringIsTheSameRuleForThePerEntityForm(t *testing.T) {
	t.Parallel()

	body := map[string]any{
		"unique_id":       "loom_0001abc_temperature",
		"state_topic":     "loom/0001abc/1/values/ACTUAL_TEMPERATURE",
		"translation_key": "temperature",
	}
	if err := discovery.ValidateBody(hacatalog.PlatformSensor, body); err == nil {
		t.Error("ValidateBody stopped reporting the key by default")
	}
	if err := discovery.ValidateBodyIgnoring(hacatalog.PlatformSensor, body,
		map[string]bool{"translation_key": true}); err != nil {
		t.Errorf("ValidateBodyIgnoring: %v", err)
	}
}
