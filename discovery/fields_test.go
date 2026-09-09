// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// TestFieldsStructsCarryOnlyLegalKeys is the guard the reference
// implementations lacked. Home Assistant's discovery schema is
// extra=REMOVE_EXTRA: a key a platform does not declare is dropped without an
// error on the wire or a line in any log, so a typo costs a feature and leaves
// nothing to debug. The catalog knows the exact key set per platform, so a
// struct claiming a key that platform does not accept is a compile-time-shaped
// bug this test turns into a test failure.
func TestFieldsStructsCarryOnlyLegalKeys(t *testing.T) {
	t.Parallel()

	mqtt, err := hacatalog.LoadMQTT()
	if err != nil {
		t.Fatalf("LoadMQTT: %v", err)
	}
	if len(discovery.FieldsIndex) == 0 {
		t.Fatal("FieldsIndex is empty — the generator produced nothing")
	}

	for name, zero := range discovery.FieldsIndex {
		allowed, ok := catalogKeys(mqtt, name)
		if !ok {
			t.Errorf("%s: no schema in the catalog", name)
			continue
		}
		for _, key := range jsonTags(reflect.TypeOf(zero)) {
			if _, legal := allowed[key]; !legal {
				t.Errorf("%s: %q is not a key that platform accepts — Home Assistant would drop it silently",
					name, key)
			}
		}
	}
}

// TestFieldsStructsCoverThePlatform reports keys the catalog accepts that no
// struct models. It logs rather than fails on purpose: a Home Assistant release
// that adds a key must not turn this module's test suite red before anyone has
// a reason to use it. A failure here would make every consumer's CI depend on
// the upstream release schedule.
func TestFieldsStructsCoverThePlatform(t *testing.T) {
	t.Parallel()

	mqtt, err := hacatalog.LoadMQTT()
	if err != nil {
		t.Fatalf("LoadMQTT: %v", err)
	}
	componentKeys := jsonTags(reflect.TypeOf(discovery.Component{}))
	owned := map[string]bool{"platform": true, "device": true, "origin": true}
	for _, k := range componentKeys {
		owned[k] = true
	}

	for name, zero := range discovery.FieldsIndex {
		allowed, ok := catalogKeys(mqtt, name)
		if !ok {
			continue
		}
		modelled := map[string]bool{}
		for _, k := range jsonTags(reflect.TypeOf(zero)) {
			modelled[k] = true
		}
		var missing []string
		for key := range allowed {
			if !owned[key] && !modelled[key] {
				missing = append(missing, key)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			t.Logf("%s: unmodelled keys (regenerate with script/genfields): %s",
				name, strings.Join(missing, ", "))
		}
	}
}

// TestFieldsFlattenIntoTheComponent proves the structs are usable as Fields:
// a value must survive the merge into one JSON object under its own key, and a
// zero value must not emit anything — a nulled-out key is not the same as an
// absent one to Home Assistant.
func TestFieldsFlattenIntoTheComponent(t *testing.T) {
	t.Parallel()

	open := 100
	comp := discovery.Component{
		Platform: hacatalog.PlatformCover,
		Name:     "Blind",
		UniqueID: "u1",
		Fields:   discovery.CoverFields{PositionOpen: &open, PayloadOpen: "UP"},
	}
	raw, err := json.Marshal(comp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["position_open"] != float64(100) {
		t.Errorf("position_open = %v, want 100", got["position_open"])
	}
	if got["payload_open"] != "UP" {
		t.Errorf("payload_open = %v, want %q", got["payload_open"], "UP")
	}
	if _, present := got["payload_close"]; present {
		t.Error("an unset field was emitted — Home Assistant would read the null as a value")
	}
}

// TestZeroValuedNumbersSurvive is why the numeric and boolean fields are
// pointers. A minimum of 0 and an explicit false are exactly the values a
// consumer sets deliberately, and omitempty on a bare int or bool would drop
// both.
func TestZeroValuedNumbersSurvive(t *testing.T) {
	t.Parallel()

	closed := 0
	no := false
	raw, err := json.Marshal(discovery.CoverFields{PositionClosed: &closed, TiltOptimistic: &no})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["position_closed"] != float64(0) {
		t.Errorf("position_closed = %v, want 0 to survive omitempty", got["position_closed"])
	}
	if got["tilt_optimistic"] != false {
		t.Errorf("tilt_optimistic = %v, want false to survive omitempty", got["tilt_optimistic"])
	}
}

// catalogKeys resolves "<platform>" or "<platform>/<variant>" to its key set.
func catalogKeys(mqtt hacatalog.MQTT, name string) (map[string]hacatalog.SchemaKey, bool) {
	platform, variant, hasVariant := strings.Cut(name, "/")
	schema, ok := mqtt.Platforms[platform]
	if !ok {
		return nil, false
	}
	if hasVariant {
		v, found := schema.Variants[variant]
		return v.Keys, found
	}
	return schema.Keys, len(schema.Keys) > 0
}

func jsonTags(t reflect.Type) []string {
	out := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			out = append(out, name)
		}
	}
	return out
}

// TestPtrKeepsAZeroOnTheWire is the reason Ptr exists: the zero values are
// exactly the ones a builder sets deliberately, and a bare int or bool would
// lose them to omitempty.
func TestPtrKeepsAZeroOnTheWire(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(discovery.CoverFields{
		PositionClosed: discovery.Ptr(0),
		TiltOptimistic: discovery.Ptr(false),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["position_closed"] != float64(0) {
		t.Errorf("position_closed = %v, want 0", got["position_closed"])
	}
	if got["tilt_optimistic"] != false {
		t.Errorf("tilt_optimistic = %v, want false", got["tilt_optimistic"])
	}
}

// TestComponentCarriesThePerEntityFrame covers the discovery form five of the
// six consuming projects still publish: one retained config per entity, each
// repeating the device and origin blocks a bundle would carry once.
//
// Home Assistant declares `device` on 31 of the 32 platforms and `origin` on
// 30, so these are ordinary keys there — a consumer on that form could not
// express its payload as a Component at all without them.
func TestComponentCarriesThePerEntityFrame(t *testing.T) {
	t.Parallel()

	comp := discovery.Component{
		Platform:   hacatalog.PlatformSensor,
		UniqueID:   "u1",
		StateTopic: "gh/x",
		Device:     &discovery.DeviceInfo{Identifiers: []string{"dev-1"}, Name: "Hallway"},
		Origin:     &discovery.Origin{Name: "openccu-loom"},
	}
	raw, err := json.Marshal(comp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	device, ok := got["device"].(map[string]any)
	if !ok {
		t.Fatalf("device = %v, want an object", got["device"])
	}
	if device["name"] != "Hallway" {
		t.Errorf("device.name = %v, want Hallway", device["name"])
	}
	if origin, ok := got["origin"].(map[string]any); !ok || origin["name"] != "openccu-loom" {
		t.Errorf("origin = %v, want an object naming the bridge", got["origin"])
	}

	// And the validator must accept that body, or the form it exists for is
	// unusable.
	if err := discovery.ValidateBody(comp.Platform, got); err != nil {
		t.Errorf("the per-entity frame was rejected: %v", err)
	}
}

// TestBundleComponentOmitsTheFrame is the other half: inside a device bundle
// the frame lives at the top, and a component repeating it would describe the
// same device twice. Nil pointers keep the keys out entirely rather than
// emitting an empty object, which Home Assistant would read as a device with
// no identifiers.
func TestBundleComponentOmitsTheFrame(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(discovery.Component{
		Platform: hacatalog.PlatformSensor,
		UniqueID: "u1",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"device", "origin"} {
		if _, present := got[key]; present {
			t.Errorf("%q was emitted on a bundle component", key)
		}
	}
}
