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
