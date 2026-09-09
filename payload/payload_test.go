// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package payload_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/SukramJ/go-hamqtt/payload"
)

type embedded struct {
	Serial string `payload:"info"`
}

type device struct {
	embedded
	Manufacturer string   `payload:"info"`
	SWVersion    string   `payload:"info"`
	Modes        []string `payload:"config,alt=operation_modes"`
	Temperature  float64  `payload:"state"`
	Name         string   `payload:"info,state"`
	Ignored      string
	Skipped      string `payload:"-"`
	unexported   string //nolint:unused // present to prove it is not harvested
}

// TestPartitionsByKind is the whole contract: one declaration, three payloads,
// and no field in a partition it was not tagged for.
func TestPartitionsByKind(t *testing.T) {
	t.Parallel()

	d := device{
		embedded:     embedded{Serial: "AC-1"},
		Manufacturer: "Daikin",
		SWVersion:    "1.2.3",
		Modes:        []string{"heat", "cool"},
		Temperature:  21.5,
		Name:         "Living room",
		Ignored:      "no tag",
		Skipped:      "explicitly skipped",
	}

	info := payload.For(d, payload.Info)
	want := map[string]any{
		"serial":       "AC-1",
		"manufacturer": "Daikin",
		"sw_version":   "1.2.3",
		"name":         "Living room",
	}
	if !reflect.DeepEqual(info, want) {
		t.Errorf("Info = %#v\nwant %#v", info, want)
	}

	cfg := payload.For(d, payload.Config)
	if _, ok := cfg["operation_modes"]; !ok || len(cfg) != 1 {
		t.Errorf("Config = %#v, want just the renamed modes", cfg)
	}

	state := payload.For(d, payload.State)
	if state["temperature"] != 21.5 || state["name"] != "Living room" {
		t.Errorf("State = %#v", state)
	}
	if _, leaked := state["manufacturer"]; leaked {
		t.Error("an info field leaked into the state payload")
	}
}

// TestSnakeCaseHandlesInitialisms pins the naming: SWVersion is sw_version,
// not s_w_version.
func TestSnakeCaseHandlesInitialisms(t *testing.T) {
	t.Parallel()

	type s struct {
		SWVersion string `payload:"info"`
		HTTPPort  int    `payload:"info"`
		ModelID   string `payload:"info"`
		Simple    string `payload:"info"`
	}
	got := payload.For(s{SWVersion: "a", HTTPPort: 1, ModelID: "b", Simple: "c"}, payload.Info)
	for _, key := range []string{"sw_version", "http_port", "model_id", "simple"} {
		if _, ok := got[key]; !ok {
			t.Errorf("missing key %q in %#v", key, got)
		}
	}
}

// TestZeroValuesAreOmittedByDefault keeps a device with forty capabilities and
// three set values from publishing thirty-seven nulls.
func TestZeroValuesAreOmittedByDefault(t *testing.T) {
	t.Parallel()

	d := device{Manufacturer: "Daikin"}
	if got := payload.For(d, payload.Info); len(got) != 1 {
		t.Errorf("For = %#v, want only the set field", got)
	}
	got := payload.ForWith(d, payload.Info, payload.Options{IncludeZero: true})
	if len(got) < 4 {
		t.Errorf("IncludeZero = %#v, want every tagged field", got)
	}
}

// TestNilAndNonStructAreHarmless: a partially built device is a normal
// intermediate state, not a programming error.
func TestNilAndNonStructAreHarmless(t *testing.T) {
	t.Parallel()

	for name, in := range map[string]any{
		"nil":          nil,
		"nil pointer":  (*device)(nil),
		"not a struct": "string",
	} {
		if got := payload.For(in, payload.Info); len(got) != 0 {
			t.Errorf("%s: For = %#v, want empty", name, got)
		}
	}
}

type withExtra struct {
	Public string `payload:"info"`

	mu     sync.Mutex
	hidden string
}

func (w *withExtra) ExtraPayload(k payload.Kind) map[string]any {
	if k != payload.Info {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return map[string]any{"hidden": w.hidden, "public": "overridden"}
}

// TestExtraOverridesTaggedFields covers the escape hatch and its precedence:
// it runs last, so it can also correct a tagged field.
func TestExtraOverridesTaggedFields(t *testing.T) {
	t.Parallel()

	w := &withExtra{Public: "tagged", hidden: "behind a mutex"}
	got := payload.For(w, payload.Info)
	if got["hidden"] != "behind a mutex" {
		t.Errorf("hidden = %v, want the mutex-guarded value", got["hidden"])
	}
	if got["public"] != "overridden" {
		t.Errorf("public = %v, want ExtraPayload to win", got["public"])
	}
}

// TestCacheIsConcurrencySafe: the reflection cache is shared, and a bridge
// publishes from several goroutines.
func TestCacheIsConcurrencySafe(t *testing.T) {
	t.Parallel()

	d := device{Manufacturer: "Daikin", Temperature: 1}
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := payload.For(d, payload.Info); got["manufacturer"] != "Daikin" {
				t.Errorf("concurrent For = %#v", got)
			}
		}()
	}
	wg.Wait()
}

// TestMergePrecedence pins the one-directional fold.
func TestMergePrecedence(t *testing.T) {
	t.Parallel()

	got := payload.Merge(map[string]any{"a": 1, "b": 2}, map[string]any{"b": 3})
	if got["a"] != 1 || got["b"] != 3 {
		t.Errorf("Merge = %#v, want src to win on b", got)
	}
	if got := payload.Merge(nil, map[string]any{"a": 1}); got["a"] != 1 {
		t.Errorf("Merge into nil = %#v", got)
	}
}
