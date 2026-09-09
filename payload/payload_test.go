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

	// alt= is opt-in, so the plain harvest uses the field's own name.
	cfg := payload.For(d, payload.Config)
	if _, ok := cfg["modes"]; !ok || len(cfg) != 1 {
		t.Errorf("Config = %#v, want the field-named modes", cfg)
	}

	state := payload.For(d, payload.State)
	if state["temperature"] != 21.5 || state["name"] != "Living room" {
		t.Errorf("State = %#v", state)
	}
	if _, leaked := state["manufacturer"]; leaked {
		t.Error("an info field leaked into the state payload")
	}
}

// TestAltNamesAreOptIn pins the two-audience requirement the flag exists for:
// one struct feeds both a device's own info topic, which wants the model's
// vocabulary, and Home Assistant's device block, which wants its own.
func TestAltNamesAreOptIn(t *testing.T) {
	t.Parallel()

	d := device{Modes: []string{"heat"}}

	plain := payload.For(d, payload.Config)
	if _, ok := plain["modes"]; !ok {
		t.Errorf("without the flag: %#v, want the field name", plain)
	}

	renamed := payload.ForWith(d, payload.Config, payload.Options{UseAltNames: true})
	if _, ok := renamed["operation_modes"]; !ok {
		t.Errorf("with the flag: %#v, want the alt spelling", renamed)
	}
	if _, leaked := renamed["modes"]; leaked {
		t.Errorf("with the flag: %#v, the field name survived alongside the alt", renamed)
	}

	// A field with no alt= is unaffected either way.
	both := payload.ForWith(device{Manufacturer: "Daikin"}, payload.Info, payload.Options{UseAltNames: true})
	if _, ok := both["manufacturer"]; !ok {
		t.Errorf("a field without alt= changed name: %#v", both)
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

func (w *withExtra) ExtraPayload(k payload.Kind, opts payload.Options) map[string]any {
	if k != payload.Info {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	// Honouring IncludeZero is the implementation's job, and the reason the
	// options are passed in at all.
	if w.hidden == "" && !opts.IncludeZero {
		return map[string]any{"public": "overridden"}
	}
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

// TestExtraHonoursOptions covers why Extra receives them: a contributed
// property must be able to drop itself under the same rule the reflected
// fields follow, or the escape hatch emits keys the rest of the payload would
// have omitted.
func TestExtraHonoursOptions(t *testing.T) {
	t.Parallel()

	empty := &withExtra{Public: "tagged"}
	if got := payload.For(empty, payload.Info); got["hidden"] != nil {
		t.Errorf("hidden = %v, want it omitted at its zero value", got["hidden"])
	}
	got := payload.ForWith(empty, payload.Info, payload.Options{IncludeZero: true})
	if _, ok := got["hidden"]; !ok {
		t.Errorf("IncludeZero = %#v, want the zero-valued contribution kept", got)
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

// TestNamingPolicy covers why the policy is a choice rather than a constant:
// two published surfaces disagree on the spelling, both are already on the
// wire, and a renamed key is a break for whoever reads it.
func TestNamingPolicy(t *testing.T) {
	t.Parallel()

	// Address is the interesting field: all three spellings differ.
	type s struct {
		SWVersion   string `payload:"info"`
		InterfaceID string `payload:"info"`
		Address     string `payload:"info,alt=serial_number"`
	}
	in := s{SWVersion: "1", InterfaceID: "2", Address: "3"}

	snake := payload.For(in, payload.Info)
	for _, want := range []string{"sw_version", "interface_id", "address"} {
		if _, ok := snake[want]; !ok {
			t.Errorf("default naming: missing %q in %#v", want, snake)
		}
	}

	lower := payload.ForWith(in, payload.Info, payload.Options{Naming: payload.NamingLower})
	for _, want := range []string{"swversion", "interfaceid", "address"} {
		if _, ok := lower[want]; !ok {
			t.Errorf("lower naming: missing %q in %#v", want, lower)
		}
	}

	// alt= outranks either policy — it names the key outright.
	for name, opts := range map[string]payload.Options{
		"snake": {UseAltNames: true},
		"lower": {Naming: payload.NamingLower, UseAltNames: true},
	} {
		got := payload.ForWith(in, payload.Info, opts)
		if _, ok := got["serial_number"]; !ok {
			t.Errorf("%s + alt=: did not outrank the naming policy: %#v", name, got)
		}
	}
}
