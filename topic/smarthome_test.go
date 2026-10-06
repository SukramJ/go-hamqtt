// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package topic_test

import (
	"errors"
	"testing"

	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

func mustSmartHome(t *testing.T, name string) topic.SmartHome {
	t.Helper()
	l, err := topic.NewSmartHome(name)
	if err != nil {
		t.Fatalf("NewSmartHome(%q): %v", name, err)
	}
	return l
}

// TestSmartHomeNameFollowsSpecSection3: the name is one topic level, so it
// may not contain the level separator or a wildcard, and it may not be empty.
// NUL and a leading `$` are refused on MQTT's own grounds.
func TestSmartHomeNameFollowsSpecSection3(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"", "home/loom", "a+", "a#", "#", "$SYS", "a\x00b"} {
		if _, err := topic.NewSmartHome(bad); !errors.Is(err, topic.ErrInvalidName) {
			t.Errorf("NewSmartHome(%q) = %v, want ErrInvalidName", bad, err)
		}
	}
	l := mustSmartHome(t, "mtec")
	if l.Name() != "mtec" || !l.Conformant() {
		t.Errorf("Name/Conformant = %q/%v", l.Name(), l.Conformant())
	}
}

// TestMultiLevelNameIsKeptAndFlagged: loom keeps an operator's `home/loom`
// verbatim (ADR 0083) and needs to know it runs outside §3 to say so once.
// Wildcards and empty levels stay refused — those would change what a
// subscription matches, not just how a scan sees the instance.
func TestMultiLevelNameIsKeptAndFlagged(t *testing.T) {
	t.Parallel()

	l, err := topic.NewSmartHomeMultiLevel("home/loom")
	if err != nil {
		t.Fatal(err)
	}
	if l.Conformant() {
		t.Error("a multi-level name reported conformant")
	}
	if got := l.Connected(); got != "home/loom/connected" {
		t.Errorf("Connected = %q, want the base verbatim", got)
	}
	single, err := topic.NewSmartHomeMultiLevel("openccu-loom")
	if err != nil || !single.Conformant() {
		t.Errorf("single level: %v, conformant %v", err, single.Conformant())
	}
	for _, bad := range []string{"", "home//loom", "/loom", "loom/", "home/+", "home/#", "$SYS/x"} {
		if _, err := topic.NewSmartHomeMultiLevel(bad); !errors.Is(err, topic.ErrInvalidName) {
			t.Errorf("NewSmartHomeMultiLevel(%q) = %v, want ErrInvalidName", bad, err)
		}
	}
}

// TestSmartHomeGrammar pins every function's topic. Status, set and meta
// share one item path (spec §3), which is what lets a consumer derive the
// command topic from the status topic it already knows.
func TestSmartHomeGrammar(t *testing.T) {
	t.Parallel()

	l := mustSmartHome(t, "zendure")
	cases := map[string]string{
		l.Status("SF1", "now", "electric_level"): "zendure/status/SF1/now/electric_level",
		l.Set("SF1", "now", "electric_level"):    "zendure/set/SF1/now/electric_level",
		l.Meta("SF1", "now", "electric_level"):   "zendure/meta/SF1/now/electric_level",
		l.Maintenance("stats"):                   "zendure/maintenance/stats",
		l.Maintenance("set", "loglevel"):         "zendure/maintenance/set/loglevel",
		l.Connected():                            "zendure/connected",
		l.Bridge():                               "zendure/connected",
		l.Info():                                 "zendure/info",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

// TestEmptyItemRendersNothing: `<name>/status` alone is no topic of this
// grammar (levels MUST NOT be empty), and the zero SmartHome names nothing.
// "" is the module's spelling of "no such topic".
func TestEmptyItemRendersNothing(t *testing.T) {
	t.Parallel()

	l := mustSmartHome(t, "unifi")
	for _, got := range []string{l.Status(), l.Set(""), l.Meta("", ""), l.Availability(model.Slot{})} {
		if got != "" {
			t.Errorf("got %q, want empty", got)
		}
	}
	var zero topic.SmartHome
	if zero.State(model.S("a", "", model.BucketUnset, "b")) != "" || zero.Connected() != "" || zero.Info() != "" {
		t.Error("the zero layout rendered a topic")
	}
}

// TestSlotItemPathFollowsDefaultOrder: scope, address, channel, bucket, path,
// with the empty ones gone. These are ADR 0083's own before/after examples.
func TestSlotItemPathFollowsDefaultOrder(t *testing.T) {
	t.Parallel()

	loom := mustSmartHome(t, "openccu-loom")
	s := model.S("000C9709AEF157", "1", model.BucketValues, "ACTUAL_TEMPERATURE").In("GoOtto", "GoOtto-HmIP-RF")
	if got, want := loom.State(s), "openccu-loom/status/GoOtto/GoOtto-HmIP-RF/000C9709AEF157/1/values/ACTUAL_TEMPERATURE"; got != want {
		t.Errorf("State = %q, want %q", got, want)
	}
	if got, want := loom.Command(s), "openccu-loom/set/GoOtto/GoOtto-HmIP-RF/000C9709AEF157/1/values/ACTUAL_TEMPERATURE"; got != want {
		t.Errorf("Command = %q, want %q", got, want)
	}
	if got, want := loom.Availability(s), "openccu-loom/status/GoOtto/GoOtto-HmIP-RF/000C9709AEF157/online"; got != want {
		t.Errorf("Availability = %q, want %q", got, want)
	}

	mtec := mustSmartHome(t, "MTEC")
	if got, want := mtec.State(model.S("MT1234567890", "", model.BucketUnset, "now_base", "grid_power")),
		"MTEC/status/MT1234567890/now_base/grid_power"; got != want {
		t.Errorf("mtec State = %q, want %q", got, want)
	}

	daikin := mustSmartHome(t, "daikin")
	if got, want := daikin.Command(model.S("uuid-1", "climateControl", model.BucketUnset, "operation_mode")),
		"daikin/set/uuid-1/climateControl/operation_mode"; got != want {
		t.Errorf("daikin Command = %q, want %q", got, want)
	}
}

// TestItemSegmentsAreSafeButTheNameIsNot: a device name cannot add a level,
// while a multi-level base has to survive verbatim.
func TestItemSegmentsAreSafeButTheNameIsNot(t *testing.T) {
	t.Parallel()

	l, err := topic.NewSmartHomeMultiLevel("home/loom")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := l.Status("a/b", "c+d"), "home/loom/status/a_b/c_d"; got != want {
		t.Errorf("Status = %q, want %q", got, want)
	}
}

// TestLiteralSubTreesAreExpressible: unifi's `device/<mac>` family and loom's
// `alarm/<zone>/panel` are not slot-shaped. Embedding the layout and building
// the item path by hand is how a consumer says so, and the result must still
// be a SmartHomeLayout the rest of the module recognises.
func TestLiteralSubTreesAreExpressible(t *testing.T) {
	t.Parallel()

	l := unifiLayout{mustSmartHome(t, "unifi")}
	var _ topic.SmartHomeLayout = l
	s := model.S("aabbccddeeff", "", model.BucketUnset, "cpu_utilization").In("default")
	if got, want := l.State(s), "unifi/status/default/device/aabbccddeeff/cpu_utilization"; got != want {
		t.Errorf("State = %q, want %q", got, want)
	}
	if got, want := l.Command(s), "unifi/set/default/device/aabbccddeeff/cpu_utilization"; got != want {
		t.Errorf("Command = %q, want %q", got, want)
	}
	if got, want := l.Bridge(), "unifi/connected"; got != want {
		t.Errorf("Bridge = %q, want %q", got, want)
	}
}

type unifiLayout struct{ topic.SmartHome }

func (l unifiLayout) item(s model.Slot) []string {
	return append([]string{s.Scope[0], "device", s.Address}, s.Path...)
}

func (l unifiLayout) State(s model.Slot) string   { return l.Status(l.item(s)...) }
func (l unifiLayout) Command(s model.Slot) string { return l.Set(l.item(s)...) }

// TestSmartHomePulsesAreStatusItems: occurrences live under `status` at the
// channel's coordinate, and the per-type datapoint event — dropped by ADR
// 0083, the type travels in `val` — is declined rather than invented.
func TestSmartHomePulsesAreStatusItems(t *testing.T) {
	t.Parallel()

	l := mustSmartHome(t, "openccu-loom")
	s := model.S("ADDR", "2", model.BucketValues, "PRESS_SHORT").In("central", "iface")
	cases := map[topic.PulseKind]string{
		topic.PulseChannelEvent:       "openccu-loom/status/central/iface/ADDR/2/event",
		topic.PulseChannelImpulse:     "openccu-loom/status/central/iface/ADDR/2/impulse",
		topic.PulseChannelDeviceError: "openccu-loom/status/central/iface/ADDR/2/device_error",
		topic.PulseDataPointEvent:     "",
		topic.PulseKind(0):            "",
	}
	for kind, want := range cases {
		if got := topic.PulseTopic(l, kind, s); got != want {
			t.Errorf("%s: got %q, want %q", kind, got, want)
		}
	}
	if got := l.Pulse(topic.PulseChannelEvent, model.Slot{}); got != "" {
		t.Errorf("no address: got %q", got)
	}
}

// TestFunctionNamesForTheReservedNameGuard: a consumer refuses an operator
// identifier that spells a function, so the migration sweep can tell the
// second level of a new topic from an old one.
func TestFunctionNamesForTheReservedNameGuard(t *testing.T) {
	t.Parallel()

	for _, f := range []string{"connected", "status", "set", "get", "info", "meta", "maintenance"} {
		if !topic.IsFunction(f) {
			t.Errorf("IsFunction(%q) = false", f)
		}
	}
	for _, s := range []string{"alarm", "Status", "", "state"} {
		if topic.IsFunction(s) {
			t.Errorf("IsFunction(%q) = true", s)
		}
	}
}

// TestFunctionHAIsReservedButNotASpecFunction: `ha` joins the reserved set a
// new guard asks about, and IsFunction's answer is unchanged — a consumer
// refuses to start on an operator identifier IsFunction accepts, so widening
// it would stop an installation whose site is literally "ha".
func TestFunctionHAIsReservedButNotASpecFunction(t *testing.T) {
	t.Parallel()

	if topic.IsFunction(topic.FunctionHA) {
		t.Error(`IsFunction("ha") = true; existing guards would change`)
	}
	if !topic.IsReservedFunction(topic.FunctionHA) {
		t.Error(`IsReservedFunction("ha") = false`)
	}
	for _, f := range []string{"connected", "status", "set", "get", "info", "meta", "maintenance"} {
		if !topic.IsReservedFunction(f) {
			t.Errorf("IsReservedFunction(%q) = false", f)
		}
	}
	for _, s := range []string{"alarm", "HA", "", "hass"} {
		if topic.IsReservedFunction(s) {
			t.Errorf("IsReservedFunction(%q) = true", s)
		}
	}
	l, err := topic.NewSmartHome("ccu")
	if err != nil {
		t.Fatal(err)
	}
	if got := l.HA("light", "dev1"); got != "ccu/ha/light/dev1" {
		t.Errorf("HA = %q", got)
	}
	if got := l.HA(); got != "" {
		t.Errorf("HA() = %q, want empty", got)
	}
}
