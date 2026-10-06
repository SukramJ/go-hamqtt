// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testInstance(t *testing.T, tr Transport, cfg InstanceConfig) *Instance {
	t.Helper()
	if cfg.Layout == nil {
		cfg.Layout = smartHomeLayout(t, "zendure")
	}
	if cfg.Name == "" {
		cfg.Name = "go-zendure2mqtt"
	}
	if cfg.Version == "" {
		cfg.Version = "1.2.3"
	}
	if cfg.Started.IsZero() {
		cfg.Started = time.Date(2026, 10, 6, 8, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	}
	if cfg.Logger == nil {
		cfg.Logger = discardLogger()
	}
	i := NewInstance(tr, cfg)
	i.env.host = func() (string, error) { return "pi4", nil }
	i.env.pid = 4242
	i.env.goVersion = "go1.27.1"
	return i
}

// TestInfoGolden pins `<name>/info` to the byte: the spec §6 fields, `go` as
// the runtime key, `started` in UTC ISO 8601 with milliseconds, and the
// project's own fields beside them.
func TestInfoGolden(t *testing.T) {
	t.Parallel()

	f := newFake()
	i := testInstance(t, f, InstanceConfig{Extra: map[string]any{"mode": "cloud"}})
	if err := i.AnnounceInfo(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := lastPublish(t, f)
	want := `{"go":"go1.27.1","host":"pi4","maintenance":true,"mode":"cloud","name":"go-zendure2mqtt",` +
		`"pid":4242,"spec":"2.0","started":"2026-10-06T06:00:00.000Z","version":"1.2.3"}`
	if got.topic != "zendure/info" || string(got.payload) != want {
		t.Errorf("%s = %s\nwant %s", got.topic, got.payload, want)
	}
	if !got.retain || got.qos != 0 {
		t.Errorf("retain/qos = %v/%d", got.retain, got.qos)
	}
}

// TestInfoIsASheCoreInstance replays she's services inventory
// (she-services-inventory.js): exactly two levels, an object with a non-empty
// string `name` and string `version` makes a core instance rather than a
// legacy one; `started` must parse, `pid` must be an integer, and
// `maintenance` is read as `=== true`.
func TestInfoIsASheCoreInstance(t *testing.T) {
	t.Parallel()

	i := testInstance(t, newFake(), InstanceConfig{})
	body, err := i.Info()
	if err != nil {
		t.Fatal(err)
	}
	var info map[string]any
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatal(err)
	}
	if name, ok := info["name"].(string); !ok || name == "" {
		t.Error("no string name: she would list a legacy instance")
	}
	if _, ok := info["version"].(string); !ok {
		t.Error("no string version")
	}
	if info["spec"] != SpecVersion {
		t.Errorf("spec = %v", info["spec"])
	}
	if _, err := time.Parse(time.RFC3339, info["started"].(string)); err != nil {
		t.Errorf("started does not parse: %v", err)
	}
	if pid, ok := info["pid"].(float64); !ok || pid != float64(int64(pid)) {
		t.Errorf("pid = %v", info["pid"])
	}
	if info["maintenance"] != true {
		t.Errorf("maintenance = %v", info["maintenance"])
	}
	if strings.Count(i.cfg.Layout.Info(), "/") != 1 {
		t.Errorf("%q is not two levels", i.cfg.Layout.Info())
	}
}

// TestInfoReservedKeysCannotBeOverridden: spec §6 forbids redefining them.
func TestInfoReservedKeysCannotBeOverridden(t *testing.T) {
	t.Parallel()

	i := testInstance(t, newFake(), InstanceConfig{
		Extra:               map[string]any{"name": "lgtv2mqtt", "maintenance": true, "commit": "abc"},
		MaintenanceDisabled: true,
	})
	body, _ := i.Info()
	var info map[string]any
	_ = json.Unmarshal(body, &info)
	if info["name"] != "go-zendure2mqtt" || info["maintenance"] != false || info["commit"] != "abc" {
		t.Errorf("info = %s", body)
	}
}

// maintenanceRig is an instance wired to a command router over a broker that
// echoes, so a test publishes a maintenance command the way she does.
type maintenanceRig struct {
	broker *cmdBroker
	router *CommandRouter
	log    *syncLog
}

func newMaintenanceRig(t *testing.T, cfg InstanceConfig, router CommandConfig) maintenanceRig {
	t.Helper()
	rig := maintenanceRig{broker: newCmdBroker(), log: &syncLog{}}
	cfg.Logger = rig.log.logger()
	router.Logger = rig.log.logger()
	rig.router = NewCommandRouter(rig.broker, router)
	i := testInstance(t, newFake(), cfg)
	if err := i.Register(rig.router); err != nil {
		t.Fatal(err)
	}
	if err := rig.router.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rig.router.Stop(context.Background()) })
	return rig
}

func (m maintenanceRig) send(topic, payload string, retained bool) {
	m.broker.fanout(topic, []byte(payload), retained)
	m.router.WaitIdle()
}

// TestMaintenanceLogLevel: spec §7's four levels onto slog, any case, the
// `{"val": …}` form too; anything else is a rejected request logged at warn.
func TestMaintenanceLogLevel(t *testing.T) {
	t.Parallel()

	var level slog.LevelVar
	rig := newMaintenanceRig(t, InstanceConfig{SetLogLevel: LevelVarSetter(&level)}, CommandConfig{})
	cases := []struct {
		payload string
		want    slog.Level
	}{
		{"debug", slog.LevelDebug}, {"WARN", slog.LevelWarn}, {`{"val":"error"}`, slog.LevelError}, {"info", slog.LevelInfo},
	}
	for _, c := range cases {
		rig.send("zendure/maintenance/set/loglevel", c.payload, false)
		if level.Level() != c.want {
			t.Errorf("%q: level = %v, want %v", c.payload, level.Level(), c.want)
		}
	}
	rig.send("zendure/maintenance/set/loglevel", "verbose", false)
	if level.Level() != slog.LevelInfo || !strings.Contains(rig.log.String(), "msg=publisher.maintenance.loglevel_rejected") {
		t.Errorf("unknown level: %v\n%s", level.Level(), rig.log.String())
	}
}

// TestMaintenanceRestartRefusedWhenUnsupervised: a restart without a
// supervisor is a stop, so it is refused and said at warn; supervised, it
// runs the consumer's shutdown.
func TestMaintenanceRestartRefusedWhenUnsupervised(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	shutdown := func() { calls.Add(1) }

	rig := newMaintenanceRig(t, InstanceConfig{Shutdown: shutdown, Supervised: func() bool { return false }}, CommandConfig{})
	rig.send("zendure/maintenance/set/restart", "1", false)
	if !strings.Contains(rig.log.String(), "level=WARN msg=publisher.maintenance.restart_refused") {
		t.Errorf("refusal not logged at warn:\n%s", rig.log.String())
	}

	unanswered := newMaintenanceRig(t, InstanceConfig{Shutdown: shutdown}, CommandConfig{})
	unanswered.send("zendure/maintenance/set/restart", "1", false)
	if calls.Load() != 0 {
		t.Fatalf("shutdown ran %d times without a supervisor", calls.Load())
	}

	done := make(chan struct{})
	supervised := newMaintenanceRig(t, InstanceConfig{
		Shutdown:   func() { calls.Add(1); close(done) },
		Supervised: func() bool { return true },
	}, CommandConfig{})
	supervised.send("zendure/maintenance/set/restart", "now", false)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supervised restart did not run the shutdown")
	}
	if calls.Load() != 1 {
		t.Errorf("shutdown ran %d times", calls.Load())
	}
}

// TestMaintenanceIgnoresRetainedAndUnknown: a retained command is ignored even
// on a router that delivers retained messages, and an unknown maintenance
// command is logged at warn (spec §7).
func TestMaintenanceIgnoresRetainedAndUnknown(t *testing.T) {
	t.Parallel()

	var level slog.LevelVar
	rig := newMaintenanceRig(t, InstanceConfig{SetLogLevel: LevelVarSetter(&level)}, CommandConfig{DeliverRetained: true})
	rig.send("zendure/maintenance/set/loglevel", "debug", true)
	if level.Level() != slog.LevelInfo {
		t.Error("a retained loglevel command was applied")
	}
	rig.send("zendure/maintenance/set/wipe", "1", false)
	if !strings.Contains(rig.log.String(), "level=WARN msg=publisher.maintenance.unknown topic=zendure/maintenance/set/wipe") {
		t.Errorf("unknown command not logged at warn:\n%s", rig.log.String())
	}
}

// TestMaintenanceDisabled: no route, no stats, and info says so.
func TestMaintenanceDisabled(t *testing.T) {
	t.Parallel()

	b := newCmdBroker()
	r := NewCommandRouter(b, CommandConfig{Logger: discardLogger()})
	i := testInstance(t, newFake(), InstanceConfig{MaintenanceDisabled: true})
	if err := i.Register(r); err != nil {
		t.Fatal(err)
	}
	if len(r.Filters()) != 0 {
		t.Errorf("routes = %v", r.Filters())
	}
	if err := i.RunStats(context.Background()); !errors.Is(err, ErrStatsOff) {
		t.Errorf("RunStats = %v", err)
	}
	if i.Maintenance() {
		t.Error("Maintenance() = true")
	}
}

// TestStatsInterval: the operator's 0 is off, never the default.
func TestStatsInterval(t *testing.T) {
	t.Parallel()

	if StatsInterval(0) != StatsOff || StatsInterval(-5) != StatsOff || StatsInterval(30) != 30*time.Second {
		t.Error("StatsInterval mapping")
	}
	if i := testInstance(t, newFake(), InstanceConfig{}); i.interval != DefaultStatsInterval {
		t.Errorf("default interval = %v", i.interval)
	}
	off := testInstance(t, newFake(), InstanceConfig{StatsInterval: StatsInterval(0)})
	if err := off.RunStats(context.Background()); !errors.Is(err, ErrStatsOff) {
		t.Errorf("RunStats = %v", err)
	}
}

// TestStatsWithoutProc: off Linux there is no statm, and rss is the Go
// runtime's approximation rather than missing — she drops a document
// without it.
func TestStatsWithoutProc(t *testing.T) {
	t.Parallel()

	s := newStatsSampler(time.Now().Add(-90*time.Second), filepath.Join(t.TempDir(), "absent"))
	body, err := json.Marshal(s.sample(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	if rss, ok := got["rss"].(float64); !ok || rss <= 0 {
		t.Errorf("rss missing or zero without /proc: %s", body)
	}
	for _, k := range []string{"heapUsed", "heapTotal", "uptime", "ts"} {
		if _, ok := got[k].(float64); !ok {
			t.Errorf("%s missing: %s", k, body)
		}
	}
	if got["uptime"] != float64(90) {
		t.Errorf("uptime = %v", got["uptime"])
	}
	if got["heapTotal"].(float64) < got["heapUsed"].(float64) {
		t.Errorf("heapTotal < heapUsed: %s", body)
	}
}

// TestStatsReadsStatm: the second statm field is resident pages.
func TestStatsReadsStatm(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "statm")
	if err := os.WriteFile(path, []byte("1000 250 100 1 0 300 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rss, ok := residentBytes(path)
	if !ok || rss != uint64(250*os.Getpagesize()) {
		t.Errorf("rss = %d, %v", rss, ok)
	}
	if err := os.WriteFile(path, []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := residentBytes(path); ok {
		t.Error("garbage parsed")
	}
}

// TestRunStatsPublishes: the first document goes out at start, retained on
// `<name>/maintenance/stats`, and on every platform it carries the numeric
// rss she requires before it keeps the document at all.
func TestRunStatsPublishes(t *testing.T) {
	t.Parallel()

	f := newFake()
	i := testInstance(t, f, InstanceConfig{StatsInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- i.RunStats(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for !f.holds("zendure/maintenance/stats") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("RunStats = %v", err)
	}
	got := lastPublish(t, f)
	if got.topic != "zendure/maintenance/stats" || !got.retain {
		t.Fatalf("published %s retain %v", got.topic, got.retain)
	}
	var stats map[string]any
	if err := json.Unmarshal(got.payload, &stats); err != nil {
		t.Fatal(err)
	}
	if !sheParseStats(got.payload) {
		t.Errorf("she would discard this stats document: %s", got.payload)
	}
	if _, ok := stats["eventLoopLag"]; ok {
		t.Error("eventLoopLag published")
	}
}

// sheParseStats is she's parseStats (she-services-inventory.js): the
// document is kept only when `rss` is a finite number.
func sheParseStats(payload []byte) bool {
	var doc map[string]any
	if json.Unmarshal(payload, &doc) != nil {
		return false
	}
	_, ok := doc["rss"].(float64)
	return ok
}

// TestStatsSheKeepsTheDocumentWithoutProc: the she-compat property where it
// broke — a host with no statm still publishes a document she keeps.
func TestStatsSheKeepsTheDocumentWithoutProc(t *testing.T) {
	t.Parallel()

	s := newStatsSampler(time.Now(), filepath.Join(t.TempDir(), "absent"))
	body, err := json.Marshal(s.sample(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if !sheParseStats(body) {
		t.Errorf("she would discard %s", body)
	}
	if residentApprox(10, 20) != 0 || residentApprox(30, 10) != 20 {
		t.Error("residentApprox arithmetic")
	}
}

// TestSheRestartWithEmptyPayloadFires: she's Restart button publishes
// `<name>/maintenance/set/restart` with an EMPTY payload, not retained
// (she-services-api.js). It must restart a supervised instance — on a router
// that normalises `set` payloads too, which drops empty ones everywhere else
// — while a retained empty message still does nothing.
func TestSheRestartWithEmptyPayloadFires(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	done := make(chan struct{}, 1)
	rig := newMaintenanceRig(t, InstanceConfig{
		Shutdown:   func() { calls.Add(1); done <- struct{}{} },
		Supervised: func() bool { return true },
	}, CommandConfig{NormalizeSet: true, DeliverRetained: true})

	rig.send("zendure/maintenance/set/restart", "", true)
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("a retained empty restart fired")
	}
	rig.send("zendure/maintenance/set/restart", "", false)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("she's empty restart did not run the shutdown")
	}

	var level slog.LevelVar
	ll := newMaintenanceRig(t, InstanceConfig{SetLogLevel: LevelVarSetter(&level)}, CommandConfig{NormalizeSet: true})
	ll.send("zendure/maintenance/set/loglevel", "", false)
	ll.send("zendure/maintenance/set/loglevel", "debug", false) // what she sends
	if level.Level() != slog.LevelDebug {
		t.Errorf("level = %v", level.Level())
	}
}
