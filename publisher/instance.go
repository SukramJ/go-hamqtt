// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/SukramJ/go-hamqtt/topic"
)

// SpecVersion is the mqtt-smarthome specification version this module
// implements, as `<name>/info` announces it (spec §2 and §6).
const SpecVersion = "2.0"

// DefaultStatsInterval is how often `<name>/maintenance/stats` is published
// when [InstanceConfig.StatsInterval] is zero (spec §9's default).
const DefaultStatsInterval = 60 * time.Second

// StatsOff switches the stats topic off when set as
// [InstanceConfig.StatsInterval].
// It is negative because zero is the field's unset value and means the
// default; [StatsInterval] maps an operator's `0` onto it.
const StatsOff time.Duration = -1

// StatsInterval turns an operator-supplied interval in seconds into the
// [InstanceConfig.StatsInterval] that means it: 0 is [StatsOff], as spec §7
// defines it, never "unset". `time.Duration(n) * time.Second` would turn the
// operator's 0 into the 60 s default.
func StatsInterval(seconds int) time.Duration {
	if seconds <= 0 {
		return StatsOff
	}
	return time.Duration(seconds) * time.Second
}

// reservedInfoKeys are the `<name>/info` fields the instance itself answers;
// spec §6 forbids an adapter to redefine them.
var reservedInfoKeys = map[string]bool{
	"name": true, "version": true, "spec": true, "go": true,
	"host": true, "pid": true, "started": true, "maintenance": true,
}

// InstanceConfig parameterises an [Instance].
type InstanceConfig struct {
	// Layout names the instance's topics. Required.
	Layout topic.SmartHomeLayout

	// Name is the adapter's project name, `info.name` — the Go project, not
	// an npm package a tool would offer updates for. Required.
	Name string
	// Version is the adapter version, `info.version`. Required.
	Version string
	// Started is the process start, `info.started`. Zero means when this
	// package was initialised.
	Started time.Time
	// Extra are project fields added to `info`. A key the instance answers
	// itself is dropped with a warning at construction.
	Extra map[string]any

	// MaintenanceDisabled switches spec §7 off: no maintenance commands are
	// routed, no stats are published, and `info.maintenance` says false.
	// The zero value is enabled, as the spec recommends.
	MaintenanceDisabled bool
	// SetLogLevel applies `maintenance/set/loglevel`. Nil refuses the
	// command with a warning. [LevelVarSetter] adapts a [slog.LevelVar].
	SetLogLevel func(slog.Level)
	// Supervised answers whether a supervisor restarts the process after a
	// clean exit. Nil, or false, refuses `maintenance/set/restart`: without
	// a supervisor a restart is a stop.
	Supervised func() bool
	// Shutdown starts the graceful shutdown a restart is: the consumer's
	// own path, which publishes `connected` 0 and exits 0. It runs on a
	// goroutine of its own, because a shutdown that stops the command
	// router waits for the very handler that called it.
	Shutdown func()
	// StatsInterval is the period of `maintenance/stats`. Zero means
	// [DefaultStatsInterval]; [StatsOff] switches it off.
	StatsInterval time.Duration

	// QoS applies to `info` and the stats. [QoSUnset] is QoS 0, as spec §4
	// asks for everything.
	QoS QoS
	// Logger receives the diagnostics. Nil means [slog.Default].
	Logger *slog.Logger
}

// Instance publishes what mqtt-smarthome 2.0 says about the running adapter
// itself rather than its items: the retained `<name>/info` (spec §6), and
// the maintenance topics (spec §7) — log level and restart commands, and the
// periodic process statistics.
//
// `<name>/connected` is not here: it is the Last Will, and the will belongs to
// [Runtime], whose [Runtime.SetConnected] moves it.
type Instance struct {
	tr       Transport
	cfg      InstanceConfig
	log      *slog.Logger
	qos      byte
	interval time.Duration
	extra    map[string]any
	env      instanceEnv
}

// instanceEnv is what `info` and the stats read from the process, separate
// so a test can pin it.
type instanceEnv struct {
	host      func() (string, error)
	pid       int
	goVersion string
	statm     string
}

// processStart stands in for the process start time; package initialisation
// is the closest point this module can observe.
var processStart = time.Now()

// NewInstance builds an instance publisher. A nil transport or a config
// without a layout, name or version is a programming error and panics here,
// like every constructor in this package.
func NewInstance(tr Transport, cfg InstanceConfig) *Instance {
	if tr == nil {
		panic("publisher: nil transport")
	}
	if cfg.Layout == nil || cfg.Name == "" || cfg.Version == "" {
		panic("publisher: InstanceConfig needs Layout, Name and Version")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Started.IsZero() {
		cfg.Started = processStart
	}
	interval := cfg.StatsInterval
	switch {
	case cfg.MaintenanceDisabled, interval < 0:
		interval = 0
	case interval == 0:
		interval = DefaultStatsInterval
	}
	extra := make(map[string]any, len(cfg.Extra))
	for k, v := range cfg.Extra {
		if reservedInfoKeys[k] {
			logger.Warn("publisher.instance.reserved_info_key",
				slog.String("key", k),
				slog.String("effect", "spec §6 forbids redefining it; the instance's own value is published"))
			continue
		}
		extra[k] = v
	}
	return &Instance{
		tr:       tr,
		cfg:      cfg,
		log:      logger,
		qos:      resolveQoS("publisher.InstanceConfig.QoS", cfg.QoS, QoSAtMostOnce),
		interval: interval,
		extra:    extra,
		env: instanceEnv{
			host:      os.Hostname,
			pid:       os.Getpid(),
			goVersion: runtime.Version(),
			statm:     "/proc/self/statm",
		},
	}
}

// Maintenance reports whether the maintenance topics are enabled — the
// value `info.maintenance` carries.
func (i *Instance) Maintenance() bool { return !i.cfg.MaintenanceDisabled }

// Info renders the `<name>/info` document: `name`, `version`, `spec`, `go`
// (the runtime version — spec §6's "own key" for a runtime that is not
// Node), `host`, `pid`, `started` (ISO 8601, milliseconds, UTC),
// `maintenance`, and the project's extra fields.
func (i *Instance) Info() ([]byte, error) {
	doc := make(map[string]any, len(i.extra)+len(reservedInfoKeys))
	for k, v := range i.extra {
		doc[k] = v
	}
	doc["name"] = i.cfg.Name
	doc["version"] = i.cfg.Version
	doc["spec"] = SpecVersion
	doc["go"] = i.env.goVersion
	if host, err := i.env.host(); err == nil && host != "" {
		doc["host"] = host
	}
	doc["pid"] = i.env.pid
	doc["started"] = i.cfg.Started.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	doc["maintenance"] = i.Maintenance()
	b, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("publisher: marshal info: %w", err)
	}
	return b, nil
}

// AnnounceInfo publishes `<name>/info`, retained. Call it on every broker
// (re)connect, beside [Runtime.AnnounceOnline] (spec §6).
func (i *Instance) AnnounceInfo(ctx context.Context) error {
	body, err := i.Info()
	if err != nil {
		return err
	}
	if err := i.tr.Publish(ctx, i.cfg.Layout.Info(), body, i.qos, true); err != nil {
		return fmt.Errorf("publisher: publish info: %w", err)
	}
	return nil
}

// Register routes `<name>/maintenance/set/#` on r, the consumer's command
// router, so maintenance commands share its subscription QoS and its
// workers. It registers nothing when maintenance is disabled.
//
//   - `loglevel` takes error, warn, info or debug, in any case, onto
//     [InstanceConfig.SetLogLevel]. Not persisted. Its payload is normalised
//     like any `set` (spec §5.3), so `{"val":"debug"}` works and an empty
//     one is ignored.
//   - `restart` calls [InstanceConfig.Shutdown] when
//     [InstanceConfig.Supervised] answers true, and is refused at warn
//     otherwise. Any payload fires it, the empty one included.
//   - Anything else under `maintenance/set/` is logged at warn and ignored
//     (spec §7).
//
// Restart is the one `set`-shaped topic that accepts an empty payload, and
// on purpose: spec §7 gives its payload as "any", and she — the management
// tool this convention exists for — publishes it with an empty payload.
// Normalising it like a `set` would make she's Restart button do nothing.
// The cost is that a live subscriber also receives the empty message
// somebody publishes to clear a retained restart topic; that is accepted,
// because the restart only happens behind the consumer's Supervised answer,
// and a consumer's shutdown path is latched so a second trigger is a no-op.
//
// Retained messages are ignored on every maintenance topic, whatever the
// router's own [CommandConfig.DeliverRetained] says: a retained restart
// would otherwise restart the instance on every reconnect.
func (i *Instance) Register(r *CommandRouter) error {
	if !i.Maintenance() {
		return nil
	}
	// handle rather than Handle: [CommandConfig.NormalizeSet] would drop the
	// empty restart before it got here.
	return r.handle(i.cfg.Layout.Maintenance(topic.FunctionSet)+"/#",
		func(_ context.Context, cmd Command) { i.handleMaintenance(r, cmd) })
}

func (i *Instance) handleMaintenance(r *CommandRouter, cmd Command) {
	if cmd.Retained {
		i.log.Debug("publisher.maintenance.retained_drop", slog.String("topic", cmd.Topic))
		return
	}
	switch cmd.Remainder {
	case "loglevel":
		if v, ok := r.parseSet(cmd); ok {
			i.setLogLevel(cmd, v)
		}
	case "restart":
		i.restart(cmd)
	default:
		i.log.Warn("publisher.maintenance.unknown",
			slog.String("topic", cmd.Topic), slog.String("payload", string(cmd.Payload)))
	}
}

func (i *Instance) setLogLevel(cmd Command, v SetValue) {
	var level slog.Level
	switch strings.ToLower(v.Text) {
	case "error":
		level = slog.LevelError
	case "warn":
		level = slog.LevelWarn
	case "info":
		level = slog.LevelInfo
	case "debug":
		level = slog.LevelDebug
	default:
		i.log.Warn("publisher.maintenance.loglevel_rejected",
			slog.String("topic", cmd.Topic), slog.String("payload", string(cmd.Payload)))
		return
	}
	if i.cfg.SetLogLevel == nil {
		i.log.Warn("publisher.maintenance.loglevel_unsupported", slog.String("topic", cmd.Topic))
		return
	}
	i.cfg.SetLogLevel(level)
	i.log.Warn("publisher.maintenance.loglevel", slog.String("level", strings.ToLower(v.Text)))
}

func (i *Instance) restart(cmd Command) {
	if i.cfg.Shutdown == nil || i.cfg.Supervised == nil || !i.cfg.Supervised() {
		i.log.Warn("publisher.maintenance.restart_refused",
			slog.String("topic", cmd.Topic),
			slog.String("reason", "not known to run under a supervisor that restarts it after a clean exit"))
		return
	}
	i.log.Warn("publisher.maintenance.restart", slog.String("topic", cmd.Topic))
	go i.cfg.Shutdown()
}

// LevelVarSetter adapts a [slog.LevelVar] to [InstanceConfig.SetLogLevel].
func LevelVarSetter(v *slog.LevelVar) func(slog.Level) {
	return v.Set
}

// ErrStatsOff is returned by [Instance.RunStats] when there is nothing to
// run: maintenance is disabled or the interval is [StatsOff].
var ErrStatsOff = errors.New("publisher: maintenance stats are off")

// RunStats publishes `<name>/maintenance/stats`, retained, once at start and
// then every interval until ctx ends, and returns ctx's error. A publish that
// fails — the broker is away — is logged at debug and retried at the next
// tick; the topic is retained, so a reconnect needs no extra publish.
//
// The document carries `rss` (resident set size from /proc on Linux; elsewhere
// the Go runtime's mapped-and-not-released memory, because she discards a
// document without it), `heapUsed` and `heapTotal` (runtime/metrics), `cpu`
// (percent of one core since the previous sample, where the platform reports
// process CPU time), `uptime` (seconds) and `ts` (milliseconds). There is no
// `eventLoopLag`: Go has no event loop.
func (i *Instance) RunStats(ctx context.Context) error {
	if i.interval <= 0 {
		return ErrStatsOff
	}
	sampler := newStatsSampler(i.cfg.Started, i.env.statm)
	ticker := time.NewTicker(i.interval)
	defer ticker.Stop()
	for {
		i.publishStats(ctx, sampler, time.Now())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (i *Instance) publishStats(ctx context.Context, s *statsSampler, now time.Time) {
	body, err := json.Marshal(s.sample(now))
	if err != nil {
		i.log.Warn("publisher.maintenance.stats", slog.String("err", err.Error()))
		return
	}
	if err := i.tr.Publish(ctx, i.cfg.Layout.Maintenance("stats"), body, i.qos, true); err != nil {
		i.log.Debug("publisher.maintenance.stats", slog.String("err", err.Error()))
	}
}
