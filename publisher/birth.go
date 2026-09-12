// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// BirthPayload and DeathPayload are what Home Assistant publishes on its own
// lifecycle topic: "online" once the MQTT integration has booted, "offline"
// before it disconnects. They are the same two words a consumer's own
// availability topic carries, which is why [discovery.PayloadOnline] and
// [discovery.PayloadOffline] are reused rather than spelled again here.
const (
	BirthPayload = discovery.PayloadOnline
	DeathPayload = discovery.PayloadOffline
)

// BirthTopic is where Home Assistant announces its own lifecycle,
// `<prefix>/status`.
//
// It shares the discovery root with the config topics but not their grammar:
// this is Home Assistant's topic, not a `.../config` entry, so it is built
// from the prefix directly and [ParseConfigTopic] deliberately does not match
// it. A consumer that puts its OWN availability topic here — one measured
// bridge does, at `<prefix>/status/lwt` — has published it into somebody
// else's tree, where no entity references it and nothing reads it.
func BirthTopic(prefix string) string { return topicPrefix(prefix) + "status" }

// Will is the Last Will a consumer must configure on its MQTT connection for
// its availability policy to mean anything.
//
// Returned as data rather than applied, because the will is part of CONNECT
// and therefore belongs to the client the consumer builds, which this runtime
// deliberately cannot reach. Handing it over as a value is what lets the two
// halves agree by construction: the same topic and the same two payloads that
// [Runtime.AnnounceOnline] and [Runtime.AnnounceOffline] use.
//
// That agreement is the measured defect. Two reference bridges configure a
// will whose topic no published entity references, so the broker dutifully
// writes "offline" on a hard crash and every entity in Home Assistant stays
// available forever, showing the last value it ever saw. A will nobody reads
// is indistinguishable from no will at all.
type Will struct {
	// Topic is [Config.StatusTopic].
	Topic string
	// Payload is [DeathPayload]. The broker publishes it when the
	// connection drops without a clean DISCONNECT.
	Payload []byte
	// QoS is [Config.QoS].
	QoS byte
	// Retain is always true: an availability marker that is not retained
	// tells nothing to a Home Assistant that subscribes after the crash,
	// which is exactly when it needs to be told.
	Retain bool
}

// Will returns the Last Will this runtime's availability policy assumes.
// An empty [Config.StatusTopic] yields the zero Will and
// [ErrNoStatusTopic], so a consumer cannot silently connect without one.
func (r *Runtime) Will() (Will, error) {
	if r.cfg.StatusTopic == "" {
		return Will{}, ErrNoStatusTopic
	}
	return Will{
		Topic:   r.cfg.StatusTopic,
		Payload: []byte(DeathPayload),
		QoS:     r.cfg.QoS,
		Retain:  true,
	}, nil
}

// AnnounceOnline publishes the retained "online" marker on
// [Config.StatusTopic] — the counterpart the Last Will clears.
//
// Call it after every (re)connect, not only at boot: the broker publishes the
// will on the drop, so a reconnected consumer that does not re-announce stays
// offline in Home Assistant while happily publishing state nobody displays.
func (r *Runtime) AnnounceOnline(ctx context.Context) error {
	return r.announce(ctx, BirthPayload)
}

// AnnounceOffline publishes the retained "offline" marker — the same payload
// the broker would have published as the will, sent deliberately on a clean
// shutdown where no will fires.
func (r *Runtime) AnnounceOffline(ctx context.Context) error {
	return r.announce(ctx, DeathPayload)
}

func (r *Runtime) announce(ctx context.Context, payload string) error {
	if r.cfg.StatusTopic == "" {
		return ErrNoStatusTopic
	}
	return r.tr.Publish(ctx, r.cfg.StatusTopic, []byte(payload), r.cfg.QoS, true)
}

// WatchBirth subscribes to [BirthTopic] and replays every declared config
// when Home Assistant comes back.
//
// Home Assistant keeps the retained configs across its own restart, but does
// not reliably re-read them across every addon reload and firmware update.
// The birth message is the one deterministic signal that it is ready again,
// and [Runtime.Republish] on its rising edge closes the race — an entity that
// would otherwise be missing until the consumer happens to restart.
//
// The retained replay of the birth topic at subscribe time is deliberately
// treated as a real event rather than filtered out: Home Assistant publishes
// its status retained, so that first delivery is the signal that it is
// currently up, and a consumer that connects after Home Assistant gets no
// other one.
//
// The republish does not run on the read loop. The handler parses inline —
// microseconds — and hands the work to a single worker goroutine, because a
// replay is one blocking retained publish per declared topic, each waiting on
// an acknowledgement only that same read loop could deliver. Doing it inline
// is a self-deadlock on the first birth message, and it is the reason this
// package owns a worker at all. Call [Runtime.Close] to drain it.
//
// One worker and a shallow queue: the replay is idempotent, so two concurrent
// ones buy nothing, and Home Assistant does not emit births in a tight loop.
// A burst collapses onto the one pending job rather than queueing a replay
// per message.
func (r *Runtime) WatchBirth(ctx context.Context) error {
	r.mu.Lock()
	if r.birth == nil {
		r.birth = newDispatcher()
	}
	d := r.birth
	r.mu.Unlock()

	handler := func(_ string, payload []byte, _ bool) {
		if strings.TrimSpace(string(payload)) != BirthPayload {
			// Home Assistant emits "offline" before its own restart.
			// Nothing to do: the configs it will re-read are already
			// retained, and the replay belongs on the way back up.
			return
		}
		d.enqueue(func() {
			// A context of the watch's own rather than the handler's: the
			// delivery's context ends with the delivery, and the replay
			// outlives it by design.
			n, err := r.Republish(context.WithoutCancel(ctx))
			if err != nil {
				r.log.Warn("publisher.birth.resync",
					slog.Int("replayed", n),
					slog.String("err", err.Error()))
			} else {
				r.log.Info("publisher.birth.resync", slog.Int("replayed", n))
			}
			if r.cfg.OnResync != nil {
				r.cfg.OnResync(n, err)
			}
		})
	}
	return r.tr.Subscribe(ctx, BirthTopic(r.cfg.Prefix), r.cfg.QoS, handler)
}

// Close stops accepting birth events and blocks until an in-flight or queued
// replay has finished. Safe on a runtime that never watched, and safe to call
// twice — a shutdown path that is reached from two places is the normal case,
// not a bug to punish with a panic on a closed channel.
func (r *Runtime) Close() {
	r.mu.Lock()
	d := r.birth
	r.mu.Unlock()
	if d != nil {
		d.close()
	}
}

// dispatcher runs one job at a time off the caller's goroutine, collapsing a
// burst onto a single pending job.
//
// Collapsing rather than queueing, because every job this carries is a full
// idempotent replay: running the second one after the first changes nothing,
// and a bounded queue that filled up would push the blocking back onto the
// read loop the dispatcher exists to keep free.
type dispatcher struct {
	mu      sync.Mutex
	pending func()
	running bool
	closed  bool
	idle    sync.WaitGroup
}

func newDispatcher() *dispatcher { return &dispatcher{} }

func (d *dispatcher) enqueue(job func()) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.pending = job
	if d.running {
		// A worker is already up and will pick this one up when it is done.
		d.mu.Unlock()
		return
	}
	d.running = true
	d.idle.Add(1)
	d.mu.Unlock()
	go d.run()
}

func (d *dispatcher) run() {
	defer d.idle.Done()
	for {
		d.mu.Lock()
		job := d.pending
		d.pending = nil
		if job == nil {
			d.running = false
			d.mu.Unlock()
			return
		}
		d.mu.Unlock()
		job()
	}
}

func (d *dispatcher) close() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	d.idle.Wait()
}
