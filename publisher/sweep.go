// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ErrSweepUnscoped is returned by [Runtime.Sweep] when no ownership predicate
// was given.
//
// Refusing is the whole safety property. A discovery prefix is shared: a
// parallel zigbee2mqtt or ESPHome deployment publishes its retained configs
// into the same tree, and a sweep that judged everything it saw against one
// consumer's declared set would clear every entity of every other
// integration on the broker. There is no sane default for "mine", so the
// sweep will not guess one.
var ErrSweepUnscoped = errors.New("publisher: sweep needs an ownership predicate")

// SweepRequest parameterises one orphan pass.
type SweepRequest struct {
	// Owns decides whether a retained config topic belongs to this
	// consumer. It sees the parsed topic, which is all the broker offers —
	// in practice a node-id namespace check. Required; see
	// [ErrSweepUnscoped].
	//
	// An Owns written before v0.29.0 is worth re-reading, and especially
	// one that does not look at [ConfigTopic.NodeID]. Since v0.29.0
	// [ParseConfigTopic] also accepts the node-id-less three-segment form,
	// so a predicate that decides on the platform or the object id alone
	// now judges a class of topics it was never shown — and it is a
	// populated class: Tasmota publishes exactly that shape into a shared
	// discovery tree. A predicate that scopes on the node id is unaffected,
	// because that form parses with an empty one and such a predicate
	// declines it.
	//
	// It is called from the transport's read loop, so it must be cheap and
	// must not publish.
	Owns func(t ConfigTopic) bool

	// Window overrides [Config.SweepWindow] for this pass.
	Window time.Duration

	// ReportOnly runs the pass without retracting anything: the window
	// opens, every owned config is parsed and handed to [Inspect], the
	// result lists what was seen in [SweepResult.Owned] and what a
	// retracting pass would have cleared in [SweepResult.Unclaimed], and
	// not one message goes out.
	//
	// It exists because looking and clearing were one act, and that
	// coupling blocked a migration outright. openccu-loom PR #797 tried
	// it: its one-off scrub has to run BEFORE the first snapshot, because
	// the retraction is what makes Home Assistant forget a stale
	// `unique_id` and the snapshot that follows re-announces under the
	// corrected one. At that moment this runtime's claim set is empty, so
	// an [Owns] wide enough for [Inspect] to see anything makes the
	// ordinary pass judge the entire retained discovery fleet an orphan and
	// delete it — the exact hazard [Runtime.Sweep] warns about. Running it
	// after the snapshot finds nothing, because the retained payload is by
	// then already the corrected one. That consumer therefore kept a second
	// hand-rolled broker snapshot beside this one, purely because the
	// library could not be asked to report without acting.
	//
	// This is the pass that is safe before the first publish, and the
	// ordinary one explicitly is not. It is also the one mode in which a
	// deliberately wide Owns — `func(ConfigTopic) bool { return true }`, to
	// see a whole shared discovery tree — costs nothing, because a pass
	// that retracts nothing cannot retract another writer's config either.
	// The caller then decides, and retracts through [Runtime.Retract] with
	// a list it chose itself.
	//
	// False — the zero value — is the retracting pass every release before
	// v0.27.0 performed, so a consumer that says nothing is unchanged.
	ReportOnly bool

	// Inspect, when set, receives the retained body of every owned config
	// the window delivers, before the pass decides whether to retract it.
	//
	// It exists because the sweep is the LAST moment an orphan's other
	// topics can be found. A config removed while the consumer was down is
	// remembered by nobody: the availability plane clears only what it
	// wrote, and after a restart it wrote nothing. The config body is the
	// one place that still names the entity's availability and state
	// topics, and discarding it leaves a retained `online` standing
	// forever — Home Assistant then keeps a device that no longer exists
	// permanently available, showing its last value.
	//
	// Same contract as Owns: it is called from the transport's read loop,
	// so it must be cheap and must not publish. Collect the topics here and
	// retract them after Sweep returns, which is also the order a removal
	// needs — the config retraction is what removes the entity, and
	// clearing availability first only greys it out in between, which an
	// operator reads as a fault.
	Inspect func(t ConfigTopic, body []byte)
}

// SweepResult is what one pass saw and did.
//
// Inspected is reported alongside Retracted because the pair is what makes a
// silent sweep diagnosable: zero inspected means the window saw none of this
// consumer's retained configs at all, which is a completely different fault
// from a window that saw them all and correctly found nothing orphaned. Both
// look like "0 removed" in a log line that reports only the second number.
type SweepResult struct {
	// Inspected counts the owned config topics the window delivered.
	Inspected int
	// Owned lists those same topics, in arrival order.
	//
	// A caller doing its own judging needs the list it judged, not just its
	// size. It is EVERY owned topic, the ones this process claims included,
	// so it is not the list to retract — [Unclaimed] is. Retracting this
	// one clears the live fleet: `Retract(res.Owned...)` is the composition
	// that cleared 29 live configs in a sibling repo, and it is harmless
	// only in the documented pre-publish case where the claim set is still
	// empty and the two lists are therefore the same.
	//
	// It is filled on both kinds of pass. On a retracting pass, Owned minus
	// Retracted is NOT what this process still claims: a retraction that
	// fails warns and the pass continues, so the difference is the claimed
	// topics plus whatever the broker refused.
	Owned []string
	// Unclaimed lists the owned topics this process does not claim — what a
	// retracting pass would clear — in arrival order.
	//
	// It exists because [SweepRequest.ReportOnly] could not report. The
	// pass computed exactly this list, logged its length at Debug and threw
	// it away, so the one mode whose entire output IS the result had
	// nothing to hand back and the only list on offer was [Owned], which
	// includes the entities the consumer is publishing right now. A caller
	// that means to act on a report-only pass retracts these, after the
	// pass returns, with [Runtime.Retract].
	//
	// On a retracting pass it is the verdict the window reached and
	// [Retracted] is what the retraction loop then managed: a topic claimed
	// in between, or refused by the broker, is in this list and not in that
	// one.
	Unclaimed []string
	// Retracted lists the topics actually cleared, sorted by arrival.
	// Always empty under [SweepRequest.ReportOnly].
	Retracted []string
}

// Sweep clears the retained discovery configs this process no longer
// publishes.
//
// A retained config outlives the build that wrote it. Drop an entity from a
// consumer's emit set and the broker keeps handing Home Assistant the old
// payload forever, which re-creates the entity as a permanently unavailable
// phantom on every integration restart. Before this existed, operators ran a
// shell script by hand.
//
// The mechanism is a short snapshot subscription over `<prefix>/#`: every
// retained config the broker replays is parsed, scoped by
// [SweepRequest.Owns], and compared against what this process has claimed.
// Anything owned and unclaimed is a leftover and gets retracted.
//
// Ordering matters twice, and both are measured rather than chosen:
//
//   - Run it AFTER the boot snapshot has published, not before. The
//     comparison is against what this process declared, so a sweep that runs
//     while a plane has not published yet judges that plane's entire fleet to
//     be orphans and deletes it — once per boot, with nothing left to
//     re-declare it. That is how a consumer lost its whole security plane on
//     every restart, automations and dashboard cards included.
//   - Run it BEFORE publishing the other discovery form, if a consumer is
//     migrating between them. Home Assistant refuses the new form while the
//     old one is retained (see [Runtime.PublishBundle]), and the sweep would
//     clear it only after the refusal had already happened.
//     [Runtime.PublishBundle] therefore does its own targeted retraction and
//     does not wait for this pass.
//
// All three topic forms are recognised — see [ParseConfigTopic]. One snapshot
// window runs at a time per runtime; a second call waits, bounded by its own
// context.
//
// [SweepRequest.ReportOnly] turns the pass into a look without a touch, and
// that is the version which may run before the first publish. This one may
// not.
func (r *Runtime) Sweep(ctx context.Context, req SweepRequest) (SweepResult, error) {
	if req.Owns == nil {
		return SweepResult{}, ErrSweepUnscoped
	}
	window := req.Window
	if window <= 0 {
		window = r.cfg.SweepWindow
	}

	// One pass at a time per runtime, and the slot spans the retractions
	// too, not just the window. Every pass rides the same client, which
	// keys its subscriptions by filter: two concurrent windows on the same
	// filter leave the second handler installed over the first, and the
	// first teardown unsubscribes for both — after which both report
	// nothing and the orphans they exist to clear survive. Holding the slot
	// through the retractions as well is what makes a later pass see the
	// tree an earlier one left behind rather than the one it found.
	if !r.acquireSweepSlot(ctx) {
		return SweepResult{}, fmt.Errorf("publisher: waiting for the snapshot slot: %w", ctx.Err())
	}
	defer r.sweepMu.Unlock()

	var (
		mu        sync.Mutex
		candidate []string
		owned     []string
		inspected int
	)
	collect := func(topic string, payload []byte, _ bool) {
		// An empty retained payload is a topic the broker is already
		// clearing. Retracting it again would be a message for nothing.
		if len(payload) == 0 {
			return
		}
		parsed, ok := ParseConfigTopic(r.cfg.Prefix, topic)
		if !ok || !req.Owns(parsed) {
			return
		}
		mu.Lock()
		inspected++
		owned = append(owned, topic)
		mu.Unlock()

		if req.Inspect != nil {
			req.Inspect(parsed, payload)
		}

		if r.claims(topic) {
			return
		}
		mu.Lock()
		candidate = append(candidate, topic)
		mu.Unlock()
	}

	err := r.snapshot(ctx, topicPrefix(r.cfg.Prefix)+"#", window, collect)

	// Read under the lock the deliveries write under: the window is closed
	// by an atomic flag, so a delivery that passed the gate a moment earlier
	// can still be inside the append while this goroutine reads.
	mu.Lock()
	topics := append([]string(nil), candidate...)
	result := SweepResult{
		Inspected: inspected,
		Owned:     append([]string(nil), owned...),
		Unclaimed: append([]string(nil), candidate...),
	}
	mu.Unlock()

	// What the window saw is returned even when it ended badly, which is
	// the whole value of a failed pass. The snapshot reports the caller's
	// context ending as an error even after a full window has run, and a
	// boot context that expires on the window boundary used to take the
	// entire result with it — for a [SweepRequest.ReportOnly] pass, whose
	// only output IS the result, that is everything the pass was for. The
	// error is still returned and still governs: a caller that acts on a
	// partial list is choosing to, with the error in hand to say so.
	if err != nil {
		return result, err
	}

	if req.ReportOnly {
		// Returned before the retraction loop rather than skipped inside
		// it: there is no branch further down that could be reached with
		// ReportOnly set, so the promise in the doc comment is a property
		// of the control flow and not of five conditions staying in
		// agreement.
		r.log.Debug("publisher.sweep.report_only",
			slog.Int("inspected", result.Inspected),
			slog.Int("unclaimed", len(topics)))
		return result, nil
	}

	for _, t := range topics {
		if err := ctx.Err(); err != nil {
			break
		}
		// Re-checked immediately before the retraction rather than trusted
		// from the window's verdict: clearing thousands of topics takes
		// seconds, and a publisher that claimed one of them in the meantime
		// has made it live again. Retracting it would delete an entity that
		// exists.
		if r.claims(t) {
			continue
		}
		if err := r.tr.Publish(ctx, t, nil, r.qos, true); err != nil {
			r.log.Warn("publisher.sweep.retract_failed",
				slog.String("topic", t),
				slog.String("err", err.Error()))
			continue
		}
		// Dropped from the declared set too, so a later publish of the same
		// topic is not dedup-suppressed against the payload just cleared.
		r.mu.Lock()
		delete(r.declared, t)
		r.mu.Unlock()
		result.Retracted = append(result.Retracted, t)
	}

	r.log.Debug("publisher.sweep",
		slog.Int("inspected", result.Inspected),
		slog.Int("retracted", len(result.Retracted)))
	return result, nil
}

// claims reports whether this process is responsible for topic.
//
// `announced` is consulted alongside `declared` because a config still inside
// its Publish call is already on the broker — and already delivered to the
// sweep's own subscription — while `declared` records it only afterwards.
// Without the in-flight claim, a sweep running concurrently with a publish
// retracts the config that publish just wrote.
func (r *Runtime) claims(topic string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, declared := r.declared[topic]
	return declared || r.announced[topic]
}

// snapshot installs filter for the length of window, feeds every delivery to
// collect, and takes the subscription down again on every exit path — a
// cancelled context and a broker that refuses the UNSUBSCRIBE included.
//
// Both halves matter and both used to be missing in the reference
// implementation. A one-shot boot pass over a broad wildcard whose handler
// closes over a growing worklist keeps that worklist alive for the rest of
// the process if the subscription is left installed — and a client that
// replays its subscriptions on reconnect carries it across the very broker
// restart that stranded it. The gate bounds the damage when the teardown
// itself fails: the handler stays registered but stops accumulating once its
// window has closed.
//
// The caller holds the snapshot slot; see [Runtime.Sweep].
func (r *Runtime) snapshot(
	ctx context.Context,
	filter string,
	window time.Duration,
	collect Handler,
) error {
	// Spend what is left of the caller's budget rather than the window it
	// asked for. Waiting for the slot eats into that budget, and a caller
	// whose deadline is the window plus a small margin would otherwise open
	// a window it cannot finish and return having cleared nothing. A short
	// window only means fewer retained messages are seen, and the sweep
	// clears strictly what it saw.
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		usable := remaining - min(time.Second, remaining/4)
		if usable <= 0 {
			return errors.New("publisher: no budget left for a snapshot window")
		}
		window = min(window, usable)
	}

	var closed atomic.Bool
	gated := func(topic string, payload []byte, retained bool) {
		if closed.Load() {
			return
		}
		collect(topic, payload, retained)
	}
	if err := r.tr.Subscribe(ctx, filter, r.qos, gated); err != nil {
		return fmt.Errorf("publisher: snapshot subscribe %s: %w", filter, err)
	}
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
	closed.Store(true)

	// The unsubscribe runs on a context of its own: the caller's may already
	// be cancelled, and that is precisely the case where leaving the
	// wildcard subscription installed does the most damage.
	teardown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.tr.Unsubscribe(teardown, filter); err != nil {
		r.log.Warn("publisher.snapshot.unsubscribe_failed",
			slog.String("filter", filter),
			slog.String("err", err.Error()))
	}
	return ctx.Err()
}

// acquireSweepSlot takes the snapshot lock, giving up if ctx ends first.
// Reported rather than waited out unconditionally, so a shutdown during a
// long boot pass does not block on a window it will never use.
func (r *Runtime) acquireSweepSlot(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		r.sweepMu.Lock()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		// The goroutine still holds or will take the lock; hand it back as
		// soon as it does, rather than leaking the slot forever.
		go func() {
			<-done
			r.sweepMu.Unlock()
		}()
		return false
	}
}
