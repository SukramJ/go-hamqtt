// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// Errors reported by the command router. They are sentinels rather than
// strings because every one of them names a wiring mistake a consumer can
// only fix at its composition root, and a composition root that wants to
// tolerate one — a plugin registering a route twice, say — needs
// [errors.Is] to tell which.
var (
	// ErrRouterStarted is returned by [CommandRouter.Handle] after
	// [CommandRouter.Start].
	//
	// Registration is closed once the filters are on the wire because the
	// two invariants this type enforces — no two routes ambiguous with each
	// other, no route overlapping the state plane — are checked across the
	// whole route set. A route added afterwards would be checked against a
	// set the caller has already validated and acted on, which is how a
	// guard becomes advisory.
	ErrRouterStarted = errors.New("publisher: command router already started")

	// ErrDuplicateRoute is returned when the same filter is registered
	// twice. Silently replacing the first handler is the worse outcome: the
	// measured consumer wires its routes from several optional sinks, and a
	// second registration there means two subsystems both believe they own
	// a shape.
	ErrDuplicateRoute = errors.New("publisher: duplicate command route")

	// ErrAmbiguousRoutes is returned when two registered filters can both
	// match some topic — any overlap, not only one no specificity rule
	// could order.
	//
	// The router cannot resolve an overlap after the fact, because by the
	// time a message reaches it the overlap has already multiplied it.
	// Two fan-outs compose: the broker sends one copy per matching
	// subscription (MQTT 3.1.1 §4.7.3 / 5.0 §3.3.4 permit it and both
	// Mosquitto and EMQX do it), and go-mqtt then re-matches each arriving
	// copy against its whole local filter list and calls every matching
	// handler, never correlating a copy with the subscription it arrived
	// on. N overlapping routes therefore turn one message into N
	// indistinguishable dispatches, and nothing the router can see tells
	// them apart from N genuine publishes on the same topic.
	//
	// Measured against Mosquitto 2.1.2, on 3.1.1 and 5.0 alike and with
	// two separate clients so No-Local is not in play: the routes
	// `ccu/+/+/set` and `ccu/+/PRESS_SHORT/set` — exactly the "one
	// wildcard per command shape" granularity — turned one published
	// message into two handler runs. A toggle toggles twice, a relay pulse
	// fires twice, a dimmer step doubles, and nothing logs anything.
	//
	// Refusing the pair at registration is the only resolution that holds
	// on both protocol versions; [CommandRouter.Handle] documents what it
	// costs and what to do instead.
	ErrAmbiguousRoutes = errors.New("publisher: ambiguous command routes")

	// ErrInvalidFilter is returned for a filter MQTT does not permit —
	// empty, or with a wildcard that is not a whole level.
	ErrInvalidFilter = errors.New("publisher: invalid topic filter")

	// ErrStateCommandCollision is returned by [CommandRouter.CheckDisjoint],
	// and by [StatePublisher.Publish] for a topic matching one of
	// [StateConfig.CommandFilters].
	//
	// The measured invariant behind it: a broker delivers a client's own
	// publishes back to it — the consumers subscribe without MQTT 5's
	// No-Local — so a state topic that matches a command filter is a write
	// the process performs on itself. The reference implementation shipped
	// exactly that, mirroring a program's state onto its own trigger topic:
	// the echo ran the program on the CCU on every boot, on every republish
	// and once per freshly discovered program, with nothing in the logs
	// saying so. It is an error on both sides rather than a lint because the
	// two planes are declared in different places and only their
	// intersection is wrong.
	ErrStateCommandCollision = errors.New("publisher: state topic matches a command filter")
)

// DefaultCommandWorkers is how many command handlers the router runs
// concurrently.
//
// Eight, taken from the measured consumer: high enough that one device
// stalled behind a retry stack does not stall unrelated devices, low enough
// to bound the goroutine and downstream-request fan-out a single command
// burst can produce. Order within one topic is preserved regardless of the
// count — see [CommandRouter].
const DefaultCommandWorkers = 8

// DefaultCommandQueueDepth is the per-worker backlog beyond which the router
// logs backpressure.
//
// A soft limit, never a blocking one. The producer is the transport's read
// loop, and the measured failure of blocking it is total: the read loop is
// also the goroutine that delivers the acknowledgement a busy worker is
// waiting on, so the queue can only drain once the producer returns.
const DefaultCommandQueueDepth = 32

// Command is one inbound message the router has routed to a handler.
//
// A struct rather than the flat [Handler] triple, because a command handler
// needs what the route matched — the device address, the parameter name, the
// zone id — and the measured consumer recovered those by splitting the topic
// itself at every one of its thirteen handlers, indexing by position. Two of
// its confirmed defects were index arithmetic: one counted absolute segments
// and broke on every installation whose topic base carried a slash, and one
// read a literal segment of a sibling route as a parameter name.
type Command struct {
	// Topic is the topic the message arrived on, complete.
	Topic string
	// Payload is the message body, owned by the handler: the router clones
	// it before handing it over, because the handler runs after the
	// transport's read loop has moved on and may reuse its buffer.
	Payload []byte
	// Retained reports the broker's retain flag. It is false on every
	// command the router delivers by default — see
	// [CommandConfig.DeliverRetained].
	Retained bool
	// Filter is the registered route that claimed this topic, verbatim.
	// A handler wired to several routes reads it to tell them apart.
	Filter string
	// Wildcards holds what the route's `+` levels matched, in order. This
	// is the topic arithmetic the measured consumer did by hand.
	Wildcards []string
	// Remainder holds what a trailing `#` matched, as an unsplit topic
	// suffix, or "" for a route without one.
	Remainder string
}

// CommandHandler is a consumer callback for one routed command.
//
// # Which goroutine this runs on
//
// A router worker, never the transport's read loop — the one deliberate
// difference between this type and [Handler], and the reason the router owns
// goroutines at all.
//
// [Handler] documents the inherited contract: go-mqtt delivers inline on the
// goroutine that also decodes PUBACK and PINGRESP, so a handler that blocks
// stalls acknowledgement processing and eventually trips the keep-alive
// watchdog into a spurious reconnect. That contract is survivable for the
// runtime's own handlers, which parse and return in microseconds. It is not
// survivable for a command handler: a command is by definition a write to
// something outside this process — a CCU behind a retry stack, an appliance
// API, a serial bus — and the measured consumer has commands that block for
// seconds. Worse, a handler that answers by publishing waits for an
// acknowledgement only the goroutine it is occupying could deliver, which is
// a self-deadlock on the first command rather than a slow path.
//
// The cost of moving off the read loop is paid in three places, and a
// consumer should know all three:
//
//   - Delivery is no longer synchronous with the broker's acknowledgement.
//     The router acknowledges by returning; a QoS 1 command is acked before
//     the handler has run, so a process that dies in between loses it. A
//     command that must survive that needs the consumer's own durable queue,
//     which the router deliberately does not try to be.
//   - Order is preserved per topic and nowhere else. Two commands on the
//     same topic run in arrival order on the same worker; commands on
//     different topics may run concurrently. A consumer whose device cannot
//     take two concurrent writes must serialise them itself.
//   - Shutdown has to be waited for. [CommandRouter.Stop] drains, which
//     means it blocks on whatever a handler is currently doing.
//
// ctx is derived from [CommandConfig.Lifecycle] and is cancelled when the
// handler returns, so a handler must not retain it.
type CommandHandler func(ctx context.Context, cmd Command)

// NoLocalSubscriber is the optional [Transport] capability that stops a
// broker from delivering the consumer's own publishes back to it.
//
// It exists because of the sharpest measured defect in this whole area: a
// consumer subscribes to its command topics on the same connection it
// publishes state on, the broker has no reason to treat those two as
// different, and any overlap between the two topic sets turns a state
// publish into a command the consumer issues to itself. In the measured
// case that ran every Home Assistant "program" on the device on every boot.
//
// MQTT 5.0 §3.8.3.1 has an option for exactly this, and go-mqtt exposes it
// as WithNoLocal. A transport that can pass it implements this interface and
// the router uses it; one that cannot — a consumer's own hand-rolled
// transport, or a test fake — is subscribed to normally. The shipped go-mqtt
// adapter does implement it: publisher/gomqtt compile-asserts the interface
// and passes mqtt.WithNoLocal, so a consumer on the shipped adapter takes
// the safe path without asking for it.
//
// Either way [CommandRouter.CheckDisjoint] remains the load-bearing guard,
// and on two counts. No Local is v5-only — go-mqtt sets the bit only for
// V50, so a v3.1.1 link silently ignores the option and the echo class is
// wide open — and it says nothing about a second process in the same
// deployment publishing the same tree.
type NoLocalSubscriber interface {
	// SubscribeNoLocal is [Transport.Subscribe] with the MQTT 5.0 No Local
	// option set.
	SubscribeNoLocal(ctx context.Context, filter string, qos byte, handler Handler) error
}

// CommandConfig parameterises a [CommandRouter]. The zero value is usable.
type CommandConfig struct {
	// QoS applies to every subscription the router registers. [QoSUnset] —
	// the zero value — means QoS 1: a command dropped in transit is a
	// button press that did nothing, with no error anywhere to explain it.
	//
	// A default, not a floor: [QoSAtMostOnce] states QoS 0 and is
	// honoured, for the consumer whose whole installed base subscribes at
	// QoS 0 today — go-zendure2mqtt, measured on 2026-09-12. See [QoS].
	QoS QoS

	// Workers bounds how many handlers run concurrently. Zero means
	// [DefaultCommandWorkers]; a negative value means one.
	Workers int

	// QueueDepth is the per-worker backlog the router logs beyond. Zero
	// means [DefaultCommandQueueDepth]. It never blocks and never drops.
	QueueDepth int

	// DeliverRetained lets retained messages through to handlers.
	//
	// Off by default, and the default is the measured one. Home Assistant
	// never publishes a command topic retained; a retained command is
	// almost always somebody's `mosquitto_pub -r` left behind, and the
	// broker replays it to the router on every single (re)subscribe. The
	// consumer that allowed it re-issued the last write of the previous
	// run on every daemon restart and on every reconnect, which reads from
	// the outside like a device turning itself on.
	DeliverRetained bool

	// Lifecycle is the context every handler's context derives from. Nil
	// means [context.Background].
	//
	// Separate from the context passed to [CommandRouter.Start] on
	// purpose. In the measured consumer Start is reachable from a config
	// reload, whose context ends when the reload returns; handlers derived
	// from it had their downstream writes cancelled the moment the reload
	// finished, on a router that was otherwise perfectly alive. Pass the
	// process-lifetime context here and whatever you like to Start.
	Lifecycle context.Context

	// OnUnroutable is called for a delivered message no route claims,
	// before the router logs it.
	//
	// A hook because the two consumers that care do different things: one
	// counts it as a metric to catch a discovery payload advertising a
	// command topic nobody subscribed, the other ignores it because a
	// shared broker carries traffic that is not its business. Nil is fine.
	OnUnroutable func(topic string, payload []byte)

	// Logger receives the router's diagnostics. Nil means [slog.Default].
	Logger *slog.Logger
}

// route is one registered filter, pre-split so matching never re-splits.
type route struct {
	filter  string
	parts   []string
	handler CommandHandler
}

// CommandRouter subscribes the command topics a consumer's discovery configs
// advertise and routes each inbound message to exactly one handler.
//
// It is the half of the runtime that reads. [Runtime] owns what this process
// publishes; this owns what it is told to do about it, and the two meet at
// [CommandRouter.CheckDisjoint], which is what keeps them from being the
// same topic.
//
// Three properties are worth stating before the lock order, because they are
// what the type is for:
//
//   - Exactly one handler runs per message, and the router buys that by
//     refusing overlapping routes outright — see [ErrAmbiguousRoutes] for
//     why nothing weaker works. A broker fans a message out to every
//     matching subscription and go-mqtt then calls every locally matching
//     handler per copy, so the measured consumer dispatched a profile
//     selection both to the profile handler and, as a parameter write named
//     `week_profile`, to the data-point handler. It patched that with a
//     hand-maintained list of reserved segments; this type makes the route
//     pair that causes it unregisterable.
//
//     One overlap remains outside the router's reach and is worth naming:
//     the local fan-out is the whole go-mqtt client's, so a SECOND
//     subscription on the same client whose filter also matches a command
//     topic reintroduces the multiplication. This module's own other
//     subscriptions (the discovery-tree snapshot and the birth topic) live
//     under the discovery prefix, not in the consumer's command tree, so
//     they do not; a consumer that adds a broad subscription of its own
//     must keep it off the command tree, which is the subscription-side
//     twin of [CommandRouter.CheckDisjoint].
//
//   - Handlers run off the read loop. See [CommandHandler].
//
//   - Unroutable is a diagnostic, never a failure. A shared broker delivers
//     things that are none of this consumer's business.
//
// # Locking
//
// Three locks, and the order between them is fixed:
//
//	lifeMu  →  mu  →  pool queue
//
// lifeMu serialises [CommandRouter.Start], [CommandRouter.Resubscribe] and
// [CommandRouter.Stop] end-to-end; there must never be two of those in
// flight, because each is a sequence of transport calls whose interleaving
// would leave the broker's subscription set disagreeing with the router's.
// mu guards the route set, the started/stopped flags and the pool pointer,
// and is never held across a [Transport] call or a handler call — a
// subscribe blocks on a SUBACK, and holding the route lock across one would
// stall every inbound message behind it.
//
// It IS held across the enqueue, deliberately, and that is what makes the
// shutdown promise on [CommandRouter.Stop] true rather than probable: the
// stopped check and the enqueue have to be one step, or a command whose
// check passed can still land on a queue Stop closed in between and be
// discarded with a `dropped_after_close` warning. Measured at roughly one
// in four thousand deliveries racing a Stop. The enqueue can be held under
// a lock because it never blocks — an unbounded slice, not a buffered
// channel, for the separate reason [commandQueue] documents.
type CommandRouter struct {
	tr  Transport
	cfg CommandConfig
	log *slog.Logger
	// qos is [CommandConfig.QoS] resolved once, at construction, to the
	// wire byte a [Transport] takes — so an unrecognised level is a panic
	// at the composition root rather than a subscribe at a level nobody
	// chose.
	qos byte

	lifeMu sync.Mutex

	mu      sync.Mutex
	routes  []route
	started bool
	stopped bool
	// pool is nil until Start and nil again after Stop, so a router that
	// is built and abandoned owns no goroutines. See
	// [NewCommandRouter].
	pool *commandPool
}

// NewCommandRouter builds a router over tr. A nil transport panics here
// rather than on the first subscribe, where the stack no longer names the
// composition root that got it wrong — the same bargain [New] makes.
//
// Construction starts no goroutines: the worker pool belongs to
// [CommandRouter.Start] and is reclaimed by [CommandRouter.Stop]. It used
// to be started here, and a router that never reached Start therefore
// leaked [CommandConfig.Workers] goroutines parked on a condition variable
// with nothing left to reclaim them — measured at eight per router, 2 -> 82
// goroutines over ten routers built and discarded. Three ordinary paths get
// there: a [CommandRouter.Handle] that reports an error at a composition
// root, a config reload replacing the router (see
// [CommandConfig.Lifecycle]), and a failed Start the consumer gives up on.
func NewCommandRouter(tr Transport, cfg CommandConfig) *CommandRouter {
	if tr == nil {
		panic("publisher: nil transport")
	}
	if cfg.Workers == 0 {
		cfg.Workers = DefaultCommandWorkers
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = DefaultCommandQueueDepth
	}
	if cfg.Lifecycle == nil {
		cfg.Lifecycle = context.Background()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &CommandRouter{
		tr:  tr,
		cfg: cfg,
		log: logger,
		qos: resolveQoS("publisher.CommandConfig.QoS", cfg.QoS, QoSAtLeastOnce),
	}
}

// Handle registers handler for filter. Call it for every route before
// [CommandRouter.Start]; afterwards it reports [ErrRouterStarted].
//
// # Choosing the granularity
//
// filter may be an exact topic or carry MQTT wildcards, and the choice is
// the one real decision a consumer makes here. Three granularities exist and
// all three have been shipped in the family:
//
//   - One exact topic per entity, straight out of [CommandTopics]. Correct
//     by construction — nothing can overlap, nothing unroutable can arrive,
//     and a topic that is not advertised is not subscribed. It costs one
//     SUBSCRIBE per writable entity and a resubscribe of the same size on
//     every reconnect, which on the measured fleet of a few thousand
//     entities is a real but survivable boot cost.
//   - One wildcard per command shape, which is what the measured consumer
//     does: thirteen filters covering every device it will ever see. One
//     SUBSCRIBE each and no per-entity bookkeeping, at the price this type
//     charges loudly: the shapes must be pairwise disjoint. `ccu/+/+/set`
//     together with `ccu/+/PRESS_SHORT/set` is refused with
//     [ErrAmbiguousRoutes], because that exact pair ran one message's
//     handler twice against Mosquitto 2.1.2. A consumer that wants a
//     special case for one parameter name registers the general shape only
//     and branches inside the handler on [Command.Wildcards] — the
//     discrimination moves from the router to the handler, which is the
//     whole cost, and it is a cost the previous specificity-based scheme
//     only appeared to spare it.
//   - A single `<base>/#`. Never correct here: it subscribes the consumer
//     to its own state plane, so every state publish comes back as a
//     command.
//
// The router takes no position between the first two and supports both. It
// takes a firm one against the third, by refusing nothing but making it
// impossible to miss: [CommandRouter.CheckDisjoint] fails loudly the first
// time a state topic is run past it.
//
// Registration is rejected when filter is malformed, already registered, or
// overlaps an existing route at all — see [ErrAmbiguousRoutes]. The overlap
// test is structural rather than a comparison of the topics seen so far,
// because the measured collision (a seven-level all-wildcard filter against
// a seven-level filter with one literal) is invisible to any check that
// waits for traffic to demonstrate it.
func (r *CommandRouter) Handle(filter string, handler CommandHandler) error {
	if handler == nil {
		return fmt.Errorf("%w: nil handler for %q", ErrInvalidFilter, filter)
	}
	if err := ValidateFilter(filter); err != nil {
		return err
	}
	parts := filterParts(filter)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return fmt.Errorf("%w: cannot register %q", ErrRouterStarted, filter)
	}
	for i := range r.routes {
		existing := r.routes[i]
		if existing.filter == filter {
			return fmt.Errorf("%w: %q", ErrDuplicateRoute, filter)
		}
		if filtersOverlap(existing.parts, parts) {
			return fmt.Errorf("%w: %q and %q both match some topic, so one message would run a handler twice; "+
				"register the general shape only and branch inside the handler",
				ErrAmbiguousRoutes, existing.filter, filter)
		}
	}
	r.routes = append(r.routes, route{filter: filter, parts: parts, handler: handler})
	return nil
}

// Filters lists the registered routes, sorted.
//
// Sorted rather than registration order because the two callers — an
// operator's diagnostic dump and the disjointness check in a test — both
// compare one run against another.
func (r *CommandRouter) Filters() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.routes))
	for i := range r.routes {
		out = append(out, r.routes[i].filter)
	}
	sort.Strings(out)
	return out
}

// Start subscribes every registered route and closes registration.
//
// A failure on any one subscription aborts the whole start and unsubscribes
// the ones already registered. Coming up with a partial filter set is the
// outcome to avoid: the consumer accepts some commands and silently ignores
// the rest, which from the outside is indistinguishable from a broken device
// rather than a broken subscribe. The measured consumer aborts but leaves
// the partial set live; rolling it back is the one place this type does not
// simply copy it.
//
// The rollback itself can fail — a broker may refuse an UNSUBSCRIBE, and a
// broker that just refused a SUBSCRIBE is in exactly the state where it
// might. A subscription that could not be taken down stays live, so Start
// then closes the router for good rather than leaving its gate open: the
// live filter would otherwise deliver commands into a consumer that has
// been told its start failed and has not finished wiring itself, which is
// the hazard [CommandRouter.Stop] promises cannot happen. The returned
// error names each filter still on the broker, because that is the only
// thing an operator can act on; [CommandRouter.Start] cannot be retried
// afterwards and [CommandRouter.Stop] has nothing left to do.
//
// When every rollback succeeded the router is merely un-started and Start
// can be retried once the broker recovers — a partial start that could
// never be retried would be the worse of the two failure modes.
//
// Starting a router with no routes is not an error — a consumer whose
// entities are all read-only has nothing to subscribe, and making that the
// caller's special case buys nothing.
func (r *CommandRouter) Start(ctx context.Context) error {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()

	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return fmt.Errorf("%w: cannot restart a stopped router", ErrRouterStarted)
	}
	if r.started {
		r.mu.Unlock()
		return ErrRouterStarted
	}
	r.started = true
	// The pool must exist before the first subscribe: a broker may
	// deliver on a subscription the moment it acknowledges it.
	r.pool = newCommandPool(r.cfg.Workers, r.cfg.QueueDepth, r.log)
	filters := r.routeFiltersLocked()
	r.mu.Unlock()

	done := make([]string, 0, len(filters))
	for _, f := range filters {
		if err := r.subscribe(ctx, f); err != nil {
			live := r.rollback(ctx, done)
			r.mu.Lock()
			r.started = false
			// A subscription still on the broker is a live route into
			// a consumer whose start just failed, so the gate closes
			// permanently rather than pretending the rollback worked.
			r.stopped = len(live) > 0
			pool := r.pool
			r.pool = nil
			r.mu.Unlock()
			// Nothing will be enqueued now, so the workers this Start
			// spun up are reclaimed here rather than waiting for a
			// Stop the consumer has no reason to call.
			pool.close()
			if len(live) > 0 {
				return fmt.Errorf("publisher: subscribe %s: %w (rollback failed, still subscribed: %s; router closed)",
					f, err, strings.Join(live, ", "))
			}
			return fmt.Errorf("publisher: subscribe %s: %w", f, err)
		}
		done = append(done, f)
	}
	return nil
}

// rollback unsubscribes what a failed Start had already registered and
// reports the filters the broker would not let go of.
func (r *CommandRouter) rollback(ctx context.Context, done []string) (live []string) {
	for _, prev := range done {
		if err := r.tr.Unsubscribe(ctx, prev); err != nil {
			r.log.Warn("publisher.command.rollback",
				slog.String("filter", prev), slog.String("err", err.Error()))
			live = append(live, prev)
		}
	}
	return live
}

// Resubscribe re-registers every route on the current connection.
//
// It exists for the transport that does not replay subscriptions itself. A
// go-mqtt client does — it replays its whole subscription set after a
// reconnect, options and all — so a consumer on the shipped adapter never
// needs this, and calling it anyway is harmless: a repeat SUBSCRIBE on the
// same filter is a legal request the broker answers by replacing the
// subscription, not by adding one.
//
// It is not harmless in one respect worth naming, and it is the same respect
// a transport's own replay is not: a fresh subscription makes the broker
// replay retained messages on the matching topics. That is precisely why
// retained commands are dropped by default — see
// [CommandConfig.DeliverRetained] — and why turning them on makes every
// reconnect re-issue them.
//
// A router that was never started, or has been stopped, subscribes nothing
// and reports no error: a reconnect callback firing during shutdown is
// ordinary, not a fault.
func (r *CommandRouter) Resubscribe(ctx context.Context) error {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()

	r.mu.Lock()
	if !r.started || r.stopped {
		r.mu.Unlock()
		return nil
	}
	filters := r.routeFiltersLocked()
	r.mu.Unlock()

	var errs []error
	for _, f := range filters {
		if err := r.subscribe(ctx, f); err != nil {
			errs = append(errs, fmt.Errorf("publisher: resubscribe %s: %w", f, err))
		}
	}
	return errors.Join(errs...)
}

// Stop unsubscribes every route and drains the handlers already accepted.
//
// The two halves answer different questions and both are measured. No
// handler starts after Stop is entered — the gate is set before the first
// unsubscribe, because a broker delivers whatever was already in flight and
// a command arriving during shutdown would run against half-torn-down
// dependencies. And the boundary is exact in both directions: a delivery
// whose gate check passed before Stop entered is accepted and drained
// rather than discarded, because the check and the enqueue happen under one
// lock Stop must take. Without that, roughly one in four thousand
// deliveries racing a Stop was dropped with a `dropped_after_close`
// warning. But a command already accepted does run to completion:
// abandoning a queued write is how a consumer loses the last command of a
// session with nothing anywhere to say so. Stop therefore blocks on whatever
// a handler is currently doing, which is the shutdown cost
// [CommandHandler] names.
//
// Safe on a router that was never started, and safe to call twice: a
// shutdown path reached from two places is the normal case, not a bug to
// punish with a panic.
func (r *CommandRouter) Stop(ctx context.Context) error {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()

	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	// Set before the first unsubscribe: an in-flight delivery must not
	// find an open gate.
	r.stopped = true
	started := r.started
	filters := r.routeFiltersLocked()
	pool := r.pool
	r.pool = nil
	r.mu.Unlock()

	var errs []error
	if started {
		for _, f := range filters {
			if err := r.tr.Unsubscribe(ctx, f); err != nil {
				errs = append(errs, fmt.Errorf("publisher: unsubscribe %s: %w", f, err))
			}
		}
	}
	// Outside r.mu: close drains, so it blocks on whatever a handler is
	// doing, and a handler is free to call back into the router.
	pool.close()
	return errors.Join(errs...)
}

// WaitIdle blocks until every command accepted before this call has run.
//
// A test barrier, and it is exported because the barrier is not reachable
// from outside the package any other way: handlers run off the caller's
// goroutine, so a consumer's own test that delivers a message and then
// asserts on its fake sink is racing the router. Production code does not
// need it — commands are fire-and-forget by design, and shutdown is
// [CommandRouter.Stop]'s job.
//
// A no-op on a router that has not started or has stopped: there is
// nothing accepted to wait for.
func (r *CommandRouter) WaitIdle() {
	r.mu.Lock()
	pool := r.pool
	r.mu.Unlock()
	pool.flush()
}

// Route reports which registered filter claims topic, and what its wildcards
// matched.
//
// The router's own decision, exposed: a consumer building a conformance
// check, or an operator diagnosing why a button does nothing, needs to ask
// "who would get this?" without publishing anything. ok is false for a topic
// no route claims.
func (r *CommandRouter) Route(topic string) (cmd Command, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cmd, _, ok = r.resolveLocked(topic)
	return cmd, ok
}

// Claims reports whether any registered route matches topic — the one-bit
// form of [CommandRouter.Route], and what [CommandRouter.CheckDisjoint] is
// built from.
func (r *CommandRouter) Claims(topic string) bool {
	_, ok := r.Route(topic)
	return ok
}

// CheckDisjoint fails when any of topics would be delivered back into this
// router's own handlers.
//
// This is the guard the measured consumer learned the hard way, and it is
// worth writing down exactly what it costs to skip it. A consumer subscribes
// to its command topics and publishes its state topics on the same broker,
// usually on the same connection. The broker has no notion of "my own
// message": it fans every publish out to every matching subscription,
// including the publisher's, with the retain flag clear because live routing
// is not a retained replay (MQTT 3.1.1 and 5.0 §3.3.1.3). So a state topic
// that any command filter matches is not a cosmetic overlap — it is the
// consumer issuing itself a command every time it reports state.
//
// In the measured case the state of a "program" entity was mirrored onto the
// same topic its trigger command arrived on. Every state publish executed
// the program: on every boot, on every rediscovery, for every program in the
// house including the deliberately deactivated ones. The retain-flag check
// that would normally save a handler does not fire, because the echo is live
// traffic. The only signals were the programs running.
//
// Run every topic the consumer publishes past this — state, availability,
// attributes, the birth topic — once, at boot, and fail the boot. The
// returned error wraps [ErrStateCommandCollision] and names each colliding
// pair; all collisions are reported, not just the first, because a
// consumer that has one usually has a family of them.
//
// It does not, and cannot, prove the converse: a command topic no handler
// claims is [CommandRouter.Route]'s question, and an overlap with some other
// process's tree is nobody's.
func (r *CommandRouter) CheckDisjoint(topics ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, t := range topics {
		if t == "" {
			continue
		}
		for i := range r.routes {
			if matchFilter(r.routes[i].parts, t) {
				errs = append(errs, fmt.Errorf("%w: %q matches %q",
					ErrStateCommandCollision, t, r.routes[i].filter))
			}
		}
	}
	return errors.Join(errs...)
}

// subscribe registers one filter, preferring the No Local form when the
// transport offers it. See [NoLocalSubscriber].
func (r *CommandRouter) subscribe(ctx context.Context, filter string) error {
	// The subscribe context deliberately does not reach the handler: a
	// command's context derives from [CommandConfig.Lifecycle], because
	// Start's ctx may be a config reload's and die when the reload
	// returns. See the note on that field.
	handler := func(topic string, payload []byte, retained bool) { //nolint:contextcheck // see above
		r.deliver(filter, topic, payload, retained)
	}
	if nl, ok := r.tr.(NoLocalSubscriber); ok {
		return nl.SubscribeNoLocal(ctx, filter, r.qos, handler)
	}
	return r.tr.Subscribe(ctx, filter, r.qos, handler)
}

// deliver is the transport-facing handler for one subscription. It runs on
// the transport's read loop and must return promptly; everything it does
// beyond resolving the route is an enqueue.
//
// The locking is hand-rolled rather than deferred because the shutdown gate
// and the enqueue must be one atomic step against Stop — see the lock-order
// note on [CommandRouter] — while the two calls that can reach consumer
// code, the logger and [CommandConfig.OnUnroutable], must not run under the
// route lock at all.
func (r *CommandRouter) deliver(filter, topic string, payload []byte, retained bool) {
	r.mu.Lock()
	if r.stopped || r.pool == nil {
		r.mu.Unlock()
		r.log.Debug("publisher.command.after_stop", slog.String("topic", topic))
		return
	}
	cmd, handler, ok := r.resolveLocked(topic)
	if !ok {
		r.mu.Unlock()
		// Nothing claims it. A shared broker carries traffic that is not
		// this consumer's, and a route removed while its subscription
		// lingers lands here too, so this is a diagnostic rather than a
		// failure.
		if r.cfg.OnUnroutable != nil {
			r.cfg.OnUnroutable(topic, payload)
		}
		r.log.Warn("publisher.command.unroutable",
			slog.String("topic", topic), slog.String("filter", filter))
		return
	}
	if retained && !r.cfg.DeliverRetained {
		r.mu.Unlock()
		r.log.Debug("publisher.command.retained_drop", slog.String("topic", topic))
		return
	}

	cmd.Payload = append([]byte(nil), payload...)
	cmd.Retained = retained
	// Keyed by topic, so two commands for the same entity never reorder
	// while unrelated entities proceed in parallel.
	r.pool.enqueue(topic, func() {
		ctx, cancel := context.WithCancel(r.cfg.Lifecycle)
		defer cancel()
		handler(ctx, cmd)
	})
	r.mu.Unlock()
}

// resolveLocked finds the route matching topic. Callers hold r.mu.
//
// At most one can match: registration refuses any overlapping pair, which
// is what makes this a search rather than the specificity tournament an
// earlier version ran. The tournament was not merely redundant — it picked
// a winner per delivered COPY of a message rather than per message, so it
// was the mechanism by which one command ran its handler N times. See
// [ErrAmbiguousRoutes].
func (r *CommandRouter) resolveLocked(topic string) (cmd Command, handler CommandHandler, ok bool) {
	for i := range r.routes {
		wild, rest, matched := captureFilter(r.routes[i].parts, topic)
		if !matched {
			continue
		}
		return Command{
			Topic:     topic,
			Filter:    r.routes[i].filter,
			Wildcards: wild,
			Remainder: rest,
		}, r.routes[i].handler, true
	}
	return Command{}, nil, false
}

func (r *CommandRouter) routeFiltersLocked() []string {
	out := make([]string, 0, len(r.routes))
	for i := range r.routes {
		out = append(out, r.routes[i].filter)
	}
	return out
}

// sharedPrefix introduces a shared subscription, MQTT 5.0 §4.8.2.
const sharedPrefix = "$share/"

// splitShared decides whether filter is a well-formed shared subscription
// and, if so, returns the filter that actually takes part in matching.
//
// Well-formed means the three levels §4.8.2 requires: the literal
// `$share`, a non-empty ShareName carrying neither `+` nor `#` (a `/`
// cannot occur in one — it terminates it), and a non-empty topic filter
// after it. Anything else is not a shared subscription and is the ordinary
// literal filter it looks like on the wire, which is what keeps the literal
// filter `$share` matching the literal topic `$share`.
func splitShared(filter string) (wrapped string, ok bool) {
	rest, found := strings.CutPrefix(filter, sharedPrefix)
	if !found {
		return "", false
	}
	shareName, wrapped, found := strings.Cut(rest, "/")
	if !found || shareName == "" || wrapped == "" || strings.ContainsAny(shareName, "+#") {
		return "", false
	}
	return wrapped, true
}

// filterParts splits filter into the levels that take part in matching.
//
// For a shared subscription that is the wrapped filter alone: a PUBLISH
// carries the real topic and never the `$share/{ShareName}/` prefix
// (§4.8.2), so the prefix is structural and matching it would match
// nothing. Registration, matching and the overlap check all go through
// this, which is what keeps a shared route's overlap with a plain one
// visible.
func filterParts(filter string) []string {
	if wrapped, ok := splitShared(filter); ok {
		filter = wrapped
	}
	return strings.Split(filter, "/")
}

// ValidateFilter reports whether filter is a topic filter MQTT permits.
//
// Checked at registration rather than left to the broker, because a broker
// answers a malformed filter with a SUBACK failure code that the transport
// interface deliberately drops — so the only symptom would be a route that
// never fires. The rules are §4.7: a filter is non-empty, no longer than
// 65535 bytes, valid UTF-8 without U+0000 (§1.5.4), `+` occupies a whole
// level, and `#` occupies a whole level and is the last one.
//
// A filter starting with `$share/` is additionally held to §4.8.2's
// structure — `$share/{ShareName}/{filter}` with a non-empty ShareName
// carrying no wildcard — and its wrapped filter is then validated exactly
// like a standalone one.
//
// The rule set is deliberately the same one go-mqtt's
// protocol.ValidateTopicFilter enforces, because a filter this module
// accepts and go-mqtt routes differently is a silent routing failure. A
// differential enumeration found the two disagreeing on 109 filter shapes,
// every one of them `$share`-prefixed: they were accepted here as ordinary
// literal levels, so a multi-instance consumer registering a shared
// subscription got a route that could never fire — with nothing to show for
// it but a `publisher.command.unroutable` warning per command. The UTF-8
// and U+0000 checks were missing outright.
//
// One deliberate difference remains, in the matcher rather than here: a
// filter with `#` before its last level matches nothing instead of being
// read as if the `#` ended it. Both reject it, so it cannot be registered;
// [MatchFilter] is reachable with any string and failing closed is the
// safer answer there. See [captureFilter].
func ValidateFilter(filter string) error {
	if filter == "" {
		return fmt.Errorf("%w: empty", ErrInvalidFilter)
	}
	if len(filter) > 65535 {
		return fmt.Errorf("%w: longer than an MQTT topic may be", ErrInvalidFilter)
	}
	if !utf8.ValidString(filter) {
		return fmt.Errorf("%w: %q is not valid UTF-8", ErrInvalidFilter, filter)
	}
	if strings.ContainsRune(filter, 0) {
		return fmt.Errorf("%w: %q contains U+0000", ErrInvalidFilter, filter)
	}
	if rest, found := strings.CutPrefix(filter, sharedPrefix); found {
		shareName, wrapped, split := strings.Cut(rest, "/")
		switch {
		case !split:
			return fmt.Errorf("%w: %q must be $share/{ShareName}/{filter}", ErrInvalidFilter, filter)
		case shareName == "":
			return fmt.Errorf("%w: %q has an empty ShareName", ErrInvalidFilter, filter)
		case strings.ContainsAny(shareName, "+#"):
			return fmt.Errorf("%w: %q has a wildcard in its ShareName", ErrInvalidFilter, filter)
		case wrapped == "":
			return fmt.Errorf("%w: %q wraps an empty topic filter", ErrInvalidFilter, filter)
		}
		return validateFilterLevels(filter, wrapped)
	}
	return validateFilterLevels(filter, filter)
}

// validateFilterLevels enforces MQTT's wildcard placement on the matchable
// part of a filter, naming the whole filter in the error so an operator
// reading a log sees what they registered.
func validateFilterLevels(filter, matchable string) error {
	parts := strings.Split(matchable, "/")
	for i, p := range parts {
		switch {
		case p == "+" || p == "#":
			if p == "#" && i != len(parts)-1 {
				return fmt.Errorf("%w: %q has `#` before the last level", ErrInvalidFilter, filter)
			}
		case strings.ContainsAny(p, "+#"):
			return fmt.Errorf("%w: %q has a wildcard sharing a level", ErrInvalidFilter, filter)
		}
	}
	return nil
}

// MatchFilter reports whether topic matches the MQTT topic filter filter.
//
// Exported because the disjointness invariant is checkable outside a running
// router — a consumer's own test comparing a rendered discovery config's
// state topics against the filters it intends to subscribe should not have
// to build a router, or hand-roll the matcher a fourth time in this family.
//
// `+` matches exactly one level, `#` matches the remainder including zero
// levels, so `a/#` matches `a`. Neither wildcard matches a topic beginning
// with `$`, per §4.7.2, which is what keeps a broad filter off `$SYS`.
//
// A shared-subscription filter (`$share/{ShareName}/{filter}`, §4.8.2)
// matches against the topic a PUBLISH actually carries, which is the real
// topic and never carries the prefix: the `$share/{ShareName}/` levels are
// stripped and only the wrapped filter matches, so
// MatchFilter("$share/grp/sensors/+", "sensors/temp") is true. Treating
// them as literal levels — which this did — meant every shared
// subscription a consumer registered routed nothing at all while go-mqtt
// delivered on it. See [ValidateFilter].
func MatchFilter(filter, topic string) bool {
	return matchFilter(filterParts(filter), topic)
}

func matchFilter(parts []string, topic string) bool {
	_, _, ok := captureFilter(parts, topic)
	return ok
}

// captureFilter matches topic against a pre-split filter and returns what
// the wildcards took.
func captureFilter(parts []string, topic string) (wildcards []string, remainder string, ok bool) {
	tp := strings.Split(topic, "/")
	if len(tp) > 0 && strings.HasPrefix(tp[0], "$") && len(parts) > 0 &&
		(parts[0] == "+" || parts[0] == "#") {
		// §4.7.2: a wildcard at the first level must not reach the
		// broker's own `$SYS` tree.
		return nil, "", false
	}
	for i, f := range parts {
		if f == "#" {
			// §4.7.1.2: `#` must be the last level of the filter. An
			// exported matcher is reachable with any string, including one
			// no [ValidateFilter] ever saw, and "matches nothing" is the
			// only safe answer for a filter a broker would reject outright
			// -- treating `a/#/b` as `a/#` would silently widen a
			// subscription the consumer believes is narrow.
			if i != len(parts)-1 {
				return nil, "", false
			}
			return wildcards, strings.Join(tp[i:], "/"), true
		}
		if i >= len(tp) {
			return nil, "", false
		}
		if f == "+" {
			wildcards = append(wildcards, tp[i])
			continue
		}
		if f != tp[i] {
			return nil, "", false
		}
	}
	if len(parts) != len(tp) {
		return nil, "", false
	}
	return wildcards, "", true
}

// filtersOverlap reports whether some topic matches both pre-split filters.
//
// Decided structurally rather than by enumerating topics, because the point
// is to catch the overlap at registration — before any topic exists that
// would demonstrate it. The measured consumer's collision (a seven-level
// all-wildcard filter against a seven-level filter with one literal) is
// invisible to any check that only looks at the filters it happens to have
// seen traffic for.
func filtersOverlap(a, b []string) bool {
	for {
		switch {
		case len(a) == 0 && len(b) == 0:
			return true
		case len(a) == 0:
			return len(b) == 1 && b[0] == "#"
		case len(b) == 0:
			return len(a) == 1 && a[0] == "#"
		case a[0] == "#" || b[0] == "#":
			return true
		case a[0] != "+" && b[0] != "+" && a[0] != b[0]:
			return false
		}
		a, b = a[1:], b[1:]
	}
}

// commandTopicKeys are the discovery keys that name a topic Home Assistant
// publishes TO and this consumer must therefore subscribe, but whose name
// does not end in `command_topic`.
//
// Two of them, and they are the reason the extraction is a table plus a
// suffix rule rather than a suffix rule alone: `cover.set_position_topic`
// and `vacuum.set_fan_speed_topic` are command topics with a different
// naming convention, and a consumer deriving its subscriptions from the
// suffix alone ships a cover whose position slider does nothing.
var commandTopicKeys = map[string]bool{
	"command_topic":       true,
	"set_position_topic":  true,
	"set_fan_speed_topic": true,
}

// CommandTopics lists the topics a component tells Home Assistant to publish
// commands to, sorted and deduplicated.
//
// Read out of the component's own rendered JSON rather than off its fields,
// because the fields are the point: a climate entity names five command
// topics across [discovery.ClimateFields], a light names ten, and the set
// grows with the catalog. A consumer subscribing what it thinks it
// advertises, rather than what it actually advertises, is how a `light` gets
// an effect selector that silently does nothing — and that list is
// regenerated from the catalog, not maintained here.
//
// The classification is `command_topic`, any key ending in
// `_command_topic`, and the two exceptions in the paragraph above.
func CommandTopics(comp discovery.Component) ([]string, error) {
	body, err := componentBody(comp)
	if err != nil {
		return nil, err
	}
	return topicsOfKind(body, true), nil
}

// StateTopics lists the topics a component tells Home Assistant to read
// from, sorted and deduplicated — every `*_topic` key that is not a command
// topic, availability and JSON attributes included.
//
// It is the other half of [CommandRouter.CheckDisjoint]'s input, and it is
// deliberately generous: availability and attribute topics are published by
// this consumer just like state is, so a command filter matching one of them
// is the same self-inflicted write, just harder to spot.
func StateTopics(comp discovery.Component) ([]string, error) {
	body, err := componentBody(comp)
	if err != nil {
		return nil, err
	}
	return topicsOfKind(body, false), nil
}

// BundleCommandTopics lists every command topic a device document
// advertises, across all its components, sorted and deduplicated.
//
// The shape a consumer actually has at boot: it renders one bundle per
// device, and wants the subscription set for the fleet. Deduplicated because
// two components of one device legitimately share a command topic — a
// climate entity's mode and a select mirroring it, in the measured case.
func BundleCommandTopics(b *discovery.Bundle) ([]string, error) {
	return bundleTopics(b, true)
}

// BundleStateTopics lists every non-command topic a device document
// advertises. Pass it to [CommandRouter.CheckDisjoint] at boot; see there
// for what it buys.
func BundleStateTopics(b *discovery.Bundle) ([]string, error) {
	return bundleTopics(b, false)
}

func bundleTopics(b *discovery.Bundle, command bool) ([]string, error) {
	if b == nil {
		return nil, nil
	}
	seen := map[string]bool{}
	for _, key := range b.Keys() {
		body, err := componentBody(b.Components[key])
		if err != nil {
			return nil, fmt.Errorf("publisher: component %s: %w", key, err)
		}
		for _, t := range topicsOfKind(body, command) {
			seen[t] = true
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out, nil
}

// componentBody renders a component to the generic map the key rules run
// over. It goes through the component's own MarshalJSON so the typed keys,
// the per-platform Fields struct and Extra are all flattened exactly as they
// will be on the wire — the whole point being to read what is advertised,
// not what a second implementation believes is.
func componentBody(comp discovery.Component) (map[string]any, error) {
	raw, err := json.Marshal(comp)
	if err != nil {
		return nil, fmt.Errorf("publisher: encode component: %w", err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("publisher: decode component: %w", err)
	}
	return body, nil
}

// topicsOfKind picks the string-valued topic keys of one kind out of a
// rendered component body.
func topicsOfKind(body map[string]any, command bool) []string {
	out := make([]string, 0, 4)
	seen := map[string]bool{}
	add := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}
	for key, v := range body {
		// `availability` is a LIST of objects, each with its own "topic",
		// and it is the only form this module's discovery pipeline
		// produces. A walk over the top level alone therefore finds every
		// state topic and no availability topic -- so a consumer following
		// [StateTopics]'s documented use, checking its writing surface for
		// collisions, would miss exactly the echo that fires on every
		// availability flip.
		if !command && key == "availability" {
			entries, isList := v.([]any)
			if !isList {
				continue
			}
			for _, e := range entries {
				entry, isMap := e.(map[string]any)
				if !isMap {
					continue
				}
				if t, isString := entry["topic"].(string); isString {
					add(t)
				}
			}
			continue
		}
		if !isTopicKey(key) {
			continue
		}
		if isCommandTopicKey(key) != command {
			continue
		}
		if s, isString := v.(string); isString {
			add(s)
		}
	}
	sort.Strings(out)
	return out
}

func isTopicKey(key string) bool {
	return key == "topic" || strings.HasSuffix(key, "_topic")
}

func isCommandTopicKey(key string) bool {
	return commandTopicKeys[key] || strings.HasSuffix(key, "_command_topic")
}
