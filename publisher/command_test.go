// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// cmdBroker is a transport that behaves the way a real broker behaves
// toward a consumer's own connection: every Publish is routed straight back
// to each MATCHING subscription with the retain flag clear.
//
// The echo is the point, and it is what the package's other fake
// (fakeTransport) also does — this one is separate because the routing tests
// need to fail subscribes selectively, to drop the subscription set the way
// a reconnect does, and to record which filters were asked for in order.
// Live routing carries Retain=0 (MQTT 3.1.1 / 5.0 §3.3.1.3); only the
// replay to a fresh subscription is retained, which is why the retained-drop
// guard does not save a consumer from an echo.
type cmdBroker struct {
	mu     sync.Mutex
	subs   map[string]Handler
	subbed []string
	unsub  []string
	noLoc  bool // record SubscribeNoLocal use; see cmdBrokerNoLocal

	failSubscribe func(filter string) error
	// failUnsubscribe exists because without it the rollback path in
	// Start and the teardown path in Stop are unreachable by
	// construction: every other fixture in this package returns nil from
	// Unsubscribe unconditionally, and a broker that refuses an
	// UNSUBSCRIBE is exactly the case that used to leave a live,
	// ungated subscription behind a failed Start.
	failUnsubscribe func(filter string) error
}

func newCmdBroker() *cmdBroker {
	return &cmdBroker{subs: map[string]Handler{}}
}

func (b *cmdBroker) Publish(_ context.Context, topic string, payload []byte, _ byte, _ bool) error {
	b.fanout(topic, payload, false)
	return nil
}

func (b *cmdBroker) Subscribe(_ context.Context, filter string, _ byte, h Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failSubscribe != nil {
		if err := b.failSubscribe(filter); err != nil {
			return err
		}
	}
	b.subs[filter] = h
	b.subbed = append(b.subbed, filter)
	return nil
}

func (b *cmdBroker) Unsubscribe(_ context.Context, filter string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unsub = append(b.unsub, filter)
	if b.failUnsubscribe != nil {
		if err := b.failUnsubscribe(filter); err != nil {
			// The subscription stays live, which is what a broker
			// refusing an UNSUBSCRIBE leaves behind.
			return err
		}
	}
	delete(b.subs, filter)
	return nil
}

// deliver drives one inbound message through the two fan-outs a command
// actually survives, and returns the number of copies the broker produced.
//
// Both multiplications are real and they compose, which is the thing the
// previous version of this fake got wrong by modelling only the first:
//
//   - The BROKER sends one copy of the message per matching subscription.
//     MQTT 3.1.1 §4.7.3 and 5.0 §3.3.4 permit it and both Mosquitto and
//     EMQX do it; it was measured against Mosquitto 2.1.2 on v3.1.1 and
//     v5 alike, with two separate clients so No-Local is not in play.
//   - The CLIENT then re-matches every arriving copy against its whole
//     local filter list and calls EVERY matching handler, without
//     correlating a copy with the subscription it arrived on
//     (go-mqtt's TCPClient.dispatch).
//
// So N overlapping routes cost N copies x N handler invocations, of which N
// reach a handler — which is why the router refuses overlapping routes
// outright instead of resolving them by specificity. See
// [CommandRouter.Handle].
func (b *cmdBroker) deliver(topic string, payload []byte, retained bool) int {
	return b.fanout(topic, payload, retained)
}

// fanout is the two-stage delivery both Publish and deliver go through.
func (b *cmdBroker) fanout(topic string, payload []byte, retained bool) (copies int) {
	b.mu.Lock()
	targets := make([]Handler, 0, len(b.subs))
	for filter, h := range b.subs {
		if MatchFilter(filter, topic) {
			targets = append(targets, h)
		}
	}
	b.mu.Unlock()
	// One copy per matching subscription; each copy visits every matching
	// local handler.
	for range targets {
		for _, h := range targets {
			h(topic, payload, retained)
		}
	}
	return len(targets)
}

// drop forgets every subscription without telling the router — a reconnect
// against a broker that did not keep the session.
func (b *cmdBroker) drop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = map[string]Handler{}
}

func (b *cmdBroker) filters() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.subs))
	for f := range b.subs {
		out = append(out, f)
	}
	return out
}

func (b *cmdBroker) subscribed() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.subbed...)
}

func (b *cmdBroker) unsubscribed() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.unsub...)
}

// cmdBrokerNoLocal is a cmdBroker that also offers the No Local capability.
type cmdBrokerNoLocal struct{ *cmdBroker }

func (b cmdBrokerNoLocal) SubscribeNoLocal(ctx context.Context, filter string, qos byte, h Handler) error {
	b.mu.Lock()
	b.noLoc = true
	b.mu.Unlock()
	return b.Subscribe(ctx, filter, qos, h)
}

// recorder collects the commands a handler received.
type recorder struct {
	mu   sync.Mutex
	got  []Command
	hook func(Command)
}

func (rc *recorder) handle(_ context.Context, cmd Command) {
	rc.mu.Lock()
	rc.got = append(rc.got, cmd)
	hook := rc.hook
	rc.mu.Unlock()
	if hook != nil {
		hook(cmd)
	}
}

func (rc *recorder) snapshot() []Command {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]Command(nil), rc.got...)
}

func (rc *recorder) count() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.got)
}

// quietRouter builds a router whose diagnostics do not pollute test output;
// the warn-level unroutable line is deliberate production behaviour and
// there is one per unroutable-path test.
func quietRouter(tb testing.TB, tr Transport, cfg CommandConfig) *CommandRouter {
	tb.Helper()
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return NewCommandRouter(tr, cfg)
}

func TestNewCommandRouterNilTransportPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("a nil transport must panic at construction, not at the first subscribe")
		}
	}()
	NewCommandRouter(nil, CommandConfig{})
}

func TestValidateFilter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		filter string
		ok     bool
	}{
		{"a/b/c", true},
		{"a/+/c", true},
		{"a/#", true},
		{"#", true},
		{"+", true},
		{"", false},
		{"a/#/b", false},
		{"a/b+/c", false},
		{"a/#b", false},
	} {
		err := ValidateFilter(tc.filter)
		if tc.ok != (err == nil) {
			t.Errorf("ValidateFilter(%q) = %v, want ok=%v", tc.filter, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrInvalidFilter) {
			t.Errorf("ValidateFilter(%q) error does not wrap ErrInvalidFilter", tc.filter)
		}
	}
}

func TestMatchFilter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		filter, topic string
		want          bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
		{"a/+/c", "a/x/c", true},
		{"a/+/c", "a/x/y/c", false},
		{"a/#", "a", true},
		{"a/#", "a/b/c", true},
		{"#", "a/b", true},
		{"+/b", "a/b", true},
		{"a/b", "a/b/c", false},
		// §4.7.2: a leading wildcard must not reach the broker's own tree.
		{"#", "$SYS/broker/uptime", false},
		{"+/broker", "$SYS/broker", false},
		{"$SYS/#", "$SYS/broker", true},
	} {
		if got := MatchFilter(tc.filter, tc.topic); got != tc.want {
			t.Errorf("MatchFilter(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}

func TestCommandRouterHandleRejectsBadRegistrations(t *testing.T) {
	t.Parallel()
	r := quietRouter(t, newCmdBroker(), CommandConfig{})
	rc := &recorder{}

	if err := r.Handle("a/+/set", nil); !errors.Is(err, ErrInvalidFilter) {
		t.Errorf("nil handler: %v, want ErrInvalidFilter", err)
	}
	if err := r.Handle("a/#/set", rc.handle); !errors.Is(err, ErrInvalidFilter) {
		t.Errorf("malformed filter: %v, want ErrInvalidFilter", err)
	}
	if err := r.Handle("a/+/set", rc.handle); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := r.Handle("a/+/set", rc.handle); !errors.Is(err, ErrDuplicateRoute) {
		t.Errorf("duplicate: %v, want ErrDuplicateRoute", err)
	}
	// a/+/set vs +/b/set both match a/b/set and neither dominates.
	if err := r.Handle("+/b/set", rc.handle); !errors.Is(err, ErrAmbiguousRoutes) {
		t.Errorf("unorderable overlap: %v, want ErrAmbiguousRoutes", err)
	}
	// An ORDERABLE overlap is refused too, and that is the S1 fix: a
	// specificity winner is picked per delivered copy of a message, not
	// per message, so `a/week_profile/set` alongside `a/+/set` ran one
	// message's handler twice against Mosquitto 2.1.2.
	if err := r.Handle("a/week_profile/set", rc.handle); !errors.Is(err, ErrAmbiguousRoutes) {
		t.Errorf("orderable overlap: %v, want ErrAmbiguousRoutes", err)
	}
	// Non-overlapping is always fine.
	if err := r.Handle("b/+/+/set", rc.handle); err != nil {
		t.Errorf("disjoint filter rejected: %v", err)
	}
	if got, want := len(r.Filters()), 2; got != want {
		t.Errorf("Filters() has %d entries, want %d", got, want)
	}
}

func TestCommandRouterSubscribesExactlyTheRegisteredFilters(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})
	rc := &recorder{}
	for _, f := range []string{"gh/+/+/set", "gh/alarm/+/arm"} {
		if err := r.Handle(f, rc.handle); err != nil {
			t.Fatalf("handle %s: %v", f, err)
		}
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	got := strings.Join(sorted(b.subscribed()), ",")
	if want := "gh/+/+/set,gh/alarm/+/arm"; got != want {
		t.Fatalf("subscribed %q, want %q — the router must subscribe its routes and nothing broader", got, want)
	}
	if err := r.Handle("gh/x/set", rc.handle); !errors.Is(err, ErrRouterStarted) {
		t.Errorf("Handle after Start: %v, want ErrRouterStarted", err)
	}
	if err := r.Start(context.Background()); !errors.Is(err, ErrRouterStarted) {
		t.Errorf("second Start: %v, want ErrRouterStarted", err)
	}
}

func TestCommandRouterStartRollsBackOnSubscribeFailure(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	boom := errors.New("broker said no")
	b.failSubscribe = func(filter string) error {
		if filter == "gh/b/set" {
			return boom
		}
		return nil
	}
	r := quietRouter(t, b, CommandConfig{})
	rc := &recorder{}
	for _, f := range []string{"gh/a/set", "gh/b/set", "gh/c/set"} {
		if err := r.Handle(f, rc.handle); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if err := r.Start(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("start: %v, want the broker's error", err)
	}
	if live := b.filters(); len(live) != 0 {
		t.Fatalf("subscriptions %v survived a failed Start; a partial filter set silently drops commands", live)
	}
	// Having rolled back, the router can be started again once the broker
	// recovers — a partial start that could never be retried would be the
	// worse of the two failure modes.
	b.failSubscribe = nil
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("retry start: %v", err)
	}
}

func TestCommandRouterDispatchCapturesWildcards(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})
	rc := &recorder{}
	if err := r.Handle("gh/+/+/+/set", rc.handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	tail := &recorder{}
	if err := r.Handle("raw/#", tail.handle); err != nil {
		t.Fatalf("handle tail: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	b.deliver("gh/ccu/0001ABCD:1/LEVEL/set", []byte("0.5"), false)
	b.deliver("raw/deep/deeper", []byte("x"), false)
	r.WaitIdle()

	got := rc.snapshot()
	if len(got) != 1 {
		t.Fatalf("handler saw %d commands, want 1", len(got))
	}
	cmd := got[0]
	if cmd.Topic != "gh/ccu/0001ABCD:1/LEVEL/set" || cmd.Filter != "gh/+/+/+/set" {
		t.Errorf("topic/filter = %q / %q", cmd.Topic, cmd.Filter)
	}
	if strings.Join(cmd.Wildcards, "|") != "ccu|0001ABCD:1|LEVEL" {
		t.Errorf("Wildcards = %v, want the three `+` levels in order", cmd.Wildcards)
	}
	if string(cmd.Payload) != "0.5" {
		t.Errorf("Payload = %q", cmd.Payload)
	}
	if cmd.Remainder != "" {
		t.Errorf("Remainder = %q, want empty for a filter without `#`", cmd.Remainder)
	}

	tailGot := tail.snapshot()
	if len(tailGot) != 1 || tailGot[0].Remainder != "deep/deeper" {
		t.Fatalf("`#` route got %+v, want Remainder=%q", tailGot, "deep/deeper")
	}
}

// TestCommandRouterPayloadIsClonedForTheHandler pins that a handler may keep
// its payload: it runs after the read loop has moved on and may be looking
// at a buffer the transport has already reused.
func TestCommandRouterPayloadIsClonedForTheHandler(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})
	rc := &recorder{}
	if err := r.Handle("gh/+/set", rc.handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	buf := []byte("on")
	b.deliver("gh/lamp/set", buf, false)
	r.WaitIdle()
	copy(buf, "of")

	got := rc.snapshot()
	if len(got) != 1 || string(got[0].Payload) != "on" {
		t.Fatalf("payload = %q, want %q — the router must clone before handing it off", got[0].Payload, "on")
	}
}

// TestCommandRouterOneHandlerPerMessage is the regression for the measured
// double-dispatch, and it is the pair that reproduced it against Mosquitto
// 2.1.2: `gh/+/+/+/+/set` and `gh/+/+/+/week_profile/set` overlap, the
// broker sends one copy per matching subscription, and go-mqtt calls every
// locally matching handler for each copy — so the specificity winner was
// chosen once per COPY and the profile selection ran twice.
//
// The router now refuses the pair, and what a consumer keeps instead is one
// route per shape with the discrimination inside the handler. Both halves
// are asserted here, against a fake that models both fan-outs.
func TestCommandRouterOneHandlerPerMessage(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})
	generic, specific := &recorder{}, &recorder{}
	if err := r.Handle("gh/+/+/+/+/set", generic.handle); err != nil {
		t.Fatalf("handle generic: %v", err)
	}
	err := r.Handle("gh/+/+/+/week_profile/set", specific.handle)
	if !errors.Is(err, ErrAmbiguousRoutes) {
		t.Fatalf("overlapping route accepted (%v); one message would run a handler twice", err)
	}
	if !strings.Contains(err.Error(), "gh/+/+/+/+/set") ||
		!strings.Contains(err.Error(), "gh/+/+/+/week_profile/set") {
		t.Errorf("the refusal must name both filters, got %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	// One subscription matches, so the broker makes one copy and the
	// client's local fan-out has one handler to offer it to.
	if n := b.deliver("gh/ccu/HmIP/0001:1/week_profile/set", []byte("P2"), false); n != 1 {
		t.Fatalf("broker fanned out to %d subscriptions, want 1", n)
	}
	if n := b.deliver("gh/ccu/HmIP/0001:1/LEVEL/set", []byte("1"), false); n != 1 {
		t.Fatalf("plain data-point topic hit %d subscriptions, want 1", n)
	}
	r.WaitIdle()

	if got := specific.count(); got != 0 {
		t.Errorf("the refused route ran %d times, want 0", got)
	}
	got := generic.snapshot()
	if len(got) != 2 {
		t.Fatalf("the surviving route ran %d times, want exactly one per message", len(got))
	}
	// The discrimination the refused route used to buy is the handler's
	// now, and the wildcard capture is what it reads.
	if got[0].Wildcards[3] != "week_profile" || got[1].Wildcards[3] != "LEVEL" {
		t.Errorf("handler saw %v / %v, want the parameter name in the fourth `+`",
			got[0].Wildcards, got[1].Wildcards)
	}
}

// TestCommandRouterRefusesEveryOverlapShape pins the disjointness rule
// across the shapes a consumer actually writes, including the measured pair
// (`ccu/+/+/set` with `ccu/+/PRESS_SHORT/set`) whose two handler runs held
// the release.
func TestCommandRouterRefusesEveryOverlapShape(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		a, b    string
		refused bool
	}{
		{"ccu/+/+/set", "ccu/+/PRESS_SHORT/set", true},
		{"base/dev/set", "base/dev/set/#", true},
		{"base/#", "base/dev/set", true},
		{"a/+/c", "a/b/+", true},
		{"a/b/set", "a/c/set", false},
		{"a/+/set", "a/+/get", false},
		{"a/b/#", "a/c/#", false},
	} {
		r := quietRouter(t, newCmdBroker(), CommandConfig{})
		if err := r.Handle(tc.a, (&recorder{}).handle); err != nil {
			t.Fatalf("handle %q: %v", tc.a, err)
		}
		err := r.Handle(tc.b, (&recorder{}).handle)
		if refused := errors.Is(err, ErrAmbiguousRoutes); refused != tc.refused {
			t.Errorf("Handle(%q) after %q = %v, want refused=%v", tc.b, tc.a, err, tc.refused)
		}
	}
}

func TestCommandRouterUnroutableMessage(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	var (
		mu    sync.Mutex
		seen  []string
		rcCnt = &recorder{}
	)
	r := quietRouter(t, b, CommandConfig{
		OnUnroutable: func(topic string, _ []byte) {
			mu.Lock()
			seen = append(seen, topic)
			mu.Unlock()
		},
	})
	if err := r.Handle("gh/+/set", rcCnt.handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	// A transport that hands the router a topic no route claims — a
	// coarser subscription somewhere else on the same client, or a route
	// removed while its subscription lingers. It must be a diagnostic, not
	// a panic and not a dropped connection.
	r.deliver("gh/+/set", "somebody/elses/topic", []byte("x"), false)
	r.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != "somebody/elses/topic" {
		t.Fatalf("OnUnroutable saw %v, want the one unclaimed topic", seen)
	}
	if rcCnt.count() != 0 {
		t.Error("an unroutable message reached a handler")
	}
}

func TestCommandRouterRetainedIsDroppedByDefault(t *testing.T) {
	t.Parallel()
	for _, deliverRetained := range []bool{false, true} {
		b := newCmdBroker()
		rc := &recorder{}
		r := quietRouter(t, b, CommandConfig{DeliverRetained: deliverRetained})
		if err := r.Handle("gh/+/set", rc.handle); err != nil {
			t.Fatalf("handle: %v", err)
		}
		if err := r.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}
		b.deliver("gh/lamp/set", []byte("on"), true)
		r.WaitIdle()
		want := 0
		if deliverRetained {
			want = 1
		}
		if got := rc.count(); got != want {
			t.Errorf("DeliverRetained=%v: handler ran %d times, want %d — "+
				"a retained command replays on every single (re)subscribe",
				deliverRetained, got, want)
		}
		if err := r.Stop(context.Background()); err != nil {
			t.Fatalf("stop: %v", err)
		}
	}
}

func TestCommandRouterResubscribeAfterReconnect(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	rc := &recorder{}
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("gh/+/set", rc.handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	b.drop() // the broker did not keep the session across the reconnect
	if n := b.deliver("gh/lamp/set", []byte("on"), false); n != 0 {
		t.Fatalf("a dropped session still had %d subscriptions", n)
	}
	if err := r.Resubscribe(context.Background()); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	if n := b.deliver("gh/lamp/set", []byte("on"), false); n != 1 {
		t.Fatalf("after Resubscribe the topic hit %d subscriptions, want 1", n)
	}
	r.WaitIdle()
	if rc.count() != 1 {
		t.Fatalf("handler ran %d times after the reconnect, want 1", rc.count())
	}
	// Resubscribe is idempotent — the filter set is replaced, not doubled,
	// which is what keeps one-handler-per-message true across a flapping
	// link.
	if err := r.Resubscribe(context.Background()); err != nil {
		t.Fatalf("second resubscribe: %v", err)
	}
	if n := b.deliver("gh/lamp/set", []byte("on"), false); n != 1 {
		t.Fatalf("after a second Resubscribe the topic hit %d subscriptions, want 1", n)
	}
}

func TestCommandRouterResubscribeBeforeStartAndAfterStopIsQuiet(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("gh/+/set", (&recorder{}).handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Resubscribe(context.Background()); err != nil {
		t.Errorf("resubscribe before start: %v, want nil", err)
	}
	if len(b.subscribed()) != 0 {
		t.Error("resubscribe before start subscribed something")
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := r.Resubscribe(context.Background()); err != nil {
		t.Errorf("resubscribe after stop: %v, want nil — a reconnect callback during shutdown is ordinary", err)
	}
	if err := r.Start(context.Background()); !errors.Is(err, ErrRouterStarted) {
		t.Errorf("restart after stop: %v, want ErrRouterStarted", err)
	}
}

func TestCommandRouterStopUnsubscribesAndGatesHandlers(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	rc := &recorder{}
	r := quietRouter(t, b, CommandConfig{})
	for _, f := range []string{"gh/a/set", "gh/b/set"} {
		if err := r.Handle(f, rc.handle); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Keep a direct reference to the subscription handler: a broker that
	// has already queued a message delivers it while Stop is running, and
	// the router must not act on it.
	b.mu.Lock()
	h := b.subs["gh/a/set"]
	b.mu.Unlock()

	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got := strings.Join(sorted(b.unsubscribed()), ","); got != "gh/a/set,gh/b/set" {
		t.Errorf("unsubscribed %q, want both filters", got)
	}
	h("gh/a/set", []byte("late"), false)
	r.WaitIdle()
	if rc.count() != 0 {
		t.Fatal("a message delivered after Stop reached a handler")
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("second stop: %v, want nil — a shutdown path reached twice is normal", err)
	}
}

// TestCommandRouterHandlerRunsOffTheReadLoop is the pinned form of this
// package's sharpest decision. The transport delivers inline on the
// goroutine that also decodes PUBACK and PINGRESP; a command handler that
// blocks there stalls acknowledgement processing and eventually trips the
// keep-alive watchdog. The router therefore hands handlers to its own
// workers, and the delivery call must return long before the handler does.
func TestCommandRouterHandlerRunsOffTheReadLoop(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	rc := &recorder{hook: func(Command) {
		entered <- struct{}{}
		<-release
	}}
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("gh/+/set", rc.handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	start := time.Now()
	b.deliver("gh/lamp/set", []byte("on"), false)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("delivery took %v while the handler was still blocked; it must not run on the read loop", elapsed)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never ran")
	}

	// Stop drains rather than abandons: it must block on the handler that
	// is currently running.
	stopped := make(chan struct{})
	go func() {
		_ = r.Stop(context.Background())
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a handler was still running; it must drain, not abandon, accepted work")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop never returned after the handler was released — a worker leaked")
	}
}

// TestCommandRouterPreservesOrderPerTopic proves a burst on ONE topic is
// never reordered even though several workers run, which is the property
// that lets a consumer treat a topic as an ordered command channel.
func TestCommandRouterPreservesOrderPerTopic(t *testing.T) {
	t.Parallel()
	const n = 200
	b := newCmdBroker()
	var (
		mu   sync.Mutex
		seen []string
	)
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("gh/+/set", func(_ context.Context, cmd Command) {
		mu.Lock()
		seen = append(seen, string(cmd.Payload))
		mu.Unlock()
	}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	for i := range n {
		b.deliver("gh/lamp/set", []byte(strconv.Itoa(i)), false)
	}
	r.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("saw %d commands, want %d", len(seen), n)
	}
	for i, v := range seen {
		if v != strconv.Itoa(i) {
			t.Fatalf("command %d was %q — same-topic commands reordered", i, v)
		}
	}
}

func TestCommandRouterHandlerContextDerivesFromLifecycle(t *testing.T) {
	t.Parallel()
	lifecycle, cancel := context.WithCancel(context.Background())
	cancel()

	b := newCmdBroker()
	var (
		mu     sync.Mutex
		ctxErr error
		called bool
	)
	r := quietRouter(t, b, CommandConfig{Lifecycle: lifecycle})
	if err := r.Handle("gh/+/set", func(ctx context.Context, _ Command) {
		mu.Lock()
		ctxErr, called = ctx.Err(), true
		mu.Unlock()
	}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	// Start's own ctx is deliberately a different, live one: in the
	// measured consumer Start is reachable from a config reload whose
	// context dies when the reload returns.
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	b.deliver("gh/lamp/set", []byte("on"), false)
	r.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if !called {
		t.Fatal("handler never ran")
	}
	if ctxErr == nil {
		t.Error("the handler's ctx was live; it must derive from CommandConfig.Lifecycle")
	}
}

func TestCommandRouterUsesNoLocalWhenTheTransportOffersIt(t *testing.T) {
	t.Parallel()
	base := newCmdBroker()
	r := quietRouter(t, cmdBrokerNoLocal{base}, CommandConfig{})
	if err := r.Handle("gh/+/set", (&recorder{}).handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	base.mu.Lock()
	defer base.mu.Unlock()
	if !base.noLoc {
		t.Fatal("a transport offering No Local was subscribed without it; the echo class stays open on v5")
	}
}

// TestStateCommandDisjointness is the guard the reference implementation
// learned the hard way. A broker fans a publish out to every matching
// subscription INCLUDING the publisher's own, with the retain flag clear,
// so a state topic any command filter matches is the consumer issuing
// itself a command every time it reports state. In the measured case that
// executed every program in the house on every boot.
func TestStateCommandDisjointness(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	rc := &recorder{}
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("gh/+/programs/+/trigger", rc.handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	// The disjoint state topic passes the guard and, published through the
	// same connection, reaches no handler.
	good := "gh/ccu/programs/12459/state"
	if err := r.CheckDisjoint(good, "gh/ccu/hub/state"); err != nil {
		t.Fatalf("CheckDisjoint rejected a disjoint state plane: %v", err)
	}
	if err := b.Publish(context.Background(), good, []byte("on"), 1, true); err != nil {
		t.Fatalf("publish: %v", err)
	}
	r.WaitIdle()
	if rc.count() != 0 {
		t.Fatal("a disjoint state publish still reached a command handler")
	}

	// The colliding one is refused by the guard — and the second half
	// shows exactly what the guard buys: published, it comes straight back
	// as a command.
	bad := "gh/ccu/programs/12459/trigger"
	err := r.CheckDisjoint(bad)
	if !errors.Is(err, ErrStateCommandCollision) {
		t.Fatalf("CheckDisjoint(%q) = %v, want ErrStateCommandCollision", bad, err)
	}
	if !strings.Contains(err.Error(), "gh/+/programs/+/trigger") {
		t.Errorf("the error must name the colliding filter, got %v", err)
	}
	if perr := b.Publish(context.Background(), bad, []byte("on"), 1, true); perr != nil {
		t.Fatalf("publish: %v", perr)
	}
	r.WaitIdle()
	if rc.count() != 1 {
		t.Fatal("the colliding publish did NOT echo back as a command — " +
			"the fixture no longer reproduces the defect the guard exists for, so the guard is untested")
	}
	if got := rc.snapshot()[0]; got.Retained {
		t.Error("the echo arrived retained; live routing carries Retain=0, " +
			"which is why the retained-drop guard does not save a consumer from it")
	}
}

func TestCheckDisjointReportsEveryCollision(t *testing.T) {
	t.Parallel()
	r := quietRouter(t, newCmdBroker(), CommandConfig{})
	if err := r.Handle("gh/+/+/set", (&recorder{}).handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	err := r.CheckDisjoint("gh/a/b/set", "gh/c/d/set", "gh/ok/state", "")
	if err == nil {
		t.Fatal("want collisions")
	}
	if got := strings.Count(err.Error(), ErrStateCommandCollision.Error()); got != 2 {
		t.Errorf("reported %d collisions, want 2 — a consumer with one usually has a family", got)
	}
	if !r.Claims("gh/a/b/set") || r.Claims("gh/ok/state") {
		t.Error("Claims disagrees with CheckDisjoint")
	}
}

func TestCommandAndStateTopicExtraction(t *testing.T) {
	t.Parallel()
	comp := discovery.Component{
		Platform:          hacatalog.PlatformClimate,
		UniqueID:          "u1",
		StateTopic:        "gh/dev/state",
		CommandTopic:      "gh/dev/set",
		AvailabilityTopic: "gh/dev/avail",
		Fields: discovery.ClimateFields{
			ModeCommandTopic:        "gh/dev/mode/set",
			ModeStateTopic:          "gh/dev/mode",
			TemperatureCommandTopic: "gh/dev/temp/set",
			CurrentTemperatureTopic: "gh/dev/temp",
		},
	}
	cmds, err := CommandTopics(comp)
	if err != nil {
		t.Fatalf("CommandTopics: %v", err)
	}
	if got := strings.Join(cmds, ","); got != "gh/dev/mode/set,gh/dev/set,gh/dev/temp/set" {
		t.Errorf("CommandTopics = %q", got)
	}
	states, err := StateTopics(comp)
	if err != nil {
		t.Fatalf("StateTopics: %v", err)
	}
	if got := strings.Join(states, ","); got != "gh/dev/avail,gh/dev/mode,gh/dev/state,gh/dev/temp" {
		t.Errorf("StateTopics = %q", got)
	}

	// The two command keys whose names do not end in `command_topic`. A
	// consumer deriving subscriptions from the suffix alone ships a cover
	// whose position slider does nothing.
	cover := discovery.Component{
		Platform: hacatalog.PlatformCover,
		Fields:   discovery.CoverFields{SetPositionTopic: "gh/blind/position/set", PositionTopic: "gh/blind/position"},
	}
	coverCmds, err := CommandTopics(cover)
	if err != nil {
		t.Fatalf("CommandTopics(cover): %v", err)
	}
	if got := strings.Join(coverCmds, ","); got != "gh/blind/position/set" {
		t.Errorf("cover CommandTopics = %q, want set_position_topic", got)
	}
}

func TestBundleTopicExtraction(t *testing.T) {
	t.Parallel()
	if got, err := BundleCommandTopics(nil); err != nil || got != nil {
		t.Fatalf("nil bundle: %v / %v", got, err)
	}
	b := &discovery.Bundle{
		NodeID: "dev1",
		Components: map[string]discovery.Component{
			"mode":   {Platform: hacatalog.PlatformSelect, CommandTopic: "gh/dev/mode/set", StateTopic: "gh/dev/mode"},
			"mirror": {Platform: hacatalog.PlatformSwitch, CommandTopic: "gh/dev/mode/set", StateTopic: "gh/dev/mode"},
			"temp":   {Platform: hacatalog.PlatformSensor, StateTopic: "gh/dev/temp"},
		},
	}
	cmds, err := BundleCommandTopics(b)
	if err != nil {
		t.Fatalf("BundleCommandTopics: %v", err)
	}
	if got := strings.Join(cmds, ","); got != "gh/dev/mode/set" {
		t.Errorf("BundleCommandTopics = %q, want the shared topic once", got)
	}
	states, err := BundleStateTopics(b)
	if err != nil {
		t.Fatalf("BundleStateTopics: %v", err)
	}
	if got := strings.Join(states, ","); got != "gh/dev/mode,gh/dev/temp" {
		t.Errorf("BundleStateTopics = %q", got)
	}

	// The whole point: what a device advertises, subscribed exactly, is
	// disjoint from what it publishes — and provably so at boot.
	r := quietRouter(t, newCmdBroker(), CommandConfig{})
	for _, topic := range cmds {
		if err := r.Handle(topic, (&recorder{}).handle); err != nil {
			t.Fatalf("handle %s: %v", topic, err)
		}
	}
	if err := r.CheckDisjoint(states...); err != nil {
		t.Errorf("a bundle's own command and state topics collided: %v", err)
	}
}

func TestFiltersOverlap(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"a/b", "a/+", true},
		{"a/b", "a/c", false},
		{"a/b", "a/b/c", false},
		{"a/#", "a/b/c/d", true},
		{"a/#", "a", true},
		{"a", "a/#", true},
		{"a/b/#", "a/c/#", false},
	} {
		if got := filtersOverlap(strings.Split(tc.a, "/"), strings.Split(tc.b, "/")); got != tc.want {
			t.Errorf("filtersOverlap(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestCommandPoolEnqueueAfterCloseDoesNotRun(t *testing.T) {
	t.Parallel()
	p := newCommandPool(2, 1, slog.New(slog.DiscardHandler))
	p.close()
	ran := make(chan struct{})
	p.enqueue("k", func() { close(ran) })
	p.flush()
	select {
	case <-ran:
		t.Fatal("a job enqueued after close ran; no worker remains to run it")
	default:
	}
}

func TestCommandRouterStopWithoutStart(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("gh/+/set", (&recorder{}).handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("stop without start: %v", err)
	}
	if len(b.unsubscribed()) != 0 {
		t.Error("Stop unsubscribed filters that were never subscribed")
	}
}

// sorted returns a sorted copy, so assertions do not depend on map order.
func sorted(in []string) []string {
	out := append([]string(nil), in...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func TestCommandRouterResubscribeReportsEveryFailure(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})
	for _, f := range []string{"gh/a/set", "gh/b/set"} {
		if err := r.Handle(f, (&recorder{}).handle); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	boom := errors.New("broker said no")
	b.failSubscribe = func(string) error { return boom }
	err := r.Resubscribe(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("resubscribe: %v, want the broker's error", err)
	}
	// Both filters are reported, not just the first: a resubscribe that
	// gave up on the first refusal would leave the rest of the command
	// plane silently down after a reconnect.
	if got := strings.Count(err.Error(), boom.Error()); got != 2 {
		t.Errorf("reported %d failures, want 2", got)
	}
}

func TestTopicExtractionRejectsAnUnencodableComponent(t *testing.T) {
	t.Parallel()
	// A component whose Extra cannot be marshalled. The extraction reads
	// the rendered payload precisely so it sees what the broker will, so a
	// component that cannot render has no topics to report — an error, not
	// an empty list a consumer would read as "nothing to subscribe".
	bad := discovery.Component{Platform: hacatalog.PlatformSensor, Extra: map[string]any{"x": func() {}}}
	if _, err := CommandTopics(bad); err == nil {
		t.Error("CommandTopics accepted an unencodable component")
	}
	if _, err := StateTopics(bad); err == nil {
		t.Error("StateTopics accepted an unencodable component")
	}
	if _, err := BundleCommandTopics(&discovery.Bundle{
		NodeID:     "dev",
		Components: map[string]discovery.Component{"bad": bad},
	}); err == nil {
		t.Error("BundleCommandTopics accepted an unencodable component")
	}
}

func TestCommandPoolClampsItsCountsAndLogsBacklog(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	warned := 0
	log := slog.New(countingHandler{fn: func(msg string) {
		if msg == "publisher.command.backlog" {
			mu.Lock()
			warned++
			mu.Unlock()
		}
	}})
	// Zero workers and a zero depth are clamped to one, so a misconfigured
	// caller gets a serial pool rather than a panic at the far end of boot.
	p := newCommandPool(0, 0, log)
	release := make(chan struct{})
	p.enqueue("k", func() { <-release })
	for range 3 {
		p.enqueue("k", func() {})
	}
	close(release)
	p.flush()
	p.close()

	mu.Lock()
	defer mu.Unlock()
	if warned == 0 {
		t.Error("a backlog past the soft depth must be visible to an operator before the memory is")
	}
}

// countingHandler is the smallest slog.Handler that lets a test see which
// diagnostics were emitted without depending on their formatting.
type countingHandler struct{ fn func(msg string) }

func (countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h countingHandler) Handle(_ context.Context, rec slog.Record) error {
	h.fn(rec.Message)
	return nil
}

func (h countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h countingHandler) WithGroup(string) slog.Handler { return h }

// TestCommandRouterFailedStartRunsNoHandlerEvenWhenRollbackFails is the
// regression for the second half of the S3 finding. Rollback is best effort
// — a broker may refuse the UNSUBSCRIBE — so a failed Start can leave a
// live subscription. What must not survive it is an OPEN gate: the failure
// path used to clear `started` without setting `stopped`, so the live
// subscription still reached handlers and commands ran against
// half-initialised dependencies, the exact hazard Stop's doc forbids.
func TestCommandRouterFailedStartRunsNoHandlerEvenWhenRollbackFails(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	boom := errors.New("broker said no")
	b.failSubscribe = func(filter string) error {
		if filter == "gh/b/set" {
			return boom
		}
		return nil
	}
	b.failUnsubscribe = func(string) error { return errors.New("broker refused the rollback") }

	rc := &recorder{}
	r := quietRouter(t, b, CommandConfig{})
	for _, f := range []string{"gh/a/set", "gh/b/set"} {
		if err := r.Handle(f, rc.handle); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	err := r.Start(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("start: %v, want the broker's error", err)
	}
	// The consumer cannot act on what it is not told: the error names the
	// filter the rollback could not take down.
	if !strings.Contains(err.Error(), "gh/a/set") {
		t.Errorf("the error must name the subscription still live, got %v", err)
	}
	if live := b.filters(); len(live) != 1 {
		t.Fatalf("fixture broke: want exactly the un-rolled-back subscription live, got %v", live)
	}

	b.deliver("gh/a/set", []byte("on"), false)
	r.WaitIdle()
	if got := rc.count(); got != 0 {
		t.Fatalf("a handler ran %d times after Start failed; the gate must be closed, "+
			"because a command now runs against half-initialised dependencies", got)
	}
}

// TestCommandRouterOwnsNoGoroutinesUntilStarted is the regression for the
// leak: the pool used to be started in NewCommandRouter, so a router that
// never reached Start left its workers parked on a condition variable with
// nothing to reclaim them — eight goroutines per router, measured at 2 -> 82
// over ten routers built and discarded. Three ordinary paths get there: a
// Handle that errors at a composition root, a config reload replacing the
// router, and a failed Start the consumer gives up on.
//
// Asserted on the pool field rather than by counting goroutines, because a
// count is not attributable in a package whose tests run in parallel.
func TestCommandRouterOwnsNoGoroutinesUntilStarted(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})

	// Built and abandoned: the composition root's Handle failed.
	if err := r.Handle("a/#/b", (&recorder{}).handle); err == nil {
		t.Fatal("fixture broke: that filter is malformed")
	}
	r.mu.Lock()
	pool := r.pool
	r.mu.Unlock()
	if pool != nil {
		t.Fatal("construction started the worker pool; a router that never starts must own no goroutines")
	}

	if err := r.Handle("gh/+/set", (&recorder{}).handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	r.mu.Lock()
	pool = r.pool
	r.mu.Unlock()
	if pool == nil {
		t.Fatal("Start left no pool; the workers are what take handlers off the read loop")
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	r.mu.Lock()
	after := r.pool
	r.mu.Unlock()
	if after != nil {
		t.Error("Stop left the pool in place")
	}
	// close() returns only once every worker has exited, so a closed
	// queue set proves the goroutines are gone, not merely unreferenced.
	for i, q := range pool.queues {
		q.mu.Lock()
		closed := q.closed
		q.mu.Unlock()
		if !closed {
			t.Errorf("worker queue %d survived Stop", i)
		}
	}
}

// TestCommandRouterFailedStartReclaimsItsWorkers pins the third path into
// the leak: a Start that fails spins up the pool before its first subscribe
// (a broker may deliver the moment it acknowledges one) and must hand it
// back, because the consumer has been told the start failed and has no
// reason to call Stop.
func TestCommandRouterFailedStartReclaimsItsWorkers(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	b.failSubscribe = func(string) error { return errors.New("broker said no") }
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("gh/+/set", (&recorder{}).handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("fixture broke: the subscribe must fail")
	}
	r.mu.Lock()
	pool := r.pool
	r.mu.Unlock()
	if pool != nil {
		t.Error("a failed Start kept its worker pool alive")
	}
}

// TestCommandRouterStopDropsNothingItAccepted closes the window S10
// suspected and this fixture confirmed: deliver used to read the stopped
// flag under the route lock, release it, and only then enqueue, so a Stop
// completing in between discarded the command with a
// `dropped_after_close` warning — contradicting Stop's promise that
// anything already accepted runs to completion. It is rare (about one in
// four thousand deliveries racing a Stop, with both goroutines released
// from one barrier) and it is logged rather than silent, which is why it
// took a stress fixture to see; the gate check and the enqueue are one
// locked step now, so it cannot happen rather than seldom happening.
//
// The stronger property is asserted alongside it, because the fix must not
// buy one at the other's expense: no handler ever runs after Stop returns.
func TestCommandRouterStopDropsNothingItAccepted(t *testing.T) {
	t.Parallel()
	const (
		iterations = 400
		deliveries = 50
	)
	var drops atomic.Int64
	log := slog.New(countingHandler{fn: func(msg string) {
		if msg == "publisher.command.dropped_after_close" {
			drops.Add(1)
		}
	}})
	for range iterations {
		b := newCmdBroker()
		var (
			stopReturned atomic.Bool
			lateRun      atomic.Bool
		)
		r := NewCommandRouter(b, CommandConfig{Workers: 2, Logger: log})
		if err := r.Handle("gh/+/set", func(context.Context, Command) {
			if stopReturned.Load() {
				lateRun.Store(true)
			}
		}); err != nil {
			t.Fatalf("handle: %v", err)
		}
		if err := r.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}
		// Hold the subscription handler directly: a broker delivers
		// what was already in flight while Stop is running.
		b.mu.Lock()
		h := b.subs["gh/+/set"]
		b.mu.Unlock()

		barrier := make(chan struct{})
		var wg sync.WaitGroup
		// Many concurrent deliveries per Stop: the window is a handful
		// of instructions wide, so the fixture needs enough of them in
		// flight for one to be descheduled inside it.
		wg.Add(deliveries + 1)
		for i := range deliveries {
			go func() {
				defer wg.Done()
				<-barrier
				h("gh/lamp"+strconv.Itoa(i)+"/set", []byte("on"), false)
			}()
		}
		go func() {
			defer wg.Done()
			<-barrier
			_ = r.Stop(context.Background())
			stopReturned.Store(true)
		}()
		close(barrier)
		wg.Wait()

		if lateRun.Load() {
			t.Fatal("a handler ran after Stop returned")
		}
	}
	if got := drops.Load(); got != 0 {
		t.Errorf("%d of %d deliveries racing Stop were dropped after accepting the gate; "+
			"Stop must drain what it accepted, not discard it", got, iterations*deliveries)
	}
}
