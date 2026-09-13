// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hacatalog "github.com/SukramJ/go-ha-catalog"
	"github.com/SukramJ/go-mqtt/protocol"

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
		// §4.8.2: the shared-subscription structure, and the wrapped
		// filter validated exactly like a standalone one. These are the
		// shapes that used to be accepted as ordinary literal levels
		// and then routed nothing — see TestFilterRulesAgreeWithGoMQTT.
		{"$share/grp/a/+/set", true},
		{"$share/grp/#", true},
		{"$share", true}, // no separator: the literal filter it looks like
		{"$share/", false},
		{"$share/grp", false},
		{"$share/grp/", false},
		{"$share//set", false},
		{"$share/gr+p/set", false},
		{"$share/grp/a/#/b", false},
		// §1.5.4 applies to a filter as much as to a topic name.
		{"a/\x00/b", false},
		{"a/\xff/b", false},
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
		// §4.8.2: the prefix is structural — a PUBLISH carries the real
		// topic — so only the wrapped filter matches.
		{"$share/grp/sensors/+", "sensors/temp", true},
		{"$share/grp/sensors/+", "$share/grp/sensors/temp", false},
		{"$share/grp/#", "$SYS/x", false},
		{"$share", "$share", true},
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
	// now, and the wildcard capture is what it reads. Asserted as a set:
	// the two commands are on different topics, so they run on different
	// workers and order between them is deliberately not promised.
	params := sorted([]string{got[0].Wildcards[3], got[1].Wildcards[3]})
	if strings.Join(params, ",") != "LEVEL,week_profile" {
		t.Errorf("handler saw %v, want the parameter name in the fourth `+` of each", params)
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

	// The delivery itself has to be watched from another goroutine: the
	// handler blocks until this test releases it, so a delivery that DID
	// run the handler inline would never reach a `time.Since` after it.
	// Measuring elapsed time on this goroutine could only ever pass.
	returned := make(chan struct{})
	go func() {
		b.deliver("gh/lamp/set", []byte("on"), false)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("the delivery had not returned while the handler was still blocked; " +
			"a handler must not run on the transport's read loop")
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

// TestCommandRouterPreservesOrderPerTopic proves both halves of the
// dispatch promise: a burst on one topic is never reordered, and unrelated
// topics do not wait for each other.
//
// The second half is what the earlier version of this test was missing. It
// sent all 200 messages to ONE topic, which one worker delivers serially,
// so the multi-worker premise was never exercised and hardcoding poolIndex
// to 0 still passed it. Here each topic's first command parks until every
// topic has one in flight, which only completes if the topics really landed
// on different workers, and the barrier has a deadline so a serial pool
// fails with a message rather than as a whole-binary timeout.
func TestCommandRouterPreservesOrderPerTopic(t *testing.T) {
	t.Parallel()
	const (
		lanes = 4
		burst = 50
	)
	// Topics that hash to distinct workers. Asserting the premise beats
	// assuming it: a poolIndex that collapses every key onto one worker
	// is caught here, by name.
	topics := make([]string, 0, lanes)
	used := map[int]bool{}
	for i := 0; i < 200 && len(topics) < lanes; i++ {
		topic := "gh/lamp" + strconv.Itoa(i) + "/set"
		slot := poolIndex(topic, DefaultCommandWorkers)
		if used[slot] {
			continue
		}
		used[slot] = true
		topics = append(topics, topic)
	}
	if len(topics) != lanes {
		t.Fatalf("only found %d topics on distinct workers; the pool is not spreading keys", len(topics))
	}

	var (
		mu      sync.Mutex
		seen    = map[string][]string{}
		release = make(chan struct{})
		entered = make(chan string, lanes)
	)
	b := newCmdBroker()
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("gh/+/set", func(_ context.Context, cmd Command) {
		mu.Lock()
		first := len(seen[cmd.Topic]) == 0
		seen[cmd.Topic] = append(seen[cmd.Topic], string(cmd.Payload))
		mu.Unlock()
		if first {
			// Park the lane: if the four topics shared a worker, the
			// other three could never arrive.
			entered <- cmd.Topic
			<-release
		}
	}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	for i := range burst {
		for _, topic := range topics {
			b.deliver(topic, []byte(strconv.Itoa(i)), false)
		}
	}
	for range lanes {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("not every topic had a command in flight; commands on different topics " +
				"must not wait for each other, which is what several workers are for")
		}
	}
	close(release)
	r.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	for _, topic := range topics {
		got := seen[topic]
		if len(got) != burst {
			t.Fatalf("%s saw %d commands, want %d", topic, len(got), burst)
		}
		for i, v := range got {
			if v != strconv.Itoa(i) {
				t.Fatalf("%s command %d was %q — same-topic commands reordered", topic, i, v)
			}
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

// TestCommandPoolEnqueueAfterCloseDoesNotRun pins that a job arriving after
// close is refused and SAID SO.
//
// "Did not run" alone is not assertable after close: no worker remains, so
// a queue that silently accepted the job would satisfy it too — the old
// version of this test could not fail. What distinguishes the two is the
// warning, which is also the only evidence an operator would ever get. The
// flush is watched from another goroutine for the same reason: a flush that
// parked would otherwise surface as a whole-binary timeout, taking every
// parallel test in the package down with it.
func TestCommandPoolEnqueueAfterCloseDoesNotRun(t *testing.T) {
	t.Parallel()
	var (
		mu      sync.Mutex
		dropped []string
	)
	log := slog.New(countingHandler{fn: func(msg string) {
		mu.Lock()
		if msg == "publisher.command.dropped_after_close" {
			dropped = append(dropped, msg)
		}
		mu.Unlock()
	}})
	p := newCommandPool(2, 1, log)
	p.close()

	ran := make(chan struct{})
	p.enqueue("k", func() { close(ran) })

	flushed := make(chan struct{})
	go func() {
		p.flush()
		close(flushed)
	}()
	select {
	case <-flushed:
	case <-time.After(2 * time.Second):
		t.Fatal("flush parked on a closed pool")
	}
	select {
	case <-ran:
		t.Fatal("a job enqueued after close ran; no worker remains to run it")
	default:
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dropped) != 1 {
		t.Errorf("logged %d drops, want 1 — a discarded command an operator cannot see is a lost command", len(dropped))
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

	// And the router is closed for good, not merely un-started. Asserted
	// separately because the two are told apart only here: the nil pool
	// independently stops deliveries, so `stopped` being left false —
	// measured to survive the suite in the v0.27.0–v0.29.0 review — is
	// invisible to the assertion above. A retryable Start is exactly what
	// must not be offered while a subscription the rollback could not take
	// down is still live on the broker.
	b.failSubscribe = nil
	if err := r.Start(context.Background()); !errors.Is(err, ErrRouterStarted) {
		t.Fatalf("Start after a failed rollback = %v, want ErrRouterStarted — "+
			"a live subscription plus a re-startable router is the hazard Stop forbids", err)
	}
	if live := b.filters(); len(live) != 1 {
		t.Fatalf("the refused Start went to the broker anyway: %v", live)
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

// filterCorpus enumerates the filter and topic shapes the two matchers can
// disagree on: the wildcards, the empty level a leading/trailing/doubled
// slash produces, the `$`-prefixed trees §4.7.2 protects, and the `$share`
// levels §4.8.2 makes structural.
func filterCorpus() []string {
	levels := []string{"a", "b", "+", "#", "", "$SYS", "$share", "grp"}
	shapes := []string{
		"$share", "$share/", "$share//f", "$share/g", "$share/g/",
		"$share/g+/f", "$share/g#/f", "$share/g/a/#", "$share/g/a/#/b", "$share/g/$SYS/x",
		"$share/g/$share/g/a", "a\x00b", "a/\xff/b",
	}
	n := len(levels)
	out := make([]string, 0, len(shapes)+n+n*n+n*n*n)
	out = append(out, shapes...)
	for _, one := range levels {
		out = append(out, one)
		for _, two := range levels {
			out = append(out, one+"/"+two)
			for _, three := range levels {
				out = append(out, one+"/"+two+"/"+three)
			}
		}
	}
	return out
}

// TestFilterRulesAgreeWithGoMQTT is the regression for the $share class.
//
// The router's filters are handed to go-mqtt, which routes by its own
// fuzzed matcher; any shape the two decide differently is a silent routing
// failure whose only evidence is a `publisher.command.unroutable` warning
// per command. A differential enumeration over 7.84M (filter, topic) pairs
// found exactly one disagreement class, for filters BOTH validators accept:
// `$share`, which go-mqtt strips per §4.8.2 while this module matched it as
// ordinary literal levels. ValidateFilter is what let those through — 109
// shapes it accepted that protocol.ValidateTopicFilter rejects, all
// $share-prefixed — and it checked neither valid UTF-8 nor U+0000.
//
// This is the enumeration in test form, narrow enough to run every time.
// Only filters both validators accept take part in the matching half: a
// filter with `#` before its last level is rejected by both, so the one
// deliberate difference in [captureFilter] is excluded by construction
// rather than by a special case here.
func TestFilterRulesAgreeWithGoMQTT(t *testing.T) {
	t.Parallel()
	corpus := filterCorpus()
	mismatches := 0
	for _, filter := range corpus {
		ours := ValidateFilter(filter)
		theirs := protocol.ValidateTopicFilter(filter)
		if (ours == nil) != (theirs == nil) {
			mismatches++
			if mismatches <= 10 {
				t.Errorf("ValidateFilter(%q) = %v but protocol.ValidateTopicFilter = %v", filter, ours, theirs)
			}
			continue
		}
		if ours != nil {
			continue
		}
		for _, topic := range corpus {
			if protocol.ValidateTopicName(topic) != nil {
				continue
			}
			if got, want := MatchFilter(filter, topic), protocol.MatchTopic(filter, topic); got != want {
				mismatches++
				if mismatches <= 10 {
					t.Errorf("MatchFilter(%q, %q) = %v but protocol.MatchTopic = %v", filter, topic, got, want)
				}
			}
		}
	}
	if mismatches > 10 {
		t.Errorf("%d disagreements in total", mismatches)
	}
}

// TestCommandRouterRoutesASharedSubscription is the consumer-visible half:
// a multi-instance consumer registers `$share/...` so the broker load
// balances commands across its instances, and the router must claim the
// real topic the PUBLISH carries — which never contains the prefix.
func TestCommandRouterRoutesASharedSubscription(t *testing.T) {
	t.Parallel()
	b := newCmdBroker()
	rc := &recorder{}
	r := quietRouter(t, b, CommandConfig{})
	if err := r.Handle("$share/bridges/gh/+/set", rc.handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	// The overlap check runs on the wrapped filter too, or a shared route
	// and its plain twin would both be registered and one message would
	// run two handlers.
	if err := r.Handle("gh/+/set", rc.handle); !errors.Is(err, ErrAmbiguousRoutes) {
		t.Errorf("a shared route and its plain twin: %v, want ErrAmbiguousRoutes", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	cmd, ok := r.Route("gh/lamp/set")
	if !ok {
		t.Fatal("a shared subscription claimed nothing; the broker delivers the real topic, never the $share prefix")
	}
	if cmd.Filter != "$share/bridges/gh/+/set" {
		t.Errorf("Command.Filter = %q, want the route as registered", cmd.Filter)
	}
	if strings.Join(cmd.Wildcards, "|") != "lamp" {
		t.Errorf("Wildcards = %v, want the wrapped filter's `+`", cmd.Wildcards)
	}
	// CheckDisjoint reads the same parts, so the state plane is checked
	// against what a shared route really matches.
	if err := r.CheckDisjoint("gh/lamp/set"); !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("CheckDisjoint over a shared route: %v, want ErrStateCommandCollision", err)
	}
}

// attrBroker is a cmdBroker that can attribute: it records a Subscription
// Identifier per subscription and delivers a stamped copy to that
// subscription's handler alone, the way go-mqtt's TCPClient.dispatch does.
//
// It composes BOTH fan-outs, which is the only way a fixture can see the
// defect this whole area is about (see [cmdBroker.deliver]): the broker
// makes one copy per matching subscription, and the client then decides who
// each copy reaches. Modelling only the first pins the wrong number. The
// two stages are separate here on purpose — an UNSTAMPED copy still falls
// back to re-matching the topic against every filter the client holds, so
// the fixture reproduces the mixed case where one un-stamped subscription
// re-multiplies every stamped one.
type attrBroker struct {
	mu     sync.Mutex
	subs   []attrSub
	subbed []string

	// stamps is false for the case the design turns on: a transport that
	// offers attribution but is talking MQTT 3.1.1, where there is no
	// property block to carry an identifier. go-mqtt refuses the option
	// rather than dropping it, and so does this.
	stamps bool

	failSubscribe func(filter string) error
	// failsClosed models go-mqtt v1.5.1 and later, where an
	// identifier-less PUBLISH reaches ONLY subscriptions that carry no
	// identifier.
	//
	// MQTT 5.0 §3.3.4 makes a server include the identifier of every
	// subscription it forwarded a message for, so a message arriving with
	// none was forwarded for no stamped subscription — matching it by
	// topic against one delivers a copy the broker never sent. v1.5.0 did
	// exactly that, which restored the doubled handler that
	// WithSubscriptionID was added to remove: one stamped overlapping
	// route plus one unstamped broad subscription on the same client, and
	// a single published message ran the stamped handler twice. The zero
	// value keeps the permissive routing, which is what the fixture's
	// other tests describe.
	failsClosed bool
	// onSubscribe runs after a subscription is installed and before the
	// next one is asked for. It is the only place a test can stand inside
	// the subscribe window — the ordinary state of the wire during Start
	// and during a sequential resubscribe replay after a reconnect.
	onSubscribe func(filter string)
}

type attrSub struct {
	filter string
	id     uint32
	h      Handler
}

func newAttrBroker() *attrBroker { return &attrBroker{stamps: true} }

func (b *attrBroker) Publish(_ context.Context, topic string, payload []byte, _ byte, _ bool) error {
	b.fanout(topic, payload, false)
	return nil
}

func (b *attrBroker) Subscribe(_ context.Context, filter string, _ byte, h Handler) error {
	return b.add(filter, 0, h)
}

func (b *attrBroker) SubscribeAttributed(
	_ context.Context, filter string, _ byte, id uint32, h Handler,
) error {
	if !b.stamps {
		return errors.New("broker: subscription identifiers require MQTT 5.0")
	}
	if id == 0 || id > MaxSubscriptionID {
		return fmt.Errorf("broker: identifier %d out of range 1..%d", id, MaxSubscriptionID)
	}
	return b.add(filter, id, h)
}

func (b *attrBroker) add(filter string, id uint32, h Handler) error {
	if err := b.install(filter, id, h); err != nil {
		return err
	}
	b.mu.Lock()
	hook := b.onSubscribe
	b.mu.Unlock()
	if hook != nil {
		hook(filter)
	}
	return nil
}

func (b *attrBroker) install(filter string, id uint32, h Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failSubscribe != nil {
		if err := b.failSubscribe(filter); err != nil {
			return err
		}
	}
	for i := range b.subs {
		if b.subs[i].filter == filter {
			b.subs[i] = attrSub{filter: filter, id: id, h: h}
			b.subbed = append(b.subbed, filter)
			return nil
		}
	}
	b.subs = append(b.subs, attrSub{filter: filter, id: id, h: h})
	b.subbed = append(b.subbed, filter)
	return nil
}

func (b *attrBroker) Unsubscribe(_ context.Context, filter string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := b.subs[:0]
	for _, s := range b.subs {
		if s.filter != filter {
			kept = append(kept, s)
		}
	}
	b.subs = kept
	return nil
}

// deliver drives one inbound message through both fan-outs and returns the
// number of copies the broker produced.
func (b *attrBroker) deliver(topic string, payload []byte, retained bool) int {
	return b.fanout(topic, payload, retained)
}

func (b *attrBroker) fanout(topic string, payload []byte, retained bool) (copies int) {
	b.mu.Lock()
	match := make([]attrSub, 0, len(b.subs))
	for _, s := range b.subs {
		if MatchFilter(s.filter, topic) {
			match = append(match, s)
		}
	}
	b.mu.Unlock()
	for _, m := range match {
		// One copy per matching subscription. A stamped copy reaches the
		// subscription its identifier names and no other; an unstamped one
		// is re-matched against every filter, which is all a v3.1.1 link
		// or an identifier-less subscription offers.
		targets := match
		switch {
		case m.id != 0:
			targets = []attrSub{m}
		case b.failsClosed:
			// go-mqtt v1.5.1 and later: an identifier-less copy reaches
			// only the subscriptions that carry no identifier.
			targets = nil
			for _, s := range match {
				if s.id == 0 {
					targets = append(targets, s)
				}
			}
		}
		for _, t := range targets {
			t.h(topic, payload, retained)
		}
	}
	return len(match)
}

// drop forgets every subscription without telling the router — a reconnect
// against a broker that did not keep the session.
func (b *attrBroker) drop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = nil
}

// idOf reports the identifier the broker holds for filter, or 0.
func (b *attrBroker) idOf(filter string) uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.subs {
		if s.filter == filter {
			return s.id
		}
	}
	return 0
}

func (b *attrBroker) liveFilters() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.subs))
	for _, s := range b.subs {
		out = append(out, s.filter)
	}
	return sorted(out)
}

// TestCommandRouterAcceptsAnOrderedOverlapWhenDeliveriesAreAttributable is
// the point of the whole feature: the pair that held v0.27.0's release —
// a general command shape plus a special case for one parameter name — is
// registerable again, and one published message still runs exactly one
// handler.
//
// The broker really does make two copies (asserted, because a fixture that
// made one would pass this test while modelling nothing), and the router
// really does drop one of them.
func TestCommandRouterAcceptsAnOrderedOverlapWhenDeliveriesAreAttributable(t *testing.T) {
	t.Parallel()
	b := newAttrBroker()
	r := quietRouter(t, b, CommandConfig{})
	generic, specific := &recorder{}, &recorder{}
	if err := r.Handle("gh/+/+/+/+/set", generic.handle); err != nil {
		t.Fatalf("handle generic: %v", err)
	}
	if err := r.Handle("gh/+/+/+/week_profile/set", specific.handle); err != nil {
		t.Fatalf("an ordered overlap must be accepted on an attributing transport: %v", err)
	}
	if !r.Attributed() {
		t.Fatal("the router accepted an overlap without switching to attributed delivery")
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	if n := b.deliver("gh/ccu/HmIP/0001:1/week_profile/set", []byte("P2"), false); n != 2 {
		t.Fatalf("broker fanned out to %d subscriptions, want 2 — one copy per matching subscription", n)
	}
	if n := b.deliver("gh/ccu/HmIP/0001:1/LEVEL/set", []byte("1"), false); n != 1 {
		t.Fatalf("plain data-point topic hit %d subscriptions, want 1", n)
	}
	r.WaitIdle()

	got := specific.snapshot()
	if len(got) != 1 {
		t.Fatalf("the specific route ran %d times, want exactly one per published message", len(got))
	}
	if got[0].Filter != "gh/+/+/+/week_profile/set" {
		t.Errorf("specific handler saw Filter %q", got[0].Filter)
	}
	gen := generic.snapshot()
	if len(gen) != 1 {
		t.Fatalf("the general route ran %d times, want 1 — only the topic the specific route does not claim",
			len(gen))
	}
	if gen[0].Wildcards[3] != "LEVEL" {
		t.Errorf("the general route was handed %q, want the topic the specific route does not claim",
			gen[0].Topic)
	}
}

// TestCommandRouterDeliversInsideTheSubscribeWindow is the measured loss of
// the v0.27.0–v0.29.0 review: a command that arrives while the routes are
// still going out one at a time.
//
// The router drops a copy that arrived for a route a more specific one
// outranks, judged against the REGISTERED route set — which is not the set
// the broker holds until the last SUBSCRIBE is acknowledged. With the general
// shape subscribed first, the only subscription that existed inside that
// window was the one whose copies get dropped: the command ran no handler at
// all and left one Debug line. That window is the ordinary state of the wire
// during Start, and during a sequential resubscribe replay after a reconnect
// — a button pressed in the second after a reconnect did nothing.
//
// The routes are therefore registered most specific first, so a copy can only
// arrive for an outranked route once the route that outranks it is already
// subscribed on the same connection.
func TestCommandRouterDeliversInsideTheSubscribeWindow(t *testing.T) {
	t.Parallel()
	b := newAttrBroker()
	r := quietRouter(t, b, CommandConfig{})
	generic, specific := &recorder{}, &recorder{}
	if err := r.Handle("gh/+/+/set", generic.handle); err != nil {
		t.Fatalf("handle generic: %v", err)
	}
	if err := r.Handle("gh/+/PRESS_SHORT/set", specific.handle); err != nil {
		t.Fatalf("handle specific: %v", err)
	}

	// Deliver after the FIRST subscription is installed and before the
	// second is asked for. A real broker does this without being asked.
	var once sync.Once
	inWindow := func() {
		b.mu.Lock()
		live := len(b.subs)
		b.mu.Unlock()
		if live != 1 {
			t.Errorf("the fixture delivered with %d subscriptions live, want exactly 1", live)
		}
		b.deliver("gh/ccu/PRESS_SHORT/set", []byte("1"), false)
	}
	b.mu.Lock()
	b.onSubscribe = func(string) { once.Do(inWindow) }
	b.mu.Unlock()

	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	r.WaitIdle()

	ran := len(generic.snapshot()) + len(specific.snapshot())
	if ran != 1 {
		t.Fatalf("a command delivered inside the subscribe window ran %d handlers, want exactly 1 "+
			"(0 is the measured loss, 2 would be the doubling the drop exists to prevent)", ran)
	}
	if got := len(specific.snapshot()); got != 1 {
		t.Errorf("the specific route ran %d times, want 1 — it is the route that owns the topic", got)
	}

	// The same window again, this time as a reconnect replay against a
	// broker that did not keep the session.
	b.drop()
	var twice sync.Once
	b.mu.Lock()
	b.onSubscribe = func(string) { twice.Do(inWindow) }
	b.mu.Unlock()
	if err := r.Resubscribe(context.Background()); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	r.WaitIdle()

	if ran := len(generic.snapshot()) + len(specific.snapshot()); ran != 2 {
		t.Fatalf("after the replay %d handler runs, want 2 — the command in the reconnect window is lost", ran)
	}
}

// TestSpecificityScoreAgreesWithDominance pins the one property that lets a
// sort key stand in for the dominance test: whenever compareSpecificity says
// one filter is strictly more specific, its score must be strictly greater.
// The subscribe order is built from the score and the delivery verdict from
// the dominance test, so a disagreement would put the outranked route on the
// wire first again — the window TestCommandRouterDeliversInsideTheSubscribeWindow
// measures.
func TestSpecificityScoreAgreesWithDominance(t *testing.T) {
	t.Parallel()
	filters := []string{
		"gh/+/+/set", "gh/+/PRESS_SHORT/set", "gh/ccu/PRESS_SHORT/set",
		"base/dev/set", "base/dev/set/#", "base/#", "base/dev/#",
		"a/+/c", "a/b/+", "a/b", "a/b/#", "$share/g/a/b/+",
	}
	width := 1
	parts := make([][]string, len(filters))
	for i, f := range filters {
		parts[i] = filterParts(f)
		if n := len(parts[i]); n >= width {
			width = n + 1
		}
	}
	for i := range filters {
		for j := range filters {
			cmp, ok := compareSpecificity(parts[i], parts[j])
			if !ok || cmp <= 0 {
				continue
			}
			si, sj := specificityScore(parts[i], width), specificityScore(parts[j], width)
			if si <= sj {
				t.Errorf("%q dominates %q but scores %d <= %d — the subscribe order would invert them",
					filters[i], filters[j], si, sj)
			}
		}
	}
}

// TestCommandRouterRefusesAnOverlapWithoutAttribution pins that the v3.1.1
// answer is unchanged and says why. A transport that cannot attribute a
// delivery gets exactly v0.27.0's refusal — visible at the composition
// root — rather than an acceptance that double-runs a handler where nothing
// can see it.
func TestCommandRouterRefusesAnOverlapWithoutAttribution(t *testing.T) {
	t.Parallel()
	r := quietRouter(t, newCmdBroker(), CommandConfig{})
	if err := r.Handle("ccu/+/+/set", (&recorder{}).handle); err != nil {
		t.Fatalf("handle: %v", err)
	}
	err := r.Handle("ccu/+/PRESS_SHORT/set", (&recorder{}).handle)
	if !errors.Is(err, ErrAmbiguousRoutes) {
		t.Fatalf("overlap accepted on a transport that cannot attribute (%v)", err)
	}
	if !errors.Is(err, ErrAttributionUnavailable) {
		t.Errorf("the refusal must name the missing capability, got %v", err)
	}
	if !strings.Contains(err.Error(), "ccu/+/+/set") ||
		!strings.Contains(err.Error(), "ccu/+/PRESS_SHORT/set") {
		t.Errorf("the refusal must name both filters, got %v", err)
	}
	if r.Attributed() {
		t.Error("a refused overlap must not switch the router to attributed delivery")
	}
}

// TestCommandRouterOverlapAcceptanceDependsOnSpecificity walks the shapes a
// consumer writes against both kinds of transport. The two refusals have
// different causes and only one of them is liftable: a pair with no
// strictest claim (`a/+/c` against `a/b/+`, one literal each in different
// levels) has no winner for `a/b/c` no matter who attributes the delivery.
func TestCommandRouterOverlapAcceptanceDependsOnSpecificity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		a, b            string
		attributedIsOK  bool
		plainRefusesToo bool
	}{
		{"ccu/+/+/set", "ccu/+/PRESS_SHORT/set", true, true},
		{"base/dev/set", "base/dev/set/#", true, true},
		{"base/#", "base/dev/set", true, true},
		{"base/#", "base/dev/#", true, true},
		{"a/+/c", "a/b/+", false, true},
		{"a/b/set", "a/c/set", true, false},
		{"a/+/set", "a/+/get", true, false},
	} {
		attr := quietRouter(t, newAttrBroker(), CommandConfig{})
		if err := attr.Handle(tc.a, (&recorder{}).handle); err != nil {
			t.Fatalf("handle %q: %v", tc.a, err)
		}
		err := attr.Handle(tc.b, (&recorder{}).handle)
		if ok := err == nil; ok != tc.attributedIsOK {
			t.Errorf("attributing: Handle(%q) after %q = %v, want accepted=%v",
				tc.b, tc.a, err, tc.attributedIsOK)
		}
		plain := quietRouter(t, newCmdBroker(), CommandConfig{})
		if err := plain.Handle(tc.a, (&recorder{}).handle); err != nil {
			t.Fatalf("handle %q: %v", tc.a, err)
		}
		err = plain.Handle(tc.b, (&recorder{}).handle)
		if refused := errors.Is(err, ErrAmbiguousRoutes); refused != tc.plainRefusesToo {
			t.Errorf("plain: Handle(%q) after %q = %v, want refused=%v",
				tc.b, tc.a, err, tc.plainRefusesToo)
		}
	}
}

// TestCommandRouterStartFailsWhenAttributionIsClaimedButNotDelivered is the
// case the design turns on: a v5-capable adapter that turns out to be
// talking MQTT 3.1.1, where there is no property block to carry an
// identifier.
//
// The capability is a claim, Start is the proof, and the proof failing must
// fail the start — not fall back to an unattributed subscribe, which is the
// accepted-overlap-plus-double-run combination that is strictly worse than
// the refusal this feature lifts. Nothing may be left live and no handler
// may run.
func TestCommandRouterStartFailsWhenAttributionIsClaimedButNotDelivered(t *testing.T) {
	t.Parallel()
	b := newAttrBroker()
	b.stamps = false // the link is v3.1.1
	r := quietRouter(t, b, CommandConfig{})
	generic, specific := &recorder{}, &recorder{}
	if err := r.Handle("gh/+/+/set", generic.handle); err != nil {
		t.Fatalf("handle generic: %v", err)
	}
	if err := r.Handle("gh/+/toggle/set", specific.handle); err != nil {
		t.Fatalf("handle specific: %v", err)
	}
	err := r.Start(context.Background())
	if err == nil {
		t.Fatal("Start succeeded on a transport that could not honour the identifier")
	}
	if !errors.Is(err, ErrAttributionUnavailable) {
		t.Errorf("Start error must wrap ErrAttributionUnavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "identifier") {
		t.Errorf("Start error must name what was refused, got %v", err)
	}
	if live := b.liveFilters(); len(live) != 0 {
		t.Errorf("a failed attributed start left subscriptions live: %v", live)
	}
	// And the fallback really is absent: nothing to deliver to, nothing run.
	b.deliver("gh/dev/toggle/set", []byte("ON"), false)
	r.WaitIdle()
	if n := generic.count() + specific.count(); n != 0 {
		t.Errorf("%d handlers ran after a failed start", n)
	}
}

// TestCommandRouterSpecificityPrefersTheExactRoute is a regression test for
// the inversion the deleted specificity code shipped: `a/b` lost to
// `a/b/#`, so an exactly-registered route never fired. The shorter filter
// is the stricter claim — it matches its own depth and nothing deeper.
func TestCommandRouterSpecificityPrefersTheExactRoute(t *testing.T) {
	t.Parallel()
	b := newAttrBroker()
	r := quietRouter(t, b, CommandConfig{})
	exact, deep := &recorder{}, &recorder{}
	if err := r.Handle("base/dev/set/#", deep.handle); err != nil {
		t.Fatalf("handle deep: %v", err)
	}
	if err := r.Handle("base/dev/set", exact.handle); err != nil {
		t.Fatalf("handle exact: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	if n := b.deliver("base/dev/set", []byte("ON"), false); n != 2 {
		t.Fatalf("broker fanned out to %d subscriptions, want 2", n)
	}
	r.WaitIdle()
	if got := exact.count(); got != 1 {
		t.Errorf("the exact route ran %d times, want 1 — it is the stricter claim on its own topic", got)
	}
	if got := deep.count(); got != 0 {
		t.Errorf("the `#` route ran %d times on a topic the exact route claims, want 0", got)
	}

	if n := b.deliver("base/dev/set/extra", []byte("ON"), false); n != 1 {
		t.Fatalf("deeper topic hit %d subscriptions, want 1", n)
	}
	r.WaitIdle()
	if got := deep.count(); got != 1 {
		t.Errorf("the `#` route ran %d times on the topic only it claims, want 1", got)
	}
}

// TestCommandRouterStampsEveryRouteOnceAnOverlapIsAccepted pins the
// all-or-nothing rule. An unstamped copy carries no identifier, so the
// client re-matches it against every filter it holds and hands it to the
// stamped overlapping routes as well — one un-stamped subscription
// therefore restores the multiplication the identifiers were taken out for,
// including through a route that overlaps nothing.
func TestCommandRouterStampsEveryRouteOnceAnOverlapIsAccepted(t *testing.T) {
	t.Parallel()
	b := newAttrBroker()
	r := quietRouter(t, b, CommandConfig{})
	for _, f := range []string{"gh/+/+/set", "gh/+/toggle/set", "other/#"} {
		if err := r.Handle(f, (&recorder{}).handle); err != nil {
			t.Fatalf("handle %q: %v", f, err)
		}
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	seen := map[uint32]string{}
	for _, f := range []string{"gh/+/+/set", "gh/+/toggle/set", "other/#"} {
		id := r.SubscriptionID(f)
		if id == 0 || id > MaxSubscriptionID {
			t.Fatalf("route %q carries identifier %d, want one in 1..%d", f, id, MaxSubscriptionID)
		}
		if b.idOf(f) != id {
			t.Errorf("route %q went out with identifier %d, the router believes %d", f, b.idOf(f), id)
		}
		if prev, dup := seen[id]; dup {
			t.Fatalf("routes %q and %q share identifier %d", prev, f, id)
		}
		seen[id] = f
	}
}

// TestCommandRouterIdentifiersDoNotCollideAcrossRouters pins the allocation
// rule, which is process-wide and not per router.
//
// Per-router numbering would be the obvious choice and it is wrong: the
// identifier space belongs to the session, so two routers over one client —
// two consumers of this library in one binary, or a config reload building
// a second router — would both number from 1 and each would receive the
// other's commands.
func TestCommandRouterIdentifiersDoNotCollideAcrossRouters(t *testing.T) {
	t.Parallel()
	seen := map[uint32]bool{}
	for range 2 {
		r := quietRouter(t, newAttrBroker(), CommandConfig{})
		for _, f := range []string{"gh/+/+/set", "gh/+/toggle/set"} {
			if err := r.Handle(f, (&recorder{}).handle); err != nil {
				t.Fatalf("handle %q: %v", f, err)
			}
		}
		if err := r.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}
		t.Cleanup(func() { _ = r.Stop(context.Background()) })
		for _, f := range []string{"gh/+/+/set", "gh/+/toggle/set"} {
			id := r.SubscriptionID(f)
			if seen[id] {
				t.Fatalf("identifier %d handed out twice; two routers in one process collide", id)
			}
			seen[id] = true
		}
	}
}

// TestCommandRouterDisjointRoutesCarryNoIdentifier is the additive half: a
// consumer whose routes do not overlap gets exactly v0.27.0's wire, even on
// a transport that could attribute. Stamping anyway would change every
// existing consumer's SUBSCRIBE — including onto a broker that answers a
// Subscription Identifier with a SUBACK failure, turning a Start that
// worked into one that does not.
func TestCommandRouterDisjointRoutesCarryNoIdentifier(t *testing.T) {
	t.Parallel()
	b := newAttrBroker()
	r := quietRouter(t, b, CommandConfig{})
	for _, f := range []string{"gh/+/set", "gh/+/get"} {
		if err := r.Handle(f, (&recorder{}).handle); err != nil {
			t.Fatalf("handle %q: %v", f, err)
		}
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	if r.Attributed() {
		t.Error("a router with disjoint routes reported attributed delivery")
	}
	for _, f := range []string{"gh/+/set", "gh/+/get"} {
		if id := r.SubscriptionID(f); id != 0 {
			t.Errorf("disjoint route %q was stamped with identifier %d", f, id)
		}
		if id := b.idOf(f); id != 0 {
			t.Errorf("disjoint route %q went on the wire with identifier %d", f, id)
		}
	}
}

// TestCommandRouterResubscribeKeepsTheIdentifier pins that a replay carries
// the identifier it was registered under. A broker holds the identifier as
// part of the subscription and forgets it with the session, so a replay
// under a fresh one would leave the router attributing to a subscription
// that no longer exists — attribution silently working before a drop and
// silently not after it.
func TestCommandRouterResubscribeKeepsTheIdentifier(t *testing.T) {
	t.Parallel()
	b := newAttrBroker()
	r := quietRouter(t, b, CommandConfig{})
	generic, specific := &recorder{}, &recorder{}
	if err := r.Handle("gh/+/+/set", generic.handle); err != nil {
		t.Fatalf("handle generic: %v", err)
	}
	if err := r.Handle("gh/+/toggle/set", specific.handle); err != nil {
		t.Fatalf("handle specific: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })
	before := map[string]uint32{
		"gh/+/+/set":      r.SubscriptionID("gh/+/+/set"),
		"gh/+/toggle/set": r.SubscriptionID("gh/+/toggle/set"),
	}

	b.drop() // a reconnect against a broker that did not keep the session
	if err := r.Resubscribe(context.Background()); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	for f, id := range before {
		if got := b.idOf(f); got != id {
			t.Errorf("route %q replayed under identifier %d, was %d", f, got, id)
		}
	}

	// And attribution still holds after the replay.
	if n := b.deliver("gh/dev/toggle/set", []byte("ON"), false); n != 2 {
		t.Fatalf("broker fanned out to %d subscriptions after the replay, want 2", n)
	}
	r.WaitIdle()
	if got := specific.count(); got != 1 {
		t.Errorf("the specific route ran %d times after a reconnect, want 1", got)
	}
	if got := generic.count(); got != 0 {
		t.Errorf("the general route ran %d times on a topic the specific route claims, want 0", got)
	}
}

// TestCompareSpecificity is the ordering itself, including the inversion
// the deleted version shipped (`a/b` losing to `a/b/#`) and the pairs that
// have no winner at all.
func TestCompareSpecificity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		a, b string
		cmp  int
		ok   bool
	}{
		{"ccu/+/PRESS_SHORT/set", "ccu/+/+/set", 1, true},
		{"a/b", "a/b/#", 1, true},
		{"a/b/c", "a/#", 1, true},
		{"a/b/#", "a/#", 1, true},
		{"a/b/c", "a/+/c", 1, true},
		{"a/+/c", "a/#", 1, true},
		{"a/+/c", "a/b/+", 0, false},
		{"a/+/c", "a/+/c", 0, true},
		{"a/#", "a/b/c", -1, true},
	} {
		cmp, ok := compareSpecificity(strings.Split(tc.a, "/"), strings.Split(tc.b, "/"))
		if cmp != tc.cmp || ok != tc.ok {
			t.Errorf("compareSpecificity(%q, %q) = %d, %v; want %d, %v",
				tc.a, tc.b, cmp, ok, tc.cmp, tc.ok)
		}
	}
}

// TestCommandRouterSurvivesFailClosedUnstampedRouting is the consumer-side
// half of go-mqtt v1.5.1, and it is a "nothing here depended on the old
// behaviour" test.
//
// Through v1.5.0 the client matched an identifier-less PUBLISH by topic
// against STAMPED subscriptions too, so a consumer's own broad subscription
// on the same client handed every one of its copies to the router's stamped
// routes as well — one published message, one stamped handler run twice, the
// exact multiplication the identifiers were taken out for. MQTT 5.0 §3.3.4
// makes that impossible on a compliant server: a message forwarded for a
// stamped subscription carries that identifier. v1.5.1 therefore fails
// closed, and this pins that the router is correct under the stricter
// routing: every route keeps its own copies and the consumer's own
// subscription keeps its own.
//
// The router's promise is unchanged either way — the CommandRouter doc
// already warns that a second subscription on the same client re-multiplies
// — but the promise now holds without the consumer having to keep its own
// subscriptions off the command tree.
func TestCommandRouterSurvivesFailClosedUnstampedRouting(t *testing.T) {
	t.Parallel()
	b := newAttrBroker()
	b.failsClosed = true
	r := quietRouter(t, b, CommandConfig{})
	generic, specific := &recorder{}, &recorder{}
	if err := r.Handle("gh/+/+/set", generic.handle); err != nil {
		t.Fatalf("handle generic: %v", err)
	}
	if err := r.Handle("gh/+/PRESS_SHORT/set", specific.handle); err != nil {
		t.Fatalf("handle specific: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	// The consumer's own broad, unstamped subscription on the same client.
	var mine atomic.Int64
	if err := b.Subscribe(context.Background(), "gh/#", 0, func(string, []byte, bool) {
		mine.Add(1)
	}); err != nil {
		t.Fatalf("the consumer's own subscribe: %v", err)
	}

	if n := b.deliver("gh/ccu/PRESS_SHORT/set", []byte("1"), false); n != 3 {
		t.Fatalf("broker fanned out to %d subscriptions, want 3 — one copy per matching subscription", n)
	}
	r.WaitIdle()

	if got := len(specific.snapshot()); got != 1 {
		t.Fatalf("the specific route ran %d times, want exactly 1", got)
	}
	if got := len(generic.snapshot()); got != 0 {
		t.Fatalf("the outranked route ran %d times, want 0", got)
	}
	if got := mine.Load(); got != 1 {
		t.Fatalf("the consumer's own subscription saw %d copies, want 1 — fail-closed must not starve it", got)
	}
}

// TestSubscriptionIDAllocationStopsAtTheCeiling pins that exhaustion is
// permanent.
//
// The counter used to keep incrementing past MaxSubscriptionID and report the
// range error each time — correct until 2^32 allocations wrap it back to
// small values that pass the range check again, at which point the router
// hands out identifiers it has already used. That is the collision a
// process-wide counter exists to prevent, arriving by the one route the range
// guard does not cover. Unreachable in practice, cheap to close, and
// impossible to notice afterwards if it ever were reached.
func TestSubscriptionIDAllocationStopsAtTheCeiling(t *testing.T) {
	t.Parallel()
	var c atomic.Uint32

	id, ok := allocateSubscriptionID(&c)
	if !ok || id != 1 {
		t.Fatalf("first identifier = %d, ok=%v, want 1 — the router allocates upward from 1", id, ok)
	}

	c.Store(MaxSubscriptionID - 1)
	if id, ok := allocateSubscriptionID(&c); !ok || id != MaxSubscriptionID {
		t.Fatalf("last identifier = %d, ok=%v, want %d", id, ok, MaxSubscriptionID)
	}
	for range 3 {
		if id, ok := allocateSubscriptionID(&c); ok || id != 0 {
			t.Fatalf("allocation past the ceiling returned %d, ok=%v, want exhausted", id, ok)
		}
	}
	if got := c.Load(); got != MaxSubscriptionID {
		t.Fatalf("counter ran on to %d; it wraps at 2^32 and starts handing out live identifiers again", got)
	}
}
