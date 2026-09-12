// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"slices"
	"sort"
	"sync"
	"testing"
)

// This file is the command half of the conformance harness: the routing plane
// driven against the same broker and the same rendered device document as the
// discovery, state and availability planes.
//
// It is a file of its own rather than more tests beside the others because
// the invariants here are the only ones that need a reader. Everything else
// in the harness checks what this process writes; these check that what Home
// Assistant is told to write lands somewhere.

// commandFleet is a [fleet] with the routing plane wired in.
//
// The router is not part of the base fixture because a consumer whose
// entities are all read-only legitimately has none, and the base invariants
// have to hold for that consumer too.
type commandFleet struct {
	*fleet

	router *CommandRouter

	mu   sync.Mutex
	seen []Command
}

// newCommandFleet wires a router over the base fleet's broker, subscribing
// the one wildcard a consumer can actually install.
func newCommandFleet(t *testing.T) *commandFleet {
	t.Helper()

	cf := &commandFleet{fleet: newFleet(t)}
	cf.router = NewCommandRouter(cf.broker, CommandConfig{})
	if err := cf.router.Handle(commandFilter(), func(_ context.Context, cmd Command) {
		cf.mu.Lock()
		cf.seen = append(cf.seen, cmd)
		cf.mu.Unlock()
	}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	t.Cleanup(func() { _ = cf.router.Stop(context.Background()) })
	return cf
}

// delivered returns the commands the handler has run, after draining.
func (cf *commandFleet) delivered() []Command {
	cf.router.WaitIdle()
	cf.mu.Lock()
	defer cf.mu.Unlock()
	return append([]Command(nil), cf.seen...)
}

func (cf *commandFleet) forget() {
	cf.router.WaitIdle()
	cf.mu.Lock()
	cf.seen = nil
	cf.mu.Unlock()
}

// TestConformanceEveryAdvertisedCommandTopicIsRouted pins the invariant the
// task of this harness names first: the `command_topic` a config advertises is
// the topic the command plane actually subscribes.
//
// A mismatch is invisible in unit tests on either side. The discovery plane
// asserts it rendered what its Context returned; the router asserts it
// subscribed what it was registered with. Home Assistant then publishes to a
// topic nobody reads, the button in the dashboard does nothing at all, and
// there is no error anywhere — not in the broker, not in Home Assistant's log,
// not in the consumer's. The only evidence is a control that never works.
//
// [CommandTopics] is used rather than [discovery.Component.CommandTopic]
// because a climate names five command topics and a light ten, all inside a
// platform Fields struct; reading the plain field would check one entity's
// worth of an invariant that has to hold for every key the document carries.
func TestConformanceEveryAdvertisedCommandTopicIsRouted(t *testing.T) {
	cf := newCommandFleet(t)
	ctx := context.Background()
	cf.boot(ctx)
	if err := cf.router.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	advertised, err := BundleCommandTopics(cf.bundle)
	if err != nil {
		t.Fatalf("command topics: %v", err)
	}
	if len(advertised) == 0 {
		t.Fatal("the fixture advertises no command topic; this invariant would pass by iterating nothing")
	}
	for _, ct := range advertised {
		cmd, ok := cf.router.Route(ct)
		if !ok {
			t.Errorf("the document advertises command_topic %q, which no route of %v claims — "+
				"Home Assistant publishes there and nobody reads it",
				ct, cf.router.Filters())
			continue
		}
		if !slices.Contains(cf.router.Filters(), cmd.Filter) {
			t.Errorf("%q resolved to filter %q, absent from the registered set %v",
				ct, cmd.Filter, cf.router.Filters())
		}
	}
}

// TestConformanceCommandReachesItsHandlerThroughTheBroker is the same
// invariant executed rather than resolved.
//
// [CommandRouter.Route] answers from the route table; this publishes on the
// advertised topic and waits for the handler, so the subscription really is on
// the wire and the wildcards really carry the coordinate a handler needs. The
// measured consumer recovered that coordinate by splitting the topic itself at
// thirteen call sites, and two of its confirmed defects were index arithmetic.
func TestConformanceCommandReachesItsHandlerThroughTheBroker(t *testing.T) {
	cf := newCommandFleet(t)
	ctx := context.Background()
	cf.boot(ctx)
	if err := cf.router.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	advertised, err := BundleCommandTopics(cf.bundle)
	if err != nil {
		t.Fatalf("command topics: %v", err)
	}
	for _, ct := range advertised {
		cf.forget()
		// Not retained: Home Assistant never publishes a command retained,
		// and the router drops retained commands by default.
		if err := cf.broker.Publish(ctx, ct, []byte("ON"), 1, false); err != nil {
			t.Fatalf("publish command: %v", err)
		}
		got := cf.delivered()
		if len(got) != 1 {
			t.Errorf("%q: handler ran %d times, want once", ct, len(got))
			continue
		}
		if got[0].Topic != ct {
			t.Errorf("handler saw topic %q, want %q", got[0].Topic, ct)
		}
		if !slices.Contains(got[0].Wildcards, cf.dev.UID()) {
			t.Errorf("%q: wildcards %v do not name the device %q — a handler cannot tell "+
				"which device the command is for", ct, got[0].Wildcards, cf.dev.UID())
		}
	}
}

// TestConformanceNothingThisProcessPublishesIsRoutedBack is the disjointness
// guard run over the real writing surface of all three publishing planes.
//
// The broker has no notion of "my own message": it fans every publish out to
// every matching subscription, the publisher's included, with the retain flag
// clear because live routing is not a retained replay. So a topic this process
// writes that any command filter matches is the process commanding itself.
// In the measured case a program entity's state was mirrored onto the topic
// its trigger arrived on, and every state publish executed the program — on
// every boot, on every rediscovery, for every program in the house. The only
// signal was the programs running.
//
// The topic list is assembled from what actually went on the wire rather than
// from the document alone, because the document is only half the surface: the
// bridge status topic, the device document's own config topic and anything a
// consumer publishes outside its entities are all echo candidates too. It is
// also wider than the documented input on purpose — [BundleStateTopics] omits
// the availability list, so a consumer following the doc comment verbatim
// would check every state topic and no availability topic.
func TestConformanceNothingThisProcessPublishesIsRoutedBack(t *testing.T) {
	cf := newCommandFleet(t)
	ctx := context.Background()
	cf.boot(ctx)
	if err := cf.router.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	documented, err := BundleStateTopics(cf.bundle)
	if err != nil {
		t.Fatalf("state topics: %v", err)
	}
	written := append([]string(nil), documented...)
	written = append(written, cf.state.Published()...)
	written = append(written, cf.avail.Topics()...)
	written = append(written, cf.layout.Bridge(), cf.configTopic(), BirthTopic(conformPrefix))

	if err := cf.router.CheckDisjoint(written...); err != nil {
		t.Errorf("a topic this process publishes is routed back into its own handlers: %v", err)
	}
}

// TestConformanceTheDocumentsReadingSurfaceIsTheProcessesWritingSurface is
// the census invariant, and the one that most nearly says "this deployment is
// coherent".
//
// Every non-command topic a device document names is a topic Home Assistant
// will subscribe and wait on: state, availability, attributes. Every one of
// them must be written by one of this process's three publishing planes, and
// no plane may write a consumer-tree topic the document does not name. The
// first direction failing is an entity permanently `unknown` or permanently
// unavailable; the second is a value nothing reads, which is how a plane ends
// up addressing a datapoint the discovery plane described differently.
//
// Both directions are checked here because each alone is satisfiable by a
// mistake: a plane that writes a superset passes the first, a document that
// names a subset passes the second.
//
// The surface is assembled by [commandFleet.readingSurface] rather than by
// [BundleStateTopics], which omits the availability list — see
// TestConformanceBundleStateTopicsOmitsTheAvailabilityList.
func TestConformanceTheDocumentsReadingSurfaceIsTheProcessesWritingSurface(t *testing.T) {
	cf := newCommandFleet(t)
	ctx := context.Background()
	cf.boot(ctx)

	documented := cf.readingSurface()
	written := map[string]bool{}
	for _, t2 := range cf.state.Published() {
		written[t2] = true
	}
	for _, t2 := range cf.avail.Topics() {
		written[t2] = true
	}
	written[cf.layout.Bridge()] = true

	for _, t2 := range documented {
		if !written[t2] {
			t.Errorf("the document tells Home Assistant to read %q, which no plane of this process writes", t2)
		}
	}
	named := map[string]bool{}
	for _, t2 := range documented {
		named[t2] = true
	}
	for t2 := range written {
		if !named[t2] {
			t.Errorf("a plane of this process writes %q, which the document names nowhere", t2)
		}
	}
}

// TestConformanceReconnectRestoresSubscriptionsWithDiscoveryAndState pins the
// reconnect across all four planes at once.
//
// Each has its own restoration and they are not interchangeable: the discovery
// plane replays cached bytes, the state plane replays its index, the
// availability plane must clear its gate first, and the router resubscribes.
// A consumer that restores three of the four comes back looking healthy — the
// entities are populated and available — and silently accepts no commands
// until the daemon restarts.
func TestConformanceReconnectRestoresSubscriptionsWithDiscoveryAndState(t *testing.T) {
	cf := newCommandFleet(t)
	ctx := context.Background()
	cf.boot(ctx)
	if err := cf.router.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	before := cf.broker.retainedTopics()
	filtersBefore := cf.router.Filters()
	cf.broker.drop()

	if err := cf.run.AnnounceOnline(ctx); err != nil {
		t.Fatalf("re-announce: %v", err)
	}
	if _, err := cf.run.Republish(ctx); err != nil {
		t.Fatalf("republish discovery: %v", err)
	}
	cf.avail.Reset()
	if _, err := cf.avail.Device(ctx, DeviceSlot(cf.dev, cf.entities[0]), true); err != nil {
		t.Fatalf("re-flip availability: %v", err)
	}
	if _, err := cf.state.Republish(ctx); err != nil {
		t.Fatalf("republish state: %v", err)
	}
	if err := cf.router.Resubscribe(ctx); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}

	if after := cf.broker.retainedTopics(); !slices.Equal(before, after) {
		t.Errorf("the reconnect restored a different tree\nbefore: %v\nafter:  %v", before, after)
	}
	if after := cf.router.Filters(); !slices.Equal(filtersBefore, after) {
		t.Errorf("the resubscribe restored a different filter set\nbefore: %v\nafter:  %v",
			filtersBefore, after)
	}

	// And it is on the wire, not just in the table.
	advertised, err := BundleCommandTopics(cf.bundle)
	if err != nil {
		t.Fatalf("command topics: %v", err)
	}
	cf.forget()
	if err := cf.broker.Publish(ctx, advertised[0], []byte("ON"), 1, false); err != nil {
		t.Fatalf("publish command: %v", err)
	}
	if got := cf.delivered(); len(got) != 1 {
		t.Errorf("after the reconnect a command on %q reached the handler %d times, want once",
			advertised[0], len(got))
	}
}

// TestConformanceARetainedCommandDoesNotFireOnResubscribe pins the one thing a
// retained-message broker does to a command plane that no other plane feels.
//
// A command topic is not supposed to carry a retained payload — Home Assistant
// never publishes one — but somebody's `mosquitto_pub -r` does, and then the
// broker replays it to the router on every single (re)subscribe. The consumer
// that allowed it re-issued the previous run's last write on every daemon
// restart and on every reconnect, which from the outside reads as a device
// turning itself on. It is a cross-plane invariant because the retained replay
// arrives in exactly the same window as the discovery and state replays the
// other three planes are performing, which is when nobody is looking at the
// command plane.
func TestConformanceARetainedCommandDoesNotFireOnResubscribe(t *testing.T) {
	cf := newCommandFleet(t)
	ctx := context.Background()
	cf.boot(ctx)

	advertised, err := BundleCommandTopics(cf.bundle)
	if err != nil {
		t.Fatalf("command topics: %v", err)
	}
	cf.broker.seed(advertised[0], []byte("ON"))

	if err := cf.router.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := cf.delivered(); len(got) != 0 {
		t.Fatalf("a retained command on %q fired %d handler runs on subscribe", advertised[0], len(got))
	}
	if err := cf.router.Resubscribe(ctx); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	if got := cf.delivered(); len(got) != 0 {
		t.Errorf("a retained command on %q fired %d handler runs on resubscribe", advertised[0], len(got))
	}
}

// readingSurface is every topic the device document tells Home Assistant to
// read: the top-level `*_topic` keys plus the entries of each component's
// availability list.
//
// Assembled here rather than taken from [BundleStateTopics] because that
// function walks only the top level of a component body, and the availability
// topics of a device-based document are not there — they are objects inside an
// `availability` array. See
// TestConformanceBundleStateTopicsOmitsTheAvailabilityList.
func (cf *commandFleet) readingSurface() []string {
	cf.t.Helper()
	seen := map[string]bool{}
	topics, err := BundleStateTopics(cf.bundle)
	if err != nil {
		cf.t.Fatalf("state topics: %v", err)
	}
	for _, t := range topics {
		seen[t] = true
	}
	comps := cf.components()
	for i := range comps {
		for _, at := range availabilityTopicsOf(comps[i].comp) {
			seen[at] = true
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// TestConformanceBundleStateTopicsOmitsTheAvailabilityList records a gap the
// two planes only have together.
//
// [StateTopics] documents its result as "every `*_topic` key that is not a
// command topic, availability and JSON attributes included", and
// [CommandRouter.CheckDisjoint] documents [BundleStateTopics] as the input to
// pass it at boot. That promise used to break on exactly the availability
// half: `StdContext` renders availability as a LIST of objects under the
// `availability` key, and the extraction walked only the top level of a
// component body, where `availability` is neither a `*_topic` key nor a
// string. The legacy single-topic `availability_topic` form was picked up;
// the list form -- the only form this module's discovery pipeline produces
// -- was not.
//
// What it cost, and why this test is worth its length: a consumer following
// the doc comment verbatim ran CheckDisjoint over every state topic and no
// availability topic, so a command filter overlapping an availability topic
// passed the boot check. The echo was then a command issued on every
// availability flip -- the same defect the guard exists to catch, at the one
// moment, a device dropping off the bus, when nobody is reading the logs.
func TestConformanceBundleStateTopicsReportsTheAvailabilityList(t *testing.T) {
	cf := newCommandFleet(t)
	ctx := context.Background()
	cf.boot(ctx)

	documented, err := BundleStateTopics(cf.bundle)
	if err != nil {
		t.Fatalf("state topics: %v", err)
	}
	var missing []string
	var checked int
	comps := cf.components()
	for i := range comps {
		for _, at := range availabilityTopicsOf(comps[i].comp) {
			checked++
			if !slices.Contains(documented, at) && !slices.Contains(missing, at) {
				missing = append(missing, at)
			}
		}
	}
	if checked == 0 {
		t.Fatal("the fixture advertises no availability topic, so this proves nothing")
	}
	if len(missing) != 0 {
		t.Errorf("availability topics absent from BundleStateTopics: %v", missing)
	}
}
