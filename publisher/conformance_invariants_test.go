// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// TestConformanceAdvertisedStateTopicIsWhereStateIsPublished pins the first
// cross-plane invariant: the string a config carries in `state_topic` is the
// string the state plane writes to.
//
// Neither plane can check it. The discovery plane asserts that it rendered
// what its Context returned; the state plane asserts that it wrote to the
// topic it was handed. A consumer that renders through one layout and
// publishes through another — two composition roots, an operator-configured
// root read twice, a scope forgotten on one side — satisfies both and
// produces an entity that stays at `unknown` in Home Assistant for the life
// of the deployment, with a perfectly valid config on the broker and a
// perfectly valid value beside it.
func TestConformanceAdvertisedStateTopicIsWhereStateIsPublished(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	written := f.state.Published()
	for _, c := range f.components() {
		if c.comp.StateTopic == "" {
			continue
		}
		if !slices.Contains(written, c.comp.StateTopic) {
			t.Errorf("%s: config advertises state_topic %q, which the state plane never wrote (it wrote %v)",
				c.key, c.comp.StateTopic, written)
		}
		if !f.broker.holds(c.comp.StateTopic) {
			t.Errorf("%s: nothing retained on advertised state_topic %q", c.key, c.comp.StateTopic)
		}
	}

	// And the other direction: a state topic no config names is a value
	// nothing reads. It is not automatically a defect — a consumer's own
	// non-HA plane publishes such topics on purpose — but on this fleet,
	// which publishes exactly its entities, it would mean the state plane
	// addressed a datapoint the discovery plane described differently.
	named := map[string]bool{}
	for _, c := range f.components() {
		if c.comp.StateTopic != "" {
			named[c.comp.StateTopic] = true
		}
	}
	for _, t2 := range written {
		if !named[t2] {
			t.Errorf("state plane wrote %q, which no config names", t2)
		}
	}
}

// TestConformanceStateTopicSurvivesTheRoundTripThroughTheBroker checks the
// same agreement against the bytes rather than the struct.
//
// The in-memory [discovery.Component] is not what Home Assistant reads. Its
// MarshalJSON merges a Fields struct and an Extra map over the typed keys,
// later winning, so a Builder or an operator override can replace
// `state_topic` after the default projection set it — and then the struct and
// the document disagree. The document is the contract.
func TestConformanceStateTopicSurvivesTheRoundTripThroughTheBroker(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	written := f.state.Published()
	for key, body := range f.readBack() {
		advertised, _ := body["state_topic"].(string)
		if advertised == "" {
			continue
		}
		if !slices.Contains(written, advertised) {
			t.Errorf("%s: retained document advertises state_topic %q, unwritten by the state plane", key, advertised)
		}
	}
}

// TestConformanceCommandTopicIsCoveredByTheCommandSubscription pins the
// command half that is reachable without the routing plane: every
// `command_topic` a config advertises must be matched by a filter the
// consumer subscribes, and no `state_topic` may be.
//
// The second half is the load-bearing one and it is a measured defect, not a
// hypothetical. A broker delivers a client's own publishes back to it — the
// consumers subscribe without MQTT 5's No-Local — so a state topic covered by
// a command filter is a write the process performs on itself. The reference
// implementation mirrored a program's state onto its own trigger topic: the
// echo ran the program on every boot, on every republish and once per freshly
// discovered program, with nothing in the logs saying so.
// [ErrStateCommandCollision] is the state plane's guard for it; this test is
// what proves the guard is armed with the filters the consumer really
// installs.
func TestConformanceCommandTopicIsCoveredByTheCommandSubscription(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()

	filter := commandFilter()
	for _, c := range f.components() {
		if c.comp.CommandTopic == "" {
			continue
		}
		if !MatchFilter(filter, c.comp.CommandTopic) {
			t.Errorf("%s: advertises command_topic %q, which subscription %q does not cover — "+
				"Home Assistant would send commands nobody receives",
				c.key, c.comp.CommandTopic, filter)
		}
	}

	for _, c := range f.components() {
		if c.comp.StateTopic == "" {
			continue
		}
		_, err := f.state.Publish(ctx, c.comp.StateTopic, []byte(`{"value":1,"available":true}`))
		if errors.Is(err, ErrStateCommandCollision) {
			t.Errorf("%s: state_topic %q is covered by command subscription %q — "+
				"every state publish echoes back as a command", c.key, c.comp.StateTopic, filter)
		} else if err != nil {
			t.Fatalf("%s: state publish: %v", c.key, err)
		}
	}
}

// TestConformanceCommandRoutingDispatchesToTheAdvertisedTopic is the half of
// the command invariant that needs the routing plane.
//
// Skipped, not removed: the check it owes is the one no other test can make —
// that the topic a config advertises is the topic a dispatcher actually
// routes, and that a resubscribe after a reconnect restores exactly that set.
// Writing it against a guessed API would pin the guess rather than the
// contract.
func TestConformanceCommandRoutingDispatchesToTheAdvertisedTopic(t *testing.T) {
	t.Skip("the command-routing plane (publisher/command.go) is not in this tree; " +
		"see TestConformanceCommandTopicIsCoveredByTheCommandSubscription for the half " +
		"reachable through StateConfig.CommandFilters")
}

// TestConformanceStateAndCommandTopicsAreDisjoint pins that no topic is both.
//
// Checked across the whole fleet rather than per entity, because the
// dangerous case is not an entity whose own two topics collide — a layout
// producing that is broken at a glance — but entity A's command topic landing
// on entity B's state topic. Any layout that derives one from the other by
// suffix can produce it: `topic.Default` appends a `set` segment, so a
// datapoint whose own path ends in `set` renders a state topic identical to
// its sibling's command topic. The result is a device that commands itself
// whenever that sibling reports.
func TestConformanceStateAndCommandTopicsAreDisjoint(t *testing.T) {
	f := newFleet(t)

	states := map[string]string{}
	commands := map[string]string{}
	for _, c := range f.components() {
		if c.comp.StateTopic != "" {
			states[c.comp.StateTopic] = c.key
		}
		if c.comp.CommandTopic != "" {
			commands[c.comp.CommandTopic] = c.key
		}
	}
	for cmd, owner := range commands {
		if victim, ok := states[cmd]; ok {
			t.Errorf("command_topic of %q is the state_topic of %q (%s) — "+
				"reporting state fires the command", owner, victim, cmd)
		}
	}

	// The one deliberate exception, asserted so it stays deliberate: a
	// [model.LevelSelf] availability entry points AT a state topic on
	// purpose, because the availability of such an entity is a datapoint.
	// Nothing else may.
	for _, c := range f.components() {
		for _, at := range availabilityTopicsOf(c.comp) {
			if owner, ok := commands[at]; ok {
				t.Errorf("%s gates on availability topic %q, which is the command_topic of %q",
					c.key, at, owner)
			}
			if owner, ok := states[at]; ok && !c.ent.Desc().Availability.Has(model.LevelSelf) {
				t.Errorf("%s gates on availability topic %q, which is the state_topic of %q, "+
					"without declaring LevelSelf", c.key, at, owner)
			}
		}
	}
}

// TestConformanceBridgeAvailabilityIsTheRuntimeStatusTopic pins that the
// bridge-level availability topic every config carries is the one
// [Runtime.Will] names and [Runtime.AnnounceOnline] writes.
//
// Three strings have to be the same and they are produced by three different
// objects: [topic.Layout.Bridge] writes it into the config,
// [Config.StatusTopic] is what the runtime announces on, and the client's
// CONNECT carries whatever [Runtime.Will] returned. Two reference bridges got
// this wrong in opposite directions — one publishes its status inside Home
// Assistant's own birth tree, two configure a will no entity references — and
// in both cases the broker dutifully writes `offline` on a hard crash while
// every entity in Home Assistant stays available forever, showing the last
// value it ever saw.
func TestConformanceBridgeAvailabilityIsTheRuntimeStatusTopic(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()

	will, err := f.run.Will()
	if err != nil {
		t.Fatalf("will: %v", err)
	}
	if want := f.layout.Bridge(); will.Topic != want {
		t.Fatalf("will topic %q, layout bridge topic %q", will.Topic, want)
	}
	if !will.Retain {
		t.Error("the will is not retained: a Home Assistant subscribing after the crash is told nothing")
	}

	if err := f.run.AnnounceOnline(ctx); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if got := f.broker.retainedPayload(will.Topic); got != BirthPayload {
		t.Fatalf("status topic %q holds %q, want %q", will.Topic, got, BirthPayload)
	}

	for _, c := range f.components() {
		if !c.ent.Desc().Availability.Has(model.LevelBridge) {
			continue
		}
		if !slices.Contains(availabilityTopicsOf(c.comp), will.Topic) {
			t.Errorf("%s declares LevelBridge but its availability list %v does not name the will topic %q",
				c.key, availabilityTopicsOf(c.comp), will.Topic)
		}
	}
}

// TestConformanceDeviceAvailabilityIsWhereTheAvailabilityPlaneWrites pins the
// device level, which is the one v0.25.0 declared and nothing published.
//
// The coordinate is derived twice — `discovery.deviceSlot` renders the config,
// [DeviceSlot] addresses the publish — and the two are separate functions
// because the first is unexported. That duplication is the whole risk: the
// slot is not a plain device identity, it inherits the scope and channel of
// what the entity binds, and a plane that rebuilt it by hand gets the device
// root instead and writes a topic no config names.
func TestConformanceDeviceAvailabilityIsWhereTheAvailabilityPlaneWrites(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	for _, c := range f.components() {
		if !c.ent.Desc().Availability.Has(model.LevelDevice) {
			continue
		}
		want, err := f.avail.DeviceTopic(DeviceSlot(f.dev, c.ent))
		if err != nil {
			t.Fatalf("%s: device topic: %v", c.key, err)
		}
		if !slices.Contains(availabilityTopicsOf(c.comp), want) {
			t.Errorf("%s gates on %v, but the availability plane addresses %q",
				c.key, availabilityTopicsOf(c.comp), want)
		}
		online, known := f.avail.Online(want)
		if !known {
			t.Errorf("%s: the availability plane has never written %q", c.key, want)
		}
		if !online {
			t.Errorf("%s: %q was last written offline after a clean boot", c.key, want)
		}
	}
}

// TestConformanceNoConfigNamesATopicNothingPublishes is the invariant
// `hadoctor` exists to find on a live broker, asserted here before anything
// ships.
//
// Every topic a retained config points at — its state topic and every entry
// of its availability list — must be a topic some plane of this process
// actually writes. An availability source that never publishes is not
// neutral: under `availability_mode: all`, the default, the entity stays
// unavailable, and the only evidence is its absence from the dashboard.
func TestConformanceNoConfigNamesATopicNothingPublishes(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	for _, c := range f.components() {
		for _, at := range availabilityTopicsOf(c.comp) {
			if !f.broker.holds(at) {
				t.Errorf("%s gates on availability topic %q, which carries nothing — "+
					"the entity is unavailable forever and nothing says why", c.key, at)
			}
		}
		if c.comp.StateTopic != "" && !f.broker.holds(c.comp.StateTopic) {
			t.Errorf("%s reads from state topic %q, which carries nothing", c.key, c.comp.StateTopic)
		}
	}
}

// TestConformanceAvailabilityLivesOutsideTheDiscoveryTree pins that nothing
// this process writes for its own availability lands under the discovery
// prefix.
//
// The measured defect: one bridge publishes its status at
// `<discovery_prefix>/status/lwt`, inside Home Assistant's own birth tree.
// It is both in the wrong place and inert, and it is invisible from the code
// that contains it because the string is assembled from a base the operator
// configures. The prefix also means the orphan sweep's own `<prefix>/#`
// snapshot would see it, which is a second way for the same mistake to bite.
func TestConformanceAvailabilityLivesOutsideTheDiscoveryTree(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	tree := strings.TrimSuffix(conformPrefix, "/") + "/"
	for _, c := range f.components() {
		for _, at := range availabilityTopicsOf(c.comp) {
			if strings.HasPrefix(at, tree) {
				t.Errorf("%s gates on %q, inside Home Assistant's own tree %q", c.key, at, tree)
			}
		}
	}
	for _, at := range f.avail.Topics() {
		if strings.HasPrefix(at, tree) {
			t.Errorf("the availability plane wrote %q, inside Home Assistant's own tree %q", at, tree)
		}
	}
	for _, st := range f.state.Published() {
		if strings.HasPrefix(st, tree) {
			t.Errorf("the state plane wrote %q, inside Home Assistant's own tree %q", st, tree)
		}
	}
}

// TestConformanceReconnectRestoresEveryPlane pins that a broker which came
// back without a persistent retained store ends up holding exactly what it
// held before.
//
// The three planes answer this differently and the difference is the point.
// The discovery plane replays from a cache of the accepted bytes; the state
// plane replays from its own index; the availability plane's gate has to be
// cleared first, because it still believes every device is online and would
// suppress the republish. A consumer that calls two of the three leaves the
// third's whole tree empty — and since the broker holds nothing there, Home
// Assistant is not merely stale, it is waiting on a topic that will next be
// written whenever the device happens to change.
func TestConformanceReconnectRestoresEveryPlane(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	before := f.broker.retainedTopics()
	if len(before) == 0 {
		t.Fatal("the boot retained nothing")
	}

	f.broker.drop()
	if got := f.broker.retainedTopics(); len(got) != 0 {
		t.Fatalf("the drop left %v", got)
	}

	// The reconnect path, in the order the package documents.
	if err := f.run.AnnounceOnline(ctx); err != nil {
		t.Fatalf("re-announce: %v", err)
	}
	if _, err := f.run.Republish(ctx); err != nil {
		t.Fatalf("republish discovery: %v", err)
	}
	f.avail.Reset()
	if _, err := f.avail.Device(ctx, DeviceSlot(f.dev, f.entities[0]), true); err != nil {
		t.Fatalf("re-flip availability: %v", err)
	}
	if _, err := f.state.Republish(ctx); err != nil {
		t.Fatalf("republish state: %v", err)
	}

	after := f.broker.retainedTopics()
	if !slices.Equal(before, after) {
		t.Errorf("the reconnect restored a different tree\nbefore: %v\nafter:  %v", before, after)
	}
	// And the gate is honest again: the availability plane re-announcing
	// without Reset is the measured failure, so assert the reset was what
	// made the flip happen rather than the topic being new.
	if !f.broker.holds(f.layout.Availability(DeviceSlot(f.dev, f.entities[0]))) {
		t.Error("the device availability topic is empty after the reconnect")
	}
}

// TestConformanceAvailabilityGateWithoutResetLosesTheWholeFleet is the
// negative of the test above, pinned because it is the failure a consumer
// will actually write.
//
// Skipping the [AvailabilityPublisher.Reset] leaves the gate believing every
// device is online, so the re-flip publishes nothing and every entity of the
// fleet sits unavailable until the daemon restarts. Asserting the broken
// outcome is what documents that the Reset is load-bearing rather than
// defensive.
func TestConformanceAvailabilityGateWithoutResetLosesTheWholeFleet(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	devTopic := f.layout.Availability(DeviceSlot(f.dev, f.entities[0]))
	f.broker.drop()

	sent, err := f.avail.Device(ctx, DeviceSlot(f.dev, f.entities[0]), true)
	if err != nil {
		t.Fatalf("re-flip: %v", err)
	}
	if sent {
		t.Fatal("the gate published without a Reset; this test no longer describes the behaviour it documents")
	}
	if f.broker.holds(devTopic) {
		t.Fatal("the broker holds a topic the gate did not write")
	}

	f.avail.Reset()
	if sent, err = f.avail.Device(ctx, DeviceSlot(f.dev, f.entities[0]), true); err != nil || !sent {
		t.Fatalf("after Reset: sent=%v err=%v", sent, err)
	}
	if !f.broker.holds(devTopic) {
		t.Error("the Reset did not restore the availability topic")
	}
}

// TestConformanceBirthReplaysDiscoveryAndNotState pins what Home Assistant's
// birth message is and is not a trigger for.
//
// The birth says Home Assistant restarted. Its retained configs survive that,
// but it does not reliably re-read them across every addon reload, so the
// discovery plane replays. The broker's retained state survived too — Home
// Assistant re-subscribes and gets it — so a state replay on the same trigger
// is a fleet-wide write that changes nothing. The two counterparts exist for
// different events and wiring them together is the mistake this pins against.
func TestConformanceBirthReplaysDiscoveryAndNotState(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	replayed := make(chan int, 4)
	f.run.cfg.OnResync = func(n int, err error) {
		if err != nil {
			t.Errorf("resync: %v", err)
		}
		replayed <- n
	}
	if err := f.run.WatchBirth(ctx); err != nil {
		t.Fatalf("watch birth: %v", err)
	}

	stateWritesBefore := len(f.state.Published())
	if err := f.broker.Publish(ctx, BirthTopic(conformPrefix), []byte(BirthPayload), 1, true); err != nil {
		t.Fatalf("birth: %v", err)
	}
	f.run.Close()

	select {
	case n := <-replayed:
		if n != len(f.run.Declared()) {
			t.Errorf("replayed %d of %d declared configs", n, len(f.run.Declared()))
		}
	default:
		t.Fatal("the birth message triggered no discovery replay")
	}
	if got := len(f.state.Published()); got != stateWritesBefore {
		t.Errorf("the birth replay touched the state index (%d -> %d)", stateWritesBefore, got)
	}
	// The document is still the one the broker held, byte for byte: a replay
	// that re-rendered would be a second source of truth.
	if !f.broker.holds(f.configTopic()) {
		t.Error("the device document is gone after the replay")
	}
}

// TestConformanceRetractionLeavesNoGhost pins the removal path across all
// three planes.
//
// Retracting the discovery config is what removes the entity, and it is not
// enough. A retained `online` that outlives the device it describes is read
// by Home Assistant on every restart and the entity behind it stays
// permanently available, showing the last value it ever saw; a retained state
// value behind a retracted config is the same ghost with a number on it. The
// three trees are separate and the broker keeps handing out whatever is left
// in each.
//
// The order is asserted too: the config goes first. Clearing availability
// first only makes the entity unavailable for the moment in between, which an
// operator watching a restart reads as a fault.
func TestConformanceRetractionLeavesNoGhost(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	cfgTopic := f.configTopic()
	devAvail := f.layout.Availability(DeviceSlot(f.dev, f.entities[0]))
	stateTopics := f.state.Published()

	mark := len(f.broker.publishOrder())

	if err := f.run.Retract(ctx, cfgTopic); err != nil {
		t.Fatalf("retract config: %v", err)
	}
	if err := f.avail.RetractDevice(ctx, DeviceSlot(f.dev, f.entities[0])); err != nil {
		t.Fatalf("retract availability: %v", err)
	}
	if _, err := f.state.EvictPrefix(ctx, conformRoot+"/"+f.dev.UID()); err != nil {
		t.Fatalf("evict state: %v", err)
	}

	for _, leftover := range append([]string{cfgTopic, devAvail}, stateTopics...) {
		if f.broker.holds(leftover) {
			t.Errorf("%q is still retained after the removal: a ghost Home Assistant will read on its next restart",
				leftover)
		}
	}

	// The bridge status topic stays: it describes this process, not the
	// device, and clearing it would tell Home Assistant the daemon died.
	if !f.broker.holds(f.layout.Bridge()) {
		t.Error("the removal cleared the bridge status topic")
	}

	order := f.broker.publishOrder()[mark:]
	cfgAt, availAt := -1, -1
	for i, op := range order {
		switch op.topic {
		case cfgTopic:
			cfgAt = i
		case devAvail:
			availAt = i
		}
	}
	if cfgAt < 0 || availAt < 0 {
		t.Fatalf("the removal did not clear both trees: config at %d, availability at %d", cfgAt, availAt)
	}
	if cfgAt > availAt {
		t.Errorf("availability was cleared before the config (%d before %d): the entity is briefly "+
			"unavailable rather than gone, which reads as a fault", availAt, cfgAt)
	}
}

// TestConformanceSweepCanClearAnOrphansAvailabilityTopic closes a gap the two
// planes only had together.
//
// [Runtime.Sweep] is the one thing in this layer that can find a leftover no
// running process remembers: it reads the broker. The availability plane
// deliberately cannot — its tree's shape is the consumer's secret, so a
// wildcard pass there would judge another writer's topics — and it therefore
// only ever clears what this process itself wrote.
//
// Between those two correct decisions sat a ghost. On the boot after a device
// was removed while the daemon was down, the sweep found the orphan config
// and retracted it, the availability plane's memory was empty, and the
// retained `online` beside the config survived — so Home Assistant kept a
// device that no longer exists permanently available, showing its last value.
// The sweep was also the LAST moment it could be found: the config body names
// the availability topic, and the sweep used to discard the payload it read.
//
// [SweepRequest.Inspect] is what closes it. This test drives the composition
// the doc comment prescribes: collect during the pass, retract after it
// returns — which is also the order a removal needs, since the config
// retraction is what removes the entity and clearing availability first only
// greys it out in between.
func TestConformanceSweepCanClearAnOrphansAvailabilityTopic(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()

	// A previous build's device: its config and its availability marker,
	// retained on the broker, remembered by nothing.
	orphanNode := "serial_gone-1"
	orphanCfg := BundleConfigTopic(conformPrefix, orphanNode)
	orphanAvail := conformRoot + "/" + orphanNode + "/availability"
	f.broker.seed(orphanCfg, []byte(`{"device":{"identifiers":["serial:GONE-1"]},`+
		`"origin":{"name":"go-conform2mqtt"},"components":{"power":{"platform":"sensor",`+
		`"unique_id":"conform_gone_1_power","state_topic":"bridge/serial:GONE-1/values/power",`+
		`"availability":[{"topic":"`+orphanAvail+`"}]}}}`))
	f.broker.seed(orphanAvail, []byte(discovery.PayloadOnline))

	f.boot(ctx)

	// What the consumer collects during the pass. Keyed by config topic, so
	// only the configs the sweep actually retracts contribute.
	bodies := map[string][]byte{}
	res, err := f.run.Sweep(ctx, SweepRequest{
		Owns:   func(ct ConfigTopic) bool { return strings.HasPrefix(ct.NodeID, "serial_") },
		Window: time.Millisecond,
		Inspect: func(ct ConfigTopic, body []byte) {
			bodies[BundleConfigTopic(conformPrefix, ct.NodeID)] = append([]byte(nil), body...)
		},
	})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if !slices.Contains(res.Retracted, orphanCfg) {
		t.Fatalf("the sweep did not retract the orphan config: %+v", res)
	}
	if f.broker.holds(orphanCfg) {
		t.Fatal("the orphan config is still retained")
	}
	if len(bodies[orphanCfg]) == 0 {
		t.Fatal("Inspect did not hand over the orphan's body, so nothing can name its other topics")
	}

	// The body is what still names the entity's availability topic. Retract
	// after the pass, never inside it: Inspect runs on the read loop.
	for _, topic := range res.Retracted {
		for _, at := range availabilityTopicsOfBody(t, bodies[topic]) {
			if err := f.avail.Retract(ctx, at); err != nil {
				t.Fatalf("retract %s: %v", at, err)
			}
		}
	}
	if f.broker.holds(orphanAvail) {
		t.Fatal("the orphan availability topic is still retained: the ghost device survives")
	}

	// The availability plane still judges only its own memory — the ghost is
	// cleared because the sweep named it, not because the plane guessed.
	if _, known := f.avail.Online(orphanAvail); known {
		t.Fatal("the availability plane knows a topic it never wrote")
	}
}

// availabilityTopicsOfBody reads the availability topics out of a retained
// device bundle, which is the only place they survive once the process that
// published them is gone.
func availabilityTopicsOfBody(t *testing.T, body []byte) []string {
	t.Helper()
	if len(body) == 0 {
		return nil
	}
	var doc struct {
		Components map[string]struct {
			Availability []struct {
				Topic string `json:"topic"`
			} `json:"availability"`
		} `json:"components"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("read swept body: %v", err)
	}
	var out []string
	for _, c := range doc.Components {
		for _, a := range c.Availability {
			if a.Topic != "" && !slices.Contains(out, a.Topic) {
				out = append(out, a.Topic)
			}
		}
	}
	return out
}

// TestConformanceSweepSparesEveryLiveTopic pins that a sweep run at the
// documented moment — after the boot snapshot — touches nothing this process
// published, in any tree.
//
// The ordering rule has a measured cost behind it: a sweep that ran before a
// plane had published judged that plane's whole fleet to be orphans and
// deleted it, once per boot, with nothing left to re-declare it. The
// complementary risk is the wildcard itself: `<prefix>/#` sees the birth
// topic and, for a consumer that made the measured mistake of putting its own
// status inside the discovery tree, would see that too.
func TestConformanceSweepSparesEveryLiveTopic(t *testing.T) {
	f := newFleet(t)
	ctx := context.Background()
	f.boot(ctx)

	before := f.broker.retainedTopics()
	res, err := f.run.Sweep(ctx, SweepRequest{
		Owns:   func(ct ConfigTopic) bool { return strings.HasPrefix(ct.NodeID, "serial_") },
		Window: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(res.Retracted) != 0 {
		t.Errorf("the sweep retracted live topics: %v", res.Retracted)
	}
	if res.Inspected == 0 {
		t.Error("the sweep inspected nothing: the window saw none of this process's configs")
	}
	if got := f.broker.retainedTopics(); !slices.Equal(before, got) {
		t.Errorf("the sweep changed the tree\nbefore: %v\nafter:  %v", before, got)
	}
}
