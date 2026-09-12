// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// availWrite is one publish this fixture saw, in order. Order matters here
// for the same reason it does for the discovery loop: the gate's whole
// observable behaviour is which writes did NOT happen.
type availWrite struct {
	topic   string
	payload string
	qos     byte
	retain  bool
}

// availBroker is a transport that remembers every write. Named apart from
// the discovery fixture in this package on purpose — the two exercise
// different trees and must not grow a shared knob.
type availBroker struct {
	mu     sync.Mutex
	writes []availWrite
	// fail decides an error per topic, standing in for a breaker open on
	// one device.
	fail func(topic string) error
	// failSubscribe and failUnsubscribe do the same for the two
	// subscription calls, which used to return nil unconditionally.
	//
	// Nothing in the availability plane subscribes today, so these are the
	// knobs a consumer's composition test needs rather than this file's —
	// but a fixture whose Subscribe and Unsubscribe cannot fail silently
	// makes every tear-down branch reached through it unreachable, and the
	// three fixtures in this package all had that shape at once.
	failSubscribe   error
	failUnsubscribe error
}

func (b *availBroker) Publish(_ context.Context, t string, payload []byte, qos byte, retain bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail != nil {
		if err := b.fail(t); err != nil {
			return err
		}
	}
	b.writes = append(b.writes, availWrite{topic: t, payload: string(payload), qos: qos, retain: retain})
	return nil
}

func (b *availBroker) Subscribe(context.Context, string, byte, Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failSubscribe
}

func (b *availBroker) Unsubscribe(context.Context, string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failUnsubscribe
}

func (b *availBroker) all() []availWrite {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]availWrite(nil), b.writes...)
}

func (b *availBroker) payloads(t string) []string {
	var out []string
	for _, w := range b.all() {
		if w.topic == t {
			out = append(out, w.payload)
		}
	}
	return out
}

const availRoot = "loom"

func availLayout() topic.Layout { return topic.Default{Root: availRoot} }

func availDevice() *model.Device {
	return &model.Device{
		Identity: model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "VEQ0001"}}},
		Name:     model.L("Thermostat"),
	}
}

// availEntity is a sensor in a container hierarchy, which is the shape that
// makes the device availability topic more than a device address.
func availEntity(addr string, withAvailabilityBinding bool) *model.Basic {
	e := &model.Basic{
		EntityKey:      "temperature",
		EntityPlatform: hacatalog.PlatformSensor,
		Description: model.Description{
			Name: model.L("Temperature"),
		},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S(addr, "1", model.BucketValues, "TEMPERATURE").In("ccu-01", "HmIP-RF"),
			Mode: model.Read,
		}},
	}
	if withAvailabilityBinding {
		e.Binds = append(e.Binds, model.Binding{
			Role: model.RoleAvailability,
			Slot: model.S(addr, "1", model.BucketValues, "UNREACH").In("ccu-01", "HmIP-RF"),
			Mode: model.Read,
		})
	}
	return e
}

func newAvail(t *testing.T, b *availBroker) *AvailabilityPublisher {
	t.Helper()
	return NewAvailability(b, AvailabilityConfig{Layout: availLayout()})
}

// TestDeviceTopicIsTheTopicTheConfigDeclares is the conformance the whole
// file exists for: the topic the runtime writes and the topic the discovery
// config names have to be the same string, or the entity waits forever on a
// source nothing publishes.
func TestDeviceTopicIsTheTopicTheConfigDeclares(t *testing.T) {
	t.Parallel()

	dev := availDevice()
	e := availEntity(dev.UID(), false)
	ctx := discovery.StdContext{Layout: availLayout(), Namespace: availRoot}

	entries := ctx.Availability(dev, e)
	if len(entries) != 2 {
		t.Fatalf("want bridge + device entries, got %+v", entries)
	}
	declared := entries[1].Topic

	a := newAvail(t, &availBroker{})
	got, err := a.DeviceTopic(DeviceSlot(dev, e))
	if err != nil {
		t.Fatalf("DeviceTopic: %v", err)
	}
	if got != declared {
		t.Errorf("published topic %q, config declares %q", got, declared)
	}
	if !strings.Contains(got, "ccu-01/HmIP-RF") {
		t.Errorf("topic %q lost the containers the entity binds in", got)
	}
}

// TestDevicePublishesRetainedTokens checks the three things Home Assistant
// actually reads: the payload word, the retain flag and the QoS the measured
// consumer pins for this topic.
func TestDevicePublishesRetainedTokens(t *testing.T) {
	t.Parallel()

	b := &availBroker{}
	a := newAvail(t, b)
	dev := availDevice()
	slot := DeviceSlot(dev, availEntity(dev.UID(), false))

	changed, err := a.Device(context.Background(), slot, true)
	if err != nil || !changed {
		t.Fatalf("Device(online) = %v, %v", changed, err)
	}
	writes := b.all()
	if len(writes) != 1 {
		t.Fatalf("want one write, got %+v", writes)
	}
	if writes[0].payload != discovery.PayloadOnline {
		t.Errorf("payload = %q, want %q", writes[0].payload, discovery.PayloadOnline)
	}
	if !writes[0].retain {
		t.Error("availability was published unretained; a later subscriber learns nothing")
	}
	if writes[0].qos != 1 {
		t.Errorf("qos = %d, want the pinned 1", writes[0].qos)
	}
}

// TestTheGateFlipsOnlyOnTransitions is the measured defect: availability is
// re-evaluated per inbound value, so an ungated publisher writes `online`
// once per event.
func TestTheGateFlipsOnlyOnTransitions(t *testing.T) {
	t.Parallel()

	b := &availBroker{}
	a := newAvail(t, b)
	dev := availDevice()
	slot := DeviceSlot(dev, availEntity(dev.UID(), false))
	ctx := context.Background()

	for range 5 {
		if _, err := a.Device(ctx, slot, true); err != nil {
			t.Fatalf("Device: %v", err)
		}
	}
	changed, err := a.Device(ctx, slot, false)
	if err != nil || !changed {
		t.Fatalf("the offline flip was suppressed: %v, %v", changed, err)
	}
	if _, err := a.Device(ctx, slot, false); err != nil {
		t.Fatalf("Device: %v", err)
	}

	topicName, _ := a.DeviceTopic(slot)
	got := b.payloads(topicName)
	want := []string{discovery.PayloadOnline, discovery.PayloadOffline}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("wire carried %v, want exactly the transitions %v", got, want)
	}
	if online, known := a.Online(topicName); !known || online {
		t.Errorf("Online() = %v, %v; want a known offline", online, known)
	}
}

// TestAFailedFlipIsNotRemembered: recording what was merely attempted loses
// the flip for good, because the retry carries the same payload and the gate
// calls it a no-op.
func TestAFailedFlipIsNotRemembered(t *testing.T) {
	t.Parallel()

	boom := errors.New("breaker open")
	var open bool
	b := &availBroker{fail: func(string) error {
		if open {
			return boom
		}
		return nil
	}}
	open = true
	a := newAvail(t, b)
	dev := availDevice()
	slot := DeviceSlot(dev, availEntity(dev.UID(), false))

	if _, err := a.Device(context.Background(), slot, true); !errors.Is(err, boom) {
		t.Fatalf("want the transport error, got %v", err)
	}
	if _, known := a.Online(mustTopic(t, a, slot)); known {
		t.Error("the gate remembered a publish the broker refused")
	}

	open = false
	changed, err := a.Device(context.Background(), slot, true)
	if err != nil || !changed {
		t.Fatalf("the retry was suppressed: %v, %v", changed, err)
	}
}

func mustTopic(t *testing.T, a *AvailabilityPublisher, s model.Slot) string {
	t.Helper()
	out, err := a.DeviceTopic(s)
	if err != nil {
		t.Fatalf("DeviceTopic: %v", err)
	}
	return out
}

// TestSelfPayloadMatchesWhatTheConfigDeclares walks the three measured
// LevelSelf defects from the publishing side: the envelope key the template
// reads, the bare payload under raw encoding, and the true/false tokens.
func TestSelfPayloadMatchesWhatTheConfigDeclares(t *testing.T) {
	t.Parallel()

	dev := availDevice()
	e := availEntity(dev.UID(), true)
	e.Description.Availability = model.Availability{Levels: []model.AvailabilityLevel{model.LevelSelf}}

	for _, tc := range []struct {
		name string
		enc  discovery.Encoding
		want string
	}{
		{"envelope", discovery.EnvelopeEncoding, `{"value":true,"available":true}`},
		{"raw", discovery.RawEncoding, "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := discovery.StdContext{Layout: availLayout(), Namespace: availRoot, Enc: tc.enc}
			entries := ctx.Availability(dev, e)
			if len(entries) != 1 {
				t.Fatalf("want one self entry, got %+v", entries)
			}
			entry := entries[0]

			b := &availBroker{}
			a := NewAvailability(b, AvailabilityConfig{Layout: availLayout()})
			changed, err := a.Self(context.Background(), e, tc.enc, true)
			if err != nil || !changed {
				t.Fatalf("Self = %v, %v", changed, err)
			}
			writes := b.all()
			if len(writes) != 1 {
				t.Fatalf("want one write, got %+v", writes)
			}
			if writes[0].topic != entry.Topic {
				t.Errorf("published to %q, config declares %q", writes[0].topic, entry.Topic)
			}
			if writes[0].payload != tc.want {
				t.Errorf("payload = %q, want %q", writes[0].payload, tc.want)
			}
			if writes[0].payload != entry.PayloadAvailable && tc.enc == discovery.RawEncoding {
				t.Errorf("payload %q does not match payload_available %q",
					writes[0].payload, entry.PayloadAvailable)
			}
			if entry.PayloadAvailable != "true" || entry.PayloadNotAvailable != "false" {
				t.Fatalf("the declaring side changed its tokens: %+v", entry)
			}
			if bytes.Equal(SelfAvailabilityPayload(tc.enc, false), SelfAvailabilityPayload(tc.enc, true)) {
				t.Error("the two self payloads are indistinguishable")
			}
		})
	}
}

// TestSelfWithoutABindingIsNotAnError: the fallback shape reads the state
// envelope's own flag, which the state plane writes — there is nothing here
// to publish and nothing wrong with that.
func TestSelfWithoutABindingIsNotAnError(t *testing.T) {
	t.Parallel()

	b := &availBroker{}
	a := newAvail(t, b)
	dev := availDevice()

	changed, err := a.Self(context.Background(), availEntity(dev.UID(), false), discovery.EnvelopeEncoding, false)
	if err != nil || changed {
		t.Fatalf("Self = %v, %v; want a silent skip", changed, err)
	}
	if len(b.all()) != 0 {
		t.Errorf("wrote %+v for an entity with no availability binding", b.all())
	}
}

// TestRawSelfPayloadIsNotAnEnvelope states the defect the other way round:
// an envelope under raw encoding renders value_json undefined against a
// config that carries no template, and Home Assistant silently ignores it.
func TestRawSelfPayloadIsNotAnEnvelope(t *testing.T) {
	t.Parallel()

	got := string(SelfAvailabilityPayload(discovery.RawEncoding, false))
	if strings.Contains(got, "{") {
		t.Errorf("raw self payload = %q, want a bare boolean", got)
	}
	if got != "false" {
		t.Errorf("raw self payload = %q, want %q", got, "false")
	}
}

// TestResetMakesTheNextFlipUnconditional is the reconnect contract: a broker
// that came back without its retained store holds nothing, while the gate
// still believes every device is online.
//
// It also pins the half that used to be wrong. The reset opens the gate and
// keeps the index, so the two calls the package documents for a reconnect
// compose in either order: Reset followed by Republish re-sends the fleet
// instead of finding an empty worklist.
func TestResetMakesTheNextFlipUnconditional(t *testing.T) {
	t.Parallel()

	b := &availBroker{}
	a := newAvail(t, b)
	dev := availDevice()
	slot := DeviceSlot(dev, availEntity(dev.UID(), false))
	ctx := context.Background()

	if _, err := a.Device(ctx, slot, true); err != nil {
		t.Fatalf("Device: %v", err)
	}
	if changed, _ := a.Device(ctx, slot, true); changed {
		t.Fatal("the gate did not hold before the reset")
	}
	a.Reset()
	topicName := mustTopic(t, a, slot)
	if got := a.Topics(); len(got) != 1 || got[0] != topicName {
		t.Errorf("Reset dropped the index: Topics() = %v", got)
	}
	if online, known := a.Online(topicName); !known || !online {
		t.Errorf("Reset dropped the last value: Online() = %v, %v", online, known)
	}
	if sent, err := a.Republish(ctx); err != nil || sent != 1 {
		t.Errorf("Reset then Republish sent %d, %v; want the fleet re-sent", sent, err)
	}
	changed, err := a.Device(ctx, slot, true)
	if err != nil || !changed {
		t.Fatalf("the post-reconnect flip was suppressed: %v, %v", changed, err)
	}
	// And the gate closes again behind that one write.
	if changed, err = a.Device(ctx, slot, true); err != nil || changed {
		t.Errorf("the gate stayed open after the reset's one write: %v, %v", changed, err)
	}
}

// TestRepublishResendsEveryRememberedTopic covers the consumer with no
// device snapshot to re-walk on reconnect.
func TestRepublishResendsEveryRememberedTopic(t *testing.T) {
	t.Parallel()

	b := &availBroker{}
	a := newAvail(t, b)
	ctx := context.Background()

	for _, name := range []string{"loom/a/availability", "loom/b/availability"} {
		if _, err := a.Publish(ctx, name, true); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	n, err := a.Republish(ctx)
	if err != nil || n != 2 {
		t.Fatalf("Republish = %d, %v", n, err)
	}
	if got := len(b.payloads("loom/a/availability")); got != 2 {
		t.Errorf("topic a written %d times, want the initial flip plus the replay", got)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := a.Republish(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled replay returned %v", err)
	}
}

// TestRepublishSurvivesOneRefusedTopic: a breaker open for one device must
// not strand the fleet behind it.
func TestRepublishSurvivesOneRefusedTopic(t *testing.T) {
	t.Parallel()

	boom := errors.New("refused")
	b := &availBroker{}
	a := newAvail(t, b)
	ctx := context.Background()
	for _, name := range []string{"loom/a/availability", "loom/b/availability"} {
		if _, err := a.Publish(ctx, name, true); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	b.mu.Lock()
	b.fail = func(topicName string) error {
		if topicName == "loom/a/availability" {
			return boom
		}
		return nil
	}
	b.mu.Unlock()

	n, err := a.Republish(ctx)
	if n != 1 {
		t.Errorf("sent %d, want the one topic the broker accepted", n)
	}
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want the refusal joined in", err)
	}
}

// TestRetractClearsTheGhost is the removal contract: an orphaned retained
// `online` leaves a permanently available device in Home Assistant.
func TestRetractClearsTheGhost(t *testing.T) {
	t.Parallel()

	b := &availBroker{}
	a := newAvail(t, b)
	dev := availDevice()
	slot := DeviceSlot(dev, availEntity(dev.UID(), false))
	ctx := context.Background()

	if _, err := a.Device(ctx, slot, true); err != nil {
		t.Fatalf("Device: %v", err)
	}
	if err := a.RetractDevice(ctx, slot); err != nil {
		t.Fatalf("RetractDevice: %v", err)
	}
	writes := b.all()
	last := writes[len(writes)-1]
	if last.payload != "" || !last.retain {
		t.Errorf("retraction = %+v, want an empty retained payload", last)
	}
	if len(a.Topics()) != 0 {
		t.Errorf("the gate still remembers %v after the retraction", a.Topics())
	}

	// A device readopted under the same address must be able to announce
	// itself again — the measured defect behind forgetAvailability.
	changed, err := a.Device(ctx, slot, true)
	if err != nil || !changed {
		t.Fatalf("the readopted device could not announce: %v, %v", changed, err)
	}
}

// TestForgetDropsTheMemoryWithoutWriting covers the case where something
// else cleared the retained topic.
func TestForgetDropsTheMemoryWithoutWriting(t *testing.T) {
	t.Parallel()

	b := &availBroker{}
	a := newAvail(t, b)
	ctx := context.Background()
	const name = "loom/x/availability"

	if _, err := a.Publish(ctx, name, true); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	a.Forget(name)
	if _, known := a.Online(name); known {
		t.Error("Forget left the gate remembering the topic")
	}
	if len(b.all()) != 1 {
		t.Errorf("Forget wrote to the broker: %+v", b.all())
	}
	changed, err := a.Publish(ctx, name, true)
	if err != nil || !changed {
		t.Fatalf("the re-announce after Forget was suppressed: %v, %v", changed, err)
	}
}

// TestRetractIsBestEffortAndStopsOnCancellation mirrors the discovery loop's
// retraction contract.
func TestRetractIsBestEffortAndStopsOnCancellation(t *testing.T) {
	t.Parallel()

	boom := errors.New("refused")
	b := &availBroker{fail: func(topicName string) error {
		if topicName == "loom/a/availability" {
			return boom
		}
		return nil
	}}
	a := newAvail(t, b)

	err := a.Retract(context.Background(), "loom/a/availability", "loom/b/availability")
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want the refusal", err)
	}
	if len(b.all()) != 1 {
		t.Errorf("want the second topic still cleared, got %+v", b.all())
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Retract(cancelled, "loom/c/availability"); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled retraction returned %v", err)
	}
}

// TestSweepNeedsAPredicate: sweeping with no notion of "live" clears every
// device the process ever announced.
func TestSweepNeedsAPredicate(t *testing.T) {
	t.Parallel()

	a := newAvail(t, &availBroker{})
	if _, err := a.Sweep(context.Background(), nil); !errors.Is(err, ErrSweepUnscoped) {
		t.Errorf("err = %v, want ErrSweepUnscoped", err)
	}
}

// TestSweepRetractsOnlyWhatIsNoLongerLive: the pass judges only what this
// process wrote, and clears exactly the topics the consumer no longer drives.
func TestSweepRetractsOnlyWhatIsNoLongerLive(t *testing.T) {
	t.Parallel()

	b := &availBroker{}
	a := newAvail(t, b)
	ctx := context.Background()
	live := "loom/a/availability"
	gone := "loom/b/availability"
	for _, name := range []string{live, gone} {
		if _, err := a.Publish(ctx, name, true); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	cleared, err := a.Sweep(ctx, func(topicName string) bool { return topicName == live })
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(cleared) != 1 || cleared[0] != gone {
		t.Fatalf("cleared %v, want just %q", cleared, gone)
	}
	if got := a.Topics(); len(got) != 1 || got[0] != live {
		t.Errorf("gate holds %v, want just the live topic", got)
	}

	// A second pass with nothing stale writes nothing at all.
	before := len(b.all())
	cleared, err = a.Sweep(ctx, func(string) bool { return true })
	if err != nil || cleared != nil {
		t.Fatalf("second pass = %v, %v", cleared, err)
	}
	if len(b.all()) != before {
		t.Error("an idle sweep wrote to the broker")
	}
}

// TestSweepReportsWhatItActuallyCleared: a caller that logged the attempt
// list would name topics still standing on the broker.
func TestSweepReportsWhatItActuallyCleared(t *testing.T) {
	t.Parallel()

	boom := errors.New("refused")
	b := &availBroker{}
	a := newAvail(t, b)
	ctx := context.Background()
	for _, name := range []string{"loom/a/availability", "loom/b/availability"} {
		if _, err := a.Publish(ctx, name, true); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	b.mu.Lock()
	b.fail = func(topicName string) error {
		if topicName == "loom/a/availability" {
			return boom
		}
		return nil
	}
	b.mu.Unlock()

	cleared, err := a.Sweep(ctx, func(string) bool { return false })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the refusal", err)
	}
	if len(cleared) != 1 || cleared[0] != "loom/b/availability" {
		t.Errorf("cleared %v, want only the topic the broker accepted", cleared)
	}
}

// TestNoLayoutRefusesRatherThanGuessing: an invented availability topic is
// an entity waiting forever on a source nothing publishes.
func TestNoLayoutRefusesRatherThanGuessing(t *testing.T) {
	t.Parallel()

	a := NewAvailability(&availBroker{}, AvailabilityConfig{})
	ctx := context.Background()
	if _, err := a.Device(ctx, model.Slot{Address: "x"}, true); !errors.Is(err, ErrNoAvailabilityLayout) {
		t.Errorf("Device err = %v", err)
	}
	if err := a.RetractDevice(ctx, model.Slot{Address: "x"}); !errors.Is(err, ErrNoAvailabilityLayout) {
		t.Errorf("RetractDevice err = %v", err)
	}
	dev := availDevice()
	if _, err := a.Self(ctx, availEntity(dev.UID(), true), discovery.RawEncoding, true); !errors.Is(err, ErrNoAvailabilityLayout) {
		t.Errorf("Self err = %v", err)
	}
	// The topic-shaped call needs no layout and must keep working.
	if _, err := a.Publish(ctx, "loom/x/availability", true); err != nil {
		t.Errorf("Publish err = %v", err)
	}
	dev.Via = &model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "INV0001"}}}
	if _, _, err := a.ParentTopic(dev, availEntity(dev.UID(), false)); !errors.Is(err, ErrNoAvailabilityLayout) {
		t.Errorf("ParentTopic err = %v", err)
	}
	if _, err := a.Publish(ctx, "", true); err == nil {
		t.Error("an empty topic was accepted")
	}
}

// TestNilTransportPanicsAtTheCompositionRoot, where the stack still names
// the wiring that got it wrong — not on the first flip hours later.
func TestNilTransportPanicsAtTheCompositionRoot(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Error("a nil transport did not panic")
		}
	}()
	NewAvailability(nil, AvailabilityConfig{})
}

// TestDeviceSlotOfANilEntity is the shape a consumer whose devices carry no
// entity yet still has to be able to address.
func TestDeviceSlotOfANilEntity(t *testing.T) {
	t.Parallel()

	dev := availDevice()
	if got := DeviceSlot(dev, nil); got.Address != dev.UID() || len(got.Scope) != 0 {
		t.Errorf("DeviceSlot(dev, nil) = %+v", got)
	}
	if got := DeviceSlot(nil, nil); got.Address != "" {
		t.Errorf("DeviceSlot(nil, nil) = %+v", got)
	}
}

// TestConcurrentFlipsUnderTheRaceDetector drives the gate the way a bus-wide
// outage does — several hundred devices flipping at once — and asserts what
// the flips did, not only that the race detector stayed quiet.
//
// The previous version of this test could not fail. Its one assertion was
// `len(Topics()) > 4` while its goroutines wrote exactly four distinct
// topics, so it was arithmetically unfalsifiable: with Forget made a no-op it
// still passed. It also had four goroutines share each topic, which violates
// the one-writer-per-topic contract [StatePublisher] and this type document,
// so nothing per-topic could be asserted at all.
//
// One goroutine per topic makes every per-topic outcome deterministic while
// keeping the concurrency real: sixteen writers, a concurrent Republish
// walking the shared map, and the gate, index and Forget all checked
// afterwards. It is also the test standing where the failed-publish path
// lives, so it asserts the index survives a refused write rather than
// stepping around it.
func TestConcurrentFlipsUnderTheRaceDetector(t *testing.T) {
	t.Parallel()

	const writers = 16
	const flips = 8
	// One topic in the set is refused by the broker throughout, which is the
	// correlated case: a breaker open for one device while the fleet flips.
	const refused = "loom/d3/availability"
	boom := errors.New("breaker open")

	b := &availBroker{fail: func(t string) error {
		if t == refused {
			return boom
		}
		return nil
	}}
	a := newAvail(t, b)
	ctx := context.Background()

	names := make([]string, writers)
	for i := range names {
		names[i] = "loom/d" + strconv.Itoa(i) + "/availability"
	}

	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := names[i]
			for j := range flips {
				// Alternating payloads, so every call is a transition and
				// the gate must let every one of them through.
				changed, err := a.Publish(ctx, name, j%2 == 0)
				switch {
				case name == refused:
					if !errors.Is(err, boom) {
						t.Errorf("%s: want the refusal, got %v", name, err)
						return
					}
				case err != nil:
					t.Errorf("%s: Publish: %v", name, err)
					return
				case !changed:
					t.Errorf("%s: flip %d was suppressed although the payload changed", name, j)
					return
				}
			}
			// Walks the same map the writers are mutating; its result is not
			// deterministic under concurrency, only its safety.
			_, _ = a.Republish(ctx)
		}()
	}
	wg.Wait()

	// The index holds every topic the broker accepted and nothing else. The
	// refused one is absent because its first write never landed — not
	// because a failure deleted it; that half is
	// TestAFailedFlipKeepsTheTopicInTheIndex.
	got := a.Topics()
	if len(got) != writers-1 {
		t.Fatalf("Topics() = %v, want the %d accepted topics", got, writers-1)
	}
	for _, name := range got {
		if name == refused {
			t.Fatalf("the refused topic %q entered the index", name)
		}
		if _, known := a.Online(name); !known {
			t.Errorf("%s is listed but has no remembered value", name)
		}
	}

	// Every accepted topic saw at least its own flips on the wire. The exact
	// sequence is not asserted here because the concurrent Republish writes
	// into it; the gate's ordering is pinned sequentially by
	// TestTheGateFlipsOnlyOnTransitions, and what this test adds is that
	// every one of the alternating flips above returned changed=true — a
	// gate that suppressed a real transition under concurrency fails in the
	// goroutine, not here.
	for _, name := range got {
		if n := len(b.payloads(name)); n < flips {
			t.Errorf("%s carried %d writes, want at least the %d flips", name, n, flips)
		}
	}

	// And Forget actually forgets: the assertion the old shape could not
	// make, and the one that caught a no-op Forget when the reviewer
	// mutated it.
	a.Forget(got...)
	if left := a.Topics(); len(left) != 0 {
		t.Errorf("Forget left %v in the index", left)
	}
	if _, known := a.Online(got[0]); known {
		t.Errorf("Forget left a remembered value for %s", got[0])
	}
}

// TestAFailedFlipKeepsTheTopicInTheIndex is the regression for the ghost the
// old failure path stranded.
//
// The gate's map is not only the gate: it is [AvailabilityPublisher.Topics],
// the worklist of [AvailabilityPublisher.Republish] and the ownership set of
// [AvailabilityPublisher.Sweep]. Deleting the entry when a publish failed
// dropped the topic out of all three at the one moment it matters — an
// `offline` refused by a breaker open during a broker outage — while the
// broker still retained the `online` that publish was trying to replace. The
// device then had no route back: nothing listed it, nothing re-sent it and
// nothing swept it.
func TestAFailedFlipKeepsTheTopicInTheIndex(t *testing.T) {
	t.Parallel()

	boom := errors.New("breaker open")
	var open bool
	b := &availBroker{fail: func(string) error {
		if open {
			return boom
		}
		return nil
	}}
	a := newAvail(t, b)
	ctx := context.Background()
	const name = "loom/ghost/availability"

	if changed, err := a.Publish(ctx, name, true); err != nil || !changed {
		t.Fatalf("the accepted online flip = %v, %v", changed, err)
	}

	open = true
	if _, err := a.Publish(ctx, name, false); !errors.Is(err, boom) {
		t.Fatalf("want the transport error, got %v", err)
	}

	if got := a.Topics(); len(got) != 1 || got[0] != name {
		t.Fatalf("Topics() = %v, want the topic the broker still retains", got)
	}
	if online, known := a.Online(name); !known || !online {
		t.Errorf("Online() = %v, %v; want the value the broker accepted", online, known)
	}

	// Declining to record is already what keeps the retry alive: the refused
	// payload still differs from the cached one.
	open = false
	if changed, err := a.Publish(ctx, name, false); err != nil || !changed {
		t.Fatalf("the retry was suppressed: %v, %v", changed, err)
	}

	if sent, err := a.Republish(ctx); err != nil || sent != 1 {
		t.Errorf("Republish() = %d, %v; want the topic back on the worklist", sent, err)
	}

	cleared, err := a.Sweep(ctx, func(string) bool { return false })
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(cleared) != 1 || cleared[0] != name {
		t.Fatalf("Sweep cleared %v, want the ghost", cleared)
	}
	last := b.all()[len(b.all())-1]
	if last.topic != name || last.payload != "" || !last.retain {
		t.Errorf("the sweep's last write = %+v, want a retained retraction", last)
	}
	if got := a.Topics(); len(got) != 0 {
		t.Errorf("the retraction left %v in the index", got)
	}
}

// TestAvailabilityHonoursTheCollisionGuard closes the third of the three
// answers this package used to give to one question.
//
// [AvailabilityPublisher.Self] writes to [topic.Layout.State] — a state-plane
// topic, which is precisely where a collision lives — while
// [StatePublisher.Publish] refused that same topic. A consumer wiring both
// planes against one command subscription got the write refused on one and
// accepted on the other.
func TestAvailabilityHonoursTheCollisionGuard(t *testing.T) {
	t.Parallel()

	dev := availDevice()
	e := availEntity(dev.UID(), true)
	bind, ok := model.Bind(e, model.RoleAvailability)
	if !ok {
		t.Fatal("the fixture entity lost its availability binding")
	}
	selfTopic := availLayout().State(bind.Slot)

	b := &availBroker{}
	a := NewAvailability(b, AvailabilityConfig{
		Layout:         availLayout(),
		CommandFilters: []string{selfTopic, "loom/+/availability"},
	})
	ctx := context.Background()

	if _, err := a.Self(ctx, e, discovery.EnvelopeEncoding, true); !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("Self err = %v, want ErrStateCommandCollision", err)
	}
	if _, err := a.Publish(ctx, "loom/x/availability", true); !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("Publish err = %v, want ErrStateCommandCollision", err)
	}
	if err := a.Retract(ctx, "loom/x/availability"); !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("Retract err = %v, want ErrStateCommandCollision", err)
	}
	if got := b.all(); len(got) != 0 {
		t.Errorf("a guarded write reached the wire: %+v", got)
	}

	// A disjoint topic is unaffected: the guard refuses what the consumer
	// subscribes to, not availability as such.
	if _, err := a.Publish(ctx, "loom/ccu-01/VEQ0001/availability", true); err != nil {
		t.Errorf("a disjoint availability topic must pass: %v", err)
	}

	// The index walk is guarded too, for the consumer that widened its
	// filters after the flip: a republish must not echo into its own
	// handler either.
	a.mu.Lock()
	a.filters = append(a.filters, "loom/ccu-01/#")
	a.mu.Unlock()
	if sent, err := a.Republish(ctx); sent != 0 || !errors.Is(err, ErrStateCommandCollision) {
		t.Errorf("Republish = %d, %v; want the collision reported", sent, err)
	}
}

// TestParentTopicIsTheTopicTheConfigDeclares is the [model.LevelParent] half
// of the seam [DeviceSlot] closed for [model.LevelDevice]: the declaring side
// derives the parent coordinate inline, and a publishing side that rebuilt it
// by hand addresses a different topic.
//
// The two things a hand-derivation gets wrong are both pinned here: the
// channel travels with the scope, and the scope comes from the child's
// bindings rather than the parent's own identity. Either mistake greys out
// every child of the sub-device forever.
func TestParentTopicIsTheTopicTheConfigDeclares(t *testing.T) {
	t.Parallel()

	parent := model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "INV0001"}}}
	dev := availDevice()
	dev.Via = &parent
	e := availEntity(dev.UID(), false)
	e.Description.Availability = model.Availability{
		Levels: []model.AvailabilityLevel{model.LevelParent},
	}

	dctx := discovery.StdContext{Layout: availLayout(), Namespace: availRoot}
	entries := dctx.Availability(dev, e)
	if len(entries) != 1 {
		t.Fatalf("want the single parent entry, got %+v", entries)
	}

	b := &availBroker{}
	a := newAvail(t, b)
	got, ok, err := a.ParentTopic(dev, e)
	if err != nil || !ok {
		t.Fatalf("ParentTopic = %q, %v, %v", got, ok, err)
	}
	if got != entries[0].Topic {
		t.Fatalf("ParentTopic = %q, config declares %q", got, entries[0].Topic)
	}
	if !strings.Contains(got, parent.UID()) {
		t.Errorf("the parent topic %q does not name the parent address", got)
	}
	// The containers travel with the entity, not with the parent — which is
	// the half a hand-derivation off dev.Via alone cannot see, because an
	// identity carries no scope.
	if !strings.Contains(got, "ccu-01/HmIP-RF") {
		t.Errorf("the parent topic %q lost the containers the entity binds in", got)
	}
	if s, ok := ParentSlot(dev, e); !ok || s.Channel != "1" {
		t.Errorf("ParentSlot = %+v, %v; want the channel kept", s, ok)
	}

	changed, err := a.Parent(context.Background(), dev, e, true)
	if err != nil || !changed {
		t.Fatalf("Parent = %v, %v", changed, err)
	}
	writes := b.all()
	if len(writes) != 1 || writes[0].topic != entries[0].Topic {
		t.Fatalf("Parent wrote %+v, want the declared topic", writes)
	}

	// A device with no parent is the normal case for a reused description,
	// and the declaring side skips the level rather than erroring.
	orphan := availDevice()
	if _, ok, err = a.ParentTopic(orphan, e); ok || err != nil {
		t.Errorf("ParentTopic(no parent) = %v, %v; want the level skipped", ok, err)
	}
	if changed, err = a.Parent(context.Background(), orphan, e, true); changed || err != nil {
		t.Errorf("Parent(no parent) = %v, %v; want the level skipped", changed, err)
	}
	if _, ok = ParentSlot(nil, nil); ok {
		t.Error("ParentSlot(nil, nil) claimed a parent")
	}
}

// TestBridgeIsTheStringEveryConfigReferences exposes the one availability
// string this module structurally could not cross-check: the declaring side
// writes [topic.Layout.Bridge], the publishing side writes
// [Config.StatusTopic], a free-form string, and [Runtime] holds no layout. A
// typo greys out the whole fleet under the default availability_mode.
func TestBridgeIsTheStringEveryConfigReferences(t *testing.T) {
	t.Parallel()

	dev := availDevice()
	e := availEntity(dev.UID(), false)
	dctx := discovery.StdContext{Layout: availLayout(), Namespace: availRoot}
	entries := dctx.Availability(dev, e)
	if len(entries) == 0 {
		t.Fatal("the fixture declares no availability")
	}

	a := newAvail(t, &availBroker{})
	got, err := a.Bridge()
	if err != nil {
		t.Fatalf("Bridge: %v", err)
	}
	if got != entries[0].Topic {
		t.Errorf("Bridge() = %q, the bridge-level entry names %q", got, entries[0].Topic)
	}

	// The assertion a consumer can now make about its own Config.StatusTopic.
	if got != availLayout().Bridge() {
		t.Errorf("Bridge() = %q, layout renders %q", got, availLayout().Bridge())
	}
	if _, err = NewAvailability(&availBroker{}, AvailabilityConfig{}).Bridge(); !errors.Is(err, ErrNoAvailabilityLayout) {
		t.Errorf("Bridge without a layout = %v", err)
	}
}
