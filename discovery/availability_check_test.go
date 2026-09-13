// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// bridgeOnlyPublisher is the consumer that publishes one availability topic
// and no per-device one — go-mtec2mqtt's shape, and go-daikin2mqtt's.
func bridgeOnlyPublisher(bridge string) func(string) bool {
	return func(t string) bool { return t == bridge }
}

// TestCheckAvailabilityRefusesTheDefaultOnABridgeOnlyConsumer is the measured
// accident: a consumer that publishes no per-device availability topic says
// nothing, takes the library's default, and every entity it has is
// permanently unavailable — 100 for one bridge, 264 for another, with nothing
// on the wire and nothing in any log.
//
// Driven through the real render path rather than a hand-built component, so
// it is the default itself that is being checked and not a literal.
func TestCheckAvailabilityRefusesTheDefaultOnABridgeOnlyConsumer(t *testing.T) {
	t.Parallel()
	layout := topic.Default{Root: "mtec"}
	ctx := StdContext{Layout: layout, Namespace: "mtec"}
	dev := &model.Device{Identity: model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "dev1"}}}, Name: model.L("Inverter")}

	// Says nothing about availability: the zero model.Availability, which
	// resolves to {LevelBridge, LevelDevice}.
	silent := availabilityEntity{key: "power", platform: "sensor"}
	comp, err := RenderComponent(ctx, dev, silent, Origin{Name: "x"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	err = CheckAvailability(bridgeOnlyPublisher(layout.Bridge()), comp)
	if !errors.Is(err, ErrAvailabilityUnpublished) {
		t.Fatalf("the default must be refused by a bridge-only consumer, got %v", err)
	}
	if want := layout.Availability(DeviceSlot(dev, silent)); !strings.Contains(err.Error(), want) {
		t.Fatalf("the error must name the topic nobody publishes (%q): %v", want, err)
	}

	// And the statement that fixes it — model.BridgeOnly — passes.
	stated := availabilityEntity{key: "power", platform: "sensor", avail: model.BridgeOnly()}
	comp, err = RenderComponent(ctx, dev, stated, Origin{Name: "x"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if err := CheckAvailability(bridgeOnlyPublisher(layout.Bridge()), comp); err != nil {
		t.Fatalf("BridgeOnly must pass: %v", err)
	}
}

// TestCheckAvailabilityAcceptsAConsumerThatPublishesBoth is the inverse
// consumer, and the reason the default cannot simply be narrowed: for a
// bridge that does publish a per-device availability topic, the default is
// the right answer.
func TestCheckAvailabilityAcceptsAConsumerThatPublishesBoth(t *testing.T) {
	t.Parallel()
	layout := topic.Default{Root: "homeconnect"}
	ctx := StdContext{Layout: layout, Namespace: "homeconnect"}
	dev := &model.Device{Identity: model.Identity{IDs: []model.Identifier{{Namespace: "haId", Value: "oven"}}}, Name: model.L("Oven")}
	comp, err := RenderComponent(ctx, dev, availabilityEntity{key: "door", platform: "binary_sensor"}, Origin{Name: "x"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	publishes := func(topic string) bool {
		return topic == layout.Bridge() || strings.HasSuffix(topic, "/availability")
	}
	if err := CheckAvailability(publishes, comp); err != nil {
		t.Fatalf("a consumer that publishes both levels must pass: %v", err)
	}
}

// TestCheckAvailabilityReadsBothSpellings pins that collapsing a one-entry
// list into Home Assistant's singular keys — what one consumer's builder does
// — does not slip past the check.
func TestCheckAvailabilityReadsBothSpellings(t *testing.T) {
	t.Parallel()
	comp := Component{
		Platform:          hacatalog.Platform("button"),
		UniqueID:          "daikin_restart",
		AvailabilityTopic: "daikin/device/availability",
	}
	err := CheckAvailability(bridgeOnlyPublisher("daikin/bridge/status"), comp)
	if !errors.Is(err, ErrAvailabilityUnpublished) {
		t.Fatalf("availability_topic must be checked too, got %v", err)
	}
	if !strings.Contains(err.Error(), "daikin_restart") {
		t.Fatalf("the error must name the entity: %v", err)
	}

	// A component with no identity at all still has to be nameable, or the
	// error is a topic with no owner.
	err = CheckAvailability(bridgeOnlyPublisher("daikin/bridge/status"),
		Component{AvailabilityTopic: "elsewhere"})
	if err == nil || !strings.Contains(err.Error(), "component") {
		t.Fatalf("an anonymous component must still be named: %v", err)
	}
}

// TestCheckBundleAvailabilityNamesTheComponentKey pins the bundle form, which
// can report the key an operator sees in the document rather than a unique id.
func TestCheckBundleAvailabilityNamesTheComponentKey(t *testing.T) {
	t.Parallel()
	b := &Bundle{
		NodeID: "n1",
		Components: map[string]Component{
			"temperature": {
				Platform:     hacatalog.Platform("sensor"),
				UniqueID:     "u1",
				Availability: []AvailabilityEntry{{Topic: "root/bridge/status"}, {Topic: "root/dev1/availability"}},
			},
		},
	}
	e := CheckBundleAvailability(b, bridgeOnlyPublisher("root/bridge/status"))
	if !errors.Is(e, ErrAvailabilityUnpublished) {
		t.Fatalf("want a refusal, got %v", e)
	}
	if !strings.Contains(e.Error(), "temperature") || !strings.Contains(e.Error(), "root/dev1/availability") {
		t.Fatalf("the error must name the component key and the topic: %v", e)
	}
	if e := CheckBundleAvailability(b, func(string) bool { return true }); e != nil {
		t.Fatalf("a consumer that publishes both must pass: %v", e)
	}
	if e := CheckBundleAvailability(b, nil); e == nil {
		t.Fatal("a nil predicate asserts nothing and must be refused")
	}
	if e := CheckAvailability(nil, b.Components["temperature"]); e == nil {
		t.Fatal("a nil predicate asserts nothing and must be refused")
	}
	if got := BundleAvailabilityTopics(b); !reflect.DeepEqual(got, []string{"root/bridge/status", "root/dev1/availability"}) {
		t.Fatalf("topics %v", got)
	}
	if got := BundleAvailabilityTopics(nil); got != nil {
		t.Fatalf("topics of no bundle: %v", got)
	}
}

// TestAvailabilityTopicsIsTheAssertionAFleetCanBePinnedOn pins the shape of
// the measured assertion: "distinct availability values across all 200
// payloads | exactly one".
func TestAvailabilityTopicsIsTheAssertionAFleetCanBePinnedOn(t *testing.T) {
	t.Parallel()
	comps := []Component{
		{Availability: []AvailabilityEntry{{Topic: "mtec/bridge/status"}}},
		{Availability: []AvailabilityEntry{{Topic: "mtec/bridge/status"}}},
		{AvailabilityTopic: "mtec/bridge/status"},
	}
	if got := AvailabilityTopics(comps...); !reflect.DeepEqual(got, []string{"mtec/bridge/status"}) {
		t.Fatalf("one fleet, one availability topic; got %v", got)
	}
	if got := AvailabilityTopics(); len(got) != 0 {
		t.Fatalf("no components, no topics; got %v", got)
	}
}

// TestValidateRefusesAnAvailabilityEntryWithNoTopic is the half of the trap
// this module can see on its own: a level the consumer's Layout could not
// render at all. Home Assistant declares `topic` required inside the object,
// so the entity waits forever on a string that is not one.
func TestValidateRefusesAnAvailabilityEntryWithNoTopic(t *testing.T) {
	t.Parallel()
	b := &Bundle{
		NodeID: "n1",
		Device: DeviceInfo{Identifiers: []string{"n1"}},
		Origin: Origin{Name: "x"},
		Components: map[string]Component{
			"temperature": {
				Platform:     hacatalog.Platform("sensor"),
				UniqueID:     "u1",
				StateTopic:   "root/t",
				Availability: []AvailabilityEntry{{Topic: ""}},
			},
		},
	}
	e := Validate(b)
	if e == nil || !strings.Contains(e.Error(), "availability[0] has no topic") {
		t.Fatalf("an empty availability topic must be refused, got %v", e)
	}
	b.Components["temperature"] = Component{
		Platform: hacatalog.Platform("sensor"), UniqueID: "u1", StateTopic: "root/t",
		Availability: []AvailabilityEntry{{Topic: "root/bridge/status"}},
	}
	if e := Validate(b); e != nil {
		t.Fatalf("a topic that is a string must pass: %v", e)
	}
}

// availabilityEntity is the smallest entity that renders: a key, a platform
// and an availability statement.
type availabilityEntity struct {
	key      string
	platform string
	avail    model.Availability
}

func (e availabilityEntity) Key() string { return e.key }
func (e availabilityEntity) Platform() hacatalog.Platform {
	return hacatalog.Platform(e.platform)
}

func (e availabilityEntity) Desc() *model.Description {
	return &model.Description{Availability: e.avail}
}
func (e availabilityEntity) Bindings() []model.Binding { return nil }
