// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"slices"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// TestSmartHomePlanesAgree is the conformance invariant for the convention:
// every topic a config names is one some plane writes or subscribes. The
// state topic is where PublishStatus writes, the command topic is the one the
// router claims, and the two availability topics are the runtime's will and
// the device's online status item — the convention moves all of them at once,
// so a plane left on the old vocabulary shows up here rather than as an
// entity that is never available.
func TestSmartHomePlanesAgree(t *testing.T) {
	t.Parallel()

	layout := smartHomeLayout(t, "mtec")
	dctx := discovery.StdContext{Layout: layout, Namespace: "mtec", Enc: discovery.StatusObjectEncoding}
	dev := &model.Device{Identity: model.Identity{IDs: []model.Identifier{{Value: "MT1"}}}, Name: model.L("Inverter")}
	slot := model.S(dev.UID(), "", model.BucketUnset, "now_base", "grid_export")
	sw := &model.Basic{
		EntityKey:      "grid_export",
		EntityPlatform: hacatalog.Platform("switch"),
		Description:    model.Description{Name: model.L("Grid export")},
		Binds: []model.Binding{
			{Role: model.RoleState, Slot: slot, Mode: model.Read},
			{Role: model.RoleCommand, Slot: slot, Mode: model.Write},
		},
	}
	comp, err := discovery.RenderComponent(dctx, dev, sw, discovery.Origin{Name: "go-mtec2mqtt"})
	if err != nil {
		t.Fatal(err)
	}

	f := newFake()
	run := New(f, Config{Layout: layout, Logger: discardLogger()})
	state := StateFor(run, StateConfig{Encoding: dctx.Encoding(), Logger: discardLogger()})
	ctx := context.Background()

	if _, err := state.PublishComponentValue(ctx, comp, true, true); err != nil {
		t.Fatal(err)
	}
	if got := lastPublish(t, f).topic; got != "mtec/status/MT1/now_base/grid_export" || got != comp.StateTopic {
		t.Errorf("state written to %q, config reads %q", got, comp.StateTopic)
	}
	if comp.CommandTopic != "mtec/set/MT1/now_base/grid_export" {
		t.Errorf("command_topic = %q", comp.CommandTopic)
	}

	router := NewCommandRouter(newCmdBroker(), CommandConfig{NormalizeSet: true, Logger: discardLogger()})
	if err := router.Handle(layout.Name()+"/set/#", func(context.Context, Command) {}); err != nil {
		t.Fatal(err)
	}
	if !router.Claims(comp.CommandTopic) || router.Claims(comp.StateTopic) {
		t.Error("the router's claim does not separate set from status")
	}

	will, _ := run.Will()
	online := layout.Availability(DeviceSlot(dev, sw))
	if _, err := state.PublishStatus(ctx, online, Observation{Value: true}); err != nil {
		t.Fatal(err)
	}
	if got := discovery.AvailabilityTopics(comp); !slices.Equal(got, []string{will.Topic, online}) {
		t.Errorf("availability topics %v, want the will %q and the online item %q", got, will.Topic, online)
	}
}
