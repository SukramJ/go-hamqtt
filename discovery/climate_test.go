// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// climate is the design's litmus test: a composite entity that consumes seven
// datapoints, hides the entities a catalog produced for them, derives one role
// from several inputs, and translates commands that do not map one-to-one.
//
// It implements four capability interfaces and gets no special case anywhere in
// the render pipeline. If this needs one, the design is wrong.
type climate struct {
	model.Basic
	temp, tempSet, mode, fan, power model.Slot
	commands                        []model.Command
}

func newClimate(addr string) *climate {
	c := &climate{
		temp:    model.S(addr, "", model.BucketValues, "temperature"),
		tempSet: model.S(addr, "", model.BucketValues, "target_temperature"),
		mode:    model.S(addr, "", model.BucketValues, "mode"),
		fan:     model.S(addr, "", model.BucketValues, "fan_mode"),
		power:   model.S(addr, "", model.BucketValues, "power"),
	}
	c.EntityKey = "climate"
	c.EntityPlatform = hacatalog.PlatformClimate
	c.Description = model.Description{Name: model.L("Thermostat")}
	c.Binds = []model.Binding{
		{Role: "current_temperature", Slot: c.temp, Mode: model.Read},
		{Role: "temperature", Slot: c.tempSet, Mode: model.ReadWrite},
		{Role: "mode", Slot: c.mode, Mode: model.ReadWrite},
		{Role: "fan_mode", Slot: c.fan, Mode: model.ReadWrite},
		{Role: "power", Slot: c.power, Mode: model.Read},
	}
	return c
}

// Suppresses implements model.Suppressor.
func (c *climate) Suppresses() []string {
	return []string{"temperature", "target_temperature", "mode", "fan_mode", "power"}
}

// BuildDiscovery implements discovery.Builder — it asks the context for every
// topic and formats none itself.
func (c *climate) BuildDiscovery(ctx discovery.Context, comp *discovery.Component) error {
	comp.Fields = discovery.ClimateFields{
		CurrentTemperatureTopic: ctx.StateTopic(c.temp),
		TemperatureStateTopic:   ctx.StateTopic(c.tempSet),
		TemperatureCommandTopic: ctx.CommandTopic(c.tempSet),
		ModeStateTopic:          ctx.StateTopic(c.mode),
		ModeCommandTopic:        ctx.CommandTopic(c.mode),
		FanModeStateTopic:       ctx.StateTopic(c.fan),
		FanModeCommandTopic:     ctx.CommandTopic(c.fan),
		Modes:                   []string{"off", "heat", "cool", "auto"},
	}
	return nil
}

// Derive implements model.Deriver: power off outranks whatever mode says.
func (c *climate) Derive(states map[string]model.State) (model.State, bool) {
	mode, haveMode := states["mode"]
	if !haveMode {
		return model.State{}, false
	}
	if p, ok := states["power"]; ok && p.Value == false {
		mode.Value = "off"
	}
	return mode, true
}

// Command implements model.Commander: Home Assistant's "off" mode is a power
// write, which only the entity knows.
func (c *climate) Command(_ context.Context, cmd model.Command) error {
	c.commands = append(c.commands, cmd)
	switch cmd.Role {
	case "mode", "temperature", "fan_mode":
		return nil
	default:
		return model.ErrUnknownRole
	}
}

func sensor(key, leaf, addr string) *model.Basic {
	return &model.Basic{
		EntityKey:      key,
		EntityPlatform: hacatalog.PlatformSensor,
		Description:    model.Description{Name: model.L(key)},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S(addr, "", model.BucketValues, leaf),
			Mode: model.Read,
		}},
	}
}

func testDevice() *model.Device {
	return &model.Device{
		Identity: model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "AC-1"}}},
		Name:     model.L("Living room AC"),
		Model:    "FTXM35",
	}
}

func testContext() discovery.Context {
	return discovery.StdContext{
		Layout:    topic.Default{Root: "daikin"},
		Namespace: "daikin",
	}
}

// TestCompositeSuppressesItsConstituents is the whole point: the catalog's
// individual entities disappear from the bundle, and the composite stays.
func TestCompositeSuppressesItsConstituents(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	addr := dev.UID()
	entities := []model.Entity{
		sensor("temperature", "temperature", addr),
		sensor("target_temperature", "target_temperature", addr),
		sensor("mode", "mode", addr),
		sensor("fan_mode", "fan_mode", addr),
		sensor("power", "power", addr),
		sensor("outdoor_temperature", "outdoor_temperature", addr),
		newClimate(addr),
	}

	bundle, err := discovery.Render(testContext(), dev, entities, discovery.Origin{Name: "go-daikin2mqtt"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	if _, ok := bundle.Components["climate"]; !ok {
		t.Fatal("the composite itself was suppressed")
	}
	for _, gone := range []string{"temperature", "target_temperature", "mode", "fan_mode", "power"} {
		if _, present := bundle.Components[gone]; present {
			t.Errorf("%q survived suppression", gone)
		}
	}
	// An entity the composite does not claim must be untouched — suppression
	// is a named list, not "everything on this device".
	if _, ok := bundle.Components["outdoor_temperature"]; !ok {
		t.Error("outdoor_temperature was suppressed although the composite never named it")
	}
	if len(bundle.Components) != 2 {
		t.Errorf("bundle has %d components, want 2 (climate + outdoor_temperature): %v",
			len(bundle.Components), bundle.Keys())
	}
}

// TestCompositeReferencesSuppressedTopics pins the rule that makes suppression
// safe: the entity is hidden, the datapoint is not. If the composite's topics
// stopped being published, it would show as unavailable forever.
func TestCompositeReferencesSuppressedTopics(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	c := newClimate(dev.UID())
	bundle, err := discovery.Render(testContext(), dev, []model.Entity{
		sensor("mode", "mode", dev.UID()),
		c,
	}, discovery.Origin{Name: "go-daikin2mqtt"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	raw, err := json.Marshal(bundle.Components["climate"])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := map[string]string{
		"mode_state_topic":          "daikin/serial:AC-1/values/mode",
		"mode_command_topic":        "daikin/serial:AC-1/values/mode/set",
		"current_temperature_topic": "daikin/serial:AC-1/values/temperature",
	}
	for key, expect := range want {
		if got[key] != expect {
			t.Errorf("%s = %v, want %q", key, got[key], expect)
		}
	}
}

// TestBuilderTopicsComeFromTheContext proves the indirection actually holds:
// swapping the layout must move every topic, including the ones the entity's
// own builder wrote.
func TestBuilderTopicsComeFromTheContext(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	c := newClimate(dev.UID())
	ctx := discovery.StdContext{
		Layout:    topic.Default{Root: "somewhere/else"},
		Namespace: "daikin",
	}

	bundle, err := discovery.Render(ctx, dev, []model.Entity{c}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	fields, ok := bundle.Components["climate"].Fields.(discovery.ClimateFields)
	if !ok {
		t.Fatalf("Fields = %T, want ClimateFields", bundle.Components["climate"].Fields)
	}
	// topic.Safe turns the slash inside the root into an underscore, which is
	// the point: a configured value cannot add a topic level.
	const wantPrefix = "somewhere_else/serial:AC-1/values/"
	if got := fields.ModeStateTopic; got != wantPrefix+"mode" {
		t.Errorf("mode_state_topic = %q, want %q", got, wantPrefix+"mode")
	}
}

// TestDeriveAggregatesInputs covers the composite's other half: one published
// role computed from several datapoints, and the "not yet" answer that keeps
// an incomplete reading off the wire.
func TestDeriveAggregatesInputs(t *testing.T) {
	t.Parallel()

	c := newClimate("serial:AC-1")

	if _, ok := c.Derive(map[string]model.State{"power": {Value: true}}); ok {
		t.Error("Derive published a value while mode was still missing")
	}

	got, ok := c.Derive(map[string]model.State{
		"mode":  {Value: "heat", Available: true},
		"power": {Value: false, Available: true},
	})
	if !ok {
		t.Fatal("Derive returned nothing for a complete input set")
	}
	if got.Value != "off" {
		t.Errorf("Value = %v, want \"off\" — power off must outrank the mode", got.Value)
	}
}

// TestCommanderReceivesTheRole pins the inbound half: the command carries the
// role it arrived on, which is what lets one entity route several controls.
func TestCommanderReceivesTheRole(t *testing.T) {
	t.Parallel()

	c := newClimate("serial:AC-1")
	err := c.Command(context.Background(), model.Command{
		Entity: c, Role: "mode", Slot: c.mode, Payload: []byte("off"),
	})
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if len(c.commands) != 1 || c.commands[0].Role != "mode" {
		t.Fatalf("commands = %+v, want one with role \"mode\"", c.commands)
	}

	err = c.Command(context.Background(), model.Command{Entity: c, Role: "nonsense"})
	if !errors.Is(err, model.ErrUnknownRole) {
		t.Errorf("err = %v, want ErrUnknownRole", err)
	}
}

// TestRenderedCompositeValidates closes the loop: the litmus-test entity must
// also survive the validator, or the design only looks right on paper.
func TestRenderedCompositeValidates(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	bundle, err := discovery.Render(testContext(), dev,
		[]model.Entity{newClimate(dev.UID())},
		discovery.Origin{Name: "go-daikin2mqtt", SW: "1.0.0"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if err := discovery.Validate(bundle); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}
