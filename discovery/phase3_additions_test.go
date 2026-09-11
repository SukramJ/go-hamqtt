// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"encoding/json"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// loomContext is a consumer whose fleet is already published: its node id and
// its device identifier are deliberately different strings, and it publishes
// no entity-id seed at all. Both are measured properties of openccu-loom, and
// both were impossible to express before these additions.
type loomContext struct {
	discovery.StdContext
}

func (loomContext) NodeID(dev *model.Device) string { return "ccu1_" + strings.ToLower(dev.UID()) }

func (loomContext) ObjectID(*model.Device, model.Entity) string { return "" }

func phase3Device() *model.Device {
	return &model.Device{
		Identity: model.Identity{IDs: []model.Identifier{{Value: "openccu-loom_0001abc"}}},
		Name:     model.L("Thermostat"),
		Model:    "HmIP-eTRV-2",
	}
}

func phase3Entity() *model.Basic {
	return &model.Basic{
		EntityKey:      "1_temperature",
		EntityPlatform: hacatalog.PlatformSensor,
		Description:    model.Description{Name: model.L("Temperature")},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S("openccu-loom_0001abc", "1", model.BucketValues, "ACTUAL_TEMPERATURE").In("ccu-01", "HmIP-RF"),
			Mode: model.Read,
		}},
	}
}

func phase3Context() loomContext {
	return loomContext{discovery.StdContext{
		Layout:    topic.Default{Root: "loom"},
		Namespace: "",
		Enc:       discovery.RawEncoding,
	}}
}

// TestAnEmptyObjectIDSuppressesTheSeed. The key decides the entity id. A
// consumer whose fleet never carried one cannot accept one now: Home
// Assistant would seed an id where it currently derives its own, and it does
// not rename an entity back.
func TestAnEmptyObjectIDSuppressesTheSeed(t *testing.T) {
	t.Parallel()

	dev := phase3Device()
	bundle, err := discovery.Render(phase3Context(), dev, []model.Entity{phase3Entity()},
		discovery.Origin{Name: "openccu-loom"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	body := phase3Body(t, bundle.Components["1_temperature"])
	if _, has := body["default_entity_id"]; has {
		t.Errorf("the seed was published although the consumer declined it: %v", body["default_entity_id"])
	}

	// The default is unchanged: a consumer with no opinion still gets one.
	std := discovery.StdContext{Layout: topic.Default{Root: "loom"}, Namespace: "x"}
	plain, err := discovery.Render(std, phase3Device(), []model.Entity{phase3Entity()},
		discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got := plain.Components["1_temperature"].DefaultEntityID; got == "" {
		t.Error("StdContext stopped seeding the entity id")
	}
}

// TestNodeIDAndIdentifierCanDiffer is the gap that made the identity
// unrepresentable: one measured consumer publishes under node id
// `ccu1_<addr>` while its device block says `openccu-loom_<addr>`. Deriving
// both from Identity.UID makes one of them wrong whichever way it is filled.
func TestNodeIDAndIdentifierCanDiffer(t *testing.T) {
	t.Parallel()

	dev := phase3Device()
	bundle, err := discovery.Render(phase3Context(), dev, []model.Entity{phase3Entity()},
		discovery.Origin{Name: "openccu-loom"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got, want := bundle.NodeID, "ccu1_openccu-loom_0001abc"; got != want {
		t.Errorf("NodeID = %q, want the consumer's own %q", got, want)
	}
	if got := bundle.Device.Identifiers[0]; got != "openccu-loom_0001abc" {
		t.Errorf("device identifier = %q, want it independent of the node id", got)
	}
	if got := bundle.Topic(""); got != "homeassistant/device/ccu1_openccu-loom_0001abc/config" {
		t.Errorf("topic = %q", got)
	}
}

// TestRenderComponentCarriesTheFrameAndNoPlatform. Five of the six consuming
// projects publish the per-entity form, and the first full consumer cannot
// switch: its retained configs are on brokers and Home Assistant refuses a
// bundle while a per-entity config for the same entity is still retained.
func TestRenderComponentCarriesTheFrameAndNoPlatform(t *testing.T) {
	t.Parallel()

	dev := phase3Device()
	comp, err := discovery.RenderComponent(phase3Context(), dev, phase3Entity(),
		discovery.Origin{Name: "openccu-loom", SW: "0.77.0"})
	if err != nil {
		t.Fatalf("RenderComponent: %v", err)
	}
	body := phase3Body(t, comp)

	if _, has := body["platform"]; has {
		t.Error("the per-entity form carries a platform its topic already states")
	}
	devBlock, _ := body["device"].(map[string]any)
	if devBlock == nil || devBlock["name"] != "Thermostat" {
		t.Errorf("device block = %v, want it on the component", body["device"])
	}
	origin, _ := body["origin"].(map[string]any)
	if origin == nil || origin["name"] != "openccu-loom" {
		t.Errorf("origin = %v", body["origin"])
	}
	if body["unique_id"] == nil || body["state_topic"] == nil {
		t.Errorf("the component lost its own keys: %v", body)
	}
}

// TestRenderComponentRefusesADeviceWithNoIdentity, because Home Assistant
// registers such a device under nothing and every entity under it collides.
func TestRenderComponentRefusesADeviceWithNoIdentity(t *testing.T) {
	t.Parallel()

	if _, err := discovery.RenderComponent(phase3Context(), &model.Device{}, phase3Entity(),
		discovery.Origin{Name: "x"}); err == nil {
		t.Error("a device with no identity was rendered")
	}
}

// TestAvailabilityCarriesTheEntitysScope. The measurement found this the only
// item no workaround could reach: a device availability topic inside a
// controller and an interface cannot be rendered from a model.Identity,
// because an identity has no scope and `model` may not import `topic`.
func TestAvailabilityCarriesTheEntitysScope(t *testing.T) {
	t.Parallel()

	e := phase3Entity()
	e.Description.Availability = model.Availability{
		Levels: []model.AvailabilityLevel{model.LevelDevice},
	}
	entries := phase3Context().Availability(phase3Device(), e)
	if len(entries) != 1 {
		t.Fatalf("entries = %+v", entries)
	}
	want := "loom/ccu-01/HmIP-RF/openccu-loom_0001abc/availability"
	if entries[0].Topic != want {
		t.Errorf("availability topic = %q, want %q", entries[0].Topic, want)
	}
}

// TestParentAvailabilityKeepsTheScopeAndSwapsTheAddress: LevelParent resolves
// through Via, which gives the right identity — and before this it still
// could not format the string.
func TestParentAvailabilityKeepsTheScopeAndSwapsTheAddress(t *testing.T) {
	t.Parallel()

	dev := phase3Device()
	dev.Via = &model.Identity{IDs: []model.Identifier{{Value: "openccu-loom_central"}}}
	e := phase3Entity()
	e.Description.Availability = model.Availability{
		Levels: []model.AvailabilityLevel{model.LevelParent},
	}
	entries := phase3Context().Availability(dev, e)
	if len(entries) != 1 {
		t.Fatalf("entries = %+v", entries)
	}
	want := "loom/ccu-01/HmIP-RF/openccu-loom_central/availability"
	if entries[0].Topic != want {
		t.Errorf("parent availability = %q, want %q", entries[0].Topic, want)
	}
}

// TestValueTemplateIsPerEntity. Context.Encoding is one answer for a whole
// consumer, and a consumer is rarely uniform: bare datapoint topics,
// composites assembled by a hand-written template, and event entities that
// must carry none. Five of eleven measured planes hit this.
func TestValueTemplateIsPerEntity(t *testing.T) {
	t.Parallel()

	envelope := discovery.StdContext{Layout: topic.Default{Root: "loom"}, Namespace: "x"}

	// No opinion: the encoding decides, as before.
	derived := phase3Entity()
	if got := phase3Render(t, envelope, derived).ValueTemplate; got != discovery.ValueTemplate {
		t.Errorf("derived template = %q, want the envelope default", got)
	}

	// An explicit template wins over the encoding.
	custom := phase3Entity()
	custom.Description.ValueTemplate = "{{ value_json.level | float * 100 }}"
	if got := phase3Render(t, envelope, custom).ValueTemplate; got != custom.Description.ValueTemplate {
		t.Errorf("custom template = %q", got)
	}

	// And an entity can decline one even under envelope encoding, which an
	// empty string cannot express because that is also "no opinion".
	none := phase3Entity()
	none.Description.ValueTemplate = model.NoValueTemplate
	if got := phase3Render(t, envelope, none).ValueTemplate; got != "" {
		t.Errorf("declined template = %q, want none", got)
	}
}

func phase3Render(t *testing.T, ctx discovery.Context, e model.Entity) discovery.Component {
	t.Helper()
	bundle, err := discovery.Render(ctx, phase3Device(), []model.Entity{e}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	return bundle.Components[e.Key()]
}

func phase3Body(t *testing.T, c discovery.Component) map[string]any {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return body
}
