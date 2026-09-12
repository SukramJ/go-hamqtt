// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"encoding/json"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// gapDevice and gapEntity are the measured consumer's shapes, kept minimal:
// every test below reads one projected key, not a whole payload.
func gapDevice() *model.Device {
	return &model.Device{
		Identity: model.Identity{IDs: []model.Identifier{{Value: "openccu-loom_0001abc"}}},
		Name:     model.L("CCU"),
	}
}

func gapEntity(key string, platform hacatalog.Platform, desc model.Description) *model.Basic {
	return &model.Basic{
		EntityKey:      key,
		EntityPlatform: platform,
		Description:    desc,
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S("openccu-loom_0001abc", "", model.BucketValues, key),
			Mode: model.Read,
		}},
	}
}

func gapContext() discovery.StdContext {
	return discovery.StdContext{Layout: topic.Default{Root: "loom"}, Namespace: "loom"}
}

func gapBody(t *testing.T, key string, e model.Entity) map[string]any {
	t.Helper()
	bundle, err := discovery.Render(gapContext(), gapDevice(), []model.Entity{e},
		discovery.Origin{Name: "openccu-loom"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	raw, err := json.Marshal(bundle.Components[key])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	body := map[string]any{}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return body
}

// TestLevelNoneProjectsNoAvailabilityAtAll is the daemon-status case: the
// entity's state topic IS the bridge LWT, so gating it on that topic makes it
// unavailable in exactly the situation it exists to report. Before LevelNone
// the model answered every entity with a mode, so an empty level list still
// projected `availability_mode: "all"` beside an absent list — and the
// measured consumer cleared both fields again in a post-render Builder.
func TestLevelNoneProjectsNoAvailabilityAtAll(t *testing.T) {
	t.Parallel()

	e := gapEntity("daemon_status", hacatalog.PlatformBinarySensor, model.Description{
		Name:         model.L("Daemon status"),
		Availability: model.NoAvailability(),
	})
	body := gapBody(t, "daemon_status", e)
	if _, has := body["availability"]; has {
		t.Errorf("availability was published: %v", body["availability"])
	}
	if _, has := body["availability_mode"]; has {
		t.Errorf("availability_mode was published beside an absent list: %v", body["availability_mode"])
	}
}

// TestTheZeroAvailabilityStillMeansBridgeAndDevice. LevelNone must be the
// explicit statement it is named for: a consumer that says nothing keeps the
// behaviour it has shipped with, or every existing entity loses its gate on
// upgrade.
func TestTheZeroAvailabilityStillMeansBridgeAndDevice(t *testing.T) {
	t.Parallel()

	body := gapBody(t, "level", gapEntity("level", hacatalog.PlatformSensor, model.Description{
		Name: model.L("Level"),
	}))
	list, _ := body["availability"].([]any)
	if len(list) != 2 {
		t.Errorf("availability = %v, want the bridge and the device", body["availability"])
	}
	if body["availability_mode"] != "all" {
		t.Errorf("availability_mode = %v, want all", body["availability_mode"])
	}
}

// TestLevelNoneWinsOverTheLevelsBesideIt. A rule table accumulates levels;
// an entity that says it has none has none, whatever else was added, because
// the alternative is a payload whose meaning depends on the order two rules
// happened to run in.
func TestLevelNoneWinsOverTheLevelsBesideIt(t *testing.T) {
	t.Parallel()

	a := model.Availability{Levels: []model.AvailabilityLevel{model.LevelBridge, model.LevelNone}}
	levels, mode := a.Resolved()
	if len(levels) != 0 || mode != "" {
		t.Errorf("Resolved = %v, %q, want nothing at all", levels, mode)
	}
	if a.Has(model.LevelBridge) {
		t.Error("Has reported a level the entity resolved away")
	}
}

// TestNameArgsFillACatalogueTemplate. The measured consumer's install-mode
// and connectivity entities embed an interface id in their name. With no
// parameterised translation the name had to be resolved eagerly and passed
// as a literal Name, which bypasses the NameKey path entirely — the key
// never reaches the model, so nothing downstream can re-render it.
func TestNameArgsFillACatalogueTemplate(t *testing.T) {
	t.Parallel()

	ctx := gapContext()
	ctx.Translator = func(key string) string {
		if key == "discovery.connectivity" {
			return "Connectivity {iface}"
		}
		return key
	}
	bundle, err := discovery.Render(ctx, gapDevice(), []model.Entity{
		gapEntity("connectivity", hacatalog.PlatformBinarySensor, model.Description{
			NameKey:  "discovery.connectivity",
			NameArgs: map[string]string{"iface": "HmIP-RF"},
		}),
	}, discovery.Origin{Name: "openccu-loom"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got := bundle.Components["connectivity"].Name; got != "Connectivity HmIP-RF" {
		t.Errorf("Name = %q, want the placeholder filled", got)
	}
}

// TestNameArgsAlsoFillALiteralName, because a consumer mid-migration
// parameterises the literal before it parameterises the catalogue key, and a
// literal that kept its braces would publish them.
func TestNameArgsAlsoFillALiteralName(t *testing.T) {
	t.Parallel()

	body := gapBody(t, "install_mode", gapEntity("install_mode", hacatalog.PlatformSensor,
		model.Description{
			Name:     model.L("Install mode {iface}"),
			NameArgs: map[string]string{"iface": "BidCos-RF"},
		}))
	if body["name"] != "Install mode BidCos-RF" {
		t.Errorf("name = %v, want the literal's placeholder filled", body["name"])
	}
}

// TestSubstituteLeavesAnUnknownPlaceholderStanding. "Connectivity {iface}"
// says where the gap is; "Connectivity " says only that something went wrong
// somewhere, which is the report nobody can act on.
func TestSubstituteLeavesAnUnknownPlaceholderStanding(t *testing.T) {
	t.Parallel()

	if got := discovery.Substitute("Connectivity {iface}", map[string]string{"other": "x"}); got != "Connectivity {iface}" {
		t.Errorf("Substitute = %q, want the placeholder kept", got)
	}
	if got := discovery.Substitute("plain", nil); got != "plain" {
		t.Errorf("Substitute = %q, want the text unchanged", got)
	}
}

// TestTranslateWithoutACatalogueKeepsTheKey, including its arguments: a
// missing catalogue entry must degrade to a readable key rather than to a
// half-filled sentence.
func TestTranslateWithoutACatalogueKeepsTheKey(t *testing.T) {
	t.Parallel()

	if got := (discovery.StdContext{}).Translate("discovery.connectivity", map[string]string{"iface": "x"}); got != "discovery.connectivity" {
		t.Errorf("Translate = %q, want the key unchanged", got)
	}
}

// TestOptimisticIsProjectedFromTheDescription. Eight measured hub entities
// set `optimistic` in a post-render Builder because it had no typed home in
// the description, although switch, select, text and number all declare it —
// it is not platform-specific vocabulary.
func TestOptimisticIsProjectedFromTheDescription(t *testing.T) {
	t.Parallel()

	body := gapBody(t, "relay", gapEntity("relay", hacatalog.PlatformSwitch, model.Description{
		Name:       model.L("Relay"),
		Optimistic: model.Ptr(false),
	}))
	if body["optimistic"] != false {
		t.Errorf("optimistic = %v, want an explicit false", body["optimistic"])
	}
}

// TestOptimisticIsNotProjectedWhereTheSchemaDoesNotDeclareIt. Home
// Assistant's schemas are extra=REMOVE_EXTRA: a key on the wrong platform is
// dropped with no error on the wire and no log line, so projecting it there
// is invisible noise the validator would then report as a finding.
func TestOptimisticIsNotProjectedWhereTheSchemaDoesNotDeclareIt(t *testing.T) {
	t.Parallel()

	body := gapBody(t, "level", gapEntity("level", hacatalog.PlatformSensor, model.Description{
		Name:       model.L("Level"),
		Optimistic: model.Ptr(true),
	}))
	if _, has := body["optimistic"]; has {
		t.Error("optimistic was projected onto sensor, whose schema drops it in silence")
	}
}

// TestJSONAttributesAreProjectedFromTheDescription. Three measured message
// aggregates published their detail rows through a post-render Builder for
// want of this pair, which 30 of the 32 platforms accept — the same bar the
// description's other Component-level keys meet.
func TestJSONAttributesAreProjectedFromTheDescription(t *testing.T) {
	t.Parallel()

	body := gapBody(t, "messages", gapEntity("messages", hacatalog.PlatformSensor, model.Description{
		Name:                   model.L("Messages"),
		JSONAttributesTopic:    "loom/ccu/hub/messages",
		JSONAttributesTemplate: "{{ value_json | tojson }}",
	}))
	if body["json_attributes_topic"] != "loom/ccu/hub/messages" {
		t.Errorf("json_attributes_topic = %v", body["json_attributes_topic"])
	}
	if body["json_attributes_template"] != "{{ value_json | tojson }}" {
		t.Errorf("json_attributes_template = %v", body["json_attributes_template"])
	}
}

// TestAJSONAttributesTemplateNeedsItsTopic: a template alone selects a field
// of a document Home Assistant was never told to read, so it is projected
// with the topic or not at all.
func TestAJSONAttributesTemplateNeedsItsTopic(t *testing.T) {
	t.Parallel()

	body := gapBody(t, "messages", gapEntity("messages", hacatalog.PlatformSensor, model.Description{
		Name:                   model.L("Messages"),
		JSONAttributesTemplate: "{{ value_json | tojson }}",
	}))
	if _, has := body["json_attributes_template"]; has {
		t.Error("a template was published with no topic to read")
	}
}

// TestJSONAttributesAreNotProjectedOntoTagOrDeviceAutomation, the two
// platforms whose schemas do not declare them.
func TestJSONAttributesAreNotProjectedOntoTagOrDeviceAutomation(t *testing.T) {
	t.Parallel()

	body := gapBody(t, "scanner", gapEntity("scanner", hacatalog.PlatformTag, model.Description{
		Name:                model.L("Scanner"),
		JSONAttributesTopic: "loom/ccu/hub/tags",
	}))
	if _, has := body["json_attributes_topic"]; has {
		t.Error("json_attributes_topic was projected onto tag, whose schema drops it")
	}
}

// TestRenderedDescriptionKeysValidate ties the three projections to the
// validator: a key this pipeline emits must be one Home Assistant's own
// schema declares, or it is dropped where nobody can see it happen.
func TestRenderedDescriptionKeysValidate(t *testing.T) {
	t.Parallel()

	relay := gapEntity("relay", hacatalog.PlatformSwitch, model.Description{
		Name:                   model.L("Relay"),
		Optimistic:             model.Ptr(false),
		JSONAttributesTopic:    "loom/ccu/hub/relay/attrs",
		JSONAttributesTemplate: "{{ value_json | tojson }}",
	})
	relay.Binds = append(relay.Binds, model.Binding{
		Role: model.RoleCommand,
		Slot: model.S("openccu-loom_0001abc", "", model.BucketValues, "relay"),
		Mode: model.Write,
	})

	bundle, err := discovery.Render(gapContext(), gapDevice(), []model.Entity{
		relay,
		gapEntity("daemon_status", hacatalog.PlatformBinarySensor, model.Description{
			Name:         model.L("Daemon status"),
			Availability: model.NoAvailability(),
		}),
	}, discovery.Origin{Name: "openccu-loom"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if err := discovery.Validate(bundle); err != nil {
		t.Errorf("Validate: %v", err)
	}
}
