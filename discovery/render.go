// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"fmt"
	"sync"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/model"
)

// Context is what the model asks for the strings it must not build itself.
//
// This one indirection is what makes the model transport-agnostic: an entity's
// discovery builder calls ctx.StateTopic(slot) and never learns the topic
// schema, so the same entity renders correctly under any consumer's layout.
type Context interface {
	// StateTopic is where a datapoint publishes.
	StateTopic(s model.Slot) string
	// CommandTopic is where it listens.
	CommandTopic(s model.Slot) string
	// Availability renders the availability list for an entity.
	Availability(dev *model.Device, e model.Entity) []AvailabilityEntry
	// UniqueID is the entity's stable Home Assistant identity.
	UniqueID(dev *model.Device, e model.Entity) string

	// NodeID is the topic segment this device's discovery is published
	// under, and ObjectID seeds the entity id Home Assistant assigns.
	//
	// All three identity strings are Context methods for the same reason
	// UniqueID always was: Home Assistant has no migration path for any of
	// them. A consumer whose fleet is already published owns its spellings,
	// and they need not agree with each other — one measured consumer's node
	// id and device identifier are deliberately different strings, so
	// deriving both from [model.Identity.UID] makes one of them wrong
	// whichever way the identity is filled in.
	//
	// [StdContext] answers all three from this package's own functions, so a
	// consumer with no opinion inherits the defaults and a consumer with a
	// published fleet overrides what it must.
	NodeID(dev *model.Device) string
	// ObjectID returns the entity-id seed, published as
	// `default_entity_id`. An EMPTY string suppresses the key entirely,
	// which is what a consumer whose fleet never carried one needs: adding
	// it would seed an entity id where Home Assistant currently derives one,
	// and that is a rename nobody downstream can undo.
	ObjectID(dev *model.Device, e model.Entity) string
	// Language is the locale display strings are rendered in.
	Language() string
	// Encoding says whether state topics carry a JSON envelope or a bare
	// value, which decides whether a component needs a value template.
	Encoding() Encoding

	// EntityStateTopic is a composite entity's own aggregate topic — the one
	// carrying the curated document its roles are read out of, rather than
	// any single datapoint's.
	//
	// A composite needs both: [StateTopic] for the datapoints it reads
	// directly, and this for the fields it derives. A climate reads its
	// current temperature from the sensor's own topic and its hvac_action
	// from an aggregate no datapoint publishes.
	EntityStateTopic(dev *model.Device, e model.Entity) string

	// MethodTopic is where an entity listens for a named action.
	//
	// Not every command is a write to a datapoint. A cover's "stop", a
	// siren's "turn_on", a lock's short-time open: each reduces to one
	// operation the entity performs, and pointing Home Assistant at one of
	// the parameters involved makes the other payloads write nonsense to it.
	// Consumers whose devices expose actions rather than settable values —
	// a port reset, a scene trigger — need this as their only command
	// surface.
	MethodTopic(dev *model.Device, e model.Entity, method string) string

	// Translate resolves a catalogue key into the context's language, with
	// args filling the `{name}` placeholders the resolved string carries.
	//
	// [Language] alone is not enough: the catalogues live with the consumer,
	// so the model can ask for a label but cannot look one up. A key with no
	// entry comes back unchanged, which is what lets a caller tell a missing
	// translation from an empty one.
	//
	// The arguments are what make [model.Description.NameKey] usable for a
	// parameterised name. Without them a name like "Connectivity {iface}"
	// had to be resolved by the consumer and handed over as a literal
	// [model.Description.Name], so the key never reached the model and
	// nothing downstream could render it in another language — measured on
	// two entity families of the first full consumer. A nil map is the
	// unparameterised case and must behave exactly as the one-argument form
	// did.
	Translate(key string, args map[string]string) string
}

// Encoding is the shape of a state payload.
type Encoding uint8

const (
	// EnvelopeEncoding publishes {"value": …, "available": …, …} and points
	// Home Assistant at the value with a template. It is the default because
	// it is the only shape that carries per-datapoint availability, which is
	// what [model.LevelSelf] resolves against — the alternative needs a second
	// topic per datapoint.
	EnvelopeEncoding Encoding = iota
	// RawEncoding publishes the bare value. Simpler for anything else reading
	// the same broker, at the cost of per-datapoint availability.
	RawEncoding
)

// ValueTemplate is the Jinja template that reads a value out of the envelope.
//
// The `is defined` guard is not decoration: Home Assistant renders the template
// against a retained payload at subscribe time, and an entity whose topic has
// never been published would otherwise log a template error on every restart.
const ValueTemplate = `{% if value_json is defined and value_json.value is not none %}{{ value_json.value }}{% endif %}`

// AvailabilityTemplate reads the availability flag out of the same envelope.
// It is the shape for a datapoint that reports whether *it* is reachable.
const AvailabilityTemplate = `{{ value_json.available | lower }}`

// SelfAvailabilityTemplate reads the value of a datapoint that reports the
// entity's availability, which is a different question from whether that
// datapoint is itself reachable — see [model.RoleAvailability].
const SelfAvailabilityTemplate = `{{ value_json.value | lower }}`

// Payload strings for availability topics.
const (
	PayloadOnline  = "online"
	PayloadOffline = "offline"
)

// Builder lets an entity write its own platform-specific keys. Not
// implementing it means the default projection applies, which is right for
// every simple entity.
type Builder interface {
	BuildDiscovery(ctx Context, comp *Component) error
}

// Dynamic marks an entity whose discovery depends on datapoint values, and
// names the slots that should trigger a re-render when they change — a select
// whose options come from the device, say.
//
// Not implementing it means discovery is rendered when the device is added and
// on reconnect, which is what almost every entity wants.
type Dynamic interface {
	DiscoveryTriggers() []model.Slot
}

// Render turns a device and its entities into a bundle.
//
// The pipeline has exactly one precedence rule — a later stage overwrites an
// earlier one:
//
//	Description → default projection → Builder → Component.Extra
//
// Enrichers run before this, on the Description, so the whole chain from
// catalog default to operator override to platform builder to escape hatch is
// one ordered sequence with no special cases. The reference implementation had
// two builder interfaces with opposite precedence; that is what this replaces.
//
// Suppression is applied here: entities named by a [model.Suppressor] are left
// out of the bundle. Their datapoints keep publishing, because the suppressing
// entity's own topics point at them.
func Render(ctx Context, dev *model.Device, entities []model.Entity, origin Origin) (*Bundle, error) {
	if dev == nil {
		return nil, fmt.Errorf("discovery: no device")
	}
	if !dev.Identity.Valid() {
		return nil, fmt.Errorf("discovery: device has no identifier")
	}

	bundle := &Bundle{
		NodeID:     ctx.NodeID(dev),
		Device:     deviceInfo(dev, ctx.Language()),
		Origin:     origin,
		Components: map[string]Component{},
	}

	for _, e := range model.ApplySuppression(entities) {
		comp, err := renderComponent(ctx, dev, e)
		if err != nil {
			return nil, fmt.Errorf("discovery: entity %q: %w", e.Key(), err)
		}
		if _, dup := bundle.Components[e.Key()]; dup {
			return nil, fmt.Errorf("discovery: duplicate entity key %q on device %q", e.Key(), dev.UID())
		}
		bundle.Components[e.Key()] = comp
	}
	return bundle, nil
}

// entityName resolves the display name: a literal wins, a catalogue key is
// translated, and neither leaves the name empty so Home Assistant derives one
// from the platform or the device class.
//
// [model.Description.NameArgs] fills the placeholders of either, because a
// consumer mid-migration parameterises the literal before it parameterises
// the key, and a literal that silently kept its braces would publish them.
func entityName(ctx Context, desc *model.Description, lang string) string {
	if name := desc.Name.In(lang); name != "" {
		return Substitute(name, desc.NameArgs)
	}
	if desc.NameKey == "" {
		return ""
	}
	return ctx.Translate(desc.NameKey, desc.NameArgs)
}

// valueTemplateFor picks the template for one entity.
//
// The description wins over the context's encoding, because the encoding is
// one answer for a whole consumer and a consumer is rarely uniform. An
// explicit [model.NoValueTemplate] publishes none — which an event entity
// needs, and which an empty string cannot express, since that is also what
// "no opinion" looks like.
func valueTemplateFor(ctx Context, desc *model.Description) string {
	switch desc.ValueTemplate {
	case model.NoValueTemplate:
		return ""
	case "":
		if ctx.Encoding() == EnvelopeEncoding {
			return ValueTemplate
		}
		return ""
	default:
		return desc.ValueTemplate
	}
}

func renderComponent(ctx Context, dev *model.Device, e model.Entity) (Component, error) {
	desc := e.Desc()
	if desc == nil {
		return Component{}, fmt.Errorf("nil description")
	}
	lang := ctx.Language()

	comp := Component{
		Platform:         e.Platform(),
		Name:             entityName(ctx, desc, lang),
		UniqueID:         ctx.UniqueID(dev, e),
		DeviceClass:      string(desc.DeviceClass),
		StateClass:       desc.StateClass,
		UnitOfMeasure:    string(desc.Unit),
		Icon:             desc.Icon,
		EntityCategory:   desc.Category,
		EnabledByDefault: desc.Enabled,
		Precision:        desc.Precision,
		Availability:     ctx.Availability(dev, e),
	}

	// One schema lookup drives every conditional projection below. Home
	// Assistant drops a key its platform does not declare without a word, so a
	// key emitted on the wrong platform is not an error anyone would ever see.
	accepts, err := platformAccepts(e.Platform())
	if err != nil {
		return Component{}, err
	}

	// Bounds go to the plain keys only where the platform declares them:
	// `min` and `max` exist on number and text, `step` on number alone.
	// Climate spells them min_temp/max_temp/temp_step and water_heater
	// min_temp/max_temp, so those platforms carry bounds through their own
	// Fields struct — a Builder's job, since only it knows the spelling.
	// Projecting unconditionally emitted keys Home Assistant drops in silence.
	if accepts["min"] {
		comp.Min = desc.Min
	}
	if accepts["max"] {
		comp.Max = desc.Max
	}
	if accepts["step"] {
		comp.Step = desc.Step
	}

	// `optimistic` is declared by 17 of the 32 platforms — switch, select,
	// text and number among them, but not sensor or binary_sensor — so it is
	// description vocabulary with a platform gate rather than a Builder's
	// job. Eight measured hub entities set it after rendering for want of
	// this projection.
	if accepts["optimistic"] {
		comp.Optimistic = desc.Optimistic
	}

	// The json-attributes pair is accepted by 30 of the 32 platforms (all
	// but device_automation and tag), the same bar the other
	// description-level keys meet. The template only means anything beside
	// the topic, so it is projected with it: a template alone selects a
	// field of a document Home Assistant was never told to read.
	if desc.JSONAttributesTopic != "" && accepts["json_attributes_topic"] {
		comp.JSONAttributesTopic = desc.JSONAttributesTopic
		if accepts["json_attributes_template"] {
			comp.JSONAttributesTemplate = desc.JSONAttributesTemplate
		}
	}

	if _, mode := desc.Availability.Resolved(); mode != "" {
		comp.AvailabilityMode = string(mode)
	}
	if desc.Options != nil {
		comp.Options = desc.Options.Options(lang)
	}

	// The entity id is seeded language-independently. Left to itself, Home
	// Assistant derives it from the *translated* name at first discovery, so
	// the same device discovered under a German UI and an English one ends up
	// with different entity ids — and every automation referencing one breaks
	// on the other.
	//
	// Note this is `default_entity_id`, not `object_id`: the latter is no
	// longer a legal MQTT discovery key on any platform, and Home Assistant
	// drops unknown keys silently.
	//
	// An empty seed suppresses the key. A consumer whose fleet never carried
	// one must be able to keep it that way: publishing one now would seed an
	// entity id where Home Assistant currently derives its own, which is a
	// rename of every entity at once and one nobody downstream can undo.
	if seed := ctx.ObjectID(dev, e); seed != "" {
		comp.DefaultEntityID = string(e.Platform()) + "." + seed
	}

	// Default projection: the state and command roles map to the two plain
	// topics — but only on the platforms that declare them. Ten platforms have
	// no `state_topic` and eleven no `command_topic`: climate, water_heater and
	// lawn_mower name a topic per role instead, and button, scene and notify
	// are write-only. A composite entity on one of those fills its own Fields,
	// which is where the right spelling lives.
	if b, ok := model.Bind(e, model.RoleState); ok && b.Mode.CanRead() && accepts["state_topic"] {
		comp.StateTopic = ctx.StateTopic(b.Slot)
		if accepts["value_template"] {
			comp.ValueTemplate = valueTemplateFor(ctx, desc)
		}
	}
	if b, ok := model.Bind(e, model.RoleCommand); ok && b.Mode.CanWrite() && accepts["command_topic"] {
		comp.CommandTopic = ctx.CommandTopic(b.Slot)
	} else if accepts["command_topic"] {
		// An entity whose only inbound instruction is a named action — a
		// button that runs a program, say — has no writable binding to take a
		// topic from. One method is unambiguous, so it becomes the command
		// topic; several are not, because Home Assistant names a key per
		// action on the platforms that have them, and picking one here would
		// silently make the rest unreachable. Those entities fill their own
		// Fields through a [Builder].
		if inv, ok := e.(model.Invoker); ok {
			if methods := inv.Methods(); len(methods) == 1 {
				comp.CommandTopic = ctx.MethodTopic(dev, e, methods[0])
			}
		}
	}

	if builder, ok := e.(Builder); ok {
		if err := builder.BuildDiscovery(ctx, &comp); err != nil {
			return Component{}, err
		}
	}

	// Merge, do not replace. Extra is the only route for the platform keys that
	// have no typed field, and a Builder is the only stage that holds the
	// Context needed to compute topics for them — so overwriting here closed
	// the escape hatch exactly where it is needed. The description still wins
	// on a shared key, which is the documented precedence.
	for k, v := range desc.Extra {
		if comp.Extra == nil {
			comp.Extra = make(map[string]any, len(desc.Extra))
		}
		comp.Extra[k] = v
	}
	return comp, nil
}

func deviceInfo(dev *model.Device, lang string) DeviceInfo {
	info := DeviceInfo{
		Name:             dev.Name.In(lang),
		Manufacturer:     dev.Manufacturer,
		Model:            dev.Model,
		ModelID:          dev.ModelID,
		SWVersion:        dev.SWVersion,
		HWVersion:        dev.HWVersion,
		SerialNumber:     dev.SerialNumber,
		SuggestedArea:    dev.SuggestedArea,
		ConfigurationURL: dev.ConfigURL,
	}
	for _, id := range dev.Identity.IDs {
		info.Identifiers = append(info.Identifiers, id.String())
	}
	for _, c := range dev.Identity.Connections {
		info.Connections = append(info.Connections, [2]string{c.Type, c.Value})
	}
	if dev.Via != nil {
		info.ViaDevice = dev.Via.UID()
	}
	return info
}

// acceptedKeys caches, per platform, the set of JSON keys Home Assistant's
// discovery schema declares. The catalog is embedded and its decode is cached,
// but the per-platform set is rebuilt on every lookup without this.
var acceptedKeys sync.Map // map[hacatalog.Platform]map[string]bool

// platformAccepts reports which keys a platform's schema declares.
//
// For the two platforms that dispatch to sub-schemas (light, infrared) the
// union of the variants is used: which variant applies depends on a payload
// key, so the union is the closest answer available at projection time.
func platformAccepts(p hacatalog.Platform) (map[string]bool, error) {
	if cached, ok := acceptedKeys.Load(p); ok {
		keys, _ := cached.(map[string]bool)
		return keys, nil
	}

	mqtt, err := hacatalog.LoadMQTT()
	if err != nil {
		return nil, fmt.Errorf("discovery: load catalog: %w", err)
	}
	schema, ok := mqtt.Platforms[string(p)]
	if !ok {
		// An unknown platform is Validate's problem to report, with a better
		// message than anything this could produce. Accept nothing so no
		// illegal key is projected in the meantime.
		return map[string]bool{}, nil
	}

	keys := make(map[string]bool, len(schema.Keys))
	for k := range schema.Keys {
		keys[k] = true
	}
	for _, v := range schema.Variants {
		for k := range v.Keys {
			keys[k] = true
		}
	}
	acceptedKeys.Store(p, keys)
	return keys, nil
}

// RenderComponent renders one entity as a standalone per-entity discovery
// config: the same component a bundle carries, plus the device and origin
// blocks the per-entity form repeats in every config.
//
// It exists because CLAUDE.md's "device-based discovery only" was decided
// before this module had a consumer, and the first full consumer publishes
// the other form. That is not a preference it can revise: its retained
// configs are already on brokers, Home Assistant refuses a bundle while a
// per-entity config for the same entity is still retained, and moving an
// established fleet across is a migration with a measured ordering hazard
// (openccu-loom ADR 0070, amendment of 2026-09-10). A module whose only
// output is the form its consumer cannot publish forces that consumer to
// call [Render], throw the bundle away, pull the components back out of the
// map and re-stamp the frame — post-processing the pipeline, which is the
// pattern the extraction exists to remove.
//
// The bundle path is unchanged and remains the recommendation for a new
// consumer: one retained document per device, updated atomically, instead of
// one per entity each repeating the whole device block.
//
// The returned component KEEPS its platform, and [Component.EntityJSON] is
// what drops it from the bytes.
//
// Both are needed and they are not the same need: a per-entity consumer
// reads the platform to name the topic segment it publishes under, and Home
// Assistant declares the key on no platform and would drop it from the
// payload. An earlier version cleared the field here, which made three
// measured consumers set it again immediately afterwards — a hatch whose
// only job was to undo the pipeline.
func RenderComponent(ctx Context, dev *model.Device, e model.Entity, origin Origin) (Component, error) {
	if dev == nil || !dev.Identity.Valid() {
		return Component{}, fmt.Errorf("discovery: device has no identity")
	}
	comp, err := renderComponent(ctx, dev, e)
	if err != nil {
		return Component{}, fmt.Errorf("discovery: entity %q: %w", e.Key(), err)
	}
	info := NewDeviceInfo(dev, ctx.Language())
	comp.Device = &info
	if origin.Name != "" {
		comp.Origin = &origin
	}
	return comp, nil
}

// DeviceFromInfo is the inverse of [NewDeviceInfo]: a [model.Device] built
// from a device block a consumer already had.
//
// It exists for the middle of a migration. A consumer moving one plane at a
// time still harvests its device blocks the old way, and needs a
// [model.Device] to render the new way; without this, each plane writes the
// same converter under a different name — two measured planes did exactly
// that before this existed.
//
// The identifier is taken verbatim, with no namespace, because that is how
// an existing fleet's identifiers survive — see [model.Identifier.String].
func DeviceFromInfo(info DeviceInfo) *model.Device {
	dev := &model.Device{
		Name:          model.L(info.Name),
		Manufacturer:  info.Manufacturer,
		Model:         info.Model,
		ModelID:       info.ModelID,
		SWVersion:     info.SWVersion,
		HWVersion:     info.HWVersion,
		SerialNumber:  info.SerialNumber,
		SuggestedArea: info.SuggestedArea,
		ConfigURL:     info.ConfigurationURL,
	}
	for _, id := range info.Identifiers {
		dev.Identity.IDs = append(dev.Identity.IDs, model.Identifier{Value: id})
	}
	for _, c := range info.Connections {
		dev.Identity.Connections = append(dev.Identity.Connections,
			model.Connection{Type: c[0], Value: c[1]})
	}
	if info.ViaDevice != "" {
		dev.Via = &model.Identity{IDs: []model.Identifier{{Value: info.ViaDevice}}}
	}
	return dev
}

// NewDeviceInfo builds the `device` block for a device.
//
// Exported for the per-entity form, which needs the same block [Render] puts
// at the top of a bundle — and needs it on every config, which is the cost
// of that form rather than a fault in it.
func NewDeviceInfo(dev *model.Device, lang string) DeviceInfo {
	return deviceInfo(dev, lang)
}
