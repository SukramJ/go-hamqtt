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

	// Translate resolves a catalogue key into the context's language.
	//
	// [Language] alone is not enough: the catalogues live with the consumer,
	// so the model can ask for a label but cannot look one up. A key with no
	// entry comes back unchanged, which is what lets a caller tell a missing
	// translation from an empty one.
	Translate(key string) string
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
const AvailabilityTemplate = `{{ value_json.available | lower }}`

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
		NodeID:     NodeID(dev),
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

func renderComponent(ctx Context, dev *model.Device, e model.Entity) (Component, error) {
	desc := e.Desc()
	if desc == nil {
		return Component{}, fmt.Errorf("nil description")
	}
	lang := ctx.Language()

	comp := Component{
		Platform:         e.Platform(),
		Name:             desc.Name.In(lang),
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
	comp.DefaultEntityID = string(e.Platform()) + "." + ObjectID(dev, e)

	// Default projection: the state and command roles map to the two plain
	// topics — but only on the platforms that declare them. Ten platforms have
	// no `state_topic` and eleven no `command_topic`: climate, water_heater and
	// lawn_mower name a topic per role instead, and button, scene and notify
	// are write-only. A composite entity on one of those fills its own Fields,
	// which is where the right spelling lives.
	if b, ok := model.Bind(e, model.RoleState); ok && b.Mode.CanRead() && accepts["state_topic"] {
		comp.StateTopic = ctx.StateTopic(b.Slot)
		if ctx.Encoding() == EnvelopeEncoding && accepts["value_template"] {
			comp.ValueTemplate = ValueTemplate
		}
	}
	if b, ok := model.Bind(e, model.RoleCommand); ok && b.Mode.CanWrite() && accepts["command_topic"] {
		comp.CommandTopic = ctx.CommandTopic(b.Slot)
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
