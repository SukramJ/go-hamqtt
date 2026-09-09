// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"fmt"

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
		Min:              desc.Min,
		Max:              desc.Max,
		Step:             desc.Step,
		Availability:     ctx.Availability(dev, e),
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
	// topics. A composite entity has neither and fills its own Fields.
	if b, ok := model.Bind(e, model.RoleState); ok && b.Mode.CanRead() {
		comp.StateTopic = ctx.StateTopic(b.Slot)
		if ctx.Encoding() == EnvelopeEncoding {
			comp.ValueTemplate = ValueTemplate
		}
	}
	if b, ok := model.Bind(e, model.RoleCommand); ok && b.Mode.CanWrite() {
		comp.CommandTopic = ctx.CommandTopic(b.Slot)
	}

	if builder, ok := e.(Builder); ok {
		if err := builder.BuildDiscovery(ctx, &comp); err != nil {
			return Component{}, err
		}
	}
	comp.Extra = desc.Extra
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
