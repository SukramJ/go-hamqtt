// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package model

import hacatalog "github.com/SukramJ/go-ha-catalog"

// DeviceClass is Home Assistant's device class as a plain string.
//
// It is not one of go-ha-catalog's typed vocabularies because there is no
// single such vocabulary: Home Assistant declares a separate device-class enum
// per platform, and "energy" is a sensor class while "garage" is a cover class.
// A single typed field would have to pick one platform and be wrong for the
// rest. Consumers convert at the call site, which keeps the compile-time check
// where the value is written:
//
//	Description{DeviceClass: model.DeviceClass(hacatalog.SensorDeviceClassEnergy)}
//
// [discovery.Validate] checks the value against the entity's actual platform.
type DeviceClass string

// Unit is a unit of measurement.
//
// Deliberately untyped for the same reason go-ha-catalog does not generate unit
// constants: the values are symbols like "°C", "µg/m³" and "W/m²", which make
// poor identifiers. Take them from the catalog's tables rather than typing them
// out — Home Assistant silently rewrites some spellings, and a config that
// disagrees is discarded whole.
type Unit string

// AvailabilityLevel is one source that can mark an entity unavailable.
type AvailabilityLevel uint8

const (
	// LevelBridge is the bridge process itself, via its LWT. Every entity
	// wants this: if the daemon dies, nothing it publishes is current.
	LevelBridge AvailabilityLevel = iota + 1
	// LevelDevice is the owning device's own reachability.
	LevelDevice
	// LevelParent is the device named by [Device.Via]. A switch behind an
	// unreachable gateway is unreachable whatever it last said.
	LevelParent
	// LevelSelf is the entity's own [RoleAvailability] binding, for a
	// datapoint that reports its own validity.
	LevelSelf
	// LevelNone is the explicit absence of availability: no `availability`
	// list and no `availability_mode`, which is not the same as saying
	// nothing (the zero [Availability], which means bridge and device).
	//
	// One measured entity needs it: a bridge's own daemon-status sensor,
	// whose state topic IS the bridge LWT. Gating it on that topic makes it
	// unavailable in exactly the situation it exists to report, so the
	// consumer cleared both keys again after rendering — the only escape
	// hatch of its plane that was not platform vocabulary.
	//
	// It is a level rather than an explicitly-empty Levels slice because
	// nil and empty are the same thing at every call site that builds a
	// list conditionally, and a rule table that filtered its levels down to
	// none would then silently mean "none" where it means "the default".
	// Naming the absence makes it a statement instead of a leftover.
	//
	// LevelNone wins over anything listed beside it: an entity that says it
	// has no availability has none, whatever else the list accumulated.
	LevelNone
)

// AvailabilityMode says how several availability sources combine. The values
// are Home Assistant's own.
type AvailabilityMode string

const (
	// AvailabilityAll requires every source to report available. The default,
	// and the safe one: an entity is only trustworthy if everything between
	// it and the reader is up.
	AvailabilityAll AvailabilityMode = "all"
	// AvailabilityAny needs only one source.
	AvailabilityAny AvailabilityMode = "any"
	// AvailabilityLatest takes whichever source spoke last.
	AvailabilityLatest AvailabilityMode = "latest"
)

// Availability is how an entity reports whether it can be trusted.
//
// It is a list with a mode rather than a single topic because the consumers
// need one, two and three levels respectively, and because the interesting
// case is subtractive: a connectivity sensor whose whole job is to report that
// a device is offline must not itself go unavailable when the device does, or
// it can never deliver the news.
//
// That case is expressed by omission — Levels: {LevelBridge} — rather than by
// an opt-out flag, which is why there is no such flag. Its limit case, an
// entity that must carry no availability at all, is [LevelNone]: see
// [NoAvailability].
type Availability struct {
	// Levels are the sources. Empty means the default: bridge and device.
	Levels []AvailabilityLevel
	// Mode combines them. Empty means [AvailabilityAll].
	Mode AvailabilityMode
}

// Resolved returns the effective levels and mode, applying the defaults.
//
// [LevelNone] resolves to no levels AND no mode, which is what suppresses
// `availability_mode` along with the list — a mode beside an absent list is
// the one combination Home Assistant reads as a contradiction and the reason
// the measured consumer had to clear both by hand.
func (a Availability) Resolved() ([]AvailabilityLevel, AvailabilityMode) {
	for _, l := range a.Levels {
		if l == LevelNone {
			return nil, ""
		}
	}
	levels := a.Levels
	if len(levels) == 0 {
		levels = []AvailabilityLevel{LevelBridge, LevelDevice}
	}
	mode := a.Mode
	if mode == "" {
		mode = AvailabilityAll
	}
	return levels, mode
}

// Has reports whether the resolved levels include l.
func (a Availability) Has(l AvailabilityLevel) bool {
	levels, _ := a.Resolved()
	for _, have := range levels {
		if have == l {
			return true
		}
	}
	return false
}

// NoAvailability is the daemon-status case: an entity that must stay visible
// precisely when the thing an availability topic would gate it on is gone.
// Named so a call site reads as the intent rather than as a one-element list
// literal of a constant called None.
func NoAvailability() Availability {
	return Availability{Levels: []AvailabilityLevel{LevelNone}}
}

// BridgeOnly is the connectivity-sensor case, named so call sites read as the
// intent rather than as a list literal.
func BridgeOnly() Availability {
	return Availability{Levels: []AvailabilityLevel{LevelBridge}}
}

// Description is everything Home Assistant needs to render an entity that is
// not a topic.
//
// There is exactly one description type. The reference implementation this
// model is extracted from had three overlapping ones with a converter between
// two of them and a resolution chain that applied both table systems in
// sequence; collapsing them was a precondition for extraction, not a cleanup
// afterwards.
type Description struct {
	// Name is the entity's display name.
	Name Localized

	// NameKey is a catalogue key resolved through [discovery.Context]'s
	// Translate when Name carries no text. It exists because a rule table
	// names entities by key rather than by string: the table is written once
	// and the catalogues are per-consumer and per-language, so a rule that
	// set a literal name would pin one language into the table.
	//
	// Name wins when both are set — a literal is a deliberate override of
	// whatever the catalogue says, and the other precedence would make the
	// override unreachable. A key with no translator behind it resolves to
	// itself rather than to nothing, so a consumer with no catalogue still
	// renders something readable.
	NameKey string

	// NameArgs fills the placeholders of the string NameKey resolves to,
	// and of a literal Name that carries any: a key whose value is
	// "Connectivity {iface}" renders with NameArgs{"iface": "HmIP-RF"}.
	//
	// Without it a parameterised name has to be resolved eagerly by the
	// consumer and passed as a literal Name, which bypasses the NameKey
	// path entirely — the catalogue key never reaches the model, so nothing
	// downstream can re-render it in another language. Two measured entity
	// families (install-mode and per-interface connectivity) did exactly
	// that, each with its own substitution helper beside the translator.
	//
	// A named map rather than positional arguments because the placeholder
	// is written in the catalogue, by a translator, in a sentence whose word
	// order is not the source language's — a positional %s cannot be moved
	// by the person who has to move it.
	NameArgs map[string]string

	// DeviceClass, StateClass and Unit are Home Assistant's measurement
	// vocabulary. Their legal combinations are not free — see
	// [hacatalog.Relations] — and [discovery.Validate] enforces them.
	DeviceClass DeviceClass
	StateClass  hacatalog.StateClass
	Unit        Unit

	// Icon is an MDI name ("mdi:flash"). Home Assistant derives a sensible
	// one from DeviceClass, so setting this is usually unnecessary.
	Icon string

	// Category demotes an entity to configuration or diagnostics.
	Category hacatalog.EntityCategory

	// Enabled maps to enabled_by_default. Pointer because the tri-state
	// matters: nil defers to Home Assistant's own default, which is not the
	// same as an explicit true.
	Enabled *bool

	// Options is the value vocabulary of a select or an enum sensor.
	Options *Enum

	// Precision is suggested_display_precision.
	Precision *int

	// Min, Max and Step bound a number or a setpoint.
	Min, Max, Step *float64

	// Availability says which sources gate this entity.
	Availability Availability

	// Optimistic maps to `optimistic`: Home Assistant assumes a command
	// took effect instead of waiting for the state topic to confirm it.
	//
	// It is not platform-specific vocabulary — switch, select, text and
	// number all declare it, 17 platforms in total — but it was reachable
	// only through a [discovery.Builder] or Extra, which is why eight
	// measured hub entities set it after rendering. Pointer for the same
	// reason Enabled is: nil defers to Home Assistant's default, which is
	// not the same as an explicit false.
	//
	// It is projected only onto the platforms whose schema declares it; the
	// rest silently drop it, so emitting it there would be invisible noise.
	Optimistic *bool

	// JSONAttributesTopic and JSONAttributesTemplate attach a whole JSON
	// document to the entity as attributes — the way a consumer publishes a
	// datapoint's descriptor, or an aggregate's detail rows, without
	// inventing an entity per field.
	//
	// 30 of the 32 platforms accept them, the same bar the other
	// description-level keys meet, and three measured message aggregates
	// set them in a post-render builder because the description could not.
	// The template is only meaningful beside the topic and is projected
	// only with it.
	JSONAttributesTopic    string
	JSONAttributesTemplate string

	// ValueTemplate overrides the template the render pipeline would derive
	// from the context's encoding.
	//
	// [discovery.Context]'s Encoding is one answer for a whole consumer, and
	// a consumer is rarely uniform: the same bridge publishes bare datapoint
	// topics, composite entities whose value is assembled from several
	// fields by a hand-written template, and event entities that must carry
	// no template at all. One global answer is wrong for two of those three
	// whichever way it is set — five of one measured consumer's eleven
	// discovery planes reported exactly that.
	//
	// Set [NoValueTemplate] to publish none. Leaving this empty keeps the
	// derived behaviour, which is what a uniform consumer wants and why the
	// zero value is not "suppress".
	//
	// It is a template string rather than a topic, so it does not make
	// `model` depend on `topic` — the rule the package layout enforces.
	ValueTemplate string

	// Multiplier scales the datapoint's value before it is published, and
	// the bounds along with it. Nil means no scaling, which is not the same
	// as 1.0 — a rule that says nothing about scale and one that pins it to
	// unity are different statements in a table where a later rule can
	// refine an earlier one.
	//
	// The model carries it and never applies it. Only the consumer's value
	// layer knows whether a given publish is a value at all, what its wire
	// type is, and whether a template or a plain number is what Home
	// Assistant should receive.
	Multiplier *float64

	// Extra carries platform keys the model does not model. Applied last in
	// the discovery pipeline, so it wins over everything — which makes it
	// both the escape hatch and the footgun.
	Extra map[string]any
}

// Clone returns a deep-enough copy for an enricher to mutate without touching
// the original: the maps and slices an enricher is likely to write are copied,
// the immutable values are shared.
func (d *Description) Clone() *Description {
	if d == nil {
		return nil
	}
	out := *d
	if d.Name.Lang != nil {
		out.Name.Lang = make(map[string]string, len(d.Name.Lang))
		for k, v := range d.Name.Lang {
			out.Name.Lang[k] = v
		}
	}
	if d.NameArgs != nil {
		out.NameArgs = make(map[string]string, len(d.NameArgs))
		for k, v := range d.NameArgs {
			out.NameArgs[k] = v
		}
	}
	if d.Availability.Levels != nil {
		out.Availability.Levels = append([]AvailabilityLevel(nil), d.Availability.Levels...)
	}
	if d.Extra != nil {
		out.Extra = make(map[string]any, len(d.Extra))
		for k, v := range d.Extra {
			out.Extra[k] = v
		}
	}
	if d.Options != nil {
		opts := *d.Options
		opts.Codes = append([]string(nil), d.Options.Codes...)
		if d.Options.Labels != nil {
			opts.Labels = make(map[string]Localized, len(d.Options.Labels))
			for k, v := range d.Options.Labels {
				opts.Labels[k] = v
			}
		}
		out.Options = &opts
	}
	return &out
}

// NoValueTemplate is the [Description.ValueTemplate] value that publishes no
// template at all.
//
// A sentinel rather than a second boolean field, because the two states this
// has to distinguish from each other are "say nothing, let the pipeline
// decide" and "say explicitly that there is none" — and an empty string
// already means the first. An event entity that inherited a value template
// would read its payload through a filter that does not apply to it.
const NoValueTemplate = "-"

// Ptr returns a pointer to v. It exists because Description's tri-state fields
// are pointers and a struct literal cannot take the address of a constant.
func Ptr[T any](v T) *T { return &v }
