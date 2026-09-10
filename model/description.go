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
// an opt-out flag, which is why there is no such flag.
type Availability struct {
	// Levels are the sources. Empty means the default: bridge and device.
	Levels []AvailabilityLevel
	// Mode combines them. Empty means [AvailabilityAll].
	Mode AvailabilityMode
}

// Resolved returns the effective levels and mode, applying the defaults.
func (a Availability) Resolved() ([]AvailabilityLevel, AvailabilityMode) {
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

// Ptr returns a pointer to v. It exists because Description's tri-state fields
// are pointers and a struct literal cannot take the address of a constant.
func Ptr[T any](v T) *T { return &v }
