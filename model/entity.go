// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package model

import (
	"context"
	"errors"

	hacatalog "github.com/SukramJ/go-ha-catalog"
)

// Errors a consumer's capability implementations are expected to return.
var (
	// ErrUnknownRole is returned by a [Commander] handed a role it does not
	// implement.
	ErrUnknownRole = errors.New("hamqtt: unknown role")
	// ErrReadOnly is returned when a command reaches an entity that has no
	// writable binding for it.
	ErrReadOnly = errors.New("hamqtt: entity is read-only")
)

// Entity is one thing Home Assistant shows. The interface is small on purpose:
// everything optional is a separate capability interface, so an entity that
// needs none of them is four methods, and adding a capability later never
// breaks an existing implementation.
type Entity interface {
	// Key is unique among the entities of one device. It becomes the object
	// id suffix and the suppression key, so it is a stable identifier and not
	// a display string.
	Key() string
	// Platform is the Home Assistant platform this entity renders as.
	Platform() hacatalog.Platform
	// Desc returns the description, by pointer: enrichers mutate it in place,
	// which is what lets a catalog default and an operator override compose
	// without either knowing about the other.
	Desc() *Description
	// Bindings are the datapoints this entity reads and writes.
	Bindings() []Binding
}

// Basic is the struct form of [Entity], for catalogs and code generation.
// Behaviour opts in by embedding Basic in a type that also implements the
// capability interfaces.
type Basic struct {
	EntityKey      string
	EntityPlatform hacatalog.Platform
	Description    Description
	Binds          []Binding
}

// Key implements [Entity].
func (b *Basic) Key() string { return b.EntityKey }

// Platform implements [Entity].
func (b *Basic) Platform() hacatalog.Platform { return b.EntityPlatform }

// Desc implements [Entity].
func (b *Basic) Desc() *Description { return &b.Description }

// Bindings implements [Entity].
func (b *Basic) Bindings() []Binding { return b.Binds }

// Bind returns the binding for role, if the entity has one.
func Bind(e Entity, role string) (Binding, bool) {
	for _, b := range e.Bindings() {
		if b.Role == role {
			return b, true
		}
	}
	return Binding{}, false
}

// Command is one inbound instruction from Home Assistant.
//
// There is one inbound path, not two. The reference implementation had
// commands and a separate service registry, and that duplication is what grew
// the two contradictory discovery interfaces this model replaced with one — so
// a service here is simply a button or a select entity with a [Commander].
type Command struct {
	// Entity is the entity the command addressed.
	Entity Entity
	// Role is the writable binding it arrived on.
	Role string
	// Slot is that binding's datapoint.
	Slot Slot
	// Payload is the raw bytes Home Assistant sent.
	Payload []byte
}

// ---------------------------------------------------------------------------
// capability interfaces
// ---------------------------------------------------------------------------

// Commander handles inbound commands itself instead of letting the runtime
// write the bound datapoint directly.
//
// Implement it when the mapping is not one-to-one: Home Assistant's climate
// "off" mode is a power write, not a mode write, and only the entity knows
// that.
type Commander interface {
	Command(ctx context.Context, cmd Command) error
}

// Suppressor names entities of the same device that this one replaces.
//
// A composite climate entity consumes the datapoints a catalog would otherwise
// expose as separate sensors and selects; without suppression Home Assistant
// shows both, and a user sees a thermostat next to four loose controls that
// move it.
//
// Suppression hides the *entity*, not the datapoint: the suppressed slots keep
// publishing, because the composite's own discovery references those topics.
type Suppressor interface {
	Suppresses() []string
}

// Deriver computes one role's state from several bound datapoints.
//
// The map is keyed by role. Returning false means "nothing to publish yet",
// which is the correct answer while an input is still missing — publishing a
// derived value from incomplete inputs is how wrong numbers reach Home
// Assistant's long-term statistics, where they cannot be corrected.
type Deriver interface {
	Derive(states map[string]State) (State, bool)
}

// EntitySource produces the entities of a device.
//
// It is an interface rather than a catalog loader because the consumers get
// their entity sets three fundamentally different ways: a static catalog, a
// profile read from the device itself, and expansion over discovered hardware.
// Only the first is a file.
type EntitySource interface {
	Entities(ctx context.Context, dev *Device) ([]Entity, error)
}

// EntitySourceFunc adapts a function to [EntitySource].
type EntitySourceFunc func(ctx context.Context, dev *Device) ([]Entity, error)

// Entities implements [EntitySource].
func (f EntitySourceFunc) Entities(ctx context.Context, dev *Device) ([]Entity, error) {
	return f(ctx, dev)
}

// Enricher fills in or overrides parts of an entity's description.
//
// Enrichers run as an ordered chain, later winning, which is what makes
// catalog defaults and operator overrides the same mechanism at different
// positions rather than two systems that have to agree.
type Enricher interface {
	Enrich(dev *Device, e Entity) error
}

// EnricherFunc adapts a function to [Enricher].
type EnricherFunc func(dev *Device, e Entity) error

// Enrich implements [Enricher].
func (f EnricherFunc) Enrich(dev *Device, e Entity) error { return f(dev, e) }

// Enrich runs a chain in order. It stops at the first error, because a
// half-applied chain would produce an entity nobody declared.
func Enrich(dev *Device, e Entity, chain ...Enricher) error {
	for _, en := range chain {
		if en == nil {
			continue
		}
		if err := en.Enrich(dev, e); err != nil {
			return err
		}
	}
	return nil
}

// Suppressed returns the set of entity keys that the given entities suppress.
func Suppressed(entities []Entity) map[string]struct{} {
	out := map[string]struct{}{}
	for _, e := range entities {
		s, ok := e.(Suppressor)
		if !ok {
			continue
		}
		for _, key := range s.Suppresses() {
			// An entity suppressing itself is a bug that would silently
			// delete the composite; ignoring it keeps the composite and lets
			// a test catch the mistake.
			if key == e.Key() {
				continue
			}
			out[key] = struct{}{}
		}
	}
	return out
}

// ApplySuppression returns entities with the suppressed ones removed, in the
// original order.
func ApplySuppression(entities []Entity) []Entity {
	suppressed := Suppressed(entities)
	if len(suppressed) == 0 {
		return entities
	}
	out := make([]Entity, 0, len(entities))
	for _, e := range entities {
		if _, hidden := suppressed[e.Key()]; hidden {
			continue
		}
		out = append(out, e)
	}
	return out
}
