// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"reflect"

	"github.com/SukramJ/go-hamqtt/model"
)

// RenderFrame renders the frame *under* a component whose own keys are
// authoritative: it returns comp with every key comp leaves unset filled in
// from what [RenderComponent] would have produced for the entity.
//
// # Precedence
//
// comp always wins. A key comp sets is kept exactly as given, including a
// value that merely looks like a default; only a key comp leaves at its zero
// value is taken from the rendered frame. This is the mirror image of
// [model.Description.Extra], which is applied last and overrides everything —
// hence the different name for the different rule.
//
// "Unset" is the Go zero value: the empty string, a nil pointer, a nil slice,
// a nil map, a nil interface, false. A nil slice and an EMPTY one are
// therefore different statements: `Availability: []AvailabilityEntry{}` means
// "this entity has no availability" and is kept, exactly as
// [model.NoAvailability] means it in a description, while a nil one means "no
// opinion" and takes the frame's list. Extra is the one exception and merges
// key by key, comp's key winning, so a frame contributing a key comp never
// mentioned cannot be lost to a single non-nil map.
//
// # The measured need
//
// The combined-projection plane of the first full consumer receives a finished
// [Component] built outside the model — its name, its value template, its
// options and its bounds are all the projection's, and a description
// repeating any of them would be a second source for a string the model layer
// already owns. What it needs from the model is only the frame: the device and
// origin blocks, the state topic, the availability list and mode, the unique
// id. With nothing but [RenderComponent] it rendered that frame and then
// gap-filled by hand — six `if comp.X == ""` lines, one per key, and the
// standing risk of forgetting the seventh when a key is added here.
//
// The fill is reflective for that reason: a key added to [Component] is
// carried by this function on the day it is added, which a hand-written list
// of fields is exactly what fails to do.
func RenderFrame(ctx Context, dev *model.Device, e model.Entity, origin Origin, comp Component) (Component, error) {
	frame, err := RenderComponent(ctx, dev, e, origin)
	if err != nil {
		// RenderComponent has already named the entity and the cause.
		return Component{}, err
	}
	return mergeUnder(comp, frame), nil
}

// mergeUnder fills the unset fields of comp from frame.
//
// Reflection over the struct rather than a field list: the whole point of the
// function is that a key added to [Component] needs no second edit here, and a
// hand-written list is what the consumers were already maintaining by hand.
func mergeUnder(comp, frame Component) Component {
	out := comp
	dst := reflect.ValueOf(&out).Elem()
	src := reflect.ValueOf(frame)

	for i := range dst.NumField() {
		field := dst.Type().Field(i)
		if !field.IsExported() {
			// Component has none today. Skipped rather than assumed, because
			// reflect panics on a Set through an unexported field and that
			// would turn a future private cache into a crash on every render.
			continue
		}
		if field.Name == "Extra" {
			// Merged below, key by key.
			continue
		}
		if dst.Field(i).IsZero() {
			dst.Field(i).Set(src.Field(i))
		}
	}

	out.Extra = mergeExtraUnder(comp.Extra, frame.Extra)
	return out
}

// mergeExtraUnder merges the frame's Extra under the component's own, key by
// key, into a new map.
//
// A new map rather than a write into comp's: the caller's component is a value
// but its map is shared, and filling a key into it would change a map the
// caller may well reuse for the next entity — the kind of aliasing bug that
// only shows up on the second device.
func mergeExtraUnder(own, frame map[string]any) map[string]any {
	if len(own) == 0 && len(frame) == 0 {
		return nil
	}
	out := make(map[string]any, len(own)+len(frame))
	for k, v := range frame {
		out[k] = v
	}
	for k, v := range own {
		out[k] = v
	}
	return out
}
