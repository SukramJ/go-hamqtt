// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// StdContext is the [Context] a consumer gets by naming its layout, its
// unique-id namespace and its language.
//
// It is a struct rather than an interface implementation hidden behind a
// constructor so a consumer can override one method by embedding it.
type StdContext struct {
	// Layout renders topics.
	Layout topic.Layout
	// Namespace prefixes every unique id. Must be a constant of the bridge —
	// see [UniqueID].
	Namespace string
	// Lang is the locale for display strings. Empty means the default.
	Lang string
	// Enc selects the state payload shape. The zero value is
	// [EnvelopeEncoding].
	Enc Encoding
	// Translator resolves a catalogue key into [Lang]. Nil means the key is
	// its own label, which is what a consumer with no catalogue wants.
	Translator func(key string) string
}

var _ Context = StdContext{}

// StateTopic implements [Context].
func (c StdContext) StateTopic(s model.Slot) string { return c.Layout.State(s) }

// CommandTopic implements [Context].
func (c StdContext) CommandTopic(s model.Slot) string { return c.Layout.Command(s) }

// UniqueID implements [Context].
func (c StdContext) UniqueID(dev *model.Device, e model.Entity) string {
	return UniqueID(c.Namespace, dev, e)
}

// Language implements [Context].
func (c StdContext) Language() string { return c.Lang }

// Encoding implements [Context].
func (c StdContext) Encoding() Encoding { return c.Enc }

// EntityStateTopic implements [Context].
//
// The aggregate is addressed as [model.BucketCustom] on the entity's own
// channel, with the entity key as the path — the coordinate a composite would
// use for a datapoint it owns rather than reads. Deriving it from the first
// binding keeps scope, address and channel identical to the parts, so the
// aggregate lands beside them in the tree instead of at the device root.
func (c StdContext) EntityStateTopic(dev *model.Device, e model.Entity) string {
	return c.Layout.State(entitySlot(dev, e))
}

// MethodTopic implements [Context].
//
// The method is one segment below the aggregate's command topic, so one
// wildcard subscription covers every method an entity declares — a consumer
// that subscribed per method would have to know the list up front and would
// silently ignore a method it had not enumerated.
func (c StdContext) MethodTopic(dev *model.Device, e model.Entity, method string) string {
	base := c.Layout.Command(entitySlot(dev, e))
	if method == "" {
		return base
	}
	return base + "/" + topic.Safe(method)
}

// Translate implements [Context]. Without a translator a key is its own
// label, so a consumer with no catalogue still renders something readable
// rather than an empty string.
func (c StdContext) Translate(key string) string {
	if c.Translator == nil {
		return key
	}
	return c.Translator(key)
}

// entitySlot is the coordinate of an entity's own aggregate.
func entitySlot(dev *model.Device, e model.Entity) model.Slot {
	slot := model.Slot{Bucket: model.BucketCustom, Path: []string{e.Key()}}
	if dev != nil {
		slot.Address = dev.UID()
	}
	// Inherit the scope and channel of what the entity binds, so the
	// aggregate sits with its parts. A composite spanning channels takes the
	// first, which is the one its key is scoped to.
	if binds := e.Bindings(); len(binds) > 0 {
		slot.Scope = binds[0].Slot.Scope
		slot.Channel = binds[0].Slot.Channel
		if slot.Address == "" {
			slot.Address = binds[0].Slot.Address
		}
	}
	return slot
}

// Availability implements [Context], turning the entity's declared levels into
// Home Assistant's availability list.
//
// A level that cannot be resolved is skipped rather than rendered as a broken
// topic: [model.LevelParent] on a device with no parent, or [model.LevelSelf]
// on an entity with no availability binding, are both normal for an entity
// whose description was written once and reused across device shapes.
func (c StdContext) Availability(dev *model.Device, e model.Entity) []AvailabilityEntry {
	levels, _ := e.Desc().Availability.Resolved()
	out := make([]AvailabilityEntry, 0, len(levels))

	for _, level := range levels {
		switch level {
		case model.LevelBridge:
			out = append(out, plainAvailability(c.Layout.Bridge()))

		case model.LevelDevice:
			out = append(out, plainAvailability(c.Layout.Availability(dev.Identity)))

		case model.LevelParent:
			if dev.Via != nil {
				out = append(out, plainAvailability(c.Layout.Availability(*dev.Via)))
			}

		case model.LevelSelf:
			if entry, ok := c.selfAvailability(e); ok {
				out = append(out, entry)
			}
		}
	}
	return out
}

// selfAvailability resolves [model.LevelSelf], which has two shapes that read
// different things and must not be conflated.
//
// An explicit [model.RoleAvailability] binding is a datapoint whose *value*
// says whether the entity is available. Its envelope's own `available` flag
// says something else entirely — whether that datapoint is itself reachable —
// so the template reads `.value`, exactly as any other state binding does.
// Reading `.available` here would make the explicit binding indistinguishable
// from the fallback below except for which topic it points at, and would
// answer a question nobody asked.
//
// Without such a binding the fallback reads the state datapoint's envelope
// flag, where `.available` is the right field. That shape needs the envelope:
// a raw payload carries no flag, and the level is dropped.
func (c StdContext) selfAvailability(e model.Entity) (AvailabilityEntry, bool) {
	if b, ok := model.Bind(e, model.RoleAvailability); ok {
		entry := AvailabilityEntry{
			Topic:               c.Layout.State(b.Slot),
			PayloadAvailable:    "true",
			PayloadNotAvailable: "false",
		}
		// Raw encoding publishes the bare boolean, so there is nothing to
		// reach into. Templating it anyway renders `value_json` undefined,
		// which matches neither payload — and Home Assistant ignores an
		// availability payload it does not recognise, leaving the entity
		// permanently unavailable with nothing on the wire to show why.
		if c.Enc == EnvelopeEncoding {
			entry.ValueTemplate = SelfAvailabilityTemplate
		}
		return entry, true
	}

	if c.Enc != EnvelopeEncoding {
		return AvailabilityEntry{}, false
	}
	b, ok := model.Bind(e, model.RoleState)
	if !ok {
		return AvailabilityEntry{}, false
	}
	return AvailabilityEntry{
		Topic:               c.Layout.State(b.Slot),
		ValueTemplate:       AvailabilityTemplate,
		PayloadAvailable:    "true",
		PayloadNotAvailable: "false",
	}, true
}

func plainAvailability(t string) AvailabilityEntry {
	return AvailabilityEntry{
		Topic:               t,
		PayloadAvailable:    PayloadOnline,
		PayloadNotAvailable: PayloadOffline,
	}
}
