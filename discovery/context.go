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
			b, ok := model.Bind(e, model.RoleAvailability)
			if !ok {
				// Fall back to the state binding: with an envelope, the state
				// payload already carries the availability flag, so a
				// datapoint that reports its own validity needs no second
				// topic. With raw encoding there is nothing to read, so the
				// level is dropped.
				if c.Enc != EnvelopeEncoding {
					continue
				}
				b, ok = model.Bind(e, model.RoleState)
				if !ok {
					continue
				}
			}
			out = append(out, AvailabilityEntry{
				Topic:               c.Layout.State(b.Slot),
				ValueTemplate:       AvailabilityTemplate,
				PayloadAvailable:    "true",
				PayloadNotAvailable: "false",
			})
		}
	}
	return out
}

func plainAvailability(t string) AvailabilityEntry {
	return AvailabilityEntry{
		Topic:               t,
		PayloadAvailable:    PayloadOnline,
		PayloadNotAvailable: PayloadOffline,
	}
}
