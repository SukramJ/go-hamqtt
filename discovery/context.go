// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"strconv"
	"strings"

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
	//
	// It stays a plain key -> string lookup although [Translate] now takes
	// arguments: the catalogue answers with the template as authored, and
	// the substitution is [StdContext]'s job. A consumer whose translator
	// already exists therefore keeps it unchanged and gains the parameters
	// for free — which is what the measured consumer's own two-function
	// pairing (a lookup plus a placeholder helper beside it) collapses to.
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

// NodeID implements [Context], from this package's [NodeID].
func (c StdContext) NodeID(dev *model.Device) string { return NodeID(dev) }

// ObjectID implements [Context], from this package's [ObjectID].
//
// A consumer that publishes no entity-id seed today overrides this to return
// the empty string rather than accepting one: the key is not decoration, it
// decides the entity id, and Home Assistant will not rename an entity back.
func (c StdContext) ObjectID(dev *model.Device, e model.Entity) string {
	return ObjectID(dev, e)
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
// rather than an empty string — placeholders included, so a missing
// catalogue entry degrades to a readable key rather than to a half-filled
// sentence.
func (c StdContext) Translate(key string, args map[string]string) string {
	text := key
	if c.Translator != nil {
		text = c.Translator(key)
	}
	return Substitute(text, args)
}

// Substitute fills `{name}` placeholders in text from args.
//
// Exported because a consumer that overrides [Context.Translate] — to reach
// its own catalogue, or to pick a plural form — still wants the same
// substitution the default does, and writing it again is how two call sites
// end up disagreeing about the placeholder syntax. The measured consumer had
// a private helper doing exactly this, for exactly one placeholder.
//
// A placeholder with no matching argument is left standing rather than
// blanked: an entity named "Connectivity {iface}" says where the gap is,
// while "Connectivity " says only that something went wrong somewhere.
func Substitute(text string, args map[string]string) string {
	if len(args) == 0 || !strings.ContainsRune(text, '{') {
		return text
	}
	pairs := make([]string, 0, 2*len(args))
	for k, v := range args {
		pairs = append(pairs, "{"+k+"}", v)
	}
	return strings.NewReplacer(pairs...).Replace(text)
}

// deviceSlot is the device-level coordinate an availability topic is
// rendered from: the device's address, in the containers its entities sit in.
//
// The scope is taken from what the entity binds rather than from the device,
// because that is where it lives — a [model.Slot] is a complete coordinate by
// design, and [model.Device] deliberately carries no scope of its own. An
// entity with no bindings leaves it empty, which is right for a consumer
// whose devices hang off the root.
func deviceSlot(dev *model.Device, e model.Entity) model.Slot {
	return DeviceSlot(dev, e)
}

// DeviceSlot is the coordinate [model.LevelDevice] resolves against: the
// device's address, in the containers its entities sit in.
//
// Exported because the publishing side must not be able to address a
// different topic than the config it answers. It is not a plain device
// identity: the scope and the channel come from what the entity binds,
// because [model.Device] deliberately carries no scope of its own. A
// consumer that rebuilt the slot by hand would get the device root instead,
// which renders a topic no config names — and an entity whose availability
// topic nobody publishes to is one Home Assistant greys out forever.
//
// The parent topic of [model.LevelParent] is this same slot with its Address
// replaced by dev.Via.UID(): the address changes, the containers do not. It
// is not this call on dev.Via — [model.Device.Via] is a [model.Identity],
// which carries no scope and is not a [model.Device], so that expression does
// not compile. The publishing side has the derivation as
// publisher.ParentSlot, so a consumer does not rebuild it by hand.
func DeviceSlot(dev *model.Device, e model.Entity) model.Slot {
	if dev == nil {
		return model.Slot{}
	}
	s := model.Slot{Address: dev.UID()}
	if e == nil {
		return s
	}
	if binds := e.Bindings(); len(binds) > 0 {
		s.Scope = binds[0].Slot.Scope
		// The channel travels with the scope. A consumer whose availability
		// is per channel rather than per device — one measured plane
		// publishes it per alarm zone — could otherwise not reach its own
		// topic from LevelDevice however the slot was filled, short of
		// smuggling the segment into Scope, which inverts what Scope means.
		// A Layout that does not want it ignores it, exactly as it ignores
		// Bucket and Path.
		s.Channel = binds[0].Slot.Channel
	}
	return s
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
//
// Under a [topic.SmartHomeLayout] the bridge and device levels change
// vocabulary, not shape: the bridge entry reads `<name>/connected` through
// [ConnectedAvailability] (available at 2), and a device entry reads the
// device's `online` status item through [OnlineAvailability].
func (c StdContext) Availability(dev *model.Device, e model.Entity) []AvailabilityEntry {
	return c.availability(dev, e, func(dev *model.Device, e model.Entity) (model.Slot, bool) {
		return deviceSlot(dev, e), true
	}, false)
}

// AvailabilityFrom is [StdContext.Availability] with the device level
// resolved from a slot the caller chooses instead of [DeviceSlot].
//
// It exists for a consumer whose device-level availability item is not keyed
// on the device's identity: [DeviceSlot] addresses the device by
// [model.Device.UID] — typically the discovery identifier, such as
// `bridge_<serial>` — while the consumer's own tree keys the item on the
// bare serial, MAC or client key that only the entity's binding carries. Such
// a consumer used to override [Context.Availability] wholesale to change that
// one coordinate, and with it re-implement the level loop, the smart-home
// vocabulary switch and the parent level; it can now delegate:
//
//	func (c ctx) Availability(dev *model.Device, e model.Entity) []discovery.AvailabilityEntry {
//		return c.StdContext.AvailabilityFrom(dev, e, discovery.BindingSlot)
//	}
//
// Every level renders exactly as [StdContext.Availability] renders it, with
// two differences, both about the device and parent levels: the slot comes
// from device (which may answer false to skip the level), and a level whose
// slot the layout renders as no topic at all is skipped rather than emitted
// with an empty topic — a resolver that names a slot for every entity cannot
// know which of them have an item. [model.LevelParent] takes the resolved
// slot with its Address replaced by the parent's UID, as
// [StdContext.Availability] does.
func (c StdContext) AvailabilityFrom(
	dev *model.Device,
	e model.Entity,
	device func(*model.Device, model.Entity) (model.Slot, bool),
) []AvailabilityEntry {
	if device == nil {
		return c.Availability(dev, e)
	}
	return c.availability(dev, e, device, true)
}

// BindingSlot resolves an entity's device level to the slot of its first
// binding, for [StdContext.AvailabilityFrom]: the coordinate the consumer's
// own tree addresses the entity's object by. An entity with no binding has
// no device level.
func BindingSlot(_ *model.Device, e model.Entity) (model.Slot, bool) {
	if e == nil {
		return model.Slot{}, false
	}
	binds := e.Bindings()
	if len(binds) == 0 {
		return model.Slot{}, false
	}
	return binds[0].Slot, true
}

func (c StdContext) availability(
	dev *model.Device,
	e model.Entity,
	resolve func(*model.Device, model.Entity) (model.Slot, bool),
	skipEmpty bool,
) []AvailabilityEntry {
	levels, _ := e.Desc().Availability.Resolved()
	out := make([]AvailabilityEntry, 0, len(levels))

	_, smartHome := c.Layout.(topic.SmartHomeLayout)
	bridge, device := plainAvailability, plainAvailability
	if smartHome {
		bridge = func(t string) AvailabilityEntry { return ConnectedAvailability(t, ConnectedOperational) }
		device = func(t string) AvailabilityEntry { return OnlineAvailability(t, c.Enc) }
	}

	for _, level := range levels {
		switch level {
		case model.LevelBridge:
			out = append(out, bridge(c.Layout.Bridge()))

		case model.LevelDevice:
			slot, ok := resolve(dev, e)
			if !ok {
				continue
			}
			if t := c.Layout.Availability(slot); t != "" || !skipEmpty {
				out = append(out, device(t))
			}

		case model.LevelParent:
			if dev.Via != nil {
				parent, ok := resolve(dev, e)
				if !ok {
					continue
				}
				parent.Address = dev.Via.UID()
				if t := c.Layout.Availability(parent); t != "" || !skipEmpty {
					out = append(out, device(t))
				}
			}

		case model.LevelSelf:
			if entry, ok := c.selfAvailability(e); ok {
				out = append(out, entry)
			}

		case model.LevelNone:
			// Unreachable: [model.Availability.Resolved] answers a list
			// containing it with no levels at all. Named anyway so the
			// exhaustiveness check keeps pointing here if that changes.
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
		switch c.Enc {
		case EnvelopeEncoding:
			entry.ValueTemplate = SelfAvailabilityTemplate
		case StatusObjectEncoding:
			entry.ValueTemplate = StatusBoolValueTemplate
		case RawEncoding:
		}
		return entry, true
	}

	// The fallback reads the envelope's own `available` flag. Neither the
	// bare value nor the status object carries one.
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

// The levels of mqtt-smarthome's `<name>/connected` (spec §3.1).
const (
	// ConnectedBroker is 1: connected to the broker, the hardware or
	// upstream service not reachable.
	ConnectedBroker = 1
	// ConnectedOperational is 2: fully operational, and the level an
	// entity is available at by default (spec §8).
	ConnectedOperational = 2
)

// ConnectedTemplate renders the availability `value_template` that turns the
// plain integer on `<name>/connected` into [PayloadOnline] or
// [PayloadOffline], available from atLeast upwards. A payload that is not a
// number — an empty retained clear — reads as 0.
func ConnectedTemplate(atLeast int) string {
	return "{{ 'online' if value | int(0) >= " + strconv.Itoa(atLeast) + " else 'offline' }}"
}

// ConnectedAvailability is the availability entry for `<name>/connected`,
// available from atLeast upwards. [StdContext] renders it at
// [ConnectedOperational]; spec §8 allows [ConnectedBroker] for an entity that
// works without the device — a wake-on-LAN switch — which a [Builder] sets
// by replacing that entry.
//
// The entry carries the four keys of a list entry and nothing else. Spec §8
// is explicit that `availability_template` belongs to the single-topic form.
// Inside a component, Home Assistant strips any other key from a list entry
// (the platform schemas are `extra=REMOVE_EXTRA`, and that reaches the
// nested entries of components/mqtt/schemas.py:100-114), so the key would
// silently do nothing; only in a document-level `availability` list, which
// [Bundle] does not produce, would it refuse the whole document.
func ConnectedAvailability(t string, atLeast int) AvailabilityEntry {
	return AvailabilityEntry{
		Topic:               t,
		ValueTemplate:       ConnectedTemplate(atLeast),
		PayloadAvailable:    PayloadOnline,
		PayloadNotAvailable: PayloadOffline,
	}
}

// OnlineAvailability is the availability entry for a device's `online`
// status item: true or false, read through [StatusBoolValueTemplate] when
// the item is published as a status object and compared bare otherwise.
func OnlineAvailability(t string, enc Encoding) AvailabilityEntry {
	entry := AvailabilityEntry{
		Topic:               t,
		PayloadAvailable:    PayloadTrue,
		PayloadNotAvailable: PayloadFalse,
	}
	if enc == StatusObjectEncoding {
		entry.ValueTemplate = StatusBoolValueTemplate
	}
	return entry
}

func plainAvailability(t string) AvailabilityEntry {
	return AvailabilityEntry{
		Topic:               t,
		PayloadAvailable:    PayloadOnline,
		PayloadNotAvailable: PayloadOffline,
	}
}
