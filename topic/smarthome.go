// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package topic

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/SukramJ/go-hamqtt/model"
)

// The functions of the mqtt-smarthome 2.0 grammar `<name>/<function>/<item...>`
// (spec §3), plus `meta`, the one function this project family adds: the
// retained descriptor companion of a status item, under the same item path.
//
// `get` is reserved by the spec and implemented by nobody; it is named so a
// guard against colliding first-level items can refuse it too.
const (
	FunctionConnected   = "connected"
	FunctionStatus      = "status"
	FunctionSet         = "set"
	FunctionGet         = "get"
	FunctionInfo        = "info"
	FunctionMeta        = "meta"
	FunctionMaintenance = "maintenance"
)

var functions = []string{
	FunctionConnected, FunctionStatus, FunctionSet, FunctionGet,
	FunctionInfo, FunctionMeta, FunctionMaintenance,
}

// IsFunction reports whether s is one of the function names above.
//
// It is what a consumer's reserved-name guard calls (openccu-loom ADR 0083):
// an identifier an operator chooses — a controller name, a site — that sits
// at the item level beside literal sub-trees must not spell a function, or a
// migration sweep that tells new topics from old ones by their second level
// could mistake one for the other.
func IsFunction(s string) bool { return slices.Contains(functions, s) }

// ErrInvalidName is returned for an instance name the convention does not
// allow.
var ErrInvalidName = errors.New("topic: invalid mqtt-smarthome instance name")

// SmartHomeLayout is a [Layout] that follows mqtt-smarthome 2.0, and is the
// one switch the rest of the module reads to change vocabulary:
//
//   - [Layout.Bridge] is `<name>/connected` and carries the plain integer
//     0/1/2 rather than online/offline, so the publisher's Last Will and its
//     announcements write numbers and the discovery availability entry reads
//     them through a template;
//   - [Layout.Availability] is the device's `online` status item, a boolean
//     status object rather than an online/offline marker;
//   - the instance has an `info` topic and maintenance topics.
//
// A capability interface rather than a field on the consumers of a layout,
// for the reason [PulseLayout] is one: every existing layout keeps meaning
// exactly what it meant. A consumer that wraps [SmartHome] in a layout of its
// own — every one of the six does wrap its layout — forwards the three
// methods below, or embeds [SmartHome] and inherits them. Asserting
// `var _ topic.SmartHomeLayout = myLayout{}` is how a wrapper finds out at
// compile time that it dropped one.
type SmartHomeLayout interface {
	Layout
	// Connected is `<name>/connected`. It must equal [Layout.Bridge]; the
	// publisher refuses a layout where it does not.
	Connected() string
	// Info is `<name>/info`.
	Info() string
	// Maintenance is `<name>/maintenance/<item...>`.
	Maintenance(item ...string) string
}

// SmartHome renders the mqtt-smarthome 2.0 grammar:
//
//	<name>/status/<item...>                 State, retained status items and pulses
//	<name>/set/<item...>                    Command, same item path
//	<name>/meta/<item...>                   descriptor companion, same item path
//	<name>/status/<scope...>/<uid>/online   Availability
//	<name>/connected                        Bridge, 0/1/2
//	<name>/info                             instance introspection
//	<name>/maintenance/<item...>            maintenance topics
//
// The item path of a [model.Slot] is [SmartHome.Item]: its scope, address,
// channel, bucket and path, which is the order [Default] uses below its root.
// A consumer whose item tree is shaped differently — a literal sub-tree, a
// family selected by the first scope segment, a pack nested under a group —
// embeds SmartHome, overrides State and Command, and builds its own item path
// with [SmartHome.Status] and [SmartHome.Set], which take items directly.
//
// Every item segment goes through [Safe]; the name does not, because a
// multi-level base would not survive it. The zero value has no name and
// renders nothing; construct one with [NewSmartHome].
type SmartHome struct {
	name       string
	conformant bool
}

var (
	_ SmartHomeLayout = SmartHome{}
	_ PulseLayout     = SmartHome{}
)

// NewSmartHome returns the layout for one instance name, validated per spec
// §3: non-empty, and free of `/`, `+` and `#`. NUL is refused because MQTT
// forbids it in any topic, and a leading `$` because MQTT reserves those
// topics for the broker — a tool scanning `+/info` skips them too.
func NewSmartHome(name string) (SmartHome, error) {
	if err := checkName(name, false); err != nil {
		return SmartHome{}, err
	}
	return SmartHome{name: name, conformant: true}, nil
}

// NewSmartHomeMultiLevel accepts a name that spans several topic levels —
// `home/loom` — and otherwise validates it like [NewSmartHome]: no
// wildcards, no NUL, no empty level, no leading `$`.
//
// It exists for one consumer's deliberate decision (openccu-loom ADR 0083):
// an operator-configured multi-level base is kept verbatim rather than
// refused on upgrade. Such an instance runs outside spec §3 — a tool
// scanning `+/info` cannot see it — and [SmartHome.Conformant] reports that
// so the consumer can say so once at start. A name without a `/` is
// conformant and identical to what [NewSmartHome] returns.
func NewSmartHomeMultiLevel(name string) (SmartHome, error) {
	if err := checkName(name, true); err != nil {
		return SmartHome{}, err
	}
	return SmartHome{name: name, conformant: !strings.Contains(name, "/")}, nil
}

func checkName(name string, multiLevel bool) error {
	if name == "" {
		return fmt.Errorf("%w: empty", ErrInvalidName)
	}
	if strings.HasPrefix(name, "$") {
		return fmt.Errorf("%w: %q starts with $, which MQTT reserves for the broker", ErrInvalidName, name)
	}
	if strings.ContainsAny(name, "+#\x00") {
		return fmt.Errorf("%w: %q contains a wildcard or NUL", ErrInvalidName, name)
	}
	if !strings.Contains(name, "/") {
		return nil
	}
	if !multiLevel {
		return fmt.Errorf("%w: %q contains /", ErrInvalidName, name)
	}
	if slices.Contains(strings.Split(name, "/"), "") {
		return fmt.Errorf("%w: %q has an empty level", ErrInvalidName, name)
	}
	return nil
}

// Name is the instance name, verbatim.
func (l SmartHome) Name() string { return l.name }

// Conformant reports whether the name is a single topic level, as spec §3
// requires. Only [NewSmartHomeMultiLevel] can produce false.
func (l SmartHome) Conformant() bool { return l.conformant }

// fn renders `<name>/<function>/<item...>`, or "" when the item path is empty
// once sanitised: spec §3 requires at least one item level under status, set
// and meta, and `<name>/status` itself is no topic of this grammar.
func (l SmartHome) fn(function string, item []string) string {
	rest := Join(item...)
	if l.name == "" || rest == "" {
		return ""
	}
	return l.name + "/" + function + "/" + rest
}

// Status is `<name>/status/<item...>`, or "" for an empty item path.
func (l SmartHome) Status(item ...string) string { return l.fn(FunctionStatus, item) }

// Set is `<name>/set/<item...>`, or "" for an empty item path.
func (l SmartHome) Set(item ...string) string { return l.fn(FunctionSet, item) }

// Meta is `<name>/meta/<item...>`, or "" for an empty item path.
func (l SmartHome) Meta(item ...string) string { return l.fn(FunctionMeta, item) }

// Maintenance implements [SmartHomeLayout]: `<name>/maintenance/<item...>`,
// for example `Maintenance("stats")` or `Maintenance("set", "loglevel")`.
func (l SmartHome) Maintenance(item ...string) string { return l.fn(FunctionMaintenance, item) }

// Connected implements [SmartHomeLayout]: `<name>/connected`.
func (l SmartHome) Connected() string {
	if l.name == "" {
		return ""
	}
	return l.name + "/" + FunctionConnected
}

// Info implements [SmartHomeLayout]: `<name>/info`.
func (l SmartHome) Info() string {
	if l.name == "" {
		return ""
	}
	return l.name + "/" + FunctionInfo
}

// Item is the item path of a slot: scope, address, channel, bucket, path,
// with empty segments dropped by the rendering.
func (l SmartHome) Item(s model.Slot) []string {
	parts := make([]string, 0, len(s.Scope)+len(s.Path)+3)
	parts = append(parts, s.Scope...)
	parts = append(parts, s.Address, s.Channel, s.Bucket.String())
	return append(parts, s.Path...)
}

// State implements [Layout]: the slot's status item.
func (l SmartHome) State(s model.Slot) string { return l.Status(l.Item(s)...) }

// Command implements [Layout]: the slot's set item, the same path as its
// status item.
func (l SmartHome) Command(s model.Slot) string { return l.Set(l.Item(s)...) }

// Availability implements [Layout]: the device's `online` status item,
// `<name>/status/<scope...>/<address>/online`.
//
// Like [Default], it reads the slot's scope and address and ignores channel,
// bucket and path: reachability is a property of the device, not of one of
// its datapoints. A slot without an address renders "".
func (l SmartHome) Availability(s model.Slot) string {
	if s.Address == "" {
		return ""
	}
	parts := make([]string, 0, len(s.Scope)+2)
	parts = append(parts, s.Scope...)
	return l.Status(append(parts, s.Address, "online")...)
}

// Bridge implements [Layout]: `<name>/connected`.
func (l SmartHome) Bridge() string { return l.Connected() }

// Pulse implements [PulseLayout]. Occurrences are status items that are not
// retained (spec §3.2), at the channel's own coordinate:
//
//	<name>/status/<scope...>/<uid>[/<channel>]/event         channel event
//	<name>/status/<scope...>/<uid>[/<channel>]/impulse       channel impulse
//	<name>/status/<scope...>/<uid>[/<channel>]/device_error  channel device error
//
// [PulseDataPointEvent] renders "": under the convention the event type is the
// pulse's `val`, not a topic level of its own, so the per-type topic below
// the aggregate has no place in this grammar (openccu-loom ADR 0083 drops it).
func (l SmartHome) Pulse(kind PulseKind, s model.Slot) string {
	if !kind.Valid() || kind == PulseDataPointEvent || s.Address == "" {
		return ""
	}
	parts := make([]string, 0, len(s.Scope)+3)
	parts = append(parts, s.Scope...)
	return l.Status(append(parts, s.Address, s.Channel, kind.Leaf())...)
}
