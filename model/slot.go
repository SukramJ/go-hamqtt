// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package model

import "strings"

// Bucket says what kind of datapoint a slot addresses. It exists because the
// same device exposes values with different lifecycles and different Home
// Assistant meanings, and flattening them into one namespace loses that:
// a runtime measurement and a configuration parameter want different entity
// categories even when they sit on the same channel.
type Bucket uint8

const (
	// BucketUnset is the zero value: a datapoint that belongs to no paramset
	// at all. A hub-level value — a system variable, a program — is not on a
	// channel and has no configuration/runtime distinction to make.
	//
	// It renders as the empty string, and [topic.Join] drops empty segments,
	// so such a datapoint's topic simply has one level fewer rather than a
	// placeholder nobody can interpret.
	BucketUnset Bucket = iota
	// BucketValues is live runtime state — the default and by far the common
	// case.
	BucketValues
	// BucketMaster is device configuration: parameters an operator sets, not
	// values the device reports. Conventionally rendered as
	// entity_category: config.
	BucketMaster
	// BucketCalculated is a value the bridge derives rather than reads —
	// a dew point from temperature and humidity, an energy total from a
	// power series.
	BucketCalculated
	// BucketCustom is an aggregate the bridge composes: the several
	// datapoints behind one climate or cover entity.
	BucketCustom
)

// String returns the topic-segment spelling of the bucket. [BucketUnset]
// renders empty so the segment disappears rather than reading "unknown".
func (b Bucket) String() string {
	switch b {
	case BucketUnset:
		return ""
	case BucketValues:
		return "values"
	case BucketMaster:
		return "master"
	case BucketCalculated:
		return "calculated"
	case BucketCustom:
		return "custom"
	default:
		return "unknown"
	}
}

// Valid reports whether b is a declared bucket. [BucketUnset] is one: a
// hub-level datapoint has no paramset, and refusing it would make the model
// unable to address half of a consumer's tree.
func (b Bucket) Valid() bool { return b >= BucketUnset && b <= BucketCustom }

// Slot is a datapoint's coordinate — never a topic string.
//
// The model addresses datapoints by coordinate and asks the topic layer to
// render them, which is what keeps the topic schema a consumer's decision
// rather than the model's. A model that formatted its own topics could not
// serve six projects with six different schemas.
//
// Path is a slice and Channel a string on purpose. A Homematic parameter is
// one segment on a numbered channel; a Home Connect feature is a dotted name
// six segments deep; a UniFi port is named, not numbered. A fixed arity or an
// integer channel would fit exactly one of those.
type Slot struct {
	// Address is the owning device's [Identity.UID].
	Address string
	// Channel sub-addresses within the device. Empty at device level.
	Channel string
	// Bucket classifies the datapoint.
	Bucket Bucket
	// Path is the datapoint name, split into segments.
	Path []string
}

// S builds a slot. The variadic path is what makes the call sites readable at
// every arity the consumers need:
//
//	model.S(dev, "", model.BucketValues, "temperature")
//	model.S(dev, "3", model.BucketMaster, "BSH", "Common", "Setting", "PowerState")
func S(address, channel string, bucket Bucket, path ...string) Slot {
	return Slot{Address: address, Channel: channel, Bucket: bucket, Path: path}
}

// Leaf is the last path segment — the datapoint's own name, without its
// namespace. Catalog rules match on it.
func (s Slot) Leaf() string {
	if len(s.Path) == 0 {
		return ""
	}
	return s.Path[len(s.Path)-1]
}

// Valid reports whether the slot can address anything: it needs a device, a
// known bucket and at least one non-empty path segment.
func (s Slot) Valid() bool {
	if s.Address == "" || !s.Bucket.Valid() || len(s.Path) == 0 {
		return false
	}
	for _, p := range s.Path {
		if p == "" {
			return false
		}
	}
	return true
}

// Key is a stable, comparable rendering of the slot, for use as a map key.
//
// It is not a topic and must not be published: it makes no promises about MQTT
// legality, only about being unique and stable for a given coordinate.
func (s Slot) Key() string {
	var b strings.Builder
	b.WriteString(s.Address)
	b.WriteByte('|')
	b.WriteString(s.Channel)
	b.WriteByte('|')
	b.WriteString(s.Bucket.String())
	b.WriteByte('|')
	b.WriteString(strings.Join(s.Path, "."))
	return b.String()
}

// String is the debug rendering. It is [Slot.Key] with friendlier separators
// and carries the same warning: not a topic.
func (s Slot) String() string { return s.Key() }

// Equal reports whether two slots address the same datapoint.
func (s Slot) Equal(other Slot) bool { return s.Key() == other.Key() }

// BindMode says which directions a binding carries.
type BindMode uint8

const (
	// Read means the device reports this value.
	Read BindMode = iota + 1
	// Write means Home Assistant can command it.
	Write
	// ReadWrite means both, which is the usual case for a setpoint.
	ReadWrite
)

// CanRead reports whether the mode publishes state.
func (m BindMode) CanRead() bool { return m == Read || m == ReadWrite }

// CanWrite reports whether the mode accepts commands.
func (m BindMode) CanWrite() bool { return m == Write || m == ReadWrite }

// String returns a debug spelling.
func (m BindMode) String() string {
	switch m {
	case Read:
		return "read"
	case Write:
		return "write"
	case ReadWrite:
		return "readwrite"
	default:
		return "unknown"
	}
}

// Roles a binding can carry. A simple entity has one state binding and
// optionally one command binding; a composite entity binds several roles, each
// naming the part of the entity it feeds.
const (
	// RoleState is the entity's own value. The default for a simple entity.
	RoleState = "state"
	// RoleCommand is the entity's own command. The default write role.
	RoleCommand = "command"
	// RoleAvailability marks a binding whose value says whether the entity is
	// available, rather than what its value is. It is what
	// [LevelSelf] resolves against.
	RoleAvailability = "availability"
)

// Binding attaches one datapoint to one role of an entity.
type Binding struct {
	// Role names what this datapoint feeds. [RoleState] and [RoleCommand] for
	// a simple entity; a platform role ("temperature", "mode", "fan_mode")
	// for a composite one.
	Role string
	// Slot is the datapoint.
	Slot Slot
	// Mode says which directions it carries.
	Mode BindMode
}

// Valid reports whether the binding is addressable.
func (b Binding) Valid() bool {
	return b.Role != "" && b.Mode != 0 && b.Slot.Valid()
}
