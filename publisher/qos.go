// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"errors"
	"fmt"
)

// QoS is a configured MQTT quality-of-service level, as distinct from the
// byte that goes on the wire.
//
// It exists because a plain `byte` cannot tell *unset* from *deliberately
// QoS 0*, and all four runtime types read the zero value as unset and
// coerced it to 1. The measured need is
// [go-zendure2mqtt](https://github.com/SukramJ/go-zendure2mqtt): the ADR 0070
// phase-5 pilot measurement of 2026-09-12 found that bridge publishes **every
// message at QoS 0** (`discovery.go:78`, `coordinator.go:88,128,138,171,269`)
// and recorded "cannot be preserved" against the column — adopting the
// runtime would have changed the delivery guarantees of a whole installed
// base on the wire, inside a migration step whose purpose was something else,
// with a broker capture as the only evidence.
//
// The default is unchanged and still right: a retained config lost at QoS 0
// is a config a consumer may never publish again. What changes is that saying
// so and saying nothing are now different statements.
//
// The numeric values are chosen so that a config written before this type
// existed keeps its meaning: [QoSAtLeastOnce] and [QoSExactlyOnce] are the
// wire's own 1 and 2, so a `Config{QoS: 1}` from v0.26.0 still means QoS 1.
// Only [QoSAtMostOnce] is not its wire value — it cannot be, because 0 is
// taken by "unset" — and it never reaches a transport: the constructors
// resolve it to the wire's 0.
type QoS byte

const (
	// QoSUnset is the zero value: no opinion, so the type's own documented
	// default applies. Every runtime type in this package defaults to
	// [QoSAtLeastOnce] except [StateConfig.PulseQoS], which defaults to
	// [QoSAtMostOnce].
	//
	// A level that came from a configuration file is never this. Convert it
	// with [QoSFromWire], which maps the operator's 0 to [QoSAtMostOnce]
	// and cannot produce QoSUnset at all: `QoS(cfg.QoS)` compiles, reads
	// correctly, and turns a configured `MQTT_QOS: 0` into QoS 1 with
	// nothing anywhere saying so.
	QoSUnset QoS = 0

	// QoSAtLeastOnce is MQTT QoS 1, and is the wire value so that a config
	// written against v0.26.0's `QoS byte` field keeps its meaning.
	QoSAtLeastOnce QoS = 1

	// QoSExactlyOnce is MQTT QoS 2, likewise the wire value.
	QoSExactlyOnce QoS = 2

	// QoSAtMostOnce is MQTT QoS 0, stated deliberately.
	//
	// Its numeric value is outside the wire's 0-2 range on purpose: 0 is
	// the zero value and therefore already means "unset", so a deliberate
	// at-most-once had to be spelled with something a struct literal cannot
	// arrive at by omission. It is a configuration value only — no
	// transport ever sees it.
	QoSAtMostOnce QoS = 0x80
)

// String names the level for a log line or a panic message.
func (q QoS) String() string {
	switch q {
	case QoSUnset:
		return "unset"
	case QoSAtMostOnce:
		return "0"
	case QoSAtLeastOnce:
		return "1"
	case QoSExactlyOnce:
		return "2"
	default:
		return "invalid"
	}
}

// Or resolves q against def, so a consumer's silence takes a type's
// documented default and a consumer's [QoSAtMostOnce] does not.
func (q QoS) Or(def QoS) QoS {
	if q == QoSUnset {
		return def
	}
	return q
}

// Wire is the byte a [Transport] takes, and reports whether q has one.
// [QoSUnset] has none: resolve it with [QoS.Or] first. An unrecognised value
// has none either, which is what makes it a programming error the
// constructors can refuse rather than a publish at a level nobody chose.
func (q QoS) Wire() (byte, bool) {
	switch q {
	case QoSAtMostOnce:
		return 0, true
	case QoSAtLeastOnce:
		return 1, true
	case QoSExactlyOnce:
		return 2, true
	case QoSUnset:
		return 0, false
	default:
		return 0, false
	}
}

// resolveQoS is what every constructor in this package calls: apply the
// type's default, then insist on a level that has a wire form.
//
// A panic rather than a silent coercion, and at construction rather than on
// the first publish: a garbage QoS is a composition-root mistake, and the
// whole point of the type is that this package no longer quietly decides what
// a consumer meant. field names the struct field so the message points at the
// literal that got it wrong.
func resolveQoS(field string, q, def QoS) byte {
	resolved := q.Or(def)
	wire, ok := resolved.Wire()
	if !ok {
		panic("publisher: " + field + " = " + resolved.String() +
			" is not a QoS level; use QoSAtMostOnce, QoSAtLeastOnce or QoSExactlyOnce")
	}
	return wire
}

// ErrQoSOutOfRange is returned by [QoSFromWire] for a byte that is not an
// MQTT quality-of-service level.
var ErrQoSOutOfRange = errors.New("publisher: qos must be 0, 1 or 2")

// QoSFromWire turns an operator-supplied 0, 1 or 2 into the [QoS] that means
// it, and refuses anything else.
//
// This is the one conversion every consumer in the family has to write, and
// it is where the trap this type exists to create actually catches people.
// `QoS(cfg.MQTT.QoS)` compiles, reads correctly and is wrong for exactly one
// input: an operator who configured `MQTT_QOS: 0` gets [QoSUnset], which
// every constructor here resolves to QoS 1. Nothing on the wire says so and
// nothing in a log does either — the bridge simply publishes at a level its
// own documentation promises it does not. It was recorded as a finding in
// two separate consumer migrations (go-homeconnect2mqtt phase 7 F9,
// "`MQTT_QOS: 0` becomes QoS 1 on migration"; go-mtec2mqtt steps 4+5, where
// omitting the field "would have tripled this bridge's broker traffic and
// changed the delivery guarantee of an installed base"), and both wrote this
// function by hand to close it.
//
// The mapping is the only one that is not a guess: 0 is [QoSAtMostOnce] —
// deliberate at most once, never "unset" — 1 is [QoSAtLeastOnce] and 2 is
// [QoSExactlyOnce]. There is no input that produces [QoSUnset], which is the
// point: a value that came from a configuration file is a statement, and
// silence is the only thing that may resolve to a default.
//
// A consumer whose configuration is validated elsewhere usually wants this at
// wiring time rather than on a publish path:
//
//	qos, err := publisher.QoSFromWire(cfg.MQTT.QoS)
//	if err != nil {
//		panic(err) // a composition-root mistake, like a nil transport
//	}
//
// Pin the result off the transport call rather than off this constant: both
// consumers that verify it assert on the QoS byte a stub was actually handed,
// which is what kept the pin honest across three layers of plane migration
// underneath it.
func QoSFromWire(b byte) (QoS, error) {
	switch b {
	case 0:
		return QoSAtMostOnce, nil
	case 1:
		return QoSAtLeastOnce, nil
	case 2:
		return QoSExactlyOnce, nil
	default:
		return QoSUnset, fmt.Errorf("%w, got %d", ErrQoSOutOfRange, b)
	}
}
