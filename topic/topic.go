// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Package topic is the only place that turns a [model.Slot] into a string.
//
// Keeping it a package of its own, below discovery, is what mechanically
// enforces the rule the whole model rests on: the model addresses datapoints
// by coordinate and never formats a topic. A model that could format one would
// fix the topic schema for every consumer, and the six consuming projects have
// six different schemas for reasons that are theirs to keep.
//
// Two normalisers live here and they are deliberately different strengths.
// [Safe] only removes characters MQTT itself forbids in a topic segment, so a
// device name survives recognisably. [Slug] is aggressive, producing the
// [a-z0-9_-] Home Assistant accepts for an object id — including transliterating
// German umlauts the way Home Assistant's own slugify does, so "Größe" becomes
// "groesse" rather than "gr_e".
package topic

import (
	"strings"

	"github.com/SukramJ/go-hamqtt/model"
)

// Layout renders the topics a consumer publishes. It is an interface because
// the topic schema is the consumer's, not the model's; [Default] is a usable
// implementation for a consumer that has no opinion yet.
type Layout interface {
	// State is where a datapoint's value is published.
	State(s model.Slot) string
	// Command is where a writable datapoint listens.
	Command(s model.Slot) string
	// Availability is a device's own reachability topic.
	Availability(id model.Identity) string
	// Bridge is the daemon's own status topic, carrying its LWT.
	Bridge() string
}

// Default renders
//
//	<root>/<uid>[/<channel>]/<bucket>/<path...>          state
//	<root>/<uid>[/<channel>]/<bucket>/<path...>/set      command
//	<root>/<uid>/availability                            device
//	<root>/bridge/status                                 daemon LWT
//
// Every segment goes through [Safe], so a device identifier containing a
// slash or a wildcard cannot inject a topic level.
type Default struct {
	// Root prefixes every topic. Conventionally the bridge name.
	Root string
}

var _ Layout = Default{}

// State implements [Layout].
func (d Default) State(s model.Slot) string { return Join(d.slotParts(s)...) }

// Command implements [Layout].
func (d Default) Command(s model.Slot) string {
	return Join(append(d.slotParts(s), "set")...)
}

// Availability implements [Layout].
func (d Default) Availability(id model.Identity) string {
	return Join(d.Root, id.UID(), "availability")
}

// Bridge implements [Layout].
func (d Default) Bridge() string { return Join(d.Root, "bridge", "status") }

func (d Default) slotParts(s model.Slot) []string {
	parts := make([]string, 0, len(s.Path)+4)
	parts = append(parts, d.Root, s.Address)
	if s.Channel != "" {
		parts = append(parts, s.Channel)
	}
	parts = append(parts, s.Bucket.String())
	parts = append(parts, s.Path...)
	return parts
}

// Join builds a topic from segments, sanitising each with [Safe] and dropping
// empty ones.
//
// Dropping empties rather than emitting "//" is what makes an optional segment
// — a device-level slot with no channel — expressible without every caller
// branching on it.
func Join(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = Safe(p); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "/")
}

// Safe removes the characters MQTT forbids inside a topic segment: the level
// separator and the two wildcards, plus whitespace and the null byte.
//
// It is deliberately weak. A topic segment may contain almost anything, and
// mangling a device name beyond recognition helps nobody — the only job here
// is that a value cannot change the topic's shape.
func Safe(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '/', '+', '#', 0:
			b.WriteByte('_')
		case ' ', '\t', '\n', '\r':
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// transliterations are the multi-character expansions Home Assistant's own
// slugify performs. They must run before the general fold, or "ü" would become
// "_" and "Größe" would slug to "gr_e" — which is exactly the defect one of
// the consuming bridges shipped by omitting this step.
var transliterations = strings.NewReplacer(
	"ä", "ae", "ö", "oe", "ü", "ue",
	"Ä", "ae", "Ö", "oe", "Ü", "ue",
	"ß", "ss",
	"å", "a", "æ", "ae", "ø", "oe",
	"é", "e", "è", "e", "ê", "e", "ë", "e",
	"á", "a", "à", "a", "â", "a",
	"í", "i", "ì", "i", "î", "i",
	"ó", "o", "ò", "o", "ô", "o",
	"ú", "u", "ù", "u", "û", "u",
	"ñ", "n", "ç", "c",
)

// Slug produces the [a-z0-9_-] form Home Assistant accepts for a node id or an
// object id: transliterated, lowercased, runs of separators collapsed, ends
// trimmed. The hyphen is preserved — see the case below.
//
// An input that reduces to nothing yields "x" rather than an empty string,
// because an empty segment would produce a malformed discovery topic that
// Home Assistant silently ignores.
func Slug(s string) string {
	s = transliterations.Replace(strings.ToLower(s))

	var b strings.Builder
	b.Grow(len(s))
	lastSep := true // leading separators are trimmed by never writing them
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			// The hyphen survives. Home Assistant's own topic matcher accepts
			// [a-zA-Z0-9_-] in a node id, and a consumer that composes a
			// sub-device id as "<parent>-<group>" needs the two separators to
			// stay distinguishable — folding it to "_" collides that id with a
			// sibling that legitimately contains an underscore.
			b.WriteRune(r)
			lastSep = false
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r - 'A' + 'a')
			lastSep = false
		default:
			if !lastSep {
				b.WriteByte('_')
				lastSep = true
			}
		}
	}
	out := strings.TrimRight(b.String(), "_")
	if out == "" {
		return "x"
	}
	return out
}
