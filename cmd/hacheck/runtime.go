// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package main

import (
	"fmt"
	"sort"
	"strings"
)

// This file holds the two runtime-layer checks that fit inside one payload.
//
// hacheck's existing question is schema conformance: would Home Assistant
// accept this key, this device class, this unit. The runtime layer adds a
// second question a single config can answer — not "is this legal" but "does
// this config point its own planes at the right trees" — and both findings
// here are ones Home Assistant accepts without complaint and then behaves
// badly on.
//
// Both are advisory rather than blocking, and that is the existing
// convention rather than a hedge: a blocking finding is something Home
// Assistant rejects or silently strips, and it is what makes the exit code
// mean "this payload will not work". These two payloads work. They are
// simply wired to do the wrong thing, and the operator is the one who has to
// decide whether the wiring was deliberate.
//
// The cross-payload half of the runtime layer — a marker no config
// references, a unique id retained in both discovery forms, a command topic
// something else publishes — cannot be seen from one record and lives in
// hadoctor.

// checkRuntime inspects one component body for the two wiring defects.
//
// `prefix` is Home Assistant's discovery prefix, which is what makes the
// first check possible at all: whether a topic is inside somebody else's tree
// is not a property of the string.
func checkRuntime(topic, entity string, body map[string]any, prefix string) []finding {
	out := make([]finding, 0, 2)
	out = append(out, checkAvailabilityTree(topic, entity, body, prefix)...)
	out = append(out, checkSelfEcho(topic, entity, body)...)
	return out
}

// checkAvailabilityTree reports an availability topic inside the discovery
// prefix.
//
// The measured defect, and it is one of the five ADR 0070 names: a bridge
// publishes its own status at `<discovery_prefix>/status/lwt` — inside Home
// Assistant's own birth tree. It is in the wrong place and it is inert, and
// it is invisible from the code that produced it because the string is
// assembled from a base the operator configures. The config validates
// perfectly: `availability` is a legal key and its topic is a legal topic.
//
// Two things go wrong. Nothing else on the broker writes in that tree, so the
// marker is never published and — under `availability_mode: all`, the
// default — the entity stays unavailable forever with nothing to say why.
// And the orphan sweep's own `<prefix>/#` snapshot sees it, which is a second
// way for the same mistake to bite.
//
// Home Assistant's own `<prefix>/status` is excluded. An entity may
// legitimately gate on it: that is Home Assistant's birth topic, and an
// entity that should disappear when the MQTT integration goes down is
// entitled to say so.
func checkAvailabilityTree(topic, entity string, body map[string]any, prefix string) []finding {
	tree := strings.TrimSuffix(prefix, "/") + "/"
	birth := tree + "status"

	var out []finding
	for _, at := range availabilityTopicsIn(body) {
		if !strings.HasPrefix(at, tree) || at == birth {
			continue
		}
		out = append(out, finding{
			topic: topic, entity: entity,
			text: fmt.Sprintf("availability topic %q is inside Home Assistant's own discovery "+
				"tree %q — nothing there is the consumer's to write, so the marker is never "+
				"published and the entity stays unavailable forever", at, tree),
		})
	}
	return out
}

// checkSelfEcho reports a topic this config names both as a command topic and
// as something Home Assistant reads.
//
// A broker has no notion of "my own message": it fans every publish out to
// every matching subscription, the publisher's own included, with the retain
// flag clear because live routing is not a retained replay. So a config whose
// command topic is also its state topic describes a consumer that issues
// itself the command on every report. In the measured case a program entity's
// state was mirrored onto its trigger topic and every state publish ran the
// program — on every boot, on every rediscovery, for every program in the
// house. The only signal was the programs running.
//
// The single-payload form of this is the narrow one: the general case needs a
// whole capture, because the collision is usually between two different
// entities. hadoctor's `command-echo` is that. This catches the one shape
// that needs no capture at all, which is also the shape a golden-file test in
// a consumer's own suite can catch at build time.
func checkSelfEcho(topic, entity string, body map[string]any) []finding {
	commands := map[string]bool{}
	for _, ct := range topicsWithSuffix(body, true) {
		commands[ct] = true
	}
	reads := topicsWithSuffix(body, false)
	reads = append(reads, availabilityTopicsIn(body)...)

	var out []finding
	seen := map[string]bool{}
	for _, rt := range reads {
		if !commands[rt] || seen[rt] {
			continue
		}
		seen[rt] = true
		out = append(out, finding{
			topic: topic, entity: entity,
			text: fmt.Sprintf("%q is both a command topic and one Home Assistant reads — the "+
				"broker delivers the consumer's own publishes back to it, so every report "+
				"on that topic issues the command again", rt),
		})
	}
	return out
}

// commandTopicKeys are the discovery keys naming a topic Home Assistant
// publishes TO whose name does not end in `command_topic`.
//
// Two of them, and they are the reason this is a table plus a suffix rule
// rather than a suffix rule alone: `cover.set_position_topic` and
// `vacuum.set_fan_speed_topic` are command topics with a different naming
// convention, and a check deriving them from the suffix alone reads a cover's
// position slider as a state topic — after which the echo it forms is
// invisible.
var commandTopicKeys = map[string]bool{
	"command_topic":       true,
	"set_position_topic":  true,
	"set_fan_speed_topic": true,
}

// topicsWithSuffix picks the string-valued `*_topic` keys of one kind out of a
// component body.
//
// Read off the keys rather than a fixed field list because the set grows with
// the catalog: a climate names five command topics and a light ten, and a
// check that knew three of them would silently pass the other twelve.
func topicsWithSuffix(body map[string]any, command bool) []string {
	var out []string
	seen := map[string]bool{}
	for key, v := range body {
		if !strings.HasSuffix(key, "_topic") {
			continue
		}
		isCommand := commandTopicKeys[key] || strings.HasSuffix(key, "_command_topic")
		if isCommand != command {
			continue
		}
		s, ok := v.(string)
		if !ok || s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// availabilityTopicsIn reads both shapes: the legacy single
// `availability_topic` and the list of entries this module's discovery
// pipeline renders.
//
// Both, because a consumer mid-migration publishes one and then the other,
// and a check that knew only the list form would pass every legacy payload
// silently — which is the shape of the defect it exists to find.
func availabilityTopicsIn(body map[string]any) []string {
	var out []string
	if t, ok := body["availability_topic"].(string); ok && t != "" {
		out = append(out, t)
	}
	list, _ := body["availability"].([]any)
	for _, item := range list {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if t, ok := entry["topic"].(string); ok && t != "" {
			out = append(out, t)
		}
	}
	return out
}
