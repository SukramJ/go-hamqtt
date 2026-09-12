// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package main

import (
	"fmt"
	"sort"
	"strings"
)

// This file holds the checks the runtime layer made possible.
//
// Before it, hadoctor could ask one question of a whole capture: does every
// topic an entity depends on carry something? That is the reading half. The
// runtime layer adds three planes that write — state, commands, availability —
// and each one makes a new class of defect visible in a retained snapshot:
//
//   - Availability is now published by something other than the bridge LWT,
//     so a retained marker can outlive the device it describes. The discovery
//     config that named it is gone; the marker is not, and Home Assistant
//     reads it on every restart.
//   - Commands are now subscribed from the document rather than hardcoded, so
//     a topic can be advertised as a command and published as state by the
//     same process.
//   - Discovery now has two forms and a mandatory migration order between
//     them, so both can be retained for the same entity at once — which Home
//     Assistant refuses with a log line and nothing else.
//
// All three are cross-payload. None is visible to hacheck, which sees one
// config at a time, and none is visible to any single plane's unit tests.

// availabilityPayloads are the payload bodies that identify a retained
// message as an availability marker.
//
// Matching on the payload rather than on the topic name is what makes the
// orphan check work at all: the topic is the consumer's to shape — one
// measured plane publishes per channel, another per site — so there is no
// string pattern to look for. The four tokens are the ones this module's
// discovery pipeline writes into `payload_available`/`payload_not_available`:
// the bare words for a bridge or device level, and the lower-cased booleans
// for a [model.LevelSelf] datapoint.
var availabilityPayloads = map[string]bool{
	"online": true, "offline": true, "true": true, "false": true,
}

// maxMarkerPayload bounds what the capture keeps a body for.
//
// The orphan check needs to compare a payload against four short tokens, and
// nothing else here reads a non-discovery payload at all. Keeping every body
// of a full `#` capture — which on a busy broker is the entire retained state
// of every integration on it — to answer a question about seven bytes is the
// kind of cost that makes an operator stop running the tool.
const maxMarkerPayload = 16

// commandTopicKeys are the discovery keys naming a topic Home Assistant
// publishes TO, whose name does not end in `command_topic`.
//
// The two exceptions are the reason this is a table plus a suffix rule rather
// than a suffix rule alone: `cover.set_position_topic` and
// `vacuum.set_fan_speed_topic` are command topics with a different naming
// convention, and a check deriving them from the suffix alone would report a
// cover's position slider as a state topic.
var commandTopicKeys = map[string]bool{
	"command_topic":       true,
	"set_position_topic":  true,
	"set_fan_speed_topic": true,
}

// commandTopicsOf pulls the topics one config tells Home Assistant to publish
// to.
//
// Read off the body's keys rather than a fixed field list because the set
// grows with the catalog: a climate names five command topics, a light ten,
// and a check that knew three of them would silently pass the other twelve.
func commandTopicsOf(body map[string]any) []string {
	var out []string
	seen := map[string]bool{}
	for key, v := range body {
		if !commandTopicKeys[key] && !strings.HasSuffix(key, "_command_topic") {
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

// readTopicsOf pulls the topics one config tells Home Assistant to read.
//
// Every `*_topic` key that is not a command topic, which is wider than
// `state_topic` on purpose: a climate names one topic per role, a cover names
// its position separately, a light names ten. An echo check that knew only
// `state_topic` would pass a cover whose position slider writes to the topic
// it reads position from — the exact shape of the measured defect, one
// platform over.
func readTopicsOf(body map[string]any) []string {
	var out []string
	seen := map[string]bool{}
	for key, v := range body {
		if !strings.HasSuffix(key, "_topic") || commandTopicKeys[key] ||
			strings.HasSuffix(key, "_command_topic") {
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

// diagnoseRuntime adds the three cross-payload checks to a report.
func diagnoseRuntime(c *capture) []diagnosis {
	out := make([]diagnosis, 0, 4)
	out = append(out, diagnoseFormConflicts(c)...)
	out = append(out, diagnoseCommandEchoes(c)...)
	out = append(out, diagnoseOrphanAvailability(c)...)
	return out
}

// diagnoseFormConflicts reports a unique id retained in both discovery forms
// at once.
//
// This is the defect ADR 0070's second amendment measured against a live Home
// Assistant 2026.9 instance, and it is the worst-behaved one in the whole
// layer. Publishing a device document while a per-entity config for the same
// unique id is still retained is REFUSED: the document sits retained on the
// broker, the entity keeps its old config, and the entire signal is one
// WARNING in Home Assistant's log. The refusal is symmetric, so a rollback
// produces it the other way round. `migrate_discovery: true` was tried and
// did not lift it.
//
// A capture is the only place it can be seen. Neither payload is invalid —
// hacheck passes both — and the consumer's own logs say the publish
// succeeded, because it did. What went wrong is that two of them are
// retained.
func diagnoseFormConflicts(c *capture) []diagnosis {
	ids := make([]string, 0, len(c.byUniqueID))
	for id := range c.byUniqueID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var out []diagnosis
	for _, id := range ids {
		forms := c.byUniqueID[id]
		if len(forms.bundle) == 0 || len(forms.entity) == 0 {
			continue
		}
		out = append(out, diagnosis{
			entity: id,
			kind:   "form-conflict",
			text: fmt.Sprintf("unique_id is retained in both discovery forms at once "+
				"(%s and %s) — Home Assistant refuses the second one with a log line and "+
				"nothing else, so the entity silently keeps its old config; retract one "+
				"before publishing the other",
				strings.Join(forms.entity, ", "), strings.Join(forms.bundle, ", ")),
		})
	}
	return out
}

// diagnoseCommandEchoes reports a command topic that something in the same
// capture publishes as state or availability.
//
// A broker has no notion of "my own message": it fans every publish out to
// every matching subscription, the publisher's own included, with the retain
// flag clear because live routing is not a retained replay. So a consumer
// that advertises a command topic and publishes the same topic as state is
// commanding itself on every state change. In the measured case a program
// entity's state was mirrored onto its trigger topic, and every state publish
// ran the program — on every boot, on every rediscovery, for every program in
// the house including the deliberately deactivated ones. The only signal was
// the programs running.
//
// Reported blocking, unlike the other two checks here, because it needs no
// ownership judgement: both topics are named by configs in the same capture,
// and no reading of them is correct.
func diagnoseCommandEchoes(c *capture) []diagnosis {
	reads := map[string]string{}
	for _, e := range c.declared {
		for _, rt := range e.reads {
			if _, taken := reads[rt]; !taken {
				reads[rt] = e.label
			}
		}
		for _, at := range e.availability {
			if _, taken := reads[at]; !taken {
				reads[at] = e.label
			}
		}
	}

	var out []diagnosis
	for _, e := range c.declared {
		for _, ct := range e.commands {
			victim, clash := reads[ct]
			if !clash {
				continue
			}
			out = append(out, diagnosis{
				entity: e.label,
				kind:   "command-echo",
				text: fmt.Sprintf("command topic %q is also read by %q — the broker delivers "+
					"the consumer's own publishes back to it, so every report on that topic "+
					"issues the command again", ct, victim),
			})
		}
	}
	return out
}

// diagnoseOrphanAvailability reports a retained availability marker that no
// live config references.
//
// This is the ghost, and it is the defect the runtime layer's own orphan sweep
// cannot reach. The sweep reads the broker and clears a retained config no
// running process claims — that is what makes it able to find a leftover at
// all. Availability topics sit in the consumer's own tree, whose shape is the
// consumer's secret, so nothing sweeps them: the publishing side clears only
// what it wrote itself, and after a restart it wrote nothing. Worse, the
// config body was the only thing that named the marker, and retracting the
// config destroys it.
//
// What survives is a retained `online` describing a device that no longer
// exists. Home Assistant reads it on every restart and the entity behind it
// stays permanently available, showing the last value it ever saw — which is
// exactly the failure two of the reference bridges shipped, and exactly the
// one nobody can find from the code.
//
// Reported advisory rather than blocking, and the restriction is the reason.
// A capture cannot decide ownership: a shared broker carries zigbee2mqtt's
// `bridge/state` and ESPHome's status topics, which are correct and
// unreferenced by anything this operator publishes. So the check only
// considers a marker whose first topic segment is a segment some referenced
// availability topic already uses — "unreferenced marker inside a tree your
// own configs point into" — and still leaves the verdict to the operator.
func diagnoseOrphanAvailability(c *capture) []diagnosis {
	referenced := map[string]bool{}
	roots := map[string]bool{}
	for _, e := range c.declared {
		for _, at := range e.availability {
			referenced[at] = true
			roots[firstSegment(at)] = true
		}
	}
	if len(roots) == 0 {
		return nil
	}

	topics := make([]string, 0, len(c.markers))
	for t := range c.markers {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	var out []diagnosis
	for _, t := range topics {
		if referenced[t] || !roots[firstSegment(t)] {
			continue
		}
		out = append(out, diagnosis{
			entity: t,
			kind:   "orphan-availability",
			text: fmt.Sprintf("retains %q and no config references it — if this is a removed "+
				"device's marker, Home Assistant reads it on every restart and the entity "+
				"behind it stays available forever, showing the last value it ever saw",
				c.markers[t]),
		})
	}
	return out
}

// firstSegment is the topic's tree root, which is the coarsest ownership
// signal a capture offers.
func firstSegment(topic string) string {
	if i := strings.IndexByte(topic, '/'); i >= 0 {
		return topic[:i]
	}
	return topic
}
