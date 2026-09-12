// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package main

import (
	"bytes"
	"strings"
	"testing"
)

// A device document and a per-entity config for the SAME unique id, both
// retained. This is the migration conflict ADR 0070's second amendment
// measured against a live Home Assistant 2026.9 instance: the second publish
// is refused, the entity keeps its old config, and the only evidence anywhere
// is one WARNING line in Home Assistant's log.
const (
	conflictBundle = `{"topic":"homeassistant/device/serial_ac-1/config","payload":{` +
		`"device":{"identifiers":["serial:AC-1"]},"origin":{"name":"go-daikin2mqtt"},` +
		`"components":{"power":{"platform":"sensor","unique_id":"daikin_serial_ac_1_power",` +
		`"state_topic":"daikin/serial:AC-1/values/power",` +
		`"availability":[{"topic":"daikin/bridge/status"}]}}}}`

	conflictEntity = `{"topic":"homeassistant/sensor/serial_ac-1/power/config","payload":{` +
		`"unique_id":"daikin_serial_ac_1_power","state_topic":"daikin/serial:AC-1/values/power",` +
		`"availability":[{"topic":"daikin/bridge/status"}]}}`

	conflictTopics = `{"topic":"daikin/bridge/status","payload":"online"}
{"topic":"daikin/serial:AC-1/values/power","payload":"{\"value\":420,\"available\":true}"}`
)

// TestBothDiscoveryFormsRetainedAtOnceIsReported is the check on a capture
// that holds the refused migration.
//
// Neither payload is invalid — hacheck passes both — and the consumer's own
// log says both publishes succeeded, because they did. What went wrong is
// that two of them are retained, which only a whole-capture view can see.
func TestBothDiscoveryFormsRetainedAtOnceIsReported(t *testing.T) {
	t.Parallel()

	c, err := load(strings.NewReader(lines(conflictBundle, conflictEntity, conflictTopics)), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 1 || found[0].kind != "form-conflict" {
		t.Fatalf("found = %+v, want exactly the form conflict", found)
	}
	if found[0].entity != "daikin_serial_ac_1_power" {
		t.Errorf("entity = %q, want the unique id", found[0].entity)
	}
	// Both topics are named: "you have a conflict somewhere" is not an
	// actionable sentence, and the operator has to retract one of the two.
	for _, want := range []string{
		"homeassistant/sensor/serial_ac-1/power/config",
		"homeassistant/device/serial_ac-1/config",
	} {
		if !strings.Contains(found[0].text, want) {
			t.Errorf("the report does not name %q: %s", want, found[0].text)
		}
	}
	if got := report(&bytes.Buffer{}, found); got != 1 {
		t.Errorf("exit = %d, want 1: a refused migration is not advisory", got)
	}
}

// TestOneFormAloneIsSilent, so the conflict check means something.
func TestOneFormAloneIsSilent(t *testing.T) {
	t.Parallel()

	for name, capture := range map[string]string{
		"bundle only": lines(conflictBundle, conflictTopics),
		"entity only": lines(conflictEntity, conflictTopics),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, err := load(strings.NewReader(capture), "homeassistant")
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if found := diagnose(c); len(found) != 0 {
				t.Errorf("found = %+v", found)
			}
		})
	}
}

// A program entity whose state is mirrored onto the topic its trigger arrives
// on. The measured defect: every state publish ran the program, on every
// boot, on every rediscovery, for every program in the house — and the only
// signal was the programs running.
const echoConfig = `{"topic":"homeassistant/button/loom_hub/prog_night/config","payload":{` +
	`"unique_id":"loom_hub_prog_night","platform":"button",` +
	`"command_topic":"loom/hub/programs/night","state_topic":"loom/hub/programs/night",` +
	`"availability":[{"topic":"loom/bridge/status"}]}}`

const echoTopics = `{"topic":"loom/bridge/status","payload":"online"}
{"topic":"loom/hub/programs/night","payload":"OFF"}`

// TestACommandTopicPublishedAsStateIsReported is the echo, on the shape the
// reference implementation shipped.
//
// It needs no ownership judgement — both topics are named by configs in the
// same capture — so it is blocking, unlike the orphan check below.
func TestACommandTopicPublishedAsStateIsReported(t *testing.T) {
	t.Parallel()

	c, err := load(strings.NewReader(lines(echoConfig, echoTopics)), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 1 || found[0].kind != "command-echo" {
		t.Fatalf("found = %+v, want exactly the echo", found)
	}
	if !strings.Contains(found[0].text, "loom/hub/programs/night") {
		t.Errorf("the report does not name the topic: %s", found[0].text)
	}
	if got := report(&bytes.Buffer{}, found); got != 1 {
		t.Errorf("exit = %d, want 1: the consumer is writing to a device it did not mean to", got)
	}
}

// TestACoverPositionTopicCountsAsACommand pins the naming exception.
//
// `set_position_topic` and `set_fan_speed_topic` are command topics that do
// not end in `command_topic`. A check deriving them from the suffix alone
// would classify a cover's position slider as a state topic — and then the
// echo it forms with a state publish is invisible.
func TestACoverPositionTopicCountsAsACommand(t *testing.T) {
	t.Parallel()

	capture := lines(
		`{"topic":"homeassistant/cover/blinds/living/config","payload":{`+
			`"unique_id":"blinds_living","platform":"cover",`+
			`"set_position_topic":"blinds/living/position",`+
			`"position_topic":"blinds/living/position",`+
			`"availability":[{"topic":"blinds/bridge/status"}]}}`,
		`{"topic":"blinds/bridge/status","payload":"online"}`,
		`{"topic":"blinds/living/position","payload":"40"}`,
	)
	c, err := load(strings.NewReader(capture), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 1 || found[0].kind != "command-echo" {
		t.Fatalf("found = %+v, want the echo on set_position_topic", found)
	}
}

// The ghost: a removed device's retained `online` with no config left to name
// it. Home Assistant reads it on every restart and the entity behind it stays
// permanently available, showing the last value it ever saw.
//
// This is precisely what the runtime layer's own orphan sweep cannot reach:
// the sweep clears the config (which is why there is none here) and the
// config body was the only thing that named the marker.
const ghostTopics = `{"topic":"daikin/bridge/status","payload":"online"}
{"topic":"daikin/serial:GONE-1/availability","payload":"online"}`

// TestAnUnreferencedAvailabilityMarkerIsReportedAdvisory is the ghost check,
// and the reason it does not fail the run.
func TestAnUnreferencedAvailabilityMarkerIsReportedAdvisory(t *testing.T) {
	t.Parallel()

	c, err := load(strings.NewReader(lines(conflictBundle, conflictTopics, ghostTopics)), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 1 || found[0].kind != "orphan-availability" {
		t.Fatalf("found = %+v, want exactly the ghost", found)
	}
	if found[0].entity != "daikin/serial:GONE-1/availability" {
		t.Errorf("entity = %q", found[0].entity)
	}
	// Advisory: the verdict needs an ownership judgement a capture cannot
	// make, and failing a CI run over somebody else's zigbee2mqtt marker is
	// how a tool gets switched off.
	if got := report(&bytes.Buffer{}, found); got != 0 {
		t.Errorf("exit = %d, want 0: ownership is not decidable from a capture", got)
	}
}

// TestAnotherIntegrationsMarkerIsNotReported is the restriction that makes
// the orphan check usable on a shared broker.
//
// zigbee2mqtt's `bridge/state` and ESPHome's status topics are correct,
// retained, availability-shaped and referenced by nothing this operator
// publishes. Only a marker inside a tree the capture's own configs point into
// is considered.
func TestAnotherIntegrationsMarkerIsNotReported(t *testing.T) {
	t.Parallel()

	capture := lines(conflictBundle, conflictTopics,
		`{"topic":"zigbee2mqtt/bridge/state","payload":"online"}`,
		`{"topic":"esphome/kitchen/status","payload":"online"}`)
	c, err := load(strings.NewReader(capture), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if found := diagnose(c); len(found) != 0 {
		t.Errorf("found = %+v on another integration's markers", found)
	}
}

// TestHomeAssistantsOwnBirthTopicIsNotAGhost is the exclusion that keeps the
// ghost check off Home Assistant's own tree.
//
// `<prefix>/status` carries the bare word `online`, which is exactly the
// payload the orphan check looks for, and no config references it because it
// is not the consumer's topic. It is inside the discovery tree, which is what
// keeps it out — and it is also the topic one measured bridge wrongly put its
// OWN status under, which is why the exclusion is by tree rather than by
// name.
func TestHomeAssistantsOwnBirthTopicIsNotAGhost(t *testing.T) {
	t.Parallel()

	capture := lines(conflictBundle, conflictTopics,
		`{"topic":"homeassistant/status","payload":"online"}`)
	c, err := load(strings.NewReader(capture), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if found := diagnose(c); len(found) != 0 {
		t.Errorf("found = %+v on Home Assistant's own birth topic", found)
	}
}

// TestAMarkerGivenAsAJSONStringIsRecognised. A broker capture quotes its
// payloads and Home Assistant's diagnostics do not, and an operator should
// not have to know which they have — the same split [dump.Object] exists for.
func TestAMarkerGivenAsAJSONStringIsRecognised(t *testing.T) {
	t.Parallel()

	capture := lines(conflictBundle, conflictTopics,
		`{"topic":"daikin/serial:GONE-2/availability","payload":"offline"}`)
	c, err := load(strings.NewReader(capture), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := c.markers["daikin/serial:GONE-2/availability"]; !ok {
		t.Fatalf("markers = %v", c.markers)
	}
}

// TestALargePayloadIsNotKept pins the memory bound. A full `#` capture is the
// entire retained state of every integration on the broker, and the orphan
// check compares against four short tokens.
func TestALargePayloadIsNotKept(t *testing.T) {
	t.Parallel()

	big := `{"topic":"daikin/serial:AC-1/values/blob","payload":"` + strings.Repeat("x", 64) + `"}`
	c, err := load(strings.NewReader(lines(conflictBundle, conflictTopics, big)), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := c.markers["daikin/serial:AC-1/values/blob"]; ok {
		t.Error("a 64-byte payload was kept as an availability marker candidate")
	}
}

// TestTheRuntimeKindsOutrankTheReadingOnes pins the report order.
//
// A form conflict means the payload an operator is reading is not the one
// Home Assistant is using, so nothing below it can be trusted; an echo means
// the consumer is actively writing to a device it did not mean to. Both
// outrank a topic that merely carries nothing.
func TestTheRuntimeKindsOutrankTheReadingOnes(t *testing.T) {
	t.Parallel()

	capture := lines(conflictBundle, conflictEntity, echoConfig, mtecConfig, mtecState, ghostTopics,
		`{"topic":"daikin/bridge/status","payload":"online"}`,
		`{"topic":"loom/bridge/status","payload":"online"}`,
		`{"topic":"loom/hub/programs/night","payload":"OFF"}`)
	c, err := load(strings.NewReader(capture), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	var kinds []string
	for _, d := range found {
		if len(kinds) == 0 || kinds[len(kinds)-1] != d.kind {
			kinds = append(kinds, d.kind)
		}
	}
	want := []string{"form-conflict", "command-echo", "dead-state", "no-availability", "orphan-availability"}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Errorf("kinds = %v, want %v (findings: %+v)", kinds, want, found)
	}
}
