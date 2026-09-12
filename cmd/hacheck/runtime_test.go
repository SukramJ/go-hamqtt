// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package main

import (
	"bytes"
	"strings"
	"testing"
)

// The measured defect, as the bridge that shipped it publishes it: an
// availability topic inside Home Assistant's own discovery tree. The config
// is perfectly legal — `availability` is a legal key and its topic a legal
// topic — and nothing there is the consumer's to write, so the marker is
// never published and the entity stays unavailable forever.
const inertLWT = `{"topic":"homeassistant/sensor/mtec/power/config","payload":{` +
	`"unique_id":"mtec_power","name":"Power","device_class":"power",` +
	`"state_class":"measurement","unit_of_measurement":"W",` +
	`"state_topic":"MTEC/Z1/values/power",` +
	`"availability":[{"topic":"homeassistant/status/lwt"}],` +
	`"device":{"identifiers":["Z1"]}}}`

// TestAnAvailabilityTopicInsideTheDiscoveryTreeIsAdvisory pins the finding
// and its severity.
//
// Advisory, because Home Assistant accepts the payload: the exit code means
// "this payload will not work", and this one does work. It is simply wired to
// wait on a topic nobody writes, and only an operator can say whether that
// was deliberate.
func TestAnAvailabilityTopicInsideTheDiscoveryTreeIsAdvisory(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer
	code := cli(nil, strings.NewReader(inertLWT), &out, &errOut)
	if code != 0 {
		t.Fatalf("exit = %d, want 0 (advisory); stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "homeassistant/status/lwt") {
		t.Errorf("the report does not name the topic:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "1 advisory") {
		t.Errorf("the summary does not count it:\n%s", out.String())
	}
}

// TestGatingOnHomeAssistantsOwnBirthTopicIsAllowed is the exclusion.
//
// `<prefix>/status` is Home Assistant's own birth topic, and an entity that
// should disappear when the MQTT integration goes down is entitled to say so.
// Only the rest of the tree is nobody's to write in.
func TestGatingOnHomeAssistantsOwnBirthTopicIsAllowed(t *testing.T) {
	t.Parallel()

	legal := strings.Replace(inertLWT, "homeassistant/status/lwt", "homeassistant/status", 1)
	var out, errOut bytes.Buffer
	if code := cli(nil, strings.NewReader(legal), &out, &errOut); code != 0 {
		t.Fatalf("exit = %d; stderr: %s", code, errOut.String())
	}
	if strings.Contains(out.String(), "advisory") && !strings.Contains(out.String(), "0 advisory") {
		t.Errorf("Home Assistant's own birth topic was reported:\n%s", out.String())
	}
}

// TestADifferentPrefixMovesTheTree, because whether a topic is inside
// somebody else's tree is not a property of the string. An operator who
// renamed the discovery prefix has moved the boundary, and a check with the
// default baked in would report the wrong half of the broker.
func TestADifferentPrefixMovesTheTree(t *testing.T) {
	t.Parallel()

	// The same payload, read with a prefix that does not contain the topic:
	// now `homeassistant/status/lwt` is outside the discovery tree and this
	// check has no opinion on it.
	moved := strings.Replace(inertLWT, `"topic":"homeassistant/sensor`, `"topic":"ha/sensor`, 1)
	var out, errOut bytes.Buffer
	if code := cli([]string{"-prefix", "ha"}, strings.NewReader(moved), &out, &errOut); code != 0 {
		t.Fatalf("exit = %d; stderr: %s", code, errOut.String())
	}
	if strings.Contains(out.String(), "status/lwt") {
		t.Errorf("the check reported a topic outside the configured tree:\n%s", out.String())
	}
}

// A program entity whose state is mirrored onto its own trigger topic. Every
// state publish ran the program — on every boot, on every rediscovery, for
// every program in the house — and the only signal was the programs running.
const selfEcho = `{"topic":"homeassistant/switch/loom/prog_night/config","payload":{` +
	`"unique_id":"loom_prog_night","name":"Night",` +
	`"command_topic":"loom/hub/programs/night","state_topic":"loom/hub/programs/night",` +
	`"availability":[{"topic":"loom/bridge/status"}],` +
	`"device":{"identifiers":["loom_hub"]}}}`

// TestACommandTopicThatIsAlsoReadIsReported is the echo on the shape the
// reference implementation shipped.
//
// The single-payload form of the echo: the general case is between two
// entities and needs a whole capture, which is hadoctor's `command-echo`.
// This one shape needs no capture at all, so a consumer's golden-file test
// can catch it at build time.
func TestACommandTopicThatIsAlsoReadIsReported(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer
	if code := cli(nil, strings.NewReader(selfEcho), &out, &errOut); code != 0 {
		t.Fatalf("exit = %d; stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "loom/hub/programs/night") {
		t.Errorf("the echo was not reported:\n%s", out.String())
	}
}

// TestASeparateCommandTopicIsSilent, so a finding means something.
func TestASeparateCommandTopicIsSilent(t *testing.T) {
	t.Parallel()

	clean := strings.Replace(selfEcho,
		`"state_topic":"loom/hub/programs/night"`,
		`"state_topic":"loom/hub/programs/night/state"`, 1)
	var out, errOut bytes.Buffer
	if code := cli(nil, strings.NewReader(clean), &out, &errOut); code != 0 {
		t.Fatalf("exit = %d; stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "0 blocking, 0 advisory") {
		t.Errorf("a correctly wired entity produced findings:\n%s", out.String())
	}
}

// TestACoverPositionTopicIsTreatedAsACommand pins the naming exception.
// `set_position_topic` does not end in `command_topic`, and a suffix-only
// rule reads it as a state topic — after which the echo it forms with
// `position_topic` is invisible.
func TestACoverPositionTopicIsTreatedAsACommand(t *testing.T) {
	t.Parallel()

	body := map[string]any{
		"set_position_topic": "blinds/living/position",
		"position_topic":     "blinds/living/position",
	}
	found := checkSelfEcho("t", "blinds/living", body)
	if len(found) != 1 {
		t.Fatalf("found = %+v, want the echo on set_position_topic", found)
	}
	if found[0].blocking {
		t.Error("the echo is reported blocking; Home Assistant accepts the payload")
	}
}

// TestABundleFrameAvailabilityIsChecked. A device document may carry one
// availability list for every component, and a wrongly-treed topic there
// affects the whole device rather than one entity — so it must not be the
// one place the check does not look.
func TestABundleFrameAvailabilityIsChecked(t *testing.T) {
	t.Parallel()

	doc := `{"topic":"homeassistant/device/loom_hub/config","payload":{` +
		`"device":{"identifiers":["loom_hub"]},"origin":{"name":"loom"},` +
		`"availability":[{"topic":"homeassistant/loom/status"}],` +
		`"components":{"night":{"platform":"switch","unique_id":"loom_night",` +
		`"command_topic":"loom/hub/programs/night"}}}}`
	var out, errOut bytes.Buffer
	if code := cli(nil, strings.NewReader(doc), &out, &errOut); code != 0 {
		t.Fatalf("exit = %d; stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "homeassistant/loom/status") {
		t.Errorf("the frame's availability topic was not checked:\n%s", out.String())
	}
}

// TestTheLegacySingleAvailabilityTopicIsChecked. A consumer mid-migration
// publishes `availability_topic` before it publishes the list, and a check
// that knew only the list form would pass every legacy payload in silence —
// which is the shape of the defect it exists to find.
func TestTheLegacySingleAvailabilityTopicIsChecked(t *testing.T) {
	t.Parallel()

	found := checkAvailabilityTree("t", "e",
		map[string]any{"availability_topic": "homeassistant/status/lwt"}, "homeassistant")
	if len(found) != 1 {
		t.Fatalf("found = %+v, want the legacy form reported", found)
	}
}
