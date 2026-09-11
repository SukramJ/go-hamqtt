// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package main

import (
	"strings"
	"testing"
)

func lines(s ...string) string { return strings.Join(s, "\n") }

// A real payload off a live broker: this bridge declares no availability at
// all, which is the defect ADR 0070 predicted hadoctor would find.
const mtecConfig = `{"topic":"homeassistant/binary_sensor/MTEC_grid_inject_switch/config","payload":{"unique_id":"MTEC_grid_inject_switch","name":"Netzeinspeisung-Begrenzung (Schalter)","payload_on":"1","payload_off":"0","state_topic":"MTEC/Z112200293130249/config/grid_inject_switch/state","device":{"identifiers":["Z112200293130249"],"name":"MrBurns"}}}`

const mtecState = `{"topic":"MTEC/Z112200293130249/config/grid_inject_switch/state","payload":"1"}`

// The same bridge family done right: bridge status plus a per-device topic.
const unifiConfig = `{"topic":"homeassistant/sensor/unifi_ap/uptime/config","payload":{"unique_id":"unifi_ap_uptime","state_topic":"unifi/device/ap/uptime","availability":[{"topic":"unifi/bridge/status"},{"topic":"unifi/device/ap/state"}],"availability_mode":"all"}}`

const unifiTopics = `{"topic":"unifi/bridge/status","payload":"online"}
{"topic":"unifi/device/ap/uptime","payload":"546197"}
{"topic":"unifi/device/ap/state","payload":"ONLINE"}`

// TestAnEntityWithNoAvailabilityIsReported is the finding on a real payload.
// It is not fatal, and that is exactly why it survives: everything works
// until the bridge dies, and then Home Assistant keeps showing the last
// value as if it were current.
func TestAnEntityWithNoAvailabilityIsReported(t *testing.T) {
	t.Parallel()

	c, err := load(strings.NewReader(lines(mtecConfig, mtecState)), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 1 || found[0].kind != "no-availability" {
		t.Fatalf("found = %+v, want exactly the missing availability", found)
	}
	if found[0].entity != "MTEC_grid_inject_switch" {
		t.Errorf("entity = %q", found[0].entity)
	}
}

// TestAnEntityWiredCorrectlyIsSilent, so a finding means something.
func TestAnEntityWiredCorrectlyIsSilent(t *testing.T) {
	t.Parallel()

	c, err := load(strings.NewReader(lines(unifiConfig, unifiTopics)), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if found := diagnose(c); len(found) != 0 {
		t.Errorf("found = %+v on a correctly wired entity", found)
	}
}

// TestADeadAvailabilityTopicOutranksAMissingOne. An availability topic
// nobody publishes leaves the entity unavailable forever with nothing to say
// why; a missing declaration only lies later. The report has to lead with
// the one that is broken now.
func TestADeadAvailabilityTopicOutranksAMissingOne(t *testing.T) {
	t.Parallel()

	// unifi's per-device availability topic is absent from the capture.
	capture := lines(mtecConfig, mtecState, unifiConfig,
		`{"topic":"unifi/bridge/status","payload":"online"}`,
		`{"topic":"unifi/device/ap/uptime","payload":"1"}`)

	c, err := load(strings.NewReader(capture), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 2 {
		t.Fatalf("found = %+v, want one of each", found)
	}
	if found[0].kind != "dead-availability" {
		t.Errorf("first finding is %q, want the one that is broken right now", found[0].kind)
	}
}

// TestADeadStateTopicIsReported: an entity pointing at a topic nothing
// publishes shows as unknown and looks like a broker problem.
func TestADeadStateTopicIsReported(t *testing.T) {
	t.Parallel()

	c, err := load(strings.NewReader(lines(unifiConfig,
		`{"topic":"unifi/bridge/status","payload":"online"}`,
		`{"topic":"unifi/device/ap/state","payload":"ONLINE"}`)), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 1 || found[0].kind != "dead-state" {
		t.Fatalf("found = %+v, want the dead state topic", found)
	}
}

// TestAComponentInheritsTheDocumentsAvailability. A device bundle may carry
// one availability list at the top for every component; a component under
// it is not an entity without availability.
func TestAComponentInheritsTheDocumentsAvailability(t *testing.T) {
	t.Parallel()

	capture := lines(
		`{"topic":"homeassistant/device/n/config","payload":{"device":{"identifiers":["n"]},"availability":[{"topic":"loom/bridge/status"}],"components":{"temp":{"platform":"sensor","unique_id":"a","state_topic":"loom/n/temp"},"gone":{"platform":"sensor"}}}}`,
		`{"topic":"loom/bridge/status","payload":"online"}`,
		`{"topic":"loom/n/temp","payload":"21.5"}`)

	c, err := load(strings.NewReader(capture), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.entities != 1 {
		t.Errorf("entities = %d — the tombstone must not count as one", c.entities)
	}
	if found := diagnose(c); len(found) != 0 {
		t.Errorf("found = %+v; the component inherits the document's availability", found)
	}
}

// TestARetractedTopicCountsAsUnpublished: an empty retained payload is the
// broker clearing the topic, which is precisely the state that leaves an
// entity waiting forever.
func TestARetractedTopicCountsAsUnpublished(t *testing.T) {
	t.Parallel()

	c, err := load(strings.NewReader(lines(unifiConfig,
		`{"topic":"unifi/bridge/status","payload":""}`,
		`{"topic":"unifi/device/ap/state","payload":"ONLINE"}`,
		`{"topic":"unifi/device/ap/uptime","payload":"1"}`)), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 1 || found[0].kind != "dead-availability" {
		t.Fatalf("found = %+v, want the retracted availability topic named", found)
	}
}

// TestADiscoveryOnlyCaptureIsRefused. This is the one way to get a wrong
// answer out of this tool: subscribe to `homeassistant/#` and every state
// topic looks unpublished, so every entity looks broken. Refusing beats
// answering wrongly — an operator would act on that report.
func TestADiscoveryOnlyCaptureIsRefused(t *testing.T) {
	t.Parallel()

	var out, errOut strings.Builder
	code := cli(nil, strings.NewReader(lines(mtecConfig, unifiConfig)), &out, &errOut)
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
	if !strings.Contains(errOut.String(), "only discovery topics") {
		t.Errorf("stderr does not explain the refusal: %q", errOut.String())
	}
	if out.String() != "" {
		t.Errorf("a refused capture still produced a report: %q", out.String())
	}
}

// TestAnEmptyCaptureIsRefused, rather than reported as a clean bill of
// health — which is what "0 findings" would otherwise read as.
func TestAnEmptyCaptureIsRefused(t *testing.T) {
	t.Parallel()

	var out, errOut strings.Builder
	if code := cli(nil, strings.NewReader(`{"topic":"unifi/bridge/status","payload":"online"}`), &out, &errOut); code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
	if !strings.Contains(errOut.String(), "no discovery configs") {
		t.Errorf("stderr = %q", errOut.String())
	}
}

func TestCLIExitCodes(t *testing.T) {
	t.Parallel()

	var out, errOut strings.Builder
	// A clean capture exits 0.
	if code := cli(nil, strings.NewReader(lines(unifiConfig, unifiTopics)), &out, &errOut); code != 0 {
		t.Errorf("clean: exit %d, want 0 (%s)", code, errOut.String())
	}

	// A missing availability declaration alone is advisory: nothing is
	// broken right now, so it must not fail a pipeline.
	out.Reset()
	errOut.Reset()
	if code := cli(nil, strings.NewReader(lines(mtecConfig, mtecState)), &out, &errOut); code != 0 {
		t.Errorf("advisory only: exit %d, want 0", code)
	}
	if !strings.Contains(out.String(), "no-availability") {
		t.Errorf("the advisory was not reported: %q", out.String())
	}

	// Something broken right now does fail.
	out.Reset()
	errOut.Reset()
	broken := lines(unifiConfig, `{"topic":"unifi/bridge/status","payload":"online"}`,
		`{"topic":"unifi/device/ap/uptime","payload":"1"}`)
	if code := cli(nil, strings.NewReader(broken), &out, &errOut); code != 1 {
		t.Errorf("dead availability: exit %d, want 1", code)
	}

	out.Reset()
	errOut.Reset()
	if code := cli([]string{"-nosuchflag"}, strings.NewReader(""), &out, &errOut); code != 2 {
		t.Errorf("bad flag: exit %d, want 2", code)
	}

	out.Reset()
	errOut.Reset()
	if code := cli(nil, strings.NewReader(`{oops`), &out, &errOut); code != 2 {
		t.Errorf("unreadable capture: exit %d, want 2", code)
	}
}

// TestAvailabilityTopicSingularFormIsRead: Home Assistant accepts both
// `availability_topic` and the `availability` list, and a bridge using the
// older spelling is exactly the kind that has this defect.
func TestAvailabilityTopicSingularFormIsRead(t *testing.T) {
	t.Parallel()

	capture := lines(
		`{"topic":"homeassistant/sensor/n/a/config","payload":{"unique_id":"a","state_topic":"x/state","availability_topic":"x/avail"}}`,
		`{"topic":"x/state","payload":"1"}`)
	c, err := load(strings.NewReader(capture), "homeassistant")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	found := diagnose(c)
	if len(found) != 1 || found[0].kind != "dead-availability" {
		t.Fatalf("found = %+v, want the singular form read and its topic checked", found)
	}
}
