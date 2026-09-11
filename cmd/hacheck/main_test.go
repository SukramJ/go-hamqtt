// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/SukramJ/go-hamqtt/internal/dump"
)

// TestObjectIDIsFoundOnARealPayload is the defect this tool exists for, on a
// payload taken verbatim off a live broker.
//
// `object_id` was replaced by `default_entity_id` and is declared by none of
// Home Assistant's 32 MQTT platforms. It is dropped on arrival with no error
// on the wire and no line in any log, so two shipped bridges publish it to
// this day — one of them alongside the key that replaced it, which is how it
// went unnoticed: the entity id is right, so nothing looks wrong.
func TestObjectIDIsFoundOnARealPayload(t *testing.T) {
	t.Parallel()

	const in = `{"topic":"homeassistant/sensor/unifi_f492bf8385a8/uptime/config","payload":{"unique_id":"unifi_f492bf8385a8_uptime","object_id":"unifi_ap_og_uptime","default_entity_id":"sensor.unifi_ap_og_uptime","state_topic":"unifi/default/device/f492bf8385a8/uptime","unit_of_measurement":"s","device_class":"duration","state_class":"measurement","entity_category":"diagnostic"}}`

	findings, scanned, err := run(strings.NewReader(in), "homeassistant")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if scanned != 1 {
		t.Fatalf("scanned %d payloads, want 1", scanned)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %+v, want exactly the object_id one", findings)
	}
	f := findings[0]
	if !f.blocking {
		t.Error("a key Home Assistant drops silently is not an advisory")
	}
	if !strings.Contains(f.text, "object_id") {
		t.Errorf("finding does not name the key: %q", f.text)
	}
	if f.entity != "unifi_f492bf8385a8/uptime" {
		t.Errorf("entity = %q", f.entity)
	}
}

// TestACleanPayloadProducesNothing, so the tool's silence means something.
func TestACleanPayloadProducesNothing(t *testing.T) {
	t.Parallel()

	const in = `{"topic":"homeassistant/sensor/node/temp/config","payload":{"unique_id":"x","state_topic":"a/b","device_class":"temperature","unit_of_measurement":"°C","state_class":"measurement"}}`
	findings, scanned, err := run(strings.NewReader(in), "homeassistant")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if scanned != 1 || len(findings) != 0 {
		t.Errorf("scanned=%d findings=%+v, want one clean payload", scanned, findings)
	}
}

// TestBundleComponentsAreCheckedIndividually: Home Assistant strips an
// undeclared key from the one component that has it and keeps the rest of
// the document, so that is the granularity the report has to use.
func TestBundleComponentsAreCheckedIndividually(t *testing.T) {
	t.Parallel()

	body := map[string]any{
		"device": map[string]any{"identifiers": []string{"n"}},
		"components": map[string]any{
			"good": map[string]any{"platform": "sensor", "unique_id": "a", "state_topic": "x"},
			"bad":  map[string]any{"platform": "sensor", "unique_id": "b", "state_topic": "y", "object_id": "nope"},
			"gone": map[string]any{"platform": "sensor"},
		},
	}
	raw, err := json.Marshal(dump.Record{Topic: "homeassistant/device/n/config", Payload: mustJSON(t, body)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	findings, scanned, err := run(strings.NewReader(string(raw)), "homeassistant")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if scanned != 1 {
		t.Fatalf("scanned = %d", scanned)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %+v, want only the bad component", findings)
	}
	if findings[0].entity != "n/bad" {
		t.Errorf("entity = %q, want the component that carries the key", findings[0].entity)
	}
}

// TestATombstoneIsNotABrokenEntity. A component carrying a platform and
// nothing else is how Home Assistant is told to delete it; reporting that as
// a payload missing every required key would bury the real findings.
func TestATombstoneIsNotABrokenEntity(t *testing.T) {
	t.Parallel()

	body := map[string]any{
		"device":     map[string]any{"identifiers": []string{"n"}},
		"components": map[string]any{"gone": map[string]any{"platform": "sensor"}},
	}
	raw, err := json.Marshal(dump.Record{Topic: "homeassistant/device/n/config", Payload: mustJSON(t, body)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	findings, _, err := run(strings.NewReader(string(raw)), "homeassistant")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("a tombstone was reported as broken: %+v", findings)
	}
}

// TestABundleWithoutComponentsIsReported: it is not a deletion and not a
// valid document, and Home Assistant would simply ignore it.
func TestABundleWithoutComponentsIsReported(t *testing.T) {
	t.Parallel()

	const in = `{"topic":"homeassistant/device/n/config","payload":{"device":{"identifiers":["n"]}}}`
	findings, _, err := run(strings.NewReader(in), "homeassistant")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(findings) != 1 || !findings[0].blocking {
		t.Fatalf("findings = %+v, want one blocking finding", findings)
	}
}

// TestPayloadAcceptsBothShapes: a broker dump gives the payload as a string,
// Home Assistant's own diagnostics give it as an object, and an operator
// should not have to know which they have.
func TestPayloadAcceptsBothShapes(t *testing.T) {
	t.Parallel()

	const in = `{"topic":"homeassistant/sensor/n/a/config","payload":{"unique_id":"a","state_topic":"x","object_id":"o"}}` + "\n" +
		`{"topic":"homeassistant/sensor/n/b/config","payload":"{\"unique_id\":\"b\",\"state_topic\":\"x\",\"object_id\":\"o\"}"}`

	findings, scanned, err := run(strings.NewReader(in), "homeassistant")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if scanned != 2 {
		t.Errorf("scanned = %d, want both shapes read", scanned)
	}
	if len(findings) != 2 {
		t.Errorf("findings = %+v, want one per payload", findings)
	}
}

// TestRetractionsAndForeignTopicsAreSkipped. An operator dumping
// `homeassistant/#` catches retractions and status topics; counting either
// as a payload would make the summary lie about what was checked.
func TestRetractionsAndForeignTopicsAreSkipped(t *testing.T) {
	t.Parallel()

	in := strings.Join([]string{
		`{"topic":"homeassistant/sensor/n/a/config","payload":""}`,
		`{"topic":"homeassistant/sensor/n/b/config","payload":null}`,
		`{"topic":"homeassistant/status","payload":"online"}`,
		`{"topic":"unifi/bridge/status","payload":"online"}`,
		``,
	}, "\n")

	findings, scanned, err := run(strings.NewReader(in), "homeassistant")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("findings = %+v, want none", findings)
	}
	if scanned != 2 {
		// The two non-config topics are not payloads; the two retractions
		// are read and then skipped as configs.
		t.Errorf("scanned = %d, want the two retracted configs counted as read", scanned)
	}
}

// TestMalformedInputIsAnErrorNotAFinding: a line that does not parse is the
// operator's dump being wrong, not Home Assistant's schema being violated,
// and conflating the two sends them looking in the wrong place.
func TestMalformedInputIsAnErrorNotAFinding(t *testing.T) {
	t.Parallel()

	if _, _, err := run(strings.NewReader(`{not json`), "homeassistant"); err == nil {
		t.Error("a malformed line was accepted")
	}
	if _, _, err := run(strings.NewReader(`{"topic":"homeassistant/sensor/n/a/config","payload":"{oops"}`), "homeassistant"); err == nil {
		t.Error("a payload that does not parse was accepted")
	}
}

func TestReportCountsAndExitCode(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	code := report(&sb, []finding{
		{entity: "a", blocking: true, text: "broken"},
		{entity: "b", text: "rewritten"},
	}, 7, false)
	if code != 1 {
		t.Errorf("exit code = %d, want 1 when something is blocking", code)
	}
	out := sb.String()
	if !strings.Contains(out, "- a: broken") || !strings.Contains(out, "~ b: rewritten") {
		t.Errorf("report does not distinguish blocking from advisory:\n%s", out)
	}
	if !strings.Contains(out, "7 payload(s) checked, 1 blocking, 1 advisory") {
		t.Errorf("summary = %q", out)
	}

	var quiet strings.Builder
	if code := report(&quiet, []finding{{entity: "b", text: "rewritten"}}, 3, true); code != 0 {
		t.Errorf("exit code = %d, want 0 when only advisories remain", code)
	}
	if strings.Contains(quiet.String(), "rewritten") {
		t.Error("quiet printed a finding")
	}
	if !strings.Contains(quiet.String(), "3 payload(s) checked") {
		t.Error("quiet dropped the summary, which is the one thing it must keep")
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// TestCLIExitCodes. A dump this tool cannot read and payloads it can read but
// does not like are different problems, and an operator has to be able to
// tell them apart from the exit code alone.
func TestCLIExitCodes(t *testing.T) {
	t.Parallel()

	var out, errOut strings.Builder
	if code := cli(nil,
		strings.NewReader(`{"topic":"homeassistant/sensor/n/a/config","payload":{"unique_id":"a","state_topic":"x"}}`),
		&out, &errOut); code != 0 {
		t.Errorf("clean input: exit %d, want 0 (%s)", code, errOut.String())
	}

	out.Reset()
	errOut.Reset()
	if code := cli([]string{"-quiet"},
		strings.NewReader(`{"topic":"homeassistant/sensor/n/a/config","payload":{"unique_id":"a","state_topic":"x","object_id":"o"}}`),
		&out, &errOut); code != 1 {
		t.Errorf("blocking finding: exit %d, want 1", code)
	}

	out.Reset()
	errOut.Reset()
	if code := cli(nil, strings.NewReader(`{oops`), &out, &errOut); code != 2 {
		t.Errorf("unreadable dump: exit %d, want 2", code)
	}
	if !strings.Contains(errOut.String(), "hacheck:") {
		t.Errorf("the parse failure was not reported to stderr: %q", errOut.String())
	}

	out.Reset()
	errOut.Reset()
	if code := cli([]string{"-nosuchflag"}, strings.NewReader(""), &out, &errOut); code != 2 {
		t.Errorf("bad flag: exit %d, want 2", code)
	}
}

// TestAForeignPrefixIsHonoured, because an installation may not use the
// default one and would otherwise get a silent "0 payload(s) checked".
func TestAForeignPrefixIsHonoured(t *testing.T) {
	t.Parallel()

	const in = `{"topic":"hass/sensor/n/a/config","payload":{"unique_id":"a","state_topic":"x","object_id":"o"}}`
	if _, scanned, _ := run(strings.NewReader(in), "homeassistant"); scanned != 0 {
		t.Errorf("scanned = %d with the wrong prefix, want 0", scanned)
	}
	findings, scanned, err := run(strings.NewReader(in), "hass/")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if scanned != 1 || len(findings) != 1 {
		t.Errorf("scanned=%d findings=%d with the right prefix", scanned, len(findings))
	}
}

// TestAComponentThatIsNotAnObject is the shape a hand-edited or
// half-generated document takes, and it must be named rather than panicked
// over.
func TestAComponentThatIsNotAnObject(t *testing.T) {
	t.Parallel()

	const in = `{"topic":"homeassistant/device/n/config","payload":{"components":{"a":"not an object","b":{"unique_id":"x"}}}}`
	findings, _, err := run(strings.NewReader(in), "homeassistant")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	var sawNotObject, sawNoPlatform bool
	for _, f := range findings {
		if strings.Contains(f.text, "not an object") {
			sawNotObject = true
		}
		if strings.Contains(f.text, "no platform") {
			sawNoPlatform = true
		}
	}
	if !sawNotObject || !sawNoPlatform {
		t.Errorf("findings = %+v, want both malformed components named", findings)
	}
}
