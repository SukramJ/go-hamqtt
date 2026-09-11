// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package dump_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/SukramJ/go-hamqtt/internal/dump"
)

func TestParseTopicForms(t *testing.T) {
	t.Parallel()

	cases := []struct {
		topic string
		want  dump.Topic
		ok    bool
	}{
		{
			"homeassistant/sensor/node/obj/config",
			dump.Topic{Platform: "sensor", Node: "node", Object: "obj", Form: dump.FormEntity},
			true,
		},
		{
			"homeassistant/device/node/config",
			dump.Topic{Node: "node", Form: dump.FormBundle},
			true,
		},
		// Home Assistant permits omitting the node id.
		{
			"homeassistant/sensor/obj/config",
			dump.Topic{Platform: "sensor", Object: "obj", Form: dump.FormEntity},
			true,
		},
		// device_tracker is a platform and produces four segments, so the
		// literal `device` guard cannot swallow it.
		{
			"homeassistant/device_tracker/node/phone/config",
			dump.Topic{Platform: "device_tracker", Node: "node", Object: "phone", Form: dump.FormEntity},
			true,
		},
		{"homeassistant/sensor/node/obj/state", dump.Topic{}, false},
		{"hass/sensor/node/obj/config", dump.Topic{}, false},
		{"homeassistant/a/b/c/d/config", dump.Topic{}, false},
	}
	for _, tc := range cases {
		got, ok := dump.ParseTopic(tc.topic, "homeassistant")
		if ok != tc.ok {
			t.Errorf("%s: ok = %v, want %v", tc.topic, ok, tc.ok)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("%s: got %+v, want %+v", tc.topic, got, tc.want)
		}
	}
}

func TestLabelDropsAnAbsentSegment(t *testing.T) {
	t.Parallel()

	if got := (dump.Topic{Object: "obj"}).Label(); got != "obj" {
		t.Errorf("label = %q, want the object alone", got)
	}
	if got := (dump.Topic{Node: "node"}).Label(); got != "node" {
		t.Errorf("label = %q, want the node alone", got)
	}
	if got := (dump.Topic{Node: "node", Object: "obj"}).Label(); got != "node/obj" {
		t.Errorf("label = %q", got)
	}
}

// TestObjectAcceptsBothShapes: a broker capture gives the payload as a
// string, Home Assistant's diagnostics give it as an object, and an operator
// should not have to know which they have.
func TestObjectAcceptsBothShapes(t *testing.T) {
	t.Parallel()

	asObject, err := dump.Object(json.RawMessage(`{"a":1}`))
	if err != nil || asObject["a"] != float64(1) {
		t.Fatalf("object form: %v %v", asObject, err)
	}
	asString, err := dump.Object(json.RawMessage(`"{\"a\":1}"`))
	if err != nil || asString["a"] != float64(1) {
		t.Fatalf("string form: %v %v", asString, err)
	}
	for _, empty := range []string{``, `null`, `""`, `"   "`} {
		got, err := dump.Object(json.RawMessage(empty))
		if err != nil || got != nil {
			t.Errorf("%q: got %v %v, want nil", empty, got, err)
		}
	}
	if _, err := dump.Object(json.RawMessage(`"{oops"`)); err == nil {
		t.Error("a string that is not an object was accepted")
	}
	if _, err := dump.Object(json.RawMessage(`[1,2]`)); err == nil {
		t.Error("an array was accepted as an object")
	}
}

func TestIsEmptyRecognisesARetraction(t *testing.T) {
	t.Parallel()

	for _, empty := range []string{``, `null`, `""`} {
		if !dump.IsEmpty(json.RawMessage(empty)) {
			t.Errorf("%q is a retraction", empty)
		}
	}
	for _, full := range []string{`{"a":1}`, `"online"`, `0`} {
		if dump.IsEmpty(json.RawMessage(full)) {
			t.Errorf("%q is not a retraction", full)
		}
	}
}

// TestReadSkipsBlankLinesAndReportsTheBadOne: a capture is concatenated and
// hand-edited, so blank lines are normal — but a line that does not parse is
// the operator's capture being wrong, and saying which line matters.
func TestReadSkipsBlankLinesAndReportsTheBadOne(t *testing.T) {
	t.Parallel()

	var seen []string
	err := dump.Read(strings.NewReader("\n{\"topic\":\"a\"}\n\n{\"topic\":\"b\"}\n"), func(r dump.Record) error {
		seen = append(seen, r.Topic)
		return nil
	})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if strings.Join(seen, ",") != "a,b" {
		t.Errorf("seen = %v", seen)
	}

	err = dump.Read(strings.NewReader("{\"topic\":\"a\"}\n{oops\n"), func(dump.Record) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Errorf("err = %v, want it to name line 2", err)
	}
}

// TestReadHandlesADocumentBiggerThanTheDefaultToken. A device bundle for a
// large device runs past bufio's default 64 KiB, and truncating one reports
// the tail as a parse error on a line that is perfectly valid.
func TestReadHandlesADocumentBiggerThanTheDefaultToken(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("x", 200*1024)
	line, err := json.Marshal(dump.Record{
		Topic:   "homeassistant/device/n/config",
		Payload: json.RawMessage(`{"big":"` + big + `"}`),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	n := 0
	if err := dump.Read(strings.NewReader(string(line)), func(dump.Record) error { n++; return nil }); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 1 {
		t.Errorf("read %d records, want 1", n)
	}
}
