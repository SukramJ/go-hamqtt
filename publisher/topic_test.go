// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import "testing"

// TestParseConfigTopic pins the recognition the whole sweep rests on. The
// per-entity form has four segments after the prefix and the device document
// has three, the first of them the literal `device` — which is not a
// platform name, so the two can never be confused.
func TestParseConfigTopic(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		prefix string
		topic  string
		want   ConfigTopic
		ok     bool
	}{
		{
			name:  "per-entity form",
			topic: "homeassistant/sensor/ccu_abc/temperature/config",
			want:  ConfigTopic{Platform: "sensor", NodeID: "ccu_abc", ObjectID: "temperature"},
			ok:    true,
		},
		{
			name:  "device document form",
			topic: "homeassistant/device/ccu_abc/config",
			want:  ConfigTopic{NodeID: "ccu_abc", Bundle: true},
			ok:    true,
		},
		{
			// device_tracker is the closest platform name to the literal
			// and still produces four segments, so it parses as an entity.
			name:  "device_tracker is a platform, not the document segment",
			topic: "homeassistant/device_tracker/n/o/config",
			want:  ConfigTopic{Platform: "device_tracker", NodeID: "n", ObjectID: "o"},
			ok:    true,
		},
		{
			// Three segments not starting with `device` is the per-entity
			// form with the node id omitted, which this sweep cannot scope
			// and therefore must not claim.
			name:  "node-less per-entity form is not claimed",
			topic: "homeassistant/sensor/objectid/config",
		},
		{
			name:   "a custom prefix is honoured",
			prefix: "ha",
			topic:  "ha/device/n/config",
			want:   ConfigTopic{NodeID: "n", Bundle: true},
			ok:     true,
		},
		{
			name:   "a trailing slash on the prefix is the same prefix",
			prefix: "ha/",
			topic:  "ha/device/n/config",
			want:   ConfigTopic{NodeID: "n", Bundle: true},
			ok:     true,
		},
		{
			// Home Assistant's own lifecycle topic shares the root but not
			// the grammar. Claiming it would have the sweep retract the
			// birth message it also listens to.
			name:  "the birth topic is not a config",
			topic: "homeassistant/status",
		},
		{name: "another integration's tree", topic: "zigbee2mqtt/device/n/config"},
		{name: "not a config leaf", topic: "homeassistant/sensor/n/o/state"},
		{name: "too many segments", topic: "homeassistant/sensor/n/o/x/config"},
		{name: "empty node id", topic: "homeassistant/device//config"},
		{name: "empty platform", topic: "homeassistant//n/o/config"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ParseConfigTopic(tc.prefix, tc.topic)
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

// TestConfigTopicRenderers pins that the two renderers and the parser agree,
// which is what makes "what I published" and "what I found on the broker"
// comparable at all.
func TestConfigTopicRenderers(t *testing.T) {
	t.Parallel()
	entity := EntityConfigTopic("", "sensor", "n", "o")
	if entity != "homeassistant/sensor/n/o/config" {
		t.Fatalf("entity topic %q", entity)
	}
	bundle := BundleConfigTopic("ha", "n")
	if bundle != "ha/device/n/config" {
		t.Fatalf("bundle topic %q", bundle)
	}
	if got, ok := ParseConfigTopic("", entity); !ok || got.ObjectID != "o" {
		t.Fatalf("round trip: %+v ok=%v", got, ok)
	}
}

// TestBirthTopic pins where Home Assistant announces itself — and, by
// implication, where a consumer's own availability topic must not go.
func TestBirthTopic(t *testing.T) {
	t.Parallel()
	if got := BirthTopic(""); got != "homeassistant/status" {
		t.Fatalf("got %q", got)
	}
	if got := BirthTopic("ha/"); got != "ha/status" {
		t.Fatalf("got %q", got)
	}
}
