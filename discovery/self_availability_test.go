// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// selfEntity is a sensor gated on LevelSelf. withAvailabilityBinding decides
// whether it names a datapoint that reports the entity's availability, or
// leaves the level to fall back on the state datapoint's envelope flag.
func selfEntity(addr string, withAvailabilityBinding bool) *model.Basic {
	e := &model.Basic{
		EntityKey:      "temperature",
		EntityPlatform: hacatalog.PlatformSensor,
		Description: model.Description{
			Name:         model.L("Temperature"),
			Availability: model.Availability{Levels: []model.AvailabilityLevel{model.LevelSelf}},
		},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S(addr, "1", model.BucketValues, "TEMPERATURE"),
			Mode: model.Read,
		}},
	}
	if withAvailabilityBinding {
		e.Binds = append(e.Binds, model.Binding{
			Role: model.RoleAvailability,
			Slot: model.S(addr, "1", model.BucketValues, "UNREACH"),
			Mode: model.Read,
		})
	}
	return e
}

func selfContext(enc discovery.Encoding) discovery.StdContext {
	return discovery.StdContext{
		Layout:    topic.Default{Root: "loom"},
		Namespace: "loom",
		Enc:       enc,
	}
}

func onlyEntry(t *testing.T, entries []discovery.AvailabilityEntry) discovery.AvailabilityEntry {
	t.Helper()
	if len(entries) != 1 {
		t.Fatalf("want exactly one availability entry, got %d: %+v", len(entries), entries)
	}
	return entries[0]
}

// TestExplicitAvailabilityBindingReadsItsValue is the distinction the two
// shapes of LevelSelf turn on. A RoleAvailability binding is a datapoint whose
// *value* says whether the entity is available; the envelope's own `available`
// flag answers a different question — whether that datapoint is reachable —
// and reading it would make the explicit binding pointless.
func TestExplicitAvailabilityBindingReadsItsValue(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := selfEntity(dev.UID(), true)
	entry := onlyEntry(t, selfContext(discovery.EnvelopeEncoding).Availability(dev, e))

	if entry.Topic != "loom/"+dev.UID()+"/1/values/UNREACH" {
		t.Errorf("topic = %q, want the availability datapoint's own topic", entry.Topic)
	}
	if entry.ValueTemplate != discovery.SelfAvailabilityTemplate {
		t.Errorf("value_template = %q, want the value template", entry.ValueTemplate)
	}
	if entry.ValueTemplate == discovery.AvailabilityTemplate {
		t.Error("the explicit binding read the envelope's reachability flag instead of the datapoint's value")
	}
}

// TestSelfWithoutABindingReadsTheEnvelopeFlag: with nothing named, the state
// datapoint's own `available` flag is the right field, and the only one there
// is.
func TestSelfWithoutABindingReadsTheEnvelopeFlag(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := selfEntity(dev.UID(), false)
	entry := onlyEntry(t, selfContext(discovery.EnvelopeEncoding).Availability(dev, e))

	if entry.Topic != "loom/"+dev.UID()+"/1/values/TEMPERATURE" {
		t.Errorf("topic = %q, want the state topic", entry.Topic)
	}
	if entry.ValueTemplate != discovery.AvailabilityTemplate {
		t.Errorf("value_template = %q, want the envelope flag template", entry.ValueTemplate)
	}
}

// TestRawEncodingDoesNotTemplateABarePayload pins the failure that had nothing
// on the wire to show for it: a template against a raw payload renders
// value_json undefined, which matches neither payload_available nor
// payload_not_available. Home Assistant ignores an availability payload it
// does not recognise, so the entity stays unavailable forever.
func TestRawEncodingDoesNotTemplateABarePayload(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := selfEntity(dev.UID(), true)
	entry := onlyEntry(t, selfContext(discovery.RawEncoding).Availability(dev, e))

	if entry.ValueTemplate != "" {
		t.Errorf("value_template = %q, want none against a bare payload", entry.ValueTemplate)
	}
	if entry.Topic != "loom/"+dev.UID()+"/1/values/UNREACH" {
		t.Errorf("topic = %q", entry.Topic)
	}
	if entry.PayloadAvailable != "true" || entry.PayloadNotAvailable != "false" {
		t.Errorf("payloads = %q/%q, want true/false", entry.PayloadAvailable, entry.PayloadNotAvailable)
	}
}

// TestRawEncodingDropsTheFallback: without a binding there is no flag to read
// at all, so the level resolves to nothing rather than to a broken entry.
func TestRawEncodingDropsTheFallback(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := selfEntity(dev.UID(), false)
	if got := selfContext(discovery.RawEncoding).Availability(dev, e); len(got) != 0 {
		t.Errorf("want no entries, got %+v", got)
	}
}
