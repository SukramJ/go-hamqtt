// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// TestBundleWithoutChildDevicesEncodesAsBefore holds [discovery.Bundle]'s
// MarshalJSON to the encoding the struct tags alone produce. A payload field
// added to Bundle and forgotten in the wire struct would otherwise vanish
// from every document without a test noticing.
func TestBundleWithoutChildDevicesEncodesAsBefore(t *testing.T) {
	t.Parallel()

	b := inspectBundle()
	b.QoS = new(1)
	got, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// A defined type drops the methods and keeps the fields and their tags.
	type plain discovery.Bundle
	want, err := json.Marshal((*plain)(b))
	if err != nil {
		t.Fatalf("marshal plain: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("a bundle without child devices changed its bytes\n got %s\nwant %s", got, want)
	}
}

// TestBundleRendersChildDevices pins the document shape Home Assistant reads
// (DEVICE_DISCOVERY_SCHEMA and _CHILD_DEVICE_DISCOVERY_SCHEMA,
// components/mqtt/schemas.py:265-289 at core 2026.11.1): the device's own
// components stay in `components`, and each child device is one
// `child_devices` entry with its identifiers, name and components.
func TestBundleRendersChildDevices(t *testing.T) {
	t.Parallel()

	b := inspectBundle()
	b.AssignChild(discovery.ChildDeviceInfo{Identifiers: []string{"hc:dishwasher-1-rack"}, Name: "Rack"}, "door", "delay")
	b.AssignChild(discovery.ChildDeviceInfo{Identifiers: []string{"hc:dishwasher-1-unused"}, Name: "Unused"})

	if fs := discovery.Inspect(b, discovery.InspectOptions{}); len(fs.Errors()) > 0 {
		t.Fatalf("a well-formed child device is reported: %v", fs.Errors())
	}
	raw, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var doc struct {
		Components map[string]map[string]any `json:"components"`
		Children   []struct {
			Identifiers []string                  `json:"identifiers"`
			Name        string                    `json:"name"`
			Components  map[string]map[string]any `json:"components"`
		} `json:"child_devices"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := sortedMapKeys(doc.Components); !reflect.DeepEqual(got, []string{"power", "program"}) {
		t.Errorf("device components = %v, want the unassigned ones only", got)
	}
	if len(doc.Children) != 1 {
		t.Fatalf("child_devices = %+v, want the one child that has components", doc.Children)
	}
	child := doc.Children[0]
	if !reflect.DeepEqual(child.Identifiers, []string{"hc:dishwasher-1-rack"}) || child.Name != "Rack" {
		t.Errorf("child = %+v", child)
	}
	if got := sortedMapKeys(child.Components); !reflect.DeepEqual(got, []string{"delay", "door"}) {
		t.Errorf("child components = %v", got)
	}
	if child.Components["door"]["unique_id"] != "hc_dishwasher_1_door" {
		t.Errorf("child component lost its body: %v", child.Components["door"])
	}
}

// TestBundleTombstoneStaysOnItsChildDevice: a removal marker is a component
// like any other, so it is rendered where its key is assigned and still
// carries a platform and nothing else.
func TestBundleTombstoneStaysOnItsChildDevice(t *testing.T) {
	t.Parallel()

	b := inspectBundle()
	b.AssignChild(discovery.ChildDeviceInfo{Identifiers: []string{"hc:dishwasher-1-rack"}}, "door")
	b.Remove(map[string]hacatalog.Platform{"door": hacatalog.PlatformBinarySensor}, "door")

	raw, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var doc struct {
		Components map[string]any `json:"components"`
		Children   []struct {
			Components map[string]map[string]any `json:"components"`
		} `json:"child_devices"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, onDevice := doc.Components["door"]; onDevice {
		t.Error("the tombstone moved onto the device")
	}
	want := map[string]any{"platform": "binary_sensor"}
	if len(doc.Children) != 1 || !reflect.DeepEqual(doc.Children[0].Components["door"], want) {
		t.Errorf("child_devices = %+v, want the tombstone %v", doc.Children, want)
	}
}

// TestBundleRefusesAnUndeclaredChildDevice: moving the component onto the
// parent instead would publish a document that looks fine and puts the
// entity on the wrong device.
func TestBundleRefusesAnUndeclaredChildDevice(t *testing.T) {
	t.Parallel()

	b := inspectBundle()
	b.ChildOf = map[string]string{"door": "hc:nowhere"}
	if _, err := json.Marshal(b); err == nil {
		t.Error("a component assigned to an undeclared child device was encoded")
	}
}

// TestChildDeviceAdmitsEntityPlatformsOnly: _CHILD_DEVICE_COMPONENT_CONFIG_SCHEMA
// (components/mqtt/schemas.py:257-263 at core 2026.11.1) admits
// ENTITY_PLATFORMS, so a device trigger or a tag on a child device refuses
// the document.
func TestChildDeviceAdmitsEntityPlatformsOnly(t *testing.T) {
	t.Parallel()

	b := inspectBundle()
	b.Components["tag"] = discovery.Component{Platform: hacatalog.PlatformTag}
	b.AssignChild(discovery.ChildDeviceInfo{Identifiers: []string{"hc:dishwasher-1-rack"}}, "tag")

	var found bool
	for _, f := range discovery.Inspect(b, discovery.InspectOptions{}).ForComponent("tag") {
		if f.Kind == discovery.KindPlatformUnsupported && f.Scope == discovery.ScopeDocument && f.Severity == discovery.SeverityError {
			found = true
		}
	}
	if !found {
		t.Error("a tag on a child device is not reported as refusing the document")
	}
}

// TestContainKeepsChildAssignments: a component withheld from a child device
// is still declared there, and the kept ones must not slide onto the parent.
func TestContainKeepsChildAssignments(t *testing.T) {
	t.Parallel()

	b := inspectBundle()
	b.AssignChild(discovery.ChildDeviceInfo{Identifiers: []string{"hc:dishwasher-1-rack"}}, "door", "power")
	set(b, "power", func(c *discovery.Component) { c.DeviceClass = "not_a_class" })

	c := discovery.Contain(b, discovery.ContainOptions{})
	if _, withheld := c.Withheld["power"]; !withheld {
		t.Fatalf("withheld = %v, want power", c.Withheld)
	}
	if got := c.Bundle.ChildOf; got["door"] != "hc:dishwasher-1-rack" || got["power"] != "hc:dishwasher-1-rack" {
		t.Errorf("ChildOf = %v, want both assignments kept", got)
	}
	c.Bundle.ChildOf["door"] = "changed"
	if b.ChildOf["door"] != "hc:dishwasher-1-rack" {
		t.Error("Contain shares ChildOf with its input")
	}
}

func sortedMapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
