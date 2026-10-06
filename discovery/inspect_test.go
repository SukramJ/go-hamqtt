// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"math"
	"slices"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// inspectBundle is a device with one component of each platform the new
// rules look at, every one of them clean, so a case can break exactly one
// thing and see only that finding.
func inspectBundle() *discovery.Bundle {
	return &discovery.Bundle{
		NodeID: "dishwasher_1",
		Device: discovery.DeviceInfo{Identifiers: []string{"hc:dishwasher-1"}, Name: "Dishwasher"},
		Origin: discovery.Origin{Name: "go-homeconnect2mqtt"},
		Components: map[string]discovery.Component{
			"power": {
				Platform:      hacatalog.PlatformSensor,
				UniqueID:      "hc_dishwasher_1_power",
				StateTopic:    "hc/status/dishwasher-1/power",
				DeviceClass:   "power",
				StateClass:    hacatalog.StateClassMeasurement,
				UnitOfMeasure: "W",
			},
			"program": {
				Platform:     hacatalog.PlatformSelect,
				UniqueID:     "hc_dishwasher_1_program",
				StateTopic:   "hc/status/dishwasher-1/program",
				CommandTopic: "hc/set/dishwasher-1/program",
				Options:      []string{"eco", "auto"},
			},
			"delay": {
				Platform:     hacatalog.PlatformNumber,
				UniqueID:     "hc_dishwasher_1_delay",
				CommandTopic: "hc/set/dishwasher-1/delay",
				Min:          new(0.0),
				Max:          new(24.0),
				Step:         new(0.5),
			},
			"door": {
				Platform:   hacatalog.PlatformBinarySensor,
				UniqueID:   "hc_dishwasher_1_door",
				StateTopic: "hc/status/dishwasher-1/door",
			},
		},
	}
}

func set(b *discovery.Bundle, key string, edit func(c *discovery.Component)) {
	c := b.Components[key]
	edit(&c)
	b.Components[key] = c
}

// want is one expected finding.
type want struct {
	component string
	kind      discovery.FindingKind
	scope     discovery.Scope
	severity  discovery.Severity
	strip     bool
}

// inspectCases is every finding kind with the Home Assistant core
// (2026.10.0b2) reference that decides its scope and severity. The same
// table feeds the containment tests and the Validate-equivalence corpus.
var inspectCases = []struct {
	name string
	edit func(b *discovery.Bundle)
	want []want
}{
	{
		name: "node id outside TOPIC_MATCHER (mqtt/discovery.py:60-63) refuses the document",
		edit: func(b *discovery.Bundle) { b.NodeID = "dish.washer" },
		want: []want{{"", discovery.KindNodeID, discovery.ScopeDocument, discovery.SeverityError, false}},
	},
	{
		name: "empty node id refuses the document",
		edit: func(b *discovery.Bundle) { b.NodeID = "" },
		want: []want{{"", discovery.KindNodeID, discovery.ScopeDocument, discovery.SeverityError, false}},
	},
	{
		name: "empty origin.name passes cv.string (mqtt/schemas.py:162): document warning, not a refusal",
		edit: func(b *discovery.Bundle) { b.Origin.Name = "" },
		want: []want{{"", discovery.KindOrigin, discovery.ScopeDocument, discovery.SeverityWarning, false}},
	},
	{
		name: "device without identifier (mqtt/schemas.py:123-130) refuses the document",
		edit: func(b *discovery.Bundle) { b.Device.Identifiers = nil },
		want: []want{{"", discovery.KindDeviceIdentity, discovery.ScopeDocument, discovery.SeverityError, false}},
	},
	{
		name: "empty components is accepted by DEVICE_DISCOVERY_SCHEMA (mqtt/schemas.py:217-219): warning",
		edit: func(b *discovery.Bundle) { b.Components = map[string]discovery.Component{} },
		want: []want{{"", discovery.KindNoComponents, discovery.ScopeDocument, discovery.SeverityWarning, false}},
	},
	{
		name: "missing platform (mqtt/schemas.py:207-211) refuses the document, curable by withholding",
		edit: func(b *discovery.Bundle) { set(b, "door", func(c *discovery.Component) { c.Platform = "" }) },
		want: []want{{"door", discovery.KindPlatformMissing, discovery.ScopeDocument, discovery.SeverityError, false}},
	},
	{
		name: "platform outside SUPPORTED_COMPONENTS (mqtt/schemas.py:209) refuses the document",
		edit: func(b *discovery.Bundle) { set(b, "door", func(c *discovery.Component) { c.Platform = "sensr" }) },
		want: []want{{"door", discovery.KindPlatformUnsupported, discovery.ScopeDocument, discovery.SeverityError, false}},
	},
	{
		name: "missing unique_id on an entity platform (mqtt/schemas.py:199-204) refuses the document",
		edit: func(b *discovery.Bundle) { set(b, "door", func(c *discovery.Component) { c.UniqueID = "" }) },
		want: []want{{"door", discovery.KindUniqueIDMissing, discovery.ScopeDocument, discovery.SeverityError, false}},
	},
	{
		name: "tag is not in ENTITY_PLATFORMS (mqtt/const.py:388-419): no unique_id needed",
		edit: func(b *discovery.Bundle) {
			b.Components["scanner"] = discovery.Component{
				Platform: hacatalog.PlatformTag,
				Extra:    map[string]any{"topic": "hc/event/dishwasher-1/tag"},
			}
		},
		want: nil,
	},
	{
		name: "duplicate (platform, unique_id) is ignored by the entity platform (helpers/entity_platform.py:995-1015)",
		edit: func(b *discovery.Bundle) {
			b.Components["power2"] = b.Components["power"]
		},
		want: []want{{"power2", discovery.KindUniqueIDDuplicate, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "unknown key is stripped by REMOVE_EXTRA (mqtt/sensor.py:165-168): strippable warning",
		edit: func(b *discovery.Bundle) {
			set(b, "power", func(c *discovery.Component) { c.Extra = map[string]any{"translation_key": "power"} })
		},
		want: []want{{"power", discovery.KindUnknownKey, discovery.ScopeComponent, discovery.SeverityWarning, true}},
	},
	{
		name: "missing Required key refuses the entity (mqtt/entity.py:324-347)",
		edit: func(b *discovery.Bundle) { set(b, "door", func(c *discovery.Component) { c.StateTopic = "" }) },
		want: []want{{"door", discovery.KindRequiredKey, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "undeclared device class fails the platform's Coerce (mqtt/sensor.py:79): component error",
		edit: func(b *discovery.Bundle) { set(b, "door", func(c *discovery.Component) { c.DeviceClass = "garage" }) },
		want: []want{{"door", discovery.KindDeviceClass, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "state class impossible for device class only logs (sensor/__init__.py:620-645): strippable warning",
		edit: func(b *discovery.Bundle) {
			b.Components["finish"] = discovery.Component{
				Platform: hacatalog.PlatformSensor, UniqueID: "hc_dishwasher_1_finish",
				StateTopic: "hc/status/dishwasher-1/finish", DeviceClass: "timestamp",
				StateClass: hacatalog.StateClassMeasurement,
			}
		},
		want: []want{{"finish", discovery.KindStateClass, discovery.ScopeComponent, discovery.SeverityWarning, true}},
	},
	{
		name: "options without enum device class (mqtt/sensor.py:120-125): component error",
		edit: func(b *discovery.Bundle) {
			b.Components["phase"] = discovery.Component{
				Platform: hacatalog.PlatformSensor, UniqueID: "hc_dishwasher_1_phase",
				StateTopic: "hc/status/dishwasher-1/phase", Options: []string{"wash", "dry"},
			}
		},
		want: []want{{"phase", discovery.KindOptions, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "empty options list (mqtt/sensor.py:112-113): component error",
		edit: func(b *discovery.Bundle) {
			b.Components["phase"] = discovery.Component{
				Platform: hacatalog.PlatformSensor, UniqueID: "hc_dishwasher_1_phase",
				StateTopic: "hc/status/dishwasher-1/phase", DeviceClass: "enum",
				Extra: map[string]any{"options": []any{}},
			}
		},
		want: []want{{"phase", discovery.KindOptions, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "legacy micro sign is rewritten by AMBIGUOUS_UNITS (mqtt/sensor.py:141-143): warning, kept",
		edit: func(b *discovery.Bundle) {
			b.Components["pm"] = discovery.Component{
				Platform: hacatalog.PlatformSensor, UniqueID: "hc_dishwasher_1_pm",
				StateTopic: "hc/status/dishwasher-1/pm", DeviceClass: "pm25",
				StateClass: hacatalog.StateClassMeasurement, UnitOfMeasure: "\u00b5g/m\u00b3",
			}
		},
		want: []want{{"pm", discovery.KindLegacyUnit, discovery.ScopeComponent, discovery.SeverityWarning, false}},
	},
	{
		name: "unit outside DEVICE_CLASS_UNITS (mqtt/sensor.py:148-155): component error",
		edit: func(b *discovery.Bundle) {
			set(b, "power", func(c *discovery.Component) {
				c.DeviceClass = "energy"
				c.StateClass = hacatalog.StateClassTotalIncreasing
			})
		},
		want: []want{{"power", discovery.KindUnitDeviceClass, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "unit outside STATE_CLASS_UNITS (mqtt/sensor.py:127-136): component error",
		edit: func(b *discovery.Bundle) {
			b.Components["angle"] = discovery.Component{
				Platform: hacatalog.PlatformSensor, UniqueID: "hc_dishwasher_1_angle",
				StateTopic: "hc/status/dishwasher-1/angle", StateClass: "measurement_angle",
				UnitOfMeasure: "rad",
			}
		},
		want: []want{{"angle", discovery.KindUnitStateClass, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "last_reset_value_template without state_class total (mqtt/sensor.py:94-101): component error",
		edit: func(b *discovery.Bundle) {
			set(b, "power", func(c *discovery.Component) {
				c.Extra = map[string]any{"last_reset_value_template": "{{ value_json.reset }}"}
			})
		},
		want: []want{{"power", discovery.KindLastReset, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "non-numeric device class with a unit raises on every state (sensor/__init__.py:612-618): component error",
		edit: func(b *discovery.Bundle) {
			b.Components["finish"] = discovery.Component{
				Platform: hacatalog.PlatformSensor, UniqueID: "hc_dishwasher_1_finish",
				StateTopic: "hc/status/dishwasher-1/finish", DeviceClass: "timestamp", UnitOfMeasure: "s",
			}
		},
		want: []want{{"finish", discovery.KindNumericExpectation, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "sensor refuses entity_category config when added (sensor/__init__.py:305-313): component error",
		edit: func(b *discovery.Bundle) {
			set(b, "power", func(c *discovery.Component) { c.EntityCategory = hacatalog.EntityCategoryConfig })
		},
		want: []want{{"power", discovery.KindEntityCategory, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "binary_sensor refuses entity_category config when added (binary_sensor/__init__.py:76-83)",
		edit: func(b *discovery.Bundle) {
			set(b, "door", func(c *discovery.Component) { c.EntityCategory = hacatalog.EntityCategoryConfig })
		},
		want: []want{{"door", discovery.KindEntityCategory, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "number accepts entity_category config: no platform rule outside sensor and binary_sensor",
		edit: func(b *discovery.Bundle) {
			set(b, "delay", func(c *discovery.Component) { c.EntityCategory = hacatalog.EntityCategoryConfig })
		},
		want: nil,
	},
	{
		name: "entity_category outside the enum (helpers/entity.py:212): component error",
		edit: func(b *discovery.Bundle) {
			set(b, "delay", func(c *discovery.Component) { c.EntityCategory = "settings" })
		},
		want: []want{{"delay", discovery.KindEntityCategory, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "number step below Range(min=1e-3) (mqtt/number.py:98-100): component error",
		edit: func(b *discovery.Bundle) { set(b, "delay", func(c *discovery.Component) { c.Step = new(0.0001) }) },
		want: []want{{"delay", discovery.KindNumberBounds, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "number min above the default max 100 (mqtt/number.py:79-80, number/const.py:93-94): component error",
		edit: func(b *discovery.Bundle) {
			set(b, "delay", func(c *discovery.Component) { c.Min = new(150.0); c.Max = nil })
		},
		want: []want{{"delay", discovery.KindNumberBounds, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "number max below the default min 0 (mqtt/number.py:79-80): component error",
		edit: func(b *discovery.Bundle) {
			set(b, "delay", func(c *discovery.Component) { c.Min = nil; c.Max = new(-1.0) })
		},
		want: []want{{"delay", discovery.KindNumberBounds, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "number bound that is not a number fails Coerce(float) (mqtt/number.py:91-92)",
		edit: func(b *discovery.Bundle) {
			set(b, "delay", func(c *discovery.Component) { c.Extra = map[string]any{"max": "lots"} })
		},
		want: []want{{"delay", discovery.KindNumberBounds, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "availability entry without topic (mqtt/schemas.py:103): component error",
		edit: func(b *discovery.Bundle) {
			set(b, "door", func(c *discovery.Component) {
				c.Availability = []discovery.AvailabilityEntry{{Topic: ""}}
			})
		},
		want: []want{{"door", discovery.KindAvailabilityTopic, discovery.ScopeComponent, discovery.SeverityError, false}},
	},
	{
		name: "extra key in a component's availability entry is stripped (mqtt/schemas.py:94-116 under REMOVE_EXTRA)",
		edit: func(b *discovery.Bundle) {
			set(b, "door", func(c *discovery.Component) {
				c.Extra = map[string]any{"availability": []any{map[string]any{
					"topic": "hc/connected", "availability_template": "{{ value }}",
				}}}
			})
		},
		want: []want{{"door", discovery.KindAvailabilityKey, discovery.ScopeComponent, discovery.SeverityWarning, true}},
	},
}

func findingsMatch(t *testing.T, got discovery.Findings, wants []want) {
	t.Helper()
	if len(got) != len(wants) {
		t.Fatalf("got %d findings, want %d:\n%s", len(got), len(wants), dumpFindings(got))
	}
	for i, w := range wants {
		f := got[i]
		if f.Component != w.component || f.Kind != w.kind || f.Scope != w.scope ||
			f.Severity != w.severity || f.Strippable != w.strip {
			t.Errorf("finding %d = %+v\nwant %+v", i, f, w)
		}
		if f.Message == "" {
			t.Errorf("finding %d has no message", i)
		}
	}
}

func dumpFindings(fs discovery.Findings) string {
	lines := make([]string, 0, len(fs))
	for _, f := range fs {
		lines = append(lines, "  "+f.String())
	}
	return strings.Join(lines, "\n")
}

// TestInspectClassifiesEveryKind is the table above: one break, one finding,
// with the scope and severity Home Assistant core applies to it.
func TestInspectClassifiesEveryKind(t *testing.T) {
	t.Parallel()
	if got := discovery.Inspect(inspectBundle(), discovery.InspectOptions{}); len(got) != 0 {
		t.Fatalf("control bundle has findings:\n%s", dumpFindings(got))
	}
	for _, tc := range inspectCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := inspectBundle()
			tc.edit(b)
			findingsMatch(t, discovery.Inspect(b, discovery.InspectOptions{}), tc.want)
		})
	}
}

// TestInspectNonFiniteBoundIsADocumentError: encoding/json refuses NaN and
// infinity, so the one component makes the whole document unencodable —
// which is a document-scope error that withholding the component cures.
func TestInspectNonFiniteBoundIsADocumentError(t *testing.T) {
	t.Parallel()
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		b := inspectBundle()
		set(b, "delay", func(c *discovery.Component) { c.Max = new(v) })
		findingsMatch(t, discovery.Inspect(b, discovery.InspectOptions{}),
			[]want{{"delay", discovery.KindNonFinite, discovery.ScopeDocument, discovery.SeverityError, false}})
	}
}

// TestInspectUnencodableComponent: a value encoding/json cannot encode in
// Extra makes the whole document unencodable, attributed to its component.
func TestInspectUnencodableComponent(t *testing.T) {
	t.Parallel()
	b := inspectBundle()
	set(b, "door", func(c *discovery.Component) { c.Extra = map[string]any{"icon": make(chan int)} })
	findingsMatch(t, discovery.Inspect(b, discovery.InspectOptions{}),
		[]want{{"door", discovery.KindUnencodable, discovery.ScopeDocument, discovery.SeverityError, false}})
}

// TestInspectReportsTheHomeconnectCaseAsAWarning is the case that cost
// go-homeconnect2mqtt 0.15.0 every entity of every appliance: Validate
// reports `state_class: measurement` on a `timestamp` sensor as a blocking
// issue, and a consumer withholding the document on it lost the device.
// Home Assistant only logs a warning (sensor/__init__.py:620-645).
func TestInspectReportsTheHomeconnectCaseAsAWarning(t *testing.T) {
	t.Parallel()
	b := homeconnectBundle()
	if err := discovery.Validate(b); err == nil {
		t.Fatal("Validate accepted it; the regression this pins is gone")
	}
	got := discovery.Inspect(b, discovery.InspectOptions{})
	if errs := got.Errors(); len(errs) != 0 {
		t.Fatalf("Inspect reports errors:\n%s", dumpFindings(errs))
	}
	findingsMatch(t, got, []want{{"finish", discovery.KindStateClass, discovery.ScopeComponent, discovery.SeverityWarning, true}})
}

// TestInspectIgnoreIsValidatesIgnore: the deliberate-key set silences the
// unknown-key finding and nothing else, as it does for ValidateIgnoring.
func TestInspectIgnoreIsValidatesIgnore(t *testing.T) {
	t.Parallel()
	b := inspectBundle()
	set(b, "power", func(c *discovery.Component) {
		c.Extra = map[string]any{"translation_key": "power"}
		c.DeviceClass = "bogus"
	})
	got := discovery.Inspect(b, discovery.InspectOptions{Ignore: map[string]bool{"translation_key": true}})
	findingsMatch(t, got, []want{{"power", discovery.KindDeviceClass, discovery.ScopeComponent, discovery.SeverityError, false}})
}

// TestInspectFindingHelpers pins the accessors a consumer decides with.
func TestInspectFindingHelpers(t *testing.T) {
	t.Parallel()
	b := inspectBundle()
	b.Device.Identifiers = nil
	set(b, "door", func(c *discovery.Component) { c.UniqueID = "" })
	set(b, "power", func(c *discovery.Component) { c.Extra = map[string]any{"x": 1} })
	got := discovery.Inspect(b, discovery.InspectOptions{})
	if n := len(got.DocumentErrors()); n != 1 {
		t.Errorf("DocumentErrors = %d, want 1 (device identity)", n)
	}
	if fc := got.FailingComponents(); !slices.Equal(fc, []string{"door"}) {
		t.Errorf("FailingComponents = %v, want [door]", fc)
	}
	if n := len(got.Warnings()); n != 1 {
		t.Errorf("Warnings = %d, want 1", n)
	}
	if n := len(got.ForComponent("power")); n != 1 {
		t.Errorf("ForComponent(power) = %d, want 1", n)
	}
	if s := got[0].String(); !strings.Contains(s, "document") {
		t.Errorf("String() = %q", s)
	}
	if got := discovery.Inspect(nil, discovery.InspectOptions{}); len(got.DocumentErrors()) != 1 ||
		got[0].Kind != discovery.KindNoDocument {
		t.Errorf("Inspect(nil) = %v", got)
	}
}

// TestNonNumericSensorClassesMatchCore pins the derived set against
// NON_NUMERIC_DEVICE_CLASSES (sensor/const.py:556-561 in core 2026.10.0b2).
// The set is derived from the catalog rather than hand-kept; if a catalog
// update changes it, this is where that becomes visible.
func TestNonNumericSensorClassesMatchCore(t *testing.T) {
	t.Parallel()
	got, err := discovery.NonNumericSensorClasses()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"date", "enum", "timestamp", "uptime"}; !slices.Equal(got, want) {
		t.Errorf("non-numeric sensor classes = %v, want %v", got, want)
	}
}

// homeconnectBundle is the shape go-homeconnect2mqtt 0.15.0 published: many
// clean components and one timestamp sensor with a measurement state class.
func homeconnectBundle() *discovery.Bundle {
	b := inspectBundle()
	b.Components["finish"] = discovery.Component{
		Platform: hacatalog.PlatformSensor, UniqueID: "hc_dishwasher_1_finish",
		StateTopic: "hc/status/dishwasher-1/finish", DeviceClass: "timestamp",
		StateClass: hacatalog.StateClassMeasurement,
	}
	return b
}

// TestInspectReadsTheShapesHomeAssistantCoerces: a dispatching platform
// picks its variant's keys (and without a variant checks no required key),
// a single availability object is a one-entry list (EnsureList,
// mqtt/schemas.py:99-100), and a number bound may be a numeric string or a
// boolean (Coerce(float), mqtt/number.py:91-92).
func TestInspectReadsTheShapesHomeAssistantCoerces(t *testing.T) {
	t.Parallel()
	b := inspectBundle()
	b.Components["json_light"] = discovery.Component{
		Platform: hacatalog.PlatformLight, UniqueID: "l1", CommandTopic: "c",
		Extra: map[string]any{"schema": "json", "brightness": true},
	}
	b.Components["any_light"] = discovery.Component{
		Platform: hacatalog.PlatformLight, UniqueID: "l2", CommandTopic: "c",
		Extra: map[string]any{"bogus_light_key": 1},
	}
	set(b, "door", func(c *discovery.Component) {
		c.Extra = map[string]any{"availability": map[string]any{"topic": "t", "x": 1}}
	})
	set(b, "delay", func(c *discovery.Component) {
		c.Min, c.Max = nil, nil
		c.Extra = map[string]any{"min": "5", "max": true}
	})
	findingsMatch(t, discovery.Inspect(b, discovery.InspectOptions{}), []want{
		{"any_light", discovery.KindUnknownKey, discovery.ScopeComponent, discovery.SeverityWarning, true},
		{"delay", discovery.KindNumberBounds, discovery.ScopeComponent, discovery.SeverityError, false},
		{"door", discovery.KindAvailabilityKey, discovery.ScopeComponent, discovery.SeverityWarning, true},
	})

	c := discovery.Contain(b, discovery.ContainOptions{StripWarnings: true})
	if len(c.Stripped) != 2 {
		t.Fatalf("stripped %v, want the light key and the availability key", c.Stripped)
	}
	if got := componentJSON(t, c.Bundle.Components["door"]); strings.Contains(got, `"x"`) {
		t.Errorf("availability object kept its extra key: %s", got)
	}
}
