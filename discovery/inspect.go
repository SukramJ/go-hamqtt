// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	hacatalog "github.com/SukramJ/go-ha-catalog"
)

// # What Home Assistant refuses, and at which granularity
//
// Every classification in this file is read off Home Assistant core
// 2026.10.0b2 (`a2ebb12561f`), and every one was also exercised against the
// real schemas of that checkout by importing them into a Python process. The
// file:line references below are into that checkout.
//
// A device document is validated as a whole for very little. The MQTT
// integration's `_parse_device_payload`
// (homeassistant/components/mqtt/discovery.py:296-314) runs
// DEVICE_DISCOVERY_SCHEMA (components/mqtt/schemas.py:214-227) and, on
// failure, logs one WARNING and returns an empty payload — every component of
// the document is then lost. That schema checks the `device` block
// (schemas.py:123-156: at least one identifier or connection), the `origin`
// block (schemas.py:159-166: `name` present, any string), the document-level shared
// options, and for each component exactly two things
// (schemas.py:199-211): `platform` is one of SUPPORTED_COMPONENTS, and an
// entry with more than a platform on an entity platform carries a
// `unique_id`. Nothing else about a component is looked at there: the
// component schema is `extra=True`.
//
// Everything else is validated per component, later, by the platform's own
// DISCOVERY_SCHEMA inside `_async_setup_entity_entry_from_discovery`
// (components/mqtt/entity.py:314-347): a `probatio.Invalid` there costs that
// one entity and nothing else. Those schemas are `extra=REMOVE_EXTRA`
// (e.g. components/mqtt/sensor.py:165-168), and the setting reaches the
// entries of the `availability` list nested inside them, so an unknown key —
// top-level or inside a list entry — is stripped, not refused. And some
// rules are not schema rules at all: they run when the entity is added
// (sensor/__init__.py:305-313) or when it writes a state
// (sensor/__init__.py:614-645), where they either fail that one entity or
// only log.
//
// So a finding has two coordinates, and they are independent:
//
//   - [Scope] is what Home Assistant refuses: the whole document, or one
//     component.
//   - [Finding.Component] is the bundle key whose content caused it, empty
//     for the document's own keys.
//
// The pair that matters most is a document-scope finding WITH a component:
// a component missing its `unique_id` makes Home Assistant refuse the whole
// document, but withholding that one component makes the rest acceptable
// again. That is what [Contain] does with it.

// Scope is what Home Assistant refuses when a finding is an error, and what
// it acts on when it is a warning.
type Scope string

const (
	// ScopeDocument is the whole device document: Home Assistant logs
	// "Invalid MQTT device discovery payload" and processes none of its
	// components (components/mqtt/discovery.py:304-313).
	ScopeDocument Scope = "document"
	// ScopeComponent is one component: Home Assistant refuses or adjusts
	// that entity and processes every other one normally
	// (components/mqtt/entity.py:324-347).
	ScopeComponent Scope = "component"
)

// Severity says whether Home Assistant refuses the thing in scope.
type Severity string

const (
	// SeverityError means Home Assistant refuses the thing in [Scope]: the
	// document is dropped, or the entity is not created (or, for the
	// add-time and state-time rules, cannot be added or never holds a
	// state).
	SeverityError Severity = "error"
	// SeverityWarning means Home Assistant accepts it, and drops or
	// rewrites the key, or logs a warning about it.
	SeverityWarning Severity = "warning"
)

// FindingKind is a stable identifier for a class of finding. The values are
// part of the API: a consumer may count, filter or alert on them.
type FindingKind string

// The kinds [Inspect] reports. The comment on each gives its scope, its
// severity and the Home Assistant code that decides both.
const (
	// KindNoDocument: document, error. A nil bundle.
	KindNoDocument FindingKind = "no_document"
	// KindCatalog: document, error. The embedded catalog could not be
	// read, so nothing could be judged; fail closed.
	KindCatalog FindingKind = "catalog"
	// KindNodeID: document, error. The node id is the object-id segment of
	// `<prefix>/device/<node_id>/config`, and TOPIC_MATCHER
	// (components/mqtt/discovery.py:60-63) only matches `[a-zA-Z0-9_-]+`
	// there; a topic it does not match is logged as illegal and ignored
	// (discovery.py:399-409).
	KindNodeID FindingKind = "node_id"
	// KindOrigin: document, warning. [Origin] always encodes `name`, so
	// the key Home Assistant declares Required (components/mqtt/schemas.py:
	// 162, 220) is present, and its validator, cv.string, accepts the
	// empty string — the document is processed. [Validate] calls an empty
	// name an issue; Home Assistant does not refuse it. It is still worth
	// fixing: the origin is how an operator, a log line and an orphan sweep
	// tell one bridge's documents from another's.
	KindOrigin FindingKind = "origin"
	// KindDeviceIdentity: document, error. The device needs an identifier
	// or a connection (components/mqtt/schemas.py:123-130, 155, 216).
	KindDeviceIdentity FindingKind = "device_identity"
	// KindNoComponents: document, warning. DEVICE_DISCOVERY_SCHEMA accepts
	// an empty `components` object (schemas.py:217-219); the document then
	// does nothing. [Validate] calls this an issue; Home Assistant does not.
	KindNoComponents FindingKind = "no_components"
	// KindUnencodable: document, error, attributed to a component. The
	// component cannot be encoded as JSON, so neither can the document it
	// sits in — nothing reaches Home Assistant at all.
	KindUnencodable FindingKind = "unencodable"
	// KindNonFinite: document, error, attributed to a component. A NaN or
	// infinite min/max/step: encoding/json refuses it, so the whole
	// document fails to marshal. (Home Assistant's own schema would accept
	// it — components/mqtt/number.py:91-92 is a bare Coerce(float) — but it
	// never gets the chance.)
	KindNonFinite FindingKind = "non_finite"
	// KindPlatformMissing: document, error, attributed to a component.
	// `platform` is Required in every component entry
	// (components/mqtt/schemas.py:207-211).
	KindPlatformMissing FindingKind = "platform_missing"
	// KindPlatformUnsupported: document, error, attributed to a component.
	// `platform` must be one of SUPPORTED_COMPONENTS (schemas.py:209,
	// components/mqtt/const.py:423-456).
	KindPlatformUnsupported FindingKind = "platform_unsupported"
	// KindUniqueIDMissing: document, error, attributed to a component.
	// check_unique_id (components/mqtt/schemas.py:199-204) requires one on
	// an entity platform whose entry carries more than a platform. Not on
	// `device_automation` or `tag`, which are not ENTITY_PLATFORMS
	// (components/mqtt/const.py:388-419) — [Validate] requires it there
	// too; Home Assistant does not.
	KindUniqueIDMissing FindingKind = "unique_id_missing"
	// KindUniqueIDDuplicate: component, error. Two components on one
	// platform with one `unique_id`: the registry keys on
	// (domain, platform, unique_id), and the second entity is "ignored"
	// with an error (helpers/entity_platform.py:995-1015). Reported on the
	// later key in sorted order, which is the order the document's JSON
	// carries them in.
	KindUniqueIDDuplicate FindingKind = "unique_id_duplicate"
	// KindUnknownKey: component, warning, strippable. The platform schemas
	// are `extra=REMOVE_EXTRA` (e.g. components/mqtt/sensor.py:165-168):
	// the key is dropped and the entity is created without it.
	// [Validate] calls this an issue; Home Assistant does not refuse
	// anything over it.
	KindUnknownKey FindingKind = "unknown_key"
	// KindRequiredKey: component, error. A key the platform schema marks
	// Required is missing, and the platform refuses the entity
	// (components/mqtt/entity.py:345-347).
	KindRequiredKey FindingKind = "required_key"
	// KindDeviceClass: component, error. The platform's
	// DEVICE_CLASSES_SCHEMA is a Coerce into its enum (e.g.
	// components/mqtt/sensor.py:79), so an undeclared class refuses the
	// entity.
	KindDeviceClass FindingKind = "device_class"
	// KindStateClass: component, warning, strippable. A sensor state class
	// impossible for its device class is a WARNING logged once by the
	// sensor entity (sensor/__init__.py:620-645, "This should raise in Home
	// Assistant Core 2023.6" — it still does not). [Validate] calls this an
	// issue; Home Assistant creates the entity.
	KindStateClass FindingKind = "state_class"
	// KindOptions: component, error. A sensor's `options` must be
	// non-empty, must come with `device_class: enum`, and exclude
	// `state_class` and `unit_of_measurement`
	// (components/mqtt/sensor.py:109-125).
	KindOptions FindingKind = "options"
	// KindLegacyUnit: component, warning. A legacy unit spelling (the micro
	// sign U+00B5) that Home Assistant rewrites through AMBIGUOUS_UNITS
	// (components/mqtt/sensor.py:141-143, components/mqtt/number.py:73-77).
	// Not strippable: the key is accepted, only respelled.
	KindLegacyUnit FindingKind = "legacy_unit"
	// KindUnitDeviceClass: component, error. A sensor unit outside its
	// device class's unit set (components/mqtt/sensor.py:148-155, after the
	// AMBIGUOUS_UNITS rewrite). Sensor only: the MQTT number platform does
	// not check it.
	KindUnitDeviceClass FindingKind = "unit_device_class"
	// KindUnitStateClass: component, error. A sensor unit outside its state
	// class's unit set (components/mqtt/sensor.py:127-136) — today only
	// `measurement_angle`, which requires `°`, absent unit included.
	KindUnitStateClass FindingKind = "unit_state_class"
	// KindLastReset: component, error. `last_reset_value_template` requires
	// `state_class: total` (components/mqtt/sensor.py:94-101).
	KindLastReset FindingKind = "last_reset"
	// KindNumericExpectation: component, error. A sensor whose device class
	// is non-numeric (`date`, `enum`, `timestamp`, `uptime`;
	// sensor/const.py:556-561) but which carries a unit: the sensor's
	// `state` property raises on every write, the first one at add time
	// included (sensor/__init__.py:612-618, helpers/entity.py:1447-1452), so
	// the entity either fails to be added or never holds a state.
	KindNumericExpectation FindingKind = "numeric_expectation"
	// KindEntityCategory: component, error. `entity_category` must be
	// `config` or `diagnostic` (helpers/entity.py:212), and `sensor` and
	// `binary_sensor` refuse `config` when the entity is added
	// (sensor/__init__.py:305-313, binary_sensor/__init__.py:76-83) —
	// "Error adding entity", that entity only
	// (helpers/entity_platform.py:694-703).
	KindEntityCategory FindingKind = "entity_category"
	// KindNumberBounds: component, error. A `number` whose step is below
	// 0.001 (components/mqtt/number.py:98-100) or whose min exceeds its max
	// once the defaults 0 and 100 are applied (number.py:79-80, 91-92;
	// number/const.py:93-94), or whose bound is not a number at all.
	KindNumberBounds FindingKind = "number_bounds"
	// KindAvailabilityTopic: component, error. An `availability` entry
	// without a topic: `topic` is Required and must be a non-empty
	// subscribe topic (components/mqtt/schemas.py:103).
	KindAvailabilityTopic FindingKind = "availability_topic"
	// KindAvailabilityKey: component, warning, strippable. A key other than
	// topic, payload_available, payload_not_available and value_template in
	// a component's `availability` entry (components/mqtt/schemas.py:
	// 94-116). Inside a component it is stripped with the rest of the
	// REMOVE_EXTRA schema; it would refuse the DOCUMENT only in a
	// document-level `availability` list, which [Bundle] cannot express.
	KindAvailabilityKey FindingKind = "availability_key"
)

// Finding is one thing [Inspect] has to say about a bundle.
type Finding struct {
	// Scope is what Home Assistant refuses (error) or adjusts (warning).
	Scope Scope
	// Component is the bundle key whose content caused the finding, or
	// empty for the document's own keys (node id, device, origin). A
	// document-scope finding WITH a component is curable: withholding that
	// component makes the document acceptable again.
	Component string
	// Kind is the stable class of the finding.
	Kind FindingKind
	// Severity says whether Home Assistant refuses the thing in Scope.
	Severity Severity
	// Keys are the offending keys where one can be named. An availability
	// entry's key is spelled `availability[<i>].<key>`.
	Keys []string
	// Strippable marks a warning whose keys [Contain] may remove: Home
	// Assistant discards or ignores them anyway, so removing them changes
	// nothing it would have kept.
	Strippable bool
	// Message is the human-readable finding. For a check [Validate] also
	// performs it is the same text [Validate] reports.
	Message string
}

// String renders the finding for a log line.
func (f Finding) String() string {
	where := "document"
	if f.Component != "" {
		where = f.Component
	}
	return fmt.Sprintf("%s %s/%s [%s]: %s", f.Severity, f.Scope, where, f.Kind, f.Message)
}

// Findings is everything [Inspect] found, documents first, then by
// component key, then by message.
type Findings []Finding

// Errors returns the findings Home Assistant refuses something over.
func (fs Findings) Errors() Findings {
	return fs.filter(func(f Finding) bool { return f.Severity == SeverityError })
}

// Warnings returns the findings Home Assistant tolerates.
func (fs Findings) Warnings() Findings {
	return fs.filter(func(f Finding) bool { return f.Severity == SeverityWarning })
}

// ForComponent returns the findings attributed to one bundle key.
func (fs Findings) ForComponent(key string) Findings {
	return fs.filter(func(f Finding) bool { return f.Component == key })
}

// DocumentErrors returns the errors no component can be withheld to cure:
// the document's own node id, device and origin. While any is present Home
// Assistant refuses the document whatever it contains.
func (fs Findings) DocumentErrors() Findings {
	return fs.filter(func(f Finding) bool { return f.Severity == SeverityError && f.Component == "" })
}

// FailingComponents returns the sorted keys of the components carrying an
// error — the ones [Contain] withholds.
func (fs Findings) FailingComponents() []string {
	seen := map[string]bool{}
	for _, f := range fs {
		if f.Severity == SeverityError && f.Component != "" {
			seen[f.Component] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (fs Findings) filter(keep func(Finding) bool) Findings {
	var out Findings
	for _, f := range fs {
		if keep(f) {
			out = append(out, f)
		}
	}
	return out
}

// InspectOptions parameterises [Inspect].
type InspectOptions struct {
	// Ignore is the deliberate-key set [ValidateIgnoring] takes: keys the
	// consumer publishes on purpose. An ignored key is not reported as
	// unknown and, for `availability`, its entries are not looked into.
	Ignore map[string]bool
}

// Inspect reports what Home Assistant would do with a bundle, finding by
// finding, each with the scope Home Assistant acts on and the severity it
// acts with.
//
// It is the structured successor to [Validate], which it does not replace
// and does not change: [Validate] still returns exactly what it returned
// before, an all-or-nothing verdict a consumer has typically used to
// withhold the whole document. That verdict is stricter than Home Assistant
// in two ways that cost entities. It treats every finding as a reason to
// refuse the document, while Home Assistant refuses a document for a handful
// of document-level keys only and handles everything else per component; and
// it counts some things as issues that Home Assistant merely warns about or
// silently strips — an unknown key, a state class impossible for its device
// class. See the comment at the top of this file for the rule and the core
// references, and each [FindingKind] for its classification.
//
// Inspect also checks rules [Validate] never did, each verified against Home
// Assistant core 2026.10: the entity category a platform refuses, a number's
// bounds and step, a sensor's unit against its device class and state class,
// a non-numeric sensor that carries a unit, `last_reset_value_template`, and
// the keys of an `availability` entry. Adding them to [Validate] would make
// it withhold documents it accepted yesterday; they live here instead.
//
// What a finding means for publishing is [Contain]'s job.
func Inspect(b *Bundle, opts InspectOptions) Findings {
	in := &inspector{}
	if b == nil {
		in.doc(KindNoDocument, "", SeverityError, nil, "nil bundle")
		return in.sorted()
	}
	tables, err := loadInspectTables()
	if err != nil {
		in.doc(KindCatalog, "", SeverityError, nil, err.Error())
		return in.sorted()
	}
	in.t = tables
	in.ignore = opts.Ignore

	switch {
	case b.NodeID == "":
		in.doc(KindNodeID, "", SeverityError, nil, "node id is empty")
	case !discoveryIDPattern.MatchString(b.NodeID):
		in.doc(KindNodeID, "", SeverityError, nil, fmt.Sprintf(
			"node id %q is not a legal discovery topic segment; Home Assistant ignores <prefix>/device/%s/config (allowed: a-z A-Z 0-9 _ -)",
			b.NodeID, b.NodeID))
	}
	if b.Origin.Name == "" {
		in.doc(KindOrigin, "", SeverityWarning, []string{"origin.name"},
			"origin.name is empty; Home Assistant accepts the document, but nothing names the software that published it")
	}
	if len(b.Device.Identifiers) == 0 && len(b.Device.Connections) == 0 {
		in.doc(KindDeviceIdentity, "", SeverityError, []string{"device.identifiers", "device.connections"},
			"device needs at least one identifier or connection")
	}
	if len(b.Components) == 0 {
		in.doc(KindNoComponents, "", SeverityWarning, []string{"components"},
			"bundle has no components; Home Assistant accepts it and creates nothing")
	}

	in.seen = map[identityKey]string{}
	for _, key := range b.Keys() {
		in.component(key, b.Components[key])
	}
	return in.sorted()
}

// discoveryIDPattern is the node-id/object-id group of TOPIC_MATCHER
// (components/mqtt/discovery.py:60-63).
var discoveryIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// The hand-maintained tables. Each is the smallest set the rule needs, cited
// to the core line it mirrors, and each belongs in go-ha-catalog rather than
// here — go-ha-catalog v0.3.0 (snapshot 2026.9.4) carries no table for any
// of them.
var (
	// nonEntityPlatforms are SUPPORTED_COMPONENTS that are not
	// ENTITY_PLATFORMS (components/mqtt/const.py:388-419 against 423-456):
	// check_unique_id does not apply to them (schemas.py:202).
	nonEntityPlatforms = map[string]bool{
		string(hacatalog.PlatformDeviceAutomation): true,
		string(hacatalog.PlatformTag):              true,
	}
	// configCategoryRefused are the platforms whose entity refuses
	// `entity_category: config` when added (sensor/__init__.py:309-313,
	// binary_sensor/__init__.py:79-83). Every other MQTT-capable platform
	// was searched for such a check in its own __init__.py; there is none.
	configCategoryRefused = map[string]bool{
		string(hacatalog.PlatformSensor):       true,
		string(hacatalog.PlatformBinarySensor): true,
	}
	// availabilityEntryKeys are the four keys of an `availability` list
	// entry (components/mqtt/schemas.py:100-114).
	availabilityEntryKeys = map[string]bool{
		"topic": true, "payload_available": true, "payload_not_available": true, "value_template": true,
	}
)

// numberMinStep is the smallest step an MQTT number accepts:
// `probatio.Range(min=1e-3)` (components/mqtt/number.py:98-100). Belongs in
// go-ha-catalog, which records the key's default but not its range.
const numberMinStep = 1e-3

type inspectTables struct {
	mqtt          hacatalog.MQTT
	relations     hacatalog.Relations
	deviceClasses map[string][]string
	sensor        hacatalog.Sensor
	// nonNumeric is derived rather than hand-kept: the sensor device
	// classes minus the catalog's numeric ones, which is exactly
	// NON_NUMERIC_DEVICE_CLASSES (sensor/const.py:556-561) — a test pins
	// that.
	nonNumeric map[string]bool
}

func loadInspectTables() (inspectTables, error) {
	mqtt, relations, deviceClasses, err := loadTables()
	if err != nil {
		return inspectTables{}, err
	}
	sensor, err := hacatalog.LoadSensor()
	if err != nil {
		return inspectTables{}, fmt.Errorf("discovery: load catalog sensor tables: %w", err)
	}
	nonNumeric := map[string]bool{}
	for _, dc := range deviceClasses[string(hacatalog.PlatformSensor)] {
		if !slices.Contains(sensor.NumericDeviceClasses, dc) {
			nonNumeric[dc] = true
		}
	}
	return inspectTables{
		mqtt: mqtt, relations: relations, deviceClasses: deviceClasses,
		sensor: sensor, nonNumeric: nonNumeric,
	}, nil
}

// nonNumericSensorClasses is exported to the tests through export_test.go.
func nonNumericSensorClasses() ([]string, error) {
	t, err := loadInspectTables()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(t.nonNumeric))
	for dc := range t.nonNumeric {
		out = append(out, dc)
	}
	sort.Strings(out)
	return out, nil
}

type inspector struct {
	t      inspectTables
	ignore map[string]bool
	seen   map[identityKey]string
	out    Findings
}

func (in *inspector) doc(kind FindingKind, component string, sev Severity, keys []string, msg string) {
	in.out = append(in.out, Finding{
		Scope: ScopeDocument, Component: component, Kind: kind, Severity: sev, Keys: keys, Message: msg,
	})
}

func (in *inspector) comp(kind FindingKind, component string, sev Severity, keys []string, msg string) {
	in.out = append(in.out, Finding{
		Scope: ScopeComponent, Component: component, Kind: kind, Severity: sev, Keys: keys, Message: msg,
	})
}

func (in *inspector) strippable(kind FindingKind, component string, keys []string, msg string) {
	in.out = append(in.out, Finding{
		Scope: ScopeComponent, Component: component, Kind: kind, Severity: SeverityWarning,
		Keys: keys, Strippable: true, Message: msg,
	})
}

func (in *inspector) sorted() Findings {
	sort.SliceStable(in.out, func(i, j int) bool {
		a, b := in.out[i], in.out[j]
		if a.Component != b.Component {
			return a.Component < b.Component
		}
		return a.Message < b.Message
	})
	return in.out
}

func (in *inspector) component(key string, comp Component) {
	// Checked on the struct, before encoding: encoding/json refuses a NaN or
	// an infinity with a message that names neither the key nor the value.
	var bad []string
	for name, v := range map[string]*float64{"min": comp.Min, "max": comp.Max, "step": comp.Step} {
		if v != nil && (math.IsNaN(*v) || math.IsInf(*v, 0)) {
			bad = append(bad, name)
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		in.doc(KindNonFinite, key, SeverityError, bad, fmt.Sprintf(
			"%s: %s is not a finite number, so the component — and with it the whole document — cannot be encoded as JSON",
			key, strings.Join(bad, ", ")))
		return
	}
	body, err := componentBody(comp)
	if err != nil {
		in.doc(KindUnencodable, key, SeverityError, nil, fmt.Sprintf(
			"%s: cannot encode component, so the whole document cannot be encoded: %v", key, err))
		return
	}
	in.body(key, body)
}

func (in *inspector) body(key string, body map[string]any) {
	platform := str(body, "platform")
	if platform == "" {
		in.doc(KindPlatformMissing, key, SeverityError, []string{"platform"}, key+": platform is required")
		return
	}
	if !slices.Contains(in.t.mqtt.SupportedComponents, platform) {
		in.doc(KindPlatformUnsupported, key, SeverityError, []string{"platform"},
			fmt.Sprintf("%s: %q is not an MQTT-capable platform", key, platform))
		return
	}
	// A platform and nothing else is the removal marker: Home Assistant
	// removes the entity and validates nothing (discovery.py:443-447).
	if isRemoval(body) {
		return
	}

	uniqueID := str(body, "unique_id")
	switch {
	case uniqueID == "" && !nonEntityPlatforms[platform]:
		in.doc(KindUniqueIDMissing, key, SeverityError, []string{"unique_id"}, key+": unique_id is required")
		return
	case uniqueID != "":
		id := identityKey{platform: platform, uniqueID: uniqueID}
		if prev, dup := in.seen[id]; dup {
			in.comp(KindUniqueIDDuplicate, key, SeverityError, []string{"unique_id"},
				fmt.Sprintf("%s: unique_id %q already used by %q on platform %q", key, uniqueID, prev, platform))
		} else {
			in.seen[id] = key
		}
	}

	if schema, ok := in.t.mqtt.Platforms[platform]; ok {
		in.keys(key, platform, body, schema)
	}
	if !in.ignore["availability"] {
		in.availability(key, body)
	}

	if dc := str(body, "device_class"); dc != "" {
		if classes, known := in.t.deviceClasses[platform]; known && !slices.Contains(classes, dc) {
			in.comp(KindDeviceClass, key, SeverityError, []string{"device_class"},
				fmt.Sprintf("%s: device_class %q is not valid for platform %q", key, dc, platform))
		}
	}
	in.entityCategory(key, platform, body)

	switch platform {
	case string(hacatalog.PlatformSensor):
		in.sensor(key, body)
	case string(hacatalog.PlatformNumber):
		in.number(key, body)
	}
}

func (in *inspector) keys(key, platform string, body map[string]any, schema hacatalog.PlatformSchema) {
	allowed := schema.Keys
	checkRequired := true
	if len(allowed) == 0 {
		if variant, ok := schema.Variants[str(body, schema.Discriminator)]; ok {
			allowed = variant.Keys
		} else {
			allowed = map[string]hacatalog.SchemaKey{}
			for _, v := range schema.Variants {
				for k, e := range v.Keys {
					allowed[k] = e
				}
			}
			checkRequired = false
		}
	}
	if len(allowed) == 0 {
		return
	}
	for name := range body {
		if name == "platform" || in.ignore[name] {
			continue
		}
		if _, legal := allowed[name]; !legal {
			in.strippable(KindUnknownKey, key, []string{name}, fmt.Sprintf(
				"%s: %q is not a valid key for platform %q (Home Assistant would drop it silently)", key, name, platform))
		}
	}
	if !checkRequired {
		return
	}
	for name, entry := range allowed {
		if !entry.Required {
			continue
		}
		if _, have := body[name]; !have {
			in.comp(KindRequiredKey, key, SeverityError, []string{name},
				fmt.Sprintf("%s: %q is required by platform %q", key, name, platform))
		}
	}
}

// availability checks the entries of the component's `availability` list.
// Home Assistant accepts a single object there as a one-entry list
// (`probatio.EnsureList()`, components/mqtt/schemas.py:99-100), so both shapes
// are read.
func (in *inspector) availability(key string, body map[string]any) {
	raw, ok := body["availability"]
	if !ok || raw == nil {
		return
	}
	var entries []any
	switch v := raw.(type) {
	case []any:
		entries = v
	case map[string]any:
		entries = []any{v}
	default:
		return
	}
	for i, e := range entries {
		entry, ok := e.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := entry["topic"].(string); t == "" {
			in.comp(KindAvailabilityTopic, key, SeverityError, []string{fmt.Sprintf("availability[%d].topic", i)},
				fmt.Sprintf("%s: availability[%d] has no topic, so nothing can ever mark this entity available", key, i))
		}
		extra := make([]string, 0, len(entry))
		for name := range entry {
			if !availabilityEntryKeys[name] {
				extra = append(extra, name)
			}
		}
		sort.Strings(extra)
		for _, name := range extra {
			in.strippable(KindAvailabilityKey, key, []string{fmt.Sprintf("availability[%d].%s", i, name)},
				fmt.Sprintf("%s: availability[%d] carries %q, which is not a key of an availability entry (Home Assistant drops it)",
					key, i, name))
		}
	}
}

func (in *inspector) entityCategory(key, platform string, body map[string]any) {
	raw, present := body["entity_category"]
	if !present || raw == nil {
		return
	}
	category, isString := raw.(string)
	if !isString || !hacatalog.EntityCategory(category).Valid() {
		in.comp(KindEntityCategory, key, SeverityError, []string{"entity_category"},
			fmt.Sprintf("%s: entity_category %v is neither \"config\" nor \"diagnostic\"", key, raw))
		return
	}
	if category == string(hacatalog.EntityCategoryConfig) && configCategoryRefused[platform] {
		in.comp(KindEntityCategory, key, SeverityError, []string{"entity_category"},
			fmt.Sprintf("%s: a %s cannot have entity_category \"config\"; Home Assistant refuses to add the entity", key, platform))
	}
}

// sensor applies components/mqtt/sensor.py's
// validate_sensor_state_and_device_class_config (lines 92-157) and the
// sensor entity's own add-time and state-time rules, in that order.
func (in *inspector) sensor(key string, body map[string]any) {
	stateClass := str(body, "state_class")
	deviceClass := str(body, "device_class")
	// A blank unit is popped before anything reads it (sensor.py:103-107).
	unit := str(body, "unit_of_measurement")
	if strings.TrimSpace(unit) == "" {
		unit = ""
	}

	if _, has := body["last_reset_value_template"]; has && stateClass != string(hacatalog.StateClassTotal) {
		in.comp(KindLastReset, key, SeverityError, []string{"last_reset_value_template", "state_class"},
			fmt.Sprintf("%s: last_reset_value_template requires state_class \"total\", got %q", key, stateClass))
	}

	options, hasOptions := body["options"]
	if hasOptions && options != nil {
		if isEmptyList(options) {
			in.comp(KindOptions, key, SeverityError, []string{"options"}, key+": options must not be an empty list")
		} else {
			if deviceClass != "enum" {
				in.comp(KindOptions, key, SeverityError, []string{"options", "device_class"},
					fmt.Sprintf("%s: options require device_class \"enum\", got %q", key, deviceClass))
			}
			if stateClass != "" {
				in.comp(KindOptions, key, SeverityError, []string{"options", "state_class"},
					key+": options cannot be combined with state_class")
			}
			if unit != "" {
				in.comp(KindOptions, key, SeverityError, []string{"options", "unit_of_measurement"},
					key+": options cannot be combined with unit_of_measurement")
			}
		}
	}

	if allowed, constrained := in.t.sensor.StateClassUnits[stateClass]; stateClass != "" && constrained &&
		!slices.Contains(allowed, unit) {
		in.comp(KindUnitStateClass, key, SeverityError, []string{"unit_of_measurement", "state_class"},
			fmt.Sprintf("%s: unit_of_measurement %q is not valid for state_class %q (allowed: %s)",
				key, unit, stateClass, joinOrNone(nonEmpty(allowed))))
	}

	normalized := unit
	if canonical, ambiguous := in.t.relations.AmbiguousUnits[unit]; unit != "" && ambiguous && canonical != unit {
		in.comp(KindLegacyUnit, key, SeverityWarning, []string{"unit_of_measurement"},
			fmt.Sprintf("%s: unit_of_measurement %q is the legacy spelling; Home Assistant rewrites it to %q",
				key, unit, canonical))
		normalized = canonical
	}
	if allowed, constrained := in.t.sensor.DeviceClassUnits[deviceClass]; normalized != "" && deviceClass != "" &&
		constrained && !slices.Contains(allowed, normalized) {
		in.comp(KindUnitDeviceClass, key, SeverityError, []string{"unit_of_measurement", "device_class"},
			fmt.Sprintf("%s: unit_of_measurement %q is not valid for device_class %q (allowed: %s)",
				key, normalized, deviceClass, joinOrNone(nonEmpty(allowed))))
	}

	if stateClass != "" && deviceClass != "" {
		if allowed, known := in.t.relations.SensorDeviceClassStateClasses[deviceClass]; known &&
			!slices.Contains(allowed, stateClass) {
			in.strippable(KindStateClass, key, []string{"state_class"}, fmt.Sprintf(
				"%s: state_class %q is not allowed for device_class %q (allowed: %s); Home Assistant logs a warning and keeps the entity",
				key, stateClass, deviceClass, joinOrNone(allowed)))
		}
	}

	// Options with a unit are already refused above, by the schema, before
	// the entity exists; this is the case the schema lets through.
	if in.t.nonNumeric[deviceClass] && unit != "" && !hasOptions {
		in.comp(KindNumericExpectation, key, SeverityError, []string{"unit_of_measurement", "device_class"},
			fmt.Sprintf("%s: device_class %q is non-numeric but the sensor carries unit_of_measurement %q; Home Assistant raises on every state it writes",
				key, deviceClass, unit))
	}
}

// number applies components/mqtt/number.py's schema (lines 85-104) and its
// validate_config (lines 71-82).
func (in *inspector) number(key string, body map[string]any) {
	if unit := str(body, "unit_of_measurement"); unit != "" {
		if canonical, ambiguous := in.t.relations.AmbiguousUnits[unit]; ambiguous && canonical != unit {
			in.comp(KindLegacyUnit, key, SeverityWarning, []string{"unit_of_measurement"},
				fmt.Sprintf("%s: unit_of_measurement %q is the legacy spelling; Home Assistant rewrites it to %q",
					key, unit, canonical))
		}
	}
	keys := in.t.mqtt.Platforms[string(hacatalog.PlatformNumber)].Keys
	bound := func(name string, fallback float64) (float64, bool) {
		raw, present := body[name]
		if !present {
			if d, ok := keys[name].Default.(float64); ok {
				return d, true
			}
			return fallback, true
		}
		v, ok := asFloat(raw)
		if !ok {
			in.comp(KindNumberBounds, key, SeverityError, []string{name},
				fmt.Sprintf("%s: %s %v is not a number", key, name, raw))
		}
		return v, ok
	}
	low, lowOK := bound("min", 0)
	high, highOK := bound("max", 100)
	step, stepOK := bound("step", 1)
	if stepOK && step < numberMinStep {
		in.comp(KindNumberBounds, key, SeverityError, []string{"step"},
			fmt.Sprintf("%s: step %v is below Home Assistant's minimum of %v", key, step, numberMinStep))
	}
	if lowOK && highOK && low > high {
		in.comp(KindNumberBounds, key, SeverityError, []string{"min", "max"},
			fmt.Sprintf("%s: min %v is greater than max %v (an absent bound defaults to 0 and 100)", key, low, high))
	}
}

// asFloat mirrors probatio.Coerce(float) for the shapes a decoded JSON body
// can hold.
func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	case bool:
		if n {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

// nonEmpty drops the catalog's encoding of Python's None from a unit set.
func nonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
