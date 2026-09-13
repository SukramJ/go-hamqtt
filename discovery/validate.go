// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	hacatalog "github.com/SukramJ/go-ha-catalog"
)

// ValidationError collects every problem in a bundle rather than the first.
//
// One report per publish is what a consumer can act on; failing at the first
// bad key means fixing a catalog one entry per test run.
type ValidationError struct {
	NodeID string
	// Issues are the problems Home Assistant does not tolerate: a key it
	// drops, a required key that is missing, a device class the platform does
	// not declare. A payload with any of these is broken on arrival.
	Issues []string
	// Warnings are the things Home Assistant accepts and then rewrites. They
	// belong in a report but must not stop a publish — treating them as
	// failures is how a validator earns the reputation that gets it muted.
	Warnings []string
}

// Error implements error.
func (e *ValidationError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "discovery: bundle %q:", e.NodeID)
	if len(e.Issues)+len(e.Warnings) == 1 {
		one := append(append([]string{}, e.Issues...), e.Warnings...)[0]
		return b.String() + " " + one
	}
	for _, issue := range e.Issues {
		b.WriteString("\n  - " + issue)
	}
	for _, warning := range e.Warnings {
		b.WriteString("\n  ~ " + warning)
	}
	return b.String()
}

// Blocking reports whether the payload is broken rather than merely untidy.
// It is what a caller deciding whether to publish should ask.
func (e *ValidationError) Blocking() bool { return len(e.Issues) > 0 }

// ErrInvalidBundle matches a payload Home Assistant would reject or silently
// mutilate, so a caller can tell it from an I/O failure without a type
// assertion — and, since an advisory-only result does not match it, existing
// `if errors.Is(err, ErrInvalidBundle) { do not publish }` code keeps
// publishing the payloads Home Assistant is happy to accept.
var ErrInvalidBundle = errors.New("discovery: invalid bundle")

// ErrAdvisory matches a result that carries only warnings.
var ErrAdvisory = errors.New("discovery: advisory")

// Is implements errors.Is.
func (e *ValidationError) Is(target error) bool {
	switch target {
	case ErrInvalidBundle:
		return e.Blocking()
	case ErrAdvisory:
		return !e.Blocking()
	default:
		return false
	}
}

// Validate checks a bundle against the Home Assistant schemas the catalog
// carries, and returns every problem it finds.
//
// This runs before publishing, and a failure means nothing is published for
// that device. That is the point: Home Assistant discards a malformed
// discovery config in silence — no error on the wire, no entry in its log that
// names the cause — so an invalid payload is indistinguishable from a bridge
// that never spoke. The reference implementation published unvalidated, which
// is how a wrong micro sign could cost a whole device's entities without
// anyone learning why.
func Validate(b *Bundle) error { return ValidateIgnoring(b, nil) }

// ValidateIgnoring is [Validate] with a set of keys the consumer publishes on
// purpose and Home Assistant is known to drop.
//
// Such a key exists: one consumer publishes `translation_key` so its
// cross-stack parity tooling can compare against the Python integration it
// mirrors. Home Assistant declares that key on no platform and discards it,
// which the validator correctly reports — and on a real fleet that one key
// accounted for every blocking finding on 164 of 9,996 entities, turning 64
// of 398 device bundles Blocking(). A consumer wiring the validator into its
// publish path would therefore withhold a sixth of its devices entirely,
// because an invalid bundle publishes nothing at all.
//
// The set is a parameter rather than a field on Bundle: it is a property of
// the consumer's judgement, not of the document, and the same document
// validated by a tool that did not make that judgement should still report
// the key. That is why [Validate] ignores nothing — a caller has to say so
// deliberately.
//
// An ignored key is not checked for anything: not its type, not its
// platform. The consumer has taken responsibility for it.
func ValidateIgnoring(b *Bundle, ignore map[string]bool) error {
	if b == nil {
		return fmt.Errorf("discovery: nil bundle")
	}
	issues := &issueList{}

	if b.NodeID == "" {
		issues.add("node id is empty")
	} else if slug := sanitizeCheck(b.NodeID); slug != b.NodeID {
		issues.add("node id %q is not a legal topic segment (want %q)", b.NodeID, slug)
	}
	if b.Origin.Name == "" {
		// Required by Home Assistant on a device bundle, unlike the
		// per-entity form where it is optional.
		issues.add("origin.name is required on a device bundle")
	}
	if len(b.Device.Identifiers) == 0 && len(b.Device.Connections) == 0 {
		issues.add("device needs at least one identifier or connection")
	}
	if len(b.Components) == 0 {
		issues.add("bundle has no components")
	}

	mqtt, relations, deviceClasses, err := loadTables()
	if err != nil {
		return err
	}

	seenUnique := map[identityKey]string{}
	for _, key := range b.Keys() {
		comp := b.Components[key]
		validateComponent(issues, key, comp, mqtt, relations, deviceClasses, seenUnique, ignore)
	}

	return issues.err(b.NodeID)
}

// ValidateBody checks one already-built discovery body against the platform's
// Home Assistant schema, and returns every problem it finds.
//
// It exists for the consumers that still publish the per-entity discovery form
// — one retained config per entity at <prefix>/<platform>/<node_id>/<object_id>
// /config — rather than a device bundle. Those bodies are usually assembled as
// a plain map, never pass through [Component], and so never reach [Validate];
// this is the entry point that lets them be checked anyway.
//
// The rules are the same ones [Validate] applies per component, because they
// are the same rules: this function is what [Validate] calls.
func ValidateBody(platform hacatalog.Platform, body map[string]any) error {
	return ValidateBodyIgnoring(platform, body, nil)
}

// ValidateBodyIgnoring is [ValidateBody] with the same deliberate-key set
// [ValidateIgnoring] takes, for a consumer on the per-entity form.
func ValidateBodyIgnoring(platform hacatalog.Platform, body map[string]any, ignore map[string]bool) error {
	issues := &issueList{}
	mqtt, relations, deviceClasses, err := loadTables()
	if err != nil {
		return err
	}
	validateBody(issues, string(platform), string(platform), body, mqtt, relations, deviceClasses, nil, ignore)
	return issues.err(string(platform))
}

// componentBody encodes a component into the flat JSON object Home Assistant
// actually receives, which is what every check below reads.
//
// Checking the encoded form rather than the struct fields is deliberate: the
// keys a component carries come from three places — its typed fields, its
// platform Fields struct and its Extra map — and only the encoded object shows
// what was really published.
func componentBody(comp Component) (map[string]any, error) {
	raw, err := json.Marshal(comp)
	if err != nil {
		return nil, err
	}
	body := map[string]any{}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	return body, nil
}

// identityKey is what Home Assistant actually keys entity uniqueness on, as
// far as a bundle can observe it: the pair (platform, unique_id).
//
// The registry's index is a three-part key, not the `unique_id` alone.
// `homeassistant/helpers/entity_registry.py` declares it as
//
//	self._index: dict[tuple[str, str, str], str] = {}
//	...
//	self._index[(entry.domain, entry.platform, entry.unique_id)] = entry.entity_id
//
// where — in the registry's vocabulary, which inverts the developer docs' —
// `domain` is the entity component (`sensor`, `number`) and `platform` is the
// integration (`mqtt`). `async_get_entity_id(domain, platform, unique_id)`,
// `async_get_or_create`, the `deleted_entities` index and the unique-id-change
// guard that raises "Unique id '%s' is already in use by '%s'" all consult that
// same triple, as does `entity_platform.py`'s runtime check behind the familiar
// "Platform %s does not generate unique IDs" error. The developer documentation
// says the same thing in prose: "An entity is looked up in the registry based
// on a combination of the platform type (for example, `light`), and the
// integration name (domain) (for example, hue) and the unique ID of the
// entity."
//
//   - https://github.com/home-assistant/core/blob/dev/homeassistant/helpers/entity_registry.py
//   - https://github.com/home-assistant/core/blob/dev/homeassistant/helpers/entity_platform.py
//   - https://developers.home-assistant.io/docs/entity_registry_index/
//
// Every component in a bundle is published by the one `mqtt` integration, so
// the integration half of the triple is constant and a bundle can only vary the
// other two. A `sensor` and a `number` carrying the same `unique_id` therefore
// index to two distinct keys and both register: that is not a collision, and
// refusing it — as this validator did through v0.31.0 — is stricter than the
// platform being modelled. One measured consumer publishes nine such pairs and
// has done so in production for years.
//
// Two components sharing a platform *and* a `unique_id` really do collide, on
// the registry's own terms, so that stays a blocking issue.
//
// Nothing in the MQTT integration narrows this further for the device-bundle
// form. `components/mqtt/discovery.py` never mentions `unique_id` at all;
// `DEVICE_DISCOVERY_SCHEMA`'s validation over `cmps` is `check_unique_id`, a
// presence requirement; and the conflict behind "Received a conflicting MQTT
// discovery message" is keyed on `(component, discovery_id)` and the discovery
// topic, never on `unique_id`. The bundled path fans `cmps` out into per-
// component configs that then travel the identical per-entity machinery.
type identityKey struct {
	platform string
	uniqueID string
}

func validateComponent(
	issues *issueList,
	key string,
	comp Component,
	mqtt hacatalog.MQTT,
	relations hacatalog.Relations,
	deviceClasses map[string][]string,
	seenUnique map[identityKey]string,
	ignore map[string]bool,
) {
	body, err := componentBody(comp)
	if err != nil {
		issues.add("%s: cannot encode component: %v", key, err)
		return
	}
	validateBody(issues, key, string(comp.Platform), body, mqtt, relations, deviceClasses, seenUnique, ignore)
}

func validateBody(
	issues *issueList,
	key string,
	platform string,
	body map[string]any,
	mqtt hacatalog.MQTT,
	relations hacatalog.Relations,
	deviceClasses map[string][]string,
	seenUnique map[identityKey]string,
	ignore map[string]bool,
) {
	if platform == "" {
		issues.add("%s: platform is required", key)
		return
	}
	if !slices.Contains(mqtt.SupportedComponents, platform) {
		issues.add("%s: %q is not an MQTT-capable platform", key, platform)
		return
	}

	uniqueID := str(body, "unique_id")
	// A component with more than a platform must carry a unique id; Home
	// Assistant rejects the bundle otherwise.
	if uniqueID == "" {
		if !isRemoval(body) {
			issues.add("%s: unique_id is required", key)
		}
		return
	}
	if seenUnique != nil {
		id := identityKey{platform: platform, uniqueID: uniqueID}
		if prev, dup := seenUnique[id]; dup {
			issues.add("%s: unique_id %q already used by %q on platform %q",
				key, uniqueID, prev, platform)
		}
		seenUnique[id] = key
	}

	// Unknown keys: Home Assistant drops them silently (its discovery schema
	// is extra=REMOVE_EXTRA), so a typo costs a feature with no diagnostic
	// anywhere. Checking against the extracted schema is the only place this
	// becomes visible.
	if schema, ok := mqtt.Platforms[platform]; ok {
		validateKeys(issues, key, platform, body, schema, ignore)
	}

	deviceClass := str(body, "device_class")
	if deviceClass != "" {
		if classes, known := deviceClasses[platform]; known {
			if !slices.Contains(classes, deviceClass) {
				issues.add("%s: device_class %q is not valid for platform %q",
					key, deviceClass, platform)
			}
		}
	}

	validateSensorRelations(issues, key, platform, body, relations)
}

// validateKeys compares the body's actual JSON keys against the platform's
// discovery schema.
func validateKeys(issues *issueList, key, platform string, body map[string]any, schema hacatalog.PlatformSchema, ignore map[string]bool) {
	allowed := schema.Keys
	// A dispatching platform (light, infrared) has no flat key set: which
	// sub-schema applies depends on a payload key, so read that key.
	checkRequired := true
	if len(allowed) == 0 {
		if variant, ok := schema.Variants[str(body, schema.Discriminator)]; ok {
			allowed = variant.Keys
		} else {
			// The body names no variant, or names one the catalog does not
			// know. The union of the variants still says which keys could
			// ever be legal, but it cannot say which are required: a light on
			// the json schema would be told it is missing the template
			// schema's command_on_template, which is exactly the false alarm
			// that teaches a consumer to ignore the validator.
			allowed = map[string]hacatalog.SchemaKey{}
			for _, v := range schema.Variants {
				for k, entry := range v.Keys {
					allowed[k] = entry
				}
			}
			checkRequired = false
		}
	}
	if len(allowed) == 0 {
		return
	}

	for name := range body {
		// `platform` is the bundle's own discriminator rather than a schema
		// key, so it is legal on every component and appears in none.
		if name == "platform" {
			continue
		}
		// A key the consumer declared it publishes on purpose. Not checked
		// for anything — it has taken responsibility for it.
		if ignore[name] {
			continue
		}
		if _, legal := allowed[name]; !legal {
			issues.add("%s: %q is not a valid key for platform %q (Home Assistant would drop it silently)",
				key, name, platform)
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
			issues.add("%s: %q is required by platform %q", key, name, platform)
		}
	}
}

// validateSensorRelations checks the cross-field rules that make the
// difference between a working sensor and a corrupted statistics history.
func validateSensorRelations(issues *issueList, key, platform string, body map[string]any, relations hacatalog.Relations) {
	stateClass := str(body, "state_class")
	deviceClass := str(body, "device_class")
	unit := str(body, "unit_of_measurement")
	options, hasOptions := body["options"]

	if stateClass != "" && deviceClass != "" {
		if allowed, known := relations.SensorDeviceClassStateClasses[deviceClass]; known {
			if !slices.Contains(allowed, stateClass) {
				issues.add("%s: state_class %q is not allowed for device_class %q (allowed: %s)",
					key, stateClass, deviceClass, joinOrNone(allowed))
			}
		}
	}

	// Home Assistant's own sensor validator: options belong to enum sensors
	// and cannot coexist with a state class or a unit.
	//
	// Sensor only. `select` *requires* options and declares no device_class at
	// all, so applying this rule everywhere rejected every select ever built —
	// and since an invalid bundle publishes nothing for the whole device, one
	// enum parameter would have silenced every entity of that device.
	if hasOptions && !isEmptyList(options) && platform == string(hacatalog.PlatformSensor) {
		if deviceClass != "enum" {
			issues.add("%s: options require device_class \"enum\", got %q", key, deviceClass)
		}
		if stateClass != "" {
			issues.add("%s: options cannot be combined with state_class", key)
		}
		if unit != "" {
			issues.add("%s: options cannot be combined with unit_of_measurement", key)
		}
	}

	// The silent rewrite. Home Assistant maps a handful of unit spellings
	// through AMBIGUOUS_UNITS — most importantly the legacy micro sign U+00B5
	// to U+03BC — in sensor/__init__.py's
	// _native_unit_of_measurement_compat, which is a `.get(unit, unit)`:
	// it *accepts* the legacy spelling and rewrites it. So this is a warning,
	// not an issue. Publishing the canonical spelling is still the right thing
	// — it is what Home Assistant stores, so the two agree without a
	// translation step — but an entity spelled the old way works.
	if unit != "" {
		if canonical, ambiguous := relations.AmbiguousUnits[unit]; ambiguous && canonical != unit {
			issues.warn("%s: unit_of_measurement %q is the legacy spelling; Home Assistant rewrites it to %q",
				key, unit, canonical)
		}
	}
}

// isRemoval reports whether a body is the deletion marker: a platform and
// nothing else.
func isRemoval(body map[string]any) bool {
	for name := range body {
		if name != "platform" {
			return false
		}
	}
	return true
}

func str(body map[string]any, key string) string {
	v, _ := body[key].(string)
	return v
}

func isEmptyList(v any) bool {
	list, ok := v.([]any)
	return ok && len(list) == 0
}

func loadTables() (hacatalog.MQTT, hacatalog.Relations, map[string][]string, error) {
	mqtt, err := hacatalog.LoadMQTT()
	if err != nil {
		return mqtt, hacatalog.Relations{}, nil, fmt.Errorf("discovery: load catalog: %w", err)
	}
	relations, err := hacatalog.LoadRelations()
	if err != nil {
		return mqtt, relations, nil, fmt.Errorf("discovery: load catalog relations: %w", err)
	}
	deviceClasses, err := hacatalog.LoadDeviceClasses()
	if err != nil {
		return mqtt, relations, nil, fmt.Errorf("discovery: load catalog device classes: %w", err)
	}
	return mqtt, relations, deviceClasses, nil
}

func sanitizeCheck(s string) string {
	return strings.NewReplacer("/", "_", "+", "_", "#", "_", " ", "_").Replace(s)
}

func joinOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}

type issueList struct {
	items    []string
	warnings []string
}

func (l *issueList) add(format string, args ...any) {
	l.items = append(l.items, fmt.Sprintf(format, args...))
}

func (l *issueList) warn(format string, args ...any) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

// err renders the collected problems, or nil when there were none. Sorting
// makes one report comparable to the next, which is what lets a consumer diff
// two runs instead of re-reading both.
func (l *issueList) err(nodeID string) error {
	if len(l.items) == 0 && len(l.warnings) == 0 {
		return nil
	}
	sort.Strings(l.items)
	sort.Strings(l.warnings)
	return &ValidationError{NodeID: nodeID, Issues: l.items, Warnings: l.warnings}
}
