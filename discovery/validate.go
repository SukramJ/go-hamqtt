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
	Issues []string
}

// Error implements error.
func (e *ValidationError) Error() string {
	if len(e.Issues) == 1 {
		return fmt.Sprintf("discovery: bundle %q: %s", e.NodeID, e.Issues[0])
	}
	return fmt.Sprintf("discovery: bundle %q: %d problems:\n  - %s",
		e.NodeID, len(e.Issues), strings.Join(e.Issues, "\n  - "))
}

// ErrInvalidBundle is what [ValidationError] matches against, so a caller can
// tell a rejected payload from an I/O failure without a type assertion.
var ErrInvalidBundle = errors.New("discovery: invalid bundle")

// Is implements errors.Is.
func (e *ValidationError) Is(target error) bool { return target == ErrInvalidBundle }

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
func Validate(b *Bundle) error {
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

	mqtt, err := hacatalog.LoadMQTT()
	if err != nil {
		return fmt.Errorf("discovery: load catalog: %w", err)
	}
	relations, err := hacatalog.LoadRelations()
	if err != nil {
		return fmt.Errorf("discovery: load catalog relations: %w", err)
	}
	deviceClasses, err := hacatalog.LoadDeviceClasses()
	if err != nil {
		return fmt.Errorf("discovery: load catalog device classes: %w", err)
	}

	seenUnique := map[string]string{}
	for _, key := range b.Keys() {
		comp := b.Components[key]
		validateComponent(issues, key, comp, mqtt, relations, deviceClasses, seenUnique)
	}

	if len(issues.items) == 0 {
		return nil
	}
	sort.Strings(issues.items)
	return &ValidationError{NodeID: b.NodeID, Issues: issues.items}
}

func validateComponent(
	issues *issueList,
	key string,
	comp Component,
	mqtt hacatalog.MQTT,
	relations hacatalog.Relations,
	deviceClasses map[string][]string,
	seenUnique map[string]string,
) {
	platform := string(comp.Platform)
	if platform == "" {
		issues.add("%s: platform is required", key)
		return
	}
	if !slices.Contains(mqtt.SupportedComponents, platform) {
		issues.add("%s: %q is not an MQTT-capable platform", key, platform)
		return
	}

	// A component with more than a platform must carry a unique id; Home
	// Assistant rejects the bundle otherwise.
	if comp.UniqueID == "" {
		if !isRemoval(comp) {
			issues.add("%s: unique_id is required", key)
		}
		return
	}
	if prev, dup := seenUnique[comp.UniqueID]; dup {
		issues.add("%s: unique_id %q already used by %q", key, comp.UniqueID, prev)
	}
	seenUnique[comp.UniqueID] = key

	// Unknown keys: Home Assistant drops them silently (its discovery schema
	// is extra=REMOVE_EXTRA), so a typo costs a feature with no diagnostic
	// anywhere. Checking against the extracted schema is the only place this
	// becomes visible.
	schema, ok := mqtt.Platforms[platform]
	if ok {
		validateKeys(issues, key, comp, schema)
	}

	if comp.DeviceClass != "" {
		if classes, known := deviceClasses[platform]; known {
			if !slices.Contains(classes, comp.DeviceClass) {
				issues.add("%s: device_class %q is not valid for platform %q",
					key, comp.DeviceClass, platform)
			}
		}
	}

	validateSensorRelations(issues, key, comp, relations)
}

// validateKeys compares the component's actual JSON keys against the
// platform's discovery schema.
func validateKeys(issues *issueList, key string, comp Component, schema hacatalog.PlatformSchema) {
	allowed := schema.Keys
	if len(allowed) == 0 {
		// A dispatching platform (light, infrared) has no flat key set; the
		// union of its variants is the closest honest approximation, since
		// which variant applies depends on a payload key.
		allowed = map[string]hacatalog.SchemaKey{}
		for _, v := range schema.Variants {
			for k, entry := range v.Keys {
				allowed[k] = entry
			}
		}
	}
	if len(allowed) == 0 {
		return
	}

	raw, err := json.Marshal(comp)
	if err != nil {
		issues.add("%s: cannot encode component: %v", key, err)
		return
	}
	present := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &present); err != nil {
		issues.add("%s: cannot re-read component: %v", key, err)
		return
	}

	for name := range present {
		// `platform` is the bundle's own discriminator rather than a schema
		// key, so it is legal on every component and appears in none.
		if name == "platform" {
			continue
		}
		if _, legal := allowed[name]; !legal {
			issues.add("%s: %q is not a valid key for platform %q (Home Assistant would drop it silently)",
				key, name, comp.Platform)
		}
	}
	for name, entry := range allowed {
		if !entry.Required {
			continue
		}
		if _, have := present[name]; !have {
			issues.add("%s: %q is required by platform %q", key, name, comp.Platform)
		}
	}
}

// validateSensorRelations checks the cross-field rules that make the
// difference between a working sensor and a corrupted statistics history.
func validateSensorRelations(issues *issueList, key string, comp Component, relations hacatalog.Relations) {
	if comp.StateClass != "" && comp.DeviceClass != "" {
		if allowed, known := relations.SensorDeviceClassStateClasses[comp.DeviceClass]; known {
			if !slices.Contains(allowed, string(comp.StateClass)) {
				issues.add("%s: state_class %q is not allowed for device_class %q (allowed: %s)",
					key, comp.StateClass, comp.DeviceClass, joinOrNone(allowed))
			}
		}
	}

	// Home Assistant's own sensor validator: options belong to enum sensors
	// and cannot coexist with a state class or a unit.
	if len(comp.Options) > 0 {
		if comp.DeviceClass != "enum" {
			issues.add("%s: options require device_class \"enum\", got %q", key, comp.DeviceClass)
		}
		if comp.StateClass != "" {
			issues.add("%s: options cannot be combined with state_class", key)
		}
		if comp.UnitOfMeasure != "" {
			issues.add("%s: options cannot be combined with unit_of_measurement", key)
		}
	}

	// The silent rewrite: Home Assistant normalises some unit spellings —
	// most importantly the legacy micro sign U+00B5 to U+03BC — and discards
	// a config whose unit does not match the normalised form.
	if comp.UnitOfMeasure != "" {
		if canonical, ambiguous := relations.AmbiguousUnits[comp.UnitOfMeasure]; ambiguous && canonical != comp.UnitOfMeasure {
			issues.add("%s: unit_of_measurement %q is the non-canonical spelling; Home Assistant expects %q",
				key, comp.UnitOfMeasure, canonical)
		}
	}
}

// isRemoval reports whether a component is the deletion marker: a platform and
// nothing else.
func isRemoval(comp Component) bool {
	return comp.Name == "" && comp.UniqueID == "" && comp.StateTopic == "" &&
		comp.CommandTopic == "" && comp.Fields == nil && len(comp.Extra) == 0
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

type issueList struct{ items []string }

func (l *issueList) add(format string, args ...any) {
	l.items = append(l.items, fmt.Sprintf(format, args...))
}
