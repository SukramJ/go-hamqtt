// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

// This file is the v0.36.0 implementation of Validate, ValidateIgnoring,
// ValidateBody and ValidateBodyIgnoring, frozen: mechanically copied from
// discovery/validate.go at tag v0.36.0 with its identifiers prefixed v036
// and its comments removed. Do not edit it to follow a change in
// validate.go — it is the reference that change is compared against. See
// TestValidateIsUnchanged.

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

func v036ValidateIgnoring(b *discovery.Bundle, ignore map[string]bool) error {
	if b == nil {
		return fmt.Errorf("discovery: nil bundle")
	}
	issues := &v036issueList{}

	if b.NodeID == "" {
		issues.add("node id is empty")
	} else if slug := v036sanitizeCheck(b.NodeID); slug != b.NodeID {
		issues.add("node id %q is not a legal topic segment (want %q)", b.NodeID, slug)
	}
	if b.Origin.Name == "" {
		issues.add("origin.name is required on a device bundle")
	}
	if len(b.Device.Identifiers) == 0 && len(b.Device.Connections) == 0 {
		issues.add("device needs at least one identifier or connection")
	}
	if len(b.Components) == 0 {
		issues.add("bundle has no components")
	}

	mqtt, relations, deviceClasses, err := v036loadTables()
	if err != nil {
		return err
	}

	seenUnique := map[v036identityKey]string{}
	for _, key := range b.Keys() {
		comp := b.Components[key]
		v036validateComponent(issues, key, comp, mqtt, relations, deviceClasses, seenUnique, ignore)
	}

	return issues.err(b.NodeID)
}

func v036ValidateBody(platform hacatalog.Platform, body map[string]any) error {
	return v036ValidateBodyIgnoring(platform, body, nil)
}

func v036ValidateBodyIgnoring(platform hacatalog.Platform, body map[string]any, ignore map[string]bool) error {
	issues := &v036issueList{}
	mqtt, relations, deviceClasses, err := v036loadTables()
	if err != nil {
		return err
	}
	v036validateBody(issues, string(platform), string(platform), body, mqtt, relations, deviceClasses, nil, ignore)
	return issues.err(string(platform))
}

func v036componentBody(comp discovery.Component) (map[string]any, error) {
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

type v036identityKey struct {
	platform string
	uniqueID string
}

func v036validateComponent(
	issues *v036issueList,
	key string,
	comp discovery.Component,
	mqtt hacatalog.MQTT,
	relations hacatalog.Relations,
	deviceClasses map[string][]string,
	seenUnique map[v036identityKey]string,
	ignore map[string]bool,
) {
	for i, a := range comp.Availability {
		if a.Topic == "" {
			issues.add("%s: availability[%d] has no topic, so nothing can ever mark this entity available", key, i)
		}
	}

	body, err := v036componentBody(comp)
	if err != nil {
		issues.add("%s: cannot encode component: %v", key, err)
		return
	}
	v036validateBody(issues, key, string(comp.Platform), body, mqtt, relations, deviceClasses, seenUnique, ignore)
}

func v036validateBody(
	issues *v036issueList,
	key string,
	platform string,
	body map[string]any,
	mqtt hacatalog.MQTT,
	relations hacatalog.Relations,
	deviceClasses map[string][]string,
	seenUnique map[v036identityKey]string,
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

	uniqueID := v036str(body, "unique_id")
	if uniqueID == "" {
		if !v036isRemoval(body) {
			issues.add("%s: unique_id is required", key)
		}
		return
	}
	if seenUnique != nil {
		id := v036identityKey{platform: platform, uniqueID: uniqueID}
		if prev, dup := seenUnique[id]; dup {
			issues.add("%s: unique_id %q already used by %q on platform %q",
				key, uniqueID, prev, platform)
		}
		seenUnique[id] = key
	}

	if schema, ok := mqtt.Platforms[platform]; ok {
		v036validateKeys(issues, key, platform, body, schema, ignore)
	}

	deviceClass := v036str(body, "device_class")
	if deviceClass != "" {
		if classes, known := deviceClasses[platform]; known {
			if !slices.Contains(classes, deviceClass) {
				issues.add("%s: device_class %q is not valid for platform %q",
					key, deviceClass, platform)
			}
		}
	}

	v036validateSensorRelations(issues, key, platform, body, relations)
}

func v036validateKeys(issues *v036issueList, key, platform string, body map[string]any, schema hacatalog.PlatformSchema, ignore map[string]bool) {
	allowed := schema.Keys
	checkRequired := true
	if len(allowed) == 0 {
		if variant, ok := schema.Variants[v036str(body, schema.Discriminator)]; ok {
			allowed = variant.Keys
		} else {
			allowed = map[string]hacatalog.SchemaKey{}
			for _, v := range schema.Variants {
				maps.Copy(allowed, v.Keys)
			}
			checkRequired = false
		}
	}
	if len(allowed) == 0 {
		return
	}

	for name := range body {
		if name == "platform" {
			continue
		}
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

func v036validateSensorRelations(issues *v036issueList, key, platform string, body map[string]any, relations hacatalog.Relations) {
	stateClass := v036str(body, "state_class")
	deviceClass := v036str(body, "device_class")
	unit := v036str(body, "unit_of_measurement")
	options, hasOptions := body["options"]

	if stateClass != "" && deviceClass != "" {
		if allowed, known := relations.SensorDeviceClassStateClasses[deviceClass]; known {
			if !slices.Contains(allowed, stateClass) {
				issues.add("%s: state_class %q is not allowed for device_class %q (allowed: %s)",
					key, stateClass, deviceClass, v036joinOrNone(allowed))
			}
		}
	}

	if hasOptions && !v036isEmptyList(options) && platform == string(hacatalog.PlatformSensor) {
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

	if unit != "" {
		if canonical, ambiguous := relations.AmbiguousUnits[unit]; ambiguous && canonical != unit {
			issues.warn("%s: unit_of_measurement %q is the legacy spelling; Home Assistant rewrites it to %q",
				key, unit, canonical)
		}
	}
}

func v036isRemoval(body map[string]any) bool {
	for name := range body {
		if name != "platform" {
			return false
		}
	}
	return true
}

func v036str(body map[string]any, key string) string {
	v, _ := body[key].(string)
	return v
}

func v036isEmptyList(v any) bool {
	list, ok := v.([]any)
	return ok && len(list) == 0
}

func v036loadTables() (hacatalog.MQTT, hacatalog.Relations, map[string][]string, error) {
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

func v036sanitizeCheck(s string) string {
	return strings.NewReplacer("/", "_", "+", "_", "#", "_", " ", "_").Replace(s)
}

func v036joinOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}

type v036issueList struct {
	items    []string
	warnings []string
}

func (l *v036issueList) add(format string, args ...any) {
	l.items = append(l.items, fmt.Sprintf(format, args...))
}

func (l *v036issueList) warn(format string, args ...any) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

func (l *v036issueList) err(nodeID string) error {
	if len(l.items) == 0 && len(l.warnings) == 0 {
		return nil
	}
	sort.Strings(l.items)
	sort.Strings(l.warnings)
	return &discovery.ValidationError{NodeID: nodeID, Issues: l.items, Warnings: l.warnings}
}
