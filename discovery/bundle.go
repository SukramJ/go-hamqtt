// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Package discovery renders a device and its entities into Home Assistant's
// MQTT discovery format, and validates the result before anyone publishes it.
//
// Device-based discovery only. One retained document per device at
// <prefix>/device/<node_id>/config carries the device block, the origin block
// and every component. The per-entity form Home Assistant still accepts is not
// produced here: it repeats the full device block once per entity — forty
// copies for a device with forty entities — and updates are not atomic.
//
// This package owns every Home Assistant JSON key name in the module. Nothing
// below it knows what "state_topic" is called.
package discovery

import (
	"encoding/json"
	"fmt"
	"sort"

	hacatalog "github.com/SukramJ/go-ha-catalog"
)

// DefaultPrefix is Home Assistant's own default discovery prefix.
const DefaultPrefix = "homeassistant"

// Origin identifies the software that published a bundle. Home Assistant
// requires it on a device bundle, and it is what lets an operator — and the
// orphan sweep — tell one bridge's retained topics from another's.
type Origin struct {
	Name string `json:"name"`
	SW   string `json:"sw,omitempty"`
	URL  string `json:"url,omitempty"`
}

// DeviceInfo is the `device` block.
type DeviceInfo struct {
	Identifiers      []string    `json:"identifiers,omitempty"`
	Connections      [][2]string `json:"connections,omitempty"`
	Name             string      `json:"name,omitempty"`
	Manufacturer     string      `json:"manufacturer,omitempty"`
	Model            string      `json:"model,omitempty"`
	ModelID          string      `json:"model_id,omitempty"`
	SWVersion        string      `json:"sw_version,omitempty"`
	HWVersion        string      `json:"hw_version,omitempty"`
	SerialNumber     string      `json:"serial_number,omitempty"`
	SuggestedArea    string      `json:"suggested_area,omitempty"`
	ConfigurationURL string      `json:"configuration_url,omitempty"`
	ViaDevice        string      `json:"via_device,omitempty"`
}

// AvailabilityEntry is one entry of an `availability` list.
type AvailabilityEntry struct {
	Topic               string `json:"topic"`
	ValueTemplate       string `json:"value_template,omitempty"`
	PayloadAvailable    string `json:"payload_available,omitempty"`
	PayloadNotAvailable string `json:"payload_not_available,omitempty"`
}

// Component is one entity inside a bundle.
//
// The common keys are typed; anything platform-specific goes in Fields, and
// Extra is the last resort. All three are flattened into one JSON object on
// marshal, in that order, later winning — the same single precedence rule the
// whole render pipeline uses.
type Component struct {
	Platform hacatalog.Platform `json:"platform"`
	Name     string             `json:"name,omitempty"`
	UniqueID string             `json:"unique_id,omitempty"`
	// DefaultEntityID seeds the entity id Home Assistant assigns. There is
	// deliberately no ObjectID field: `object_id` is not a legal MQTT
	// discovery key on any platform as of Home Assistant 2026.9 — it was
	// replaced by this one — and Home Assistant drops unknown keys in
	// silence, so publishing it does nothing at all.
	DefaultEntityID  string                   `json:"default_entity_id,omitempty"`
	DeviceClass      string                   `json:"device_class,omitempty"`
	StateClass       hacatalog.StateClass     `json:"state_class,omitempty"`
	UnitOfMeasure    string                   `json:"unit_of_measurement,omitempty"`
	Icon             string                   `json:"icon,omitempty"`
	EntityCategory   hacatalog.EntityCategory `json:"entity_category,omitempty"`
	EnabledByDefault *bool                    `json:"enabled_by_default,omitempty"`
	Precision        *int                     `json:"suggested_display_precision,omitempty"`
	StateTopic       string                   `json:"state_topic,omitempty"`
	CommandTopic     string                   `json:"command_topic,omitempty"`
	ValueTemplate    string                   `json:"value_template,omitempty"`
	Availability     []AvailabilityEntry      `json:"availability,omitempty"`
	AvailabilityMode string                   `json:"availability_mode,omitempty"`
	Options          []string                 `json:"options,omitempty"`
	Min              *float64                 `json:"min,omitempty"`
	Max              *float64                 `json:"max,omitempty"`
	Step             *float64                 `json:"step,omitempty"`

	// Fields carries platform-specific keys as a typed struct — see
	// [ClimateFields]. Marshalled by flattening, so it must encode to a JSON
	// object.
	Fields any `json:"-"`
	// Extra carries anything else. Applied last, so it overrides everything,
	// which makes it both the escape hatch and the way to silently break a
	// payload. Prefer a typed field.
	Extra map[string]any `json:"-"`
}

// MarshalJSON flattens the typed keys, Fields and Extra into one object.
func (c Component) MarshalJSON() ([]byte, error) {
	// The alias sheds MarshalJSON so the typed half can be encoded normally
	// without recursing into this method.
	type alias Component
	base, err := json.Marshal(alias(c))
	if err != nil {
		return nil, fmt.Errorf("discovery: encode component: %w", err)
	}
	merged := map[string]any{}
	if err := json.Unmarshal(base, &merged); err != nil {
		return nil, fmt.Errorf("discovery: re-read component: %w", err)
	}

	if c.Fields != nil {
		raw, err := json.Marshal(c.Fields)
		if err != nil {
			return nil, fmt.Errorf("discovery: encode platform fields: %w", err)
		}
		fields := map[string]any{}
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, fmt.Errorf("discovery: platform fields must encode to an object: %w", err)
		}
		for k, v := range fields {
			merged[k] = v
		}
	}
	for k, v := range c.Extra {
		merged[k] = v
	}
	return json.Marshal(merged)
}

// Bundle is the retained document published for one device.
type Bundle struct {
	// NodeID is the topic segment the bundle is published under. Not part of
	// the payload.
	NodeID string `json:"-"`

	Device     DeviceInfo           `json:"device"`
	Origin     Origin               `json:"origin"`
	Components map[string]Component `json:"components"`

	// QoS, if set, applies to every component that does not set its own.
	QoS *int `json:"qos,omitempty"`
}

// Topic returns where the bundle is published. An empty prefix uses
// [DefaultPrefix].
func (b *Bundle) Topic(prefix string) string {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return prefix + "/device/" + b.NodeID + "/config"
}

// Keys returns the component keys in sorted order, for deterministic
// iteration in tests and diffs.
func (b *Bundle) Keys() []string {
	out := make([]string, 0, len(b.Components))
	for k := range b.Components {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Remove marks components as deleted.
//
// Home Assistant removes a component from a device bundle when its entry is
// present but carries only a platform — an empty object is not enough, and
// omitting the key entirely leaves the entity in place. Getting this wrong is
// how a device that switched from separate entities to a composite ends up
// showing both forever.
func (b *Bundle) Remove(platformOf map[string]hacatalog.Platform, keys ...string) {
	if b.Components == nil {
		b.Components = map[string]Component{}
	}
	for _, k := range keys {
		b.Components[k] = Component{Platform: platformOf[k]}
	}
}

// ClimateFields are the climate platform's own topic keys.
//
// It is hand-written rather than generated because a composite entity's field
// set is what a consumer types out, and the catalog's schema is what validates
// it. Adding a platform's fields is additive and needs no codegen run.
type ClimateFields struct {
	CurrentTemperatureTopic string   `json:"current_temperature_topic,omitempty"`
	TemperatureStateTopic   string   `json:"temperature_state_topic,omitempty"`
	TemperatureCommandTopic string   `json:"temperature_command_topic,omitempty"`
	ModeStateTopic          string   `json:"mode_state_topic,omitempty"`
	ModeCommandTopic        string   `json:"mode_command_topic,omitempty"`
	FanModeStateTopic       string   `json:"fan_mode_state_topic,omitempty"`
	FanModeCommandTopic     string   `json:"fan_mode_command_topic,omitempty"`
	SwingModeStateTopic     string   `json:"swing_mode_state_topic,omitempty"`
	SwingModeCommandTopic   string   `json:"swing_mode_command_topic,omitempty"`
	PresetModeStateTopic    string   `json:"preset_mode_state_topic,omitempty"`
	PresetModeCommandTopic  string   `json:"preset_mode_command_topic,omitempty"`
	ActionTopic             string   `json:"action_topic,omitempty"`
	Modes                   []string `json:"modes,omitempty"`
	FanModes                []string `json:"fan_modes,omitempty"`
	SwingModes              []string `json:"swing_modes,omitempty"`
	PresetModes             []string `json:"preset_modes,omitempty"`
	MinTemp                 *float64 `json:"min_temp,omitempty"`
	MaxTemp                 *float64 `json:"max_temp,omitempty"`
	TempStep                *float64 `json:"temp_step,omitempty"`
	TemperatureUnit         string   `json:"temperature_unit,omitempty"`
}
