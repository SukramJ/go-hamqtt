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
	// SW and URL carry Home Assistant's canonical spellings, `sw_version` and
	// `support_url`. Its abbreviation table maps `sw` and `url` onto them and
	// it accepts either, but every other key this package emits is the long
	// form — a payload that mixes the two reads as though one of them were a
	// different key.
	SW  string `json:"sw_version,omitempty"`
	URL string `json:"support_url,omitempty"`
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
	// Platform is the bundle's discriminator: a component inside a document
	// has no topic to say what it is. `omitempty` is what lets
	// [RenderComponent] drop it for the per-entity form, whose topic already
	// carries it and on whose platforms Home Assistant declares no such key.
	// Inside a bundle it is never empty — [Bundle.Remove] refuses to write a
	// component without one.
	Platform hacatalog.Platform `json:"platform,omitempty"`
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

	// The keys below are accepted by nearly every platform (28 to 30 of the
	// 32), so they live here rather than in a per-platform Fields struct.
	CommandTemplate   string `json:"command_template,omitempty"`
	Optimistic        *bool  `json:"optimistic,omitempty"`
	EntityPicture     string `json:"entity_picture,omitempty"`
	VisibleByDefault  *bool  `json:"visible_by_default,omitempty"`
	Encoding          string `json:"encoding,omitempty"`
	QoS               *int   `json:"qos,omitempty"`
	Retain            *bool  `json:"retain,omitempty"`
	MessageExpiry     *int   `json:"message_expiry_interval,omitempty"`
	AvailabilityTopic string `json:"availability_topic,omitempty"`
	AvailabilityTmpl  string `json:"availability_template,omitempty"`
	PayloadAvailable  string `json:"payload_available,omitempty"`
	PayloadNotAvail   string `json:"payload_not_available,omitempty"`
	// Group is accepted by all 32 platforms. It lists the unique ids of the
	// entities this one groups — a list, not a name, which is the kind of
	// thing the catalog's per-key type exists to settle.
	Group []string `json:"group,omitempty"`

	// JSONAttributesTopic and its template attach a whole JSON document to an
	// entity as attributes. It is how a consumer publishes a datapoint's
	// descriptor — ranges, value lists, units — alongside its value without
	// inventing an entity per field.
	JSONAttributesTopic    string `json:"json_attributes_topic,omitempty"`
	JSONAttributesTemplate string `json:"json_attributes_template,omitempty"`

	// NameNull publishes `name: null`.
	//
	// That is how Home Assistant is told the entity has no name of its own
	// and should be shown as the device's name alone, and it is *not* the
	// same as leaving Name empty. entity.py's _set_entity_name reads
	// `config.get(CONF_NAME, UNDEFINED)`: an explicit null comes back as None
	// and becomes the entity's name, while an absent key comes back UNDEFINED
	// and makes Home Assistant derive a default from the platform or the
	// device class instead.
	//
	// A consumer that publishes one composite entity per device needs the
	// null: without it every entity carries the device name twice.
	NameNull bool `json:"-"`

	// Device and Origin are the per-entity discovery form's frame.
	//
	// A device bundle carries them once at the top, and [Bundle] is where they
	// belong there — a component inside a bundle leaves both nil. But the
	// per-entity form Home Assistant still accepts repeats them in every
	// retained config, and that is what five of the six consuming projects
	// publish today. Without these fields such a consumer cannot express its
	// payload as a Component at all, which is what kept its frame untyped.
	//
	// Pointers rather than values so the bundle form omits them instead of
	// emitting an empty object, which Home Assistant would read as a device
	// with no identifiers.
	Device *DeviceInfo `json:"device,omitempty"`
	Origin *Origin     `json:"origin,omitempty"`

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
	if c.NameNull {
		// After Fields and Extra, so an explicit request for the device's own
		// name is not undone by a stray name key from either.
		merged["name"] = nil
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
		// A key whose platform is unknown is skipped rather than written as
		// a component with none. Such an entry marshals to `{}`, which Home
		// Assistant ignores — so it would be noise in every future publish
		// while the entity it was meant to delete stays on screen. Refusing
		// makes the caller notice it is deleting something it never
		// declared.
		platform, known := platformOf[k]
		if !known || platform == "" {
			continue
		}
		b.Components[k] = Component{Platform: platform}
	}
}

// Ptr returns a pointer to v.
//
// Every numeric and boolean discovery key is a pointer so a legitimate zero
// survives `omitempty`: a cover's position_closed is 0, a siren's
// support_duration is false, and both are values a consumer means to publish
// rather than omit. Go has no way to take the address of a literal, so without
// this every call site needs a named variable — which is how a builder ends up
// reusing one by accident.
func Ptr[T any](v T) *T { return &v }
