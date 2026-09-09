// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"strings"

	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

// NodeID is the topic segment a device's bundle is published under.
//
// It is derived from the primary identifier only, and never from the device
// name: a renamed device must keep publishing to the same topic, or its old
// retained bundle is orphaned and Home Assistant shows the device twice.
func NodeID(dev *model.Device) string {
	return topic.Slug(dev.UID())
}

// ObjectID seeds the entity id Home Assistant will assign. It is published as
// `default_entity_id` — `object_id` has not been a legal MQTT discovery key
// since Home Assistant replaced it, and unknown keys are dropped in silence.
//
// Device slug and entity key, in that order. The device part is included
// because object ids share one namespace per platform across the whole broker:
// two bridges each publishing a "temperature" sensor would otherwise collide
// into sensor.temperature and sensor.temperature_2, with which one got the
// bare name decided by discovery order.
func ObjectID(dev *model.Device, e model.Entity) string {
	return topic.Slug(dev.UID()) + "_" + topic.Slug(e.Key())
}

// UniqueID is the entity's permanent identity in Home Assistant's registry.
//
// Everything a user does to an entity — renaming it, assigning an area, hiding
// it, referencing it from an automation — is keyed on this string. Changing it
// orphans all of that, so it is derived only from values that do not change:
// a namespace, the device's primary identifier, and the entity key.
//
// The namespace must be a constant of the consuming bridge, never a
// configurable value. One of the reference bridges derived it from the
// configurable MQTT root, so an operator changing the root orphaned every
// entity at once — and the cleanup sweep could no longer recognise the old
// topics as its own either.
func UniqueID(namespace string, dev *model.Device, e model.Entity) string {
	parts := make([]string, 0, 3)
	if namespace != "" {
		parts = append(parts, topic.Slug(namespace))
	}
	parts = append(parts, topic.Slug(dev.UID()), topic.Slug(e.Key()))
	return strings.Join(parts, "_")
}
