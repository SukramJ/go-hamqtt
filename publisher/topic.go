// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"strings"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// BundleSegment is the fixed first segment of a device document's topic.
//
// Home Assistant spells it `device`, and it is not a platform name — it is
// what makes the two discovery forms tellable apart from the topic alone.
// Home Assistant declares 32 MQTT platforms and none of them is called that;
// the closest, `device_automation` and `device_tracker`, produce a
// four-segment topic anyway.
const BundleSegment = "device"

// ConfigTopic is a retained Home Assistant discovery config topic, taken
// apart.
//
// Both forms are described by one struct because every caller that
// distinguishes them does so on [ConfigTopic.Bundle] and then reads the same
// node id. A sweep that modelled them as two types would branch at every use.
type ConfigTopic struct {
	// Platform is the per-entity form's component segment, empty for a
	// device document — whose platforms live inside the payload.
	Platform string
	// NodeID is the shared middle segment, and the only part either form
	// offers for deciding ownership.
	NodeID string
	// ObjectID is the per-entity form's last segment before `config`, empty
	// for a device document.
	ObjectID string
	// Bundle reports the device-document form.
	Bundle bool
}

// EntityConfigTopic renders `<prefix>/<platform>/<nodeID>/<objectID>/config`.
func EntityConfigTopic(prefix, platform, nodeID, objectID string) string {
	return topicPrefix(prefix) + platform + "/" + nodeID + "/" + objectID + "/config"
}

// BundleConfigTopic renders `<prefix>/device/<nodeID>/config`, the same
// string [discovery.Bundle.Topic] produces, for a caller that has a node id
// and no bundle in hand — the rollback path has exactly that.
func BundleConfigTopic(prefix, nodeID string) string {
	return topicPrefix(prefix) + BundleSegment + "/" + nodeID + "/config"
}

// ParseConfigTopic takes a retained topic apart, in either of the two forms
// Home Assistant accepts:
//
//	<prefix>/<platform>/<node_id>/<object_id>/config   one entity
//	<prefix>/device/<node_id>/config                   one device document
//
// The sweep has to recognise both or it cannot do its job. Matching only the
// per-entity form made every device document invisible — never inspected,
// never retracted, and therefore retained forever by a broker that no
// consumer would ever claim it from again. Segment count plus the literal
// first segment separates them: four segments is an entity, three beginning
// with `device` is a document. The three-segment per-entity form (node id
// omitted, which Home Assistant permits) is the only other reading, and the
// literal is what tells it apart.
//
// Ownership is deliberately not decided here. A parallel deployment —
// zigbee2mqtt publishes documents of its own — produces topics that parse
// perfectly well and must not be touched, so scoping the node id is the
// caller's job and [SweepRequest.Owns] is where it happens.
func ParseConfigTopic(prefix, topic string) (ConfigTopic, bool) {
	p := topicPrefix(prefix)
	if !strings.HasPrefix(topic, p) || !strings.HasSuffix(topic, "/config") {
		return ConfigTopic{}, false
	}
	parts := strings.Split(strings.TrimPrefix(topic, p), "/")
	switch {
	case len(parts) == 4:
		if parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return ConfigTopic{}, false
		}
		return ConfigTopic{Platform: parts[0], NodeID: parts[1], ObjectID: parts[2]}, true
	case len(parts) == 3 && parts[0] == BundleSegment:
		if parts[1] == "" {
			return ConfigTopic{}, false
		}
		return ConfigTopic{NodeID: parts[1], Bundle: true}, true
	default:
		return ConfigTopic{}, false
	}
}

// topicPrefix normalises a prefix to the trailing-slash form the parsers and
// renderers both want. An empty prefix takes the Home Assistant default, so a
// zero [Config] addresses the same tree a zero [discovery.Bundle] does.
func topicPrefix(prefix string) string {
	if prefix == "" {
		prefix = discovery.DefaultPrefix
	}
	return strings.TrimSuffix(prefix, "/") + "/"
}
