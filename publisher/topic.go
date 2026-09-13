// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"reflect"
	"runtime"
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
	// NodeID is the shared middle segment, and the only part the
	// node-id-bearing forms offer for deciding ownership. It is empty for
	// the node-id-less per-entity form — `Platform != "" && NodeID == ""`
	// is exactly that form — because the topic carries no node id and
	// inventing one would be a guess an ownership predicate would then
	// trust.
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

// ParseConfigTopic takes a retained topic apart, in each of the three forms
// Home Assistant accepts:
//
//	<prefix>/<platform>/<node_id>/<object_id>/config   one entity
//	<prefix>/<platform>/<object_id>/config             one entity, no node id
//	<prefix>/device/<node_id>/config                   one device document
//
// The sweep has to recognise all of them or it cannot do its job. Matching
// only the first made every device document invisible — never inspected,
// never retracted, and therefore retained forever by a broker that no
// consumer would ever claim it from again. Segment count plus the literal
// first segment separates them: four segments is an entity, three beginning
// with `device` is a document, and three beginning with anything else is an
// entity whose node-id level was omitted.
//
// That third form used to be described here and then rejected — the function
// returned false for it, so such a config was invisible to the sweep and
// retained forever, the same defect the device document had. It is not
// hypothetical: the ADR 0070 phase-5 measurement of 2026-09-12 found
// go-zendure2mqtt publishing exactly this form,
// `<prefix>/<platform>/<unique_id>/config`, across its whole installed fleet.
// Its [ConfigTopic.NodeID] is empty, because the topic carries no node id and
// inventing one would be a guess — which means an [SweepRequest.Owns] that
// scopes on the node id declines it, as it should, and a consumer whose fleet
// is on this form has to say so. [LegacyTopicByUniqueID] is the publishing
// side of the same statement.
//
// Ownership is deliberately not decided here. A parallel deployment —
// zigbee2mqtt publishes documents of its own — produces topics that parse
// perfectly well and must not be touched, so scoping the node id is the
// caller's job and [SweepRequest.Owns] is where it happens.
//
// Widening a parser widens what a predicate is asked about, and that is the
// upgrade hazard of v0.29.0: an [SweepRequest.Owns] that does not read
// [ConfigTopic.NodeID] now judges the three-segment form as well, a shape
// Tasmota publishes into a shared discovery tree. Re-read such a predicate
// before upgrading. One that scopes on the node id is unaffected — the
// node-id-less form parses with an empty one, and it declines.
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
	case len(parts) == 3:
		if parts[0] == "" || parts[1] == "" {
			return ConfigTopic{}, false
		}
		return ConfigTopic{Platform: parts[0], ObjectID: parts[1]}, true
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

// LegacyEntity is what a [LegacyTopicFunc] is asked about: one component of a
// device document, with every string a per-entity discovery topic could be
// keyed on.
//
// A struct rather than four arguments because the set will grow — the three
// topic forms Home Assistant accepts are not a closed set of *keyings*, and a
// consumer on a fourth one should not need this module to change signature.
type LegacyEntity struct {
	// Prefix is the discovery prefix, already defaulted.
	Prefix string
	// Platform is the component's platform, the second segment of every
	// per-entity form.
	Platform string
	// NodeID is the device document's node id.
	NodeID string
	// ObjectID is the component's key inside the document, which is the
	// object-id segment [EntityConfigTopic] renders.
	ObjectID string
	// UniqueID is the component's `unique_id`, which is what Home Assistant
	// actually keys the entity on — and what a consumer that never had a
	// node-id level most likely put in the topic instead.
	UniqueID string
	// Component is the whole component, for a keying none of the above
	// covers.
	Component discovery.Component
}

// LegacyTopicFunc renders the retained per-entity config topic one component
// occupies in a consumer's installed fleet, or "" if it occupies none.
//
// It exists because this module cannot guess which of the three forms Home
// Assistant accepts a fleet is on, and guessing wrong is harmful in both
// directions: too narrow and [Runtime.PublishBundle] retracts nothing, so
// every entity of the migration is refused with one `WARNING` line; too wide
// and it retracts a topic belonging to another writer in a shared discovery
// tree. So the consumer states it, and a consumer that states nothing keeps
// today's single form.
//
// The measured need is go-zendure2mqtt, ADR 0070 phase 5, measured
// 2026-09-12: 29 retained configs at `<prefix>/<platform>/<unique_id>/config`
// — four segments, no node-id level, which Home Assistant permits — against a
// [SupersededTopics] that renders five. The measurement called it "the single
// highest-risk step in the whole migration and the one most likely to be
// missed, because everything *looks* right: the bundle publishes, the log is
// clean, and the entities keep their old configs."
type LegacyTopicFunc func(e LegacyEntity) string

// legacyFormNames names the forms a runtime will retract under, for the boot
// log and [Runtime.LegacyForms].
//
// The measured need is a fleet that spans releases: [Config.LegacyEntityTopics]
// REPLACES the default rather than adding to it, so stating one form silently
// stops retracting the other, and nothing anywhere said which forms were
// active. An operator reading a migration that quietly retracted nothing had
// no line to look at. An empty list names the default explicitly, because
// "unset" and "the five-segment form" are the same behaviour and only one of
// them is useful in a log.
func legacyFormNames(forms []LegacyTopicFunc) []string {
	if len(forms) == 0 {
		return []string{legacyFormName(LegacyTopicWithNodeID) + " (default)"}
	}
	out := make([]string, 0, len(forms))
	for _, f := range forms {
		if f == nil {
			continue
		}
		out = append(out, legacyFormName(f))
	}
	return out
}

// legacyFormName is the best name a func value has: the exported helper's own
// name for the three this package ships, and the enclosing function plus a
// counter for a consumer's closure — which is still enough to tell an
// operator that a form is there and how many.
func legacyFormName(f LegacyTopicFunc) string {
	fn := runtime.FuncForPC(reflect.ValueOf(f).Pointer())
	if fn == nil {
		return "func"
	}
	name := fn.Name()
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	return name
}

// LegacyTopicWithNodeID renders `<prefix>/<platform>/<node_id>/<object_id>/
// config` — the five-segment form, which is what [SupersededTopics] uses when
// a consumer states nothing, and therefore the behaviour of every release
// before v0.27.0.
//
// Exported so a consumer whose fleet is on two forms can name this one
// explicitly alongside the other, rather than discovering that stating a
// second form dropped the first.
func LegacyTopicWithNodeID(e LegacyEntity) string {
	if e.Platform == "" || e.NodeID == "" || e.ObjectID == "" {
		return ""
	}
	return EntityConfigTopic(e.Prefix, e.Platform, e.NodeID, e.ObjectID)
}

// LegacyTopicByUniqueID renders `<prefix>/<platform>/<unique_id>/config` —
// the four-segment form, with no node-id level.
//
// This is go-zendure2mqtt's fleet, measured on 2026-09-12: its per-entity
// configs are keyed on the `unique_id` and have no node-id segment at all, so
// the five-segment default retracted none of them and the device bundle would
// have been refused entity by entity with a single `WARNING` line as the only
// evidence. A component with no `unique_id` yields "" and is skipped, because
// there is nothing to key on and a topic built from a blank segment belongs to
// nobody.
//
// A tombstone is the case to watch, and it is the one that reaches a
// consumer: the entry [discovery.Bundle.Remove] writes carries a platform and
// nothing else, so this form has nothing to key on unless the removed
// component's identity was remembered outside the payload. It is remembered,
// by [discovery.Bundle.RemoveComponents] and by [discovery.Bundle.Remove] on
// a key that was still declared — see [SupersededTopics] for what a tombstone
// with neither costs.
func LegacyTopicByUniqueID(e LegacyEntity) string {
	if e.Platform == "" || e.UniqueID == "" {
		return ""
	}
	return topicPrefix(e.Prefix) + e.Platform + "/" + e.UniqueID + "/config"
}

// LegacyTopicByObjectID renders `<prefix>/<platform>/<object_id>/config`: the
// same four-segment form as [LegacyTopicByUniqueID], keyed on the component
// key instead.
//
// Both exist because the two differ in practice — a consumer whose object id
// is a short channel name and whose unique id carries a serial prefix
// publishes to different topics under the two — and a consumer cannot be
// asked to work out which one this module meant.
func LegacyTopicByObjectID(e LegacyEntity) string {
	if e.Platform == "" || e.ObjectID == "" {
		return ""
	}
	return topicPrefix(e.Prefix) + e.Platform + "/" + e.ObjectID + "/config"
}
