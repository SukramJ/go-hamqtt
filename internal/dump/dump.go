// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Package dump reads a capture of retained MQTT messages.
//
// It is the shared front end of the command-line tools, and it is internal
// because the capture format is a convenience between them rather than an
// interface this module offers anyone. A consumer wanting to inspect a live
// broker should use its own transport and this module's [discovery] package
// directly.
package dump

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Record is one retained message: a topic and whatever was on it.
type Record struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

// Read parses an NDJSON capture, one [Record] per line.
//
// Blank lines are skipped so a capture can be concatenated or hand-edited
// without ceremony. A line that does not parse is an error rather than a
// finding: the operator's capture being malformed and Home Assistant's
// schema being violated are different problems, and conflating them sends
// them looking in the wrong place.
func Read(r io.Reader, visit func(Record) error) error {
	sc := bufio.NewScanner(r)
	// A device bundle for a forty-entity device is well past the default
	// 64 KiB token, and truncating one reports the tail as a parse error on
	// a line that is perfectly valid.
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		var rec Record
		if err := json.Unmarshal([]byte(text), &rec); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		if err := visit(rec); err != nil {
			return fmt.Errorf("line %d (%s): %w", line, rec.Topic, err)
		}
	}
	return sc.Err()
}

// Object decodes a payload that should be a JSON object, returning nil for
// an empty one.
//
// It accepts the object itself or a string containing it, because both are
// what the sources produce: a broker capture gives strings, Home Assistant's
// own MQTT diagnostics give objects, and an operator should not have to know
// which they have.
func Object(raw json.RawMessage) (map[string]any, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" || trimmed == `""` {
		return nil, nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		if strings.TrimSpace(s) == "" {
			return nil, nil
		}
		raw = json.RawMessage(s)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	return body, nil
}

// IsEmpty reports whether a payload carries nothing — an MQTT retraction.
func IsEmpty(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" || trimmed == `""` {
		return true
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return false
		}
		return s == ""
	}
	return false
}

// Form is which of the two discovery topic shapes a topic has.
type Form int

const (
	// FormEntity is <prefix>/<platform>/[<node_id>/]<object_id>/config.
	FormEntity Form = iota
	// FormBundle is <prefix>/device/<node_id>/config.
	FormBundle
)

// Topic is a parsed discovery config topic.
type Topic struct {
	Platform string
	Node     string
	Object   string
	Form     Form
}

// Label names the entity in a report. Home Assistant lets the per-entity
// form omit the node id, and a bare "/object" reads like a path with a hole
// in it rather than like a name.
func (t Topic) Label() string {
	if t.Node == "" {
		return t.Object
	}
	if t.Object == "" {
		return t.Node
	}
	return t.Node + "/" + t.Object
}

// ParseTopic reads the two discovery forms Home Assistant accepts.
//
// `device` is not ambiguous with a platform name — Home Assistant declares 32
// and none is called that — so the first segment separates the two-segment
// bundle from the two-segment per-entity form that omits its node id.
func ParseTopic(topic, prefix string) (Topic, bool) {
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	if !strings.HasPrefix(topic, prefix) || !strings.HasSuffix(topic, "/config") {
		return Topic{}, false
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(topic, prefix), "/config"), "/")
	switch {
	case len(parts) == 3:
		return Topic{Platform: parts[0], Node: parts[1], Object: parts[2], Form: FormEntity}, true
	case len(parts) == 2 && parts[0] == "device":
		return Topic{Node: parts[1], Form: FormBundle}, true
	case len(parts) == 2:
		return Topic{Platform: parts[0], Object: parts[1], Form: FormEntity}, true
	default:
		return Topic{}, false
	}
}
