// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Command hacheck validates retained Home Assistant discovery payloads
// against the platform schemas extracted from Home Assistant itself.
//
// It exists because of the one property that makes discovery defects so
// expensive: Home Assistant's MQTT schemas are `extra=REMOVE_EXTRA`. A key a
// platform does not declare is dropped on arrival — no error on the wire, no
// line in any log, and an entity that works except for the one thing that
// key was for. Nobody notices until someone reads the payload and the schema
// side by side, which is what this does.
//
// Input is NDJSON on stdin, one record per line:
//
//	{"topic": "homeassistant/sensor/node/obj/config", "payload": {…}}
//	{"topic": "homeassistant/device/node/config",     "payload": {…}}
//
// `payload` may be the object itself or a string containing it, because both
// are what the sources produce: a broker dump gives strings, Home Assistant's
// own MQTT diagnostics give objects.
//
// Reading a dump rather than a broker is deliberate. Subscribing needs
// credentials, a network path and a transport dependency this module does not
// have and should not grow; a dump is something an operator can produce with
// mosquitto_sub, export from Home Assistant's diagnostics, or capture in a
// test. The broker-facing tool is hadoctor's job, and it can pipe into this.
//
// Exit status is 1 when any payload carries a blocking finding — something
// Home Assistant would reject or silently strip — and 0 when the only
// findings are advisories, which are values Home Assistant accepts and then
// rewrites.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// record is one retained discovery message.
type record struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

// finding is one problem with one entity.
type finding struct {
	topic    string
	entity   string
	blocking bool
	text     string
}

func main() { os.Exit(cli(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

// cli is main with its process coupling passed in, so the exit codes and the
// output are testable. Exit 2 is reserved for a dump this tool could not
// read: an operator has to be able to tell "your input is broken" from "your
// payloads are", and a single non-zero code cannot say which.
func cli(args []string, in io.Reader, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("hacheck", flag.ContinueOnError)
	fs.SetOutput(errOut)
	prefix := fs.String("prefix", discovery.DefaultPrefix, "discovery prefix to strip from topics")
	quiet := fs.Bool("quiet", false, "print only the summary")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	findings, scanned, err := run(in, *prefix)
	if err != nil {
		fprintf(errOut, "hacheck: %v\n", err)
		return 2
	}
	return report(out, findings, scanned, *quiet)
}

func run(in io.Reader, prefix string) ([]finding, int, error) {
	var (
		findings []finding
		scanned  int
	)
	sc := bufio.NewScanner(in)
	// Discovery payloads are small, but a device bundle for a forty-entity
	// device is not: the default 64 KiB token would truncate one and report
	// the tail as a parse error on a line that is perfectly valid.
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		var rec record
		if err := json.Unmarshal([]byte(text), &rec); err != nil {
			return nil, scanned, fmt.Errorf("line %d: %w", line, err)
		}
		// Decide on the topic before touching the payload. An operator
		// dumping `homeassistant/#` catches the integration's own status
		// topic, whose payload is the bare word `online` — refusing to
		// parse that as a discovery config would turn a correct dump into
		// an error about a topic this tool has no opinion on.
		platform, node, object, form, ok := parseTopic(rec.Topic, prefix)
		if !ok {
			continue
		}
		body, err := decodePayload(rec.Payload)
		if err != nil {
			return nil, scanned, fmt.Errorf("line %d (%s): %w", line, rec.Topic, err)
		}
		scanned++
		if body == nil {
			// An empty retained payload is a retraction, not a config. It
			// counts as read — a dump full of them is a fleet being torn
			// down, which the summary should not hide.
			continue
		}
		findings = append(findings, check(rec.Topic, platform, node, object, form, body)...)
	}
	if err := sc.Err(); err != nil {
		return nil, scanned, err
	}
	return findings, scanned, nil
}

// decodePayload accepts the object itself or a string containing it. A broker
// dump produces strings; Home Assistant's own diagnostics produce objects.
func decodePayload(raw json.RawMessage) (map[string]any, error) {
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

// check validates one retained config, in whichever of the two discovery
// forms its topic said it is.
func check(topic, platform, node, object string, form topicForm, body map[string]any) []finding {
	if form == formBundle {
		return checkBundle(topic, node, body)
	}
	return collect(topic, label(node, object), discovery.ValidateBody(hacatalog.Platform(platform), body))
}

func checkBundle(topic, node string, body map[string]any) []finding {
	comps, ok := body["components"].(map[string]any)
	if !ok {
		return []finding{{
			topic: topic, entity: node, blocking: true,
			text: `a device bundle needs a "components" object`,
		}}
	}
	keys := make([]string, 0, len(comps))
	for k := range comps {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out []finding
	for _, key := range keys {
		comp, ok := comps[key].(map[string]any)
		if !ok {
			out = append(out, finding{
				topic: topic, entity: node + "/" + key, blocking: true,
				text: "component is not an object",
			})
			continue
		}
		platform, _ := comp["platform"].(string)
		if platform == "" {
			out = append(out, finding{
				topic: topic, entity: node + "/" + key, blocking: true,
				text: "component carries no platform",
			})
			continue
		}
		if len(comp) == 1 {
			// Platform and nothing else is a deletion, not a broken entity.
			continue
		}
		out = append(out, collect(topic, label(node, key),
			discovery.ValidateBody(hacatalog.Platform(platform), comp))...)
	}
	return out
}

// collect turns one validation result into findings, keeping the distinction
// the validator draws: an issue is something Home Assistant would reject or
// strip, a warning is something it accepts and rewrites.
func collect(topic, entity string, err error) []finding {
	if err == nil {
		return nil
	}
	var ve *discovery.ValidationError
	if !errors.As(err, &ve) {
		return []finding{{topic: topic, entity: entity, blocking: true, text: err.Error()}}
	}
	out := make([]finding, 0, len(ve.Issues)+len(ve.Warnings))
	for _, issue := range ve.Issues {
		out = append(out, finding{topic: topic, entity: entity, blocking: true, text: issue})
	}
	for _, warning := range ve.Warnings {
		out = append(out, finding{topic: topic, entity: entity, text: warning})
	}
	return out
}

// label names an entity in a finding. Home Assistant lets the per-entity
// form omit the node id, and a bare "/object" reads like a path with a hole
// in it rather than like a name.
func label(node, object string) string {
	if node == "" {
		return object
	}
	return node + "/" + object
}

type topicForm int

const (
	formEntity topicForm = iota
	formBundle
)

// parseTopic reads the two discovery forms Home Assistant accepts:
//
//	<prefix>/<platform>/[<node_id>/]<object_id>/config
//	<prefix>/device/<node_id>/config
//
// `device` is not ambiguous with a platform name — Home Assistant declares 32
// and none is called that — so the first segment separates the three-segment
// bundle from the three-segment per-entity form that omits its node id.
func parseTopic(topic, prefix string) (platform, node, object string, form topicForm, ok bool) {
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	if !strings.HasPrefix(topic, prefix) || !strings.HasSuffix(topic, "/config") {
		return "", "", "", 0, false
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(topic, prefix), "/config"), "/")
	switch {
	case len(parts) == 3:
		return parts[0], parts[1], parts[2], formEntity, true
	case len(parts) == 2 && parts[0] == "device":
		return "", parts[1], "", formBundle, true
	case len(parts) == 2:
		return parts[0], "", parts[1], formEntity, true
	default:
		return "", "", "", 0, false
	}
}

func report(w io.Writer, findings []finding, scanned int, quiet bool) int {
	blocking := 0
	for _, f := range findings {
		if f.blocking {
			blocking++
		}
	}
	if !quiet {
		for _, f := range findings {
			mark := "~"
			if f.blocking {
				mark = "-"
			}
			fprintf(w, "%s %s: %s\n", mark, f.entity, f.text)
		}
		if len(findings) > 0 {
			fprintf(w, "\n")
		}
	}
	// Both numbers, always. "0 blocking" alone cannot be told apart from a
	// run that read nothing, which is the failure an operator is most likely
	// to have and least likely to notice.
	fprintf(w, "%d payload(s) checked, %d blocking, %d advisory\n",
		scanned, blocking, len(findings)-blocking)
	if blocking > 0 {
		return 1
	}
	return 0
}

// fprintf swallows the write error on purpose. The destination is the
// caller's stdout; a report that cannot be written is a broken pipe, and
// there is nowhere left to report that to.
func fprintf(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...)
}
