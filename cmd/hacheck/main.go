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
// rewrites, or wires to do the wrong thing. The two runtime-layer wiring
// checks are in runtime.go; the cross-payload half of the runtime layer
// cannot be seen from one record and is hadoctor's.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/internal/dump"
)

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
	err := dump.Read(in, func(rec dump.Record) error {
		// Decide on the topic before touching the payload. An operator
		// dumping `homeassistant/#` catches the integration's own status
		// topic, whose payload is the bare word `online` — refusing to
		// parse that as a discovery config would turn a correct capture
		// into an error about a topic this tool has no opinion on.
		topic, ok := dump.ParseTopic(rec.Topic, prefix)
		if !ok {
			return nil
		}
		body, err := dump.Object(rec.Payload)
		if err != nil {
			return err
		}
		scanned++
		if body == nil {
			// An empty retained payload is a retraction, not a config. It
			// counts as read — a capture full of them is a fleet being torn
			// down, which the summary should not hide.
			return nil
		}
		findings = append(findings, check(rec.Topic, topic, body, prefix)...)
		return nil
	})
	if err != nil {
		return nil, scanned, err
	}
	return findings, scanned, nil
}

// check validates one retained config, in whichever of the two discovery
// forms its topic said it is.
func check(topic string, t dump.Topic, body map[string]any, prefix string) []finding {
	if t.Form == dump.FormBundle {
		return checkBundle(topic, t.Node, body, prefix)
	}
	out := collect(topic, t.Label(), discovery.ValidateBody(hacatalog.Platform(t.Platform), body))
	return append(out, checkRuntime(topic, t.Label(), body, prefix)...)
}

func checkBundle(topic, node string, body map[string]any, prefix string) []finding {
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

	// The document's own frame may carry an availability list that applies
	// to every component, so it is checked once here rather than missed
	// entirely — a wrongly-treed topic there affects the whole device.
	out := checkAvailabilityTree(topic, node, body, prefix)
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
		out = append(out, collect(topic, node+"/"+key,
			discovery.ValidateBody(hacatalog.Platform(platform), comp))...)
		out = append(out, checkRuntime(topic, node+"/"+key, comp, prefix)...)
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
