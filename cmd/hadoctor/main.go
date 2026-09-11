// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Command hadoctor inspects a whole MQTT capture and reports what a single
// payload cannot show.
//
// hacheck reads one discovery config at a time and asks whether it satisfies
// its platform's schema. That catches a key Home Assistant drops, and it is
// blind by construction to every defect that only exists between payloads:
// a topic an entity points at that nobody publishes, an availability list
// that no producer feeds, an entity that will therefore sit unavailable
// forever with nothing anywhere to say why. Those are the defects operators
// actually report, and they need the whole capture at once.
//
// Input is the same NDJSON as hacheck, but of EVERY topic rather than only
// the discovery ones:
//
//	mosquitto_sub -h broker -t '#' -v -W 5 | <to-ndjson> | hadoctor
//
// Feeding it only `homeassistant/#` is the one way to get a wrong answer
// from it — every state topic would look unpublished — so it refuses to
// report when the capture contains nothing but discovery topics.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/SukramJ/go-hamqtt/internal/dump"
)

func main() { os.Exit(cli(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

func cli(args []string, in io.Reader, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("hadoctor", flag.ContinueOnError)
	fs.SetOutput(errOut)
	prefix := fs.String("prefix", "homeassistant", "discovery prefix")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	snap, err := load(in, *prefix)
	if err != nil {
		fprintf(errOut, "hadoctor: %v\n", err)
		return 2
	}
	if snap.entities == 0 {
		fprintf(errOut, "hadoctor: the capture holds no discovery configs\n")
		return 2
	}
	if snap.nonDiscovery == 0 {
		// Refusing beats answering wrongly: with only `homeassistant/#` in
		// hand every state topic looks unpublished and every entity looks
		// broken, which is a report an operator would act on.
		fprintf(errOut, "hadoctor: the capture holds only discovery topics — "+
			"subscribe to '#', not '%s/#', or every entity will look broken\n", *prefix)
		return 2
	}
	return report(out, diagnose(snap))
}

// capture is a whole broker snapshot: which topics carry something, and what
// entities were declared.
type capture struct {
	// published names every topic the capture saw with a non-empty payload.
	published map[string]bool
	// entities is how many discovery configs were read.
	entities int
	// nonDiscovery counts topics outside the discovery prefix, which is what
	// tells a full capture from a discovery-only one.
	nonDiscovery int
	declared     []entity
}

// entity is one declared thing and the topics it depends on.
type entity struct {
	label string
	// availability are the topics that gate it. Empty means it declared none.
	availability []string
	// state is the topic it reads its value from, empty for a write-only
	// platform.
	state string
}

func load(in io.Reader, prefix string) (*capture, error) {
	c := &capture{published: map[string]bool{}}
	err := dump.Read(in, func(rec dump.Record) error {
		if !dump.IsEmpty(rec.Payload) {
			c.published[rec.Topic] = true
		}
		topic, ok := dump.ParseTopic(rec.Topic, prefix)
		if !ok {
			if !strings.HasPrefix(rec.Topic, strings.TrimSuffix(prefix, "/")+"/") {
				c.nonDiscovery++
			}
			return nil
		}
		body, err := dump.Object(rec.Payload)
		if err != nil {
			return err
		}
		if body == nil {
			return nil
		}
		if topic.Form == dump.FormBundle {
			comps, _ := body["components"].(map[string]any)
			keys := make([]string, 0, len(comps))
			for k := range comps {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				comp, ok := comps[k].(map[string]any)
				if !ok || len(comp) <= 1 {
					// Not an object, or a platform-only tombstone.
					continue
				}
				c.entities++
				c.declared = append(c.declared, read(topic.Node+"/"+k, comp, body))
			}
			return nil
		}
		c.entities++
		c.declared = append(c.declared, read(topic.Label(), body, nil))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

// read pulls the topics one entity depends on out of its body.
//
// `frame` is the bundle's top level, where a document may put an
// availability list that applies to every component — a component inheriting
// one is not an entity without availability.
func read(label string, body, frame map[string]any) entity {
	e := entity{label: label}
	e.state, _ = body["state_topic"].(string)
	e.availability = availabilityTopics(body)
	if len(e.availability) == 0 && frame != nil {
		e.availability = availabilityTopics(frame)
	}
	return e
}

func availabilityTopics(body map[string]any) []string {
	var out []string
	if t, ok := body["availability_topic"].(string); ok && t != "" {
		out = append(out, t)
	}
	list, _ := body["availability"].([]any)
	for _, item := range list {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if t, ok := entry["topic"].(string); ok && t != "" {
			out = append(out, t)
		}
	}
	return out
}

// diagnosis is one problem with one entity, in the operator's terms.
type diagnosis struct {
	entity string
	kind   string
	text   string
}

func diagnose(c *capture) []diagnosis {
	var out []diagnosis
	for _, e := range c.declared {
		switch {
		case len(e.availability) == 0:
			// Not fatal, and that is exactly why it survives: everything
			// works until the bridge dies, and then Home Assistant keeps
			// showing the last value as if it were current.
			out = append(out, diagnosis{
				e.label, "no-availability",
				"declares no availability, so it never goes unavailable — if the bridge stops, " +
					"Home Assistant keeps showing its last value as current",
			})
		default:
			for _, topic := range e.availability {
				if !c.published[topic] {
					out = append(out, diagnosis{
						e.label, "dead-availability",
						fmt.Sprintf("availability topic %q carries nothing, so the entity is "+
							"unavailable forever and nothing says why", topic),
					})
				}
			}
		}
		if e.state != "" && !c.published[e.state] {
			out = append(out, diagnosis{
				e.label, "dead-state",
				fmt.Sprintf("state topic %q carries nothing", e.state),
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].kind != out[j].kind {
			return rank(out[i].kind) < rank(out[j].kind)
		}
		return out[i].entity < out[j].entity
	})
	return out
}

// rank orders the kinds by how much they hurt. A dead availability topic
// means the entity is unusable right now; a missing one means it lies later.
func rank(kind string) int {
	switch kind {
	case "dead-availability":
		return 0
	case "dead-state":
		return 1
	default:
		return 2
	}
}

func report(w io.Writer, found []diagnosis) int {
	byKind := map[string]int{}
	for _, d := range found {
		byKind[d.kind]++
		fprintf(w, "- [%s] %s: %s\n", d.kind, d.entity, d.text)
	}
	if len(found) > 0 {
		fprintf(w, "\n")
	}
	kinds := make([]string, 0, len(byKind))
	for k := range byKind {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		fprintf(w, "%-20s %d\n", k, byKind[k])
	}
	if byKind["dead-availability"] > 0 || byKind["dead-state"] > 0 {
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
