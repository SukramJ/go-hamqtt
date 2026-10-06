// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"

	hacatalog "github.com/SukramJ/go-ha-catalog"
)

// ErrUnpublishable matches a document Home Assistant refuses whatever it
// contains — a finding on its own node id, device or origin — or one with
// every component withheld. See [Containment.Publishable].
var ErrUnpublishable = errors.New("discovery: document is unpublishable")

// ContainOptions parameterises [Contain].
type ContainOptions struct {
	// Ignore is the deliberate-key set, as for [InspectOptions.Ignore].
	Ignore map[string]bool
	// StripWarnings removes the keys of every strippable warning — an
	// unknown key, an extra key in an availability entry, a state class
	// impossible for its device class. Home Assistant discards or ignores
	// each of them anyway, so the document it ends up with is the same;
	// what changes is that the payload says so, and that the state-class
	// warning stops appearing in its log. False leaves them in place.
	StripWarnings bool
}

// StrippedKey is one key [Contain] removed.
type StrippedKey struct {
	// Component is the bundle key.
	Component string
	// Key is the removed key, spelled as in [Finding.Keys].
	Key string
	// Kind is the finding that made it strippable.
	Kind FindingKind
}

// Containment is what [Contain] did to a bundle.
type Containment struct {
	// Bundle is the document Home Assistant would effectively accept: a
	// copy of the input without the withheld components and, with
	// [ContainOptions.StripWarnings], without the stripped keys. The input
	// is never modified. Its [Bundle.Withheld] lists what was taken out.
	Bundle *Bundle
	// Findings is [Inspect] on the input bundle, before anything was taken
	// out.
	Findings Findings
	// Withheld is every component taken out, as rendered, keyed by bundle
	// key. The same map as Bundle.Withheld.
	Withheld map[string]Component
	// Stripped lists the keys removed from the components that stayed.
	Stripped []StrippedKey
}

// Publishable reports whether the contained document is worth publishing:
// it has no document error that withholding a component cannot cure, and,
// when the input had components, at least one survived.
//
// The second condition is a judgement rather than a Home Assistant rule —
// Home Assistant accepts a document with no components and does nothing
// with it — and it is there because publishing one would replace the
// retained document that still describes the device: on Home Assistant's
// next restart none of its entities would be rediscovered.
func (c *Containment) Publishable() bool {
	if c == nil || c.Bundle == nil || len(c.Findings.DocumentErrors()) > 0 {
		return false
	}
	return len(c.Bundle.Components) > 0 || len(c.Withheld) == 0
}

// Err is nil for a publishable containment and otherwise an error matching
// [ErrUnpublishable] that names the reason.
func (c *Containment) Err() error {
	if c.Publishable() {
		return nil
	}
	if c == nil || c.Bundle == nil {
		return ErrUnpublishable
	}
	reasons := make([]string, 0, 1)
	for _, f := range c.Findings.DocumentErrors() {
		reasons = append(reasons, f.Message)
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "every component was withheld")
	}
	return &unpublishableError{nodeID: c.Bundle.NodeID, reasons: reasons}
}

type unpublishableError struct {
	nodeID  string
	reasons []string
}

func (e *unpublishableError) Error() string {
	return "discovery: document " + strconv.Quote(e.nodeID) + " is unpublishable: " + strings.Join(e.reasons, "; ")
}

func (e *unpublishableError) Is(target error) bool { return target == ErrUnpublishable }

// Contain returns the bundle Home Assistant would effectively accept, and a
// record of how it got there.
//
// It is the per-component handling Home Assistant itself applies, done
// before the publish instead of after it. A component with an error finding
// — whether Home Assistant would refuse only that entity, or, like a
// missing `unique_id`, the whole document because of it — is withheld and
// reported; every other component is published. The document itself is
// unpublishable only for an error on its own keys (see
// [Containment.Publishable]). [Inspect] explains each classification.
//
// # What withholding costs, and what it must never become
//
// A withheld component is omitted from the document, which Home Assistant
// reads as "no change": an entity it already had keeps its last good config
// until it restarts, and is then not rediscovered (its registry entry stays,
// and the entity shows as unavailable) until a document carrying it again
// is published. A component that never was valid simply does not appear.
//
// It is never turned into a tombstone. A tombstone — the platform-only entry
// [Bundle.RemoveComponents] writes — makes Home Assistant delete the entity
// and its registry entry, so a consumer whose removal diff reads "in the
// last document, not in this one" would delete every entity it withholds.
// The contained bundle prevents that by construction:
// [Bundle.Remove] and [Bundle.RemoveComponents] skip a key in
// [Bundle.Withheld], and [Bundle.KeepSet] counts it as declared.
//
// # Order
//
//  1. Render the document.
//  2. Tombstone what really left: `b.RemoveComponents(prev, gone...)`, with
//     prev being the previous cycle's [Bundle.KeepSet]. A tombstone is a
//     platform-only entry and passes containment untouched.
//  3. `c := Contain(b, opts)`; publish `c.Bundle` if
//     [Containment.Publishable], and report `c.Withheld` / `c.Findings`.
//  4. Remember `c.Bundle.KeepSet()` as next cycle's prev.
//
// Steps 2 and 3 may be swapped — tombstoning the contained bundle skips the
// withheld keys — but step 4 must read the KeepSet, not Components.
// publisher.Runtime.PublishBundle with publisher.Config.Contain set performs
// step 3 itself and keeps the sweep off a withheld component's old
// per-entity config.
func Contain(b *Bundle, opts ContainOptions) *Containment {
	findings := Inspect(b, InspectOptions{Ignore: opts.Ignore})
	c := &Containment{Findings: findings, Withheld: map[string]Component{}}
	if b == nil {
		return c
	}

	out := *b
	out.Components = make(map[string]Component, len(b.Components))
	maps.Copy(out.Components, b.Components)
	if b.Tombstones != nil {
		out.Tombstones = maps.Clone(b.Tombstones)
	}
	// A bundle contained twice keeps what the first pass withheld.
	maps.Copy(c.Withheld, b.Withheld)

	for _, key := range findings.FailingComponents() {
		if comp, ok := out.Components[key]; ok {
			c.Withheld[key] = comp
			delete(out.Components, key)
		}
	}

	if opts.StripWarnings {
		strip := map[string][]Finding{}
		for _, f := range findings {
			if f.Strippable && f.Component != "" {
				if _, kept := out.Components[f.Component]; kept {
					strip[f.Component] = append(strip[f.Component], f)
				}
			}
		}
		for _, key := range sortedKeys(strip) {
			stripped, removed, ok := stripComponent(out.Components[key], strip[key])
			if !ok {
				continue
			}
			out.Components[key] = stripped
			for _, r := range removed {
				c.Stripped = append(c.Stripped, StrippedKey{Component: key, Key: r.key, Kind: r.kind})
			}
		}
	}

	if len(c.Withheld) > 0 {
		out.Withheld = c.Withheld
	} else {
		out.Withheld = nil
		c.Withheld = nil
	}
	c.Bundle = &out
	return c
}

type removedKey struct {
	key  string
	kind FindingKind
}

// stripComponent removes the findings' keys from a component and rebuilds
// it, typed fields included, from what is left. ok is false when the
// rebuilt component would not encode to exactly the stripped object, in
// which case the caller keeps the original — a strip is an optimisation of
// the payload, never a reason to change what remains of it.
func stripComponent(comp Component, findings []Finding) (Component, []removedKey, bool) {
	body, err := componentBody(comp)
	if err != nil {
		return comp, nil, false
	}
	var removed []removedKey
	for _, f := range findings {
		for _, k := range f.Keys {
			if stripKey(body, k) {
				removed = append(removed, removedKey{key: k, kind: f.Kind})
			}
		}
	}
	if len(removed) == 0 {
		return comp, nil, false
	}
	rebuilt, ok := componentFromBody(body)
	if !ok {
		return comp, nil, false
	}
	return rebuilt, removed, true
}

// stripKey deletes one key, `availability[<i>].<key>` included.
func stripKey(body map[string]any, key string) bool {
	if rest, ok := strings.CutPrefix(key, "availability["); ok {
		idx, name, ok := strings.Cut(rest, "].")
		if !ok {
			return false
		}
		i, err := strconv.Atoi(idx)
		if err != nil {
			return false
		}
		var entry map[string]any
		switch v := body["availability"].(type) {
		case []any:
			if i < 0 || i >= len(v) {
				return false
			}
			entry, _ = v[i].(map[string]any)
		case map[string]any:
			if i == 0 {
				entry = v
			}
		}
		if _, present := entry[name]; !present {
			return false
		}
		delete(entry, name)
		return true
	}
	if _, present := body[key]; !present {
		return false
	}
	delete(body, key)
	return true
}

// componentFromBody turns an encoded component back into a [Component]
// whose typed fields carry every value they can — [Component.UniqueID]
// above all, which the legacy-topic forms read — with the rest in Extra.
func componentFromBody(body map[string]any) (Component, bool) {
	raw, err := json.Marshal(body)
	if err != nil {
		return Component{}, false
	}
	var typed Component
	if err := json.Unmarshal(raw, &typed); err != nil {
		// A value the typed field cannot hold — an availability object
		// where the struct has a list — keeps everything in Extra, with the
		// identity typed so the legacy-topic forms still find it.
		typed = Component{Extra: maps.Clone(body)}
		typed.Platform = hacatalog.Platform(str(body, "platform"))
		typed.UniqueID = str(body, "unique_id")
		got, err := json.Marshal(typed)
		if err != nil || !bytes.Equal(got, raw) {
			return Component{}, false
		}
		return typed, true
	}
	// The keys the typed half encodes on its own; everything else goes to
	// Extra, which is applied last and so also restores a value the typed
	// field cannot carry (an empty unit, a null name).
	type alias Component
	typedRaw, err := json.Marshal(alias(typed))
	if err != nil {
		return Component{}, false
	}
	covered := map[string]any{}
	if err := json.Unmarshal(typedRaw, &covered); err != nil {
		return Component{}, false
	}
	for k, v := range body {
		if _, ok := covered[k]; !ok {
			if typed.Extra == nil {
				typed.Extra = map[string]any{}
			}
			typed.Extra[k] = v
		}
	}
	got, err := json.Marshal(typed)
	if err != nil || !bytes.Equal(got, raw) {
		return Component{}, false
	}
	return typed, true
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
