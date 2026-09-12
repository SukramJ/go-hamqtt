// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Package catalog is one way to describe entities: priority-ordered rules that
// fill in descriptions, and a static table that produces them.
//
// It is deliberately *one* way rather than the way. The consuming projects get
// their entity sets three fundamentally different ways — a hand-maintained
// catalog, a profile read out of the device itself, and expansion over
// discovered hardware — and only the first is a file. That is why
// [model.EntitySource] is an interface and this package merely implements it.
//
// The rule format separates matching from effect: [Match] says when a rule
// applies, [Overlay] says what it changes. The reference implementation had
// both in one flat struct, where a field could be read as either.
package catalog

import (
	"context"
	"sort"
	"strings"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/model"
)

// Match is the condition of a rule. Every non-empty field must match — the
// criteria are ANDed — and an empty field matches anything.
type Match struct {
	// Platforms limits the rule to certain platforms.
	Platforms []hacatalog.Platform
	// Keys matches the entity key exactly.
	Keys []string
	// Leaves matches the last path segment of the entity's state slot, which
	// is usually the vendor's parameter name. Matched case-insensitively:
	// a vendor vocabulary has a house style — Homematic shouts, Home Connect
	// uses dotted CamelCase — and a rule author writes the name the way the
	// vendor prints it, which is not always the way the wire spells it.
	Leaves []string
	// Models matches the device model, case-insensitively, by prefix. A
	// device family shares a prefix far more often than an exact name.
	Models []string
	// Buckets limits the rule to datapoints of a kind — configuration
	// parameters rather than runtime values, say.
	Buckets []model.Bucket
	// Unit matches the description's current unit, for a rule that refines
	// what an earlier one established.
	Unit *model.Unit
	// KeyContains is a case-insensitive substring test on the entity key, for
	// the long tail that no exact list covers.
	//
	// Keys, by contrast, stays case-sensitive: an entity key is the
	// consumer's own identifier and is also the component key inside a
	// bundle, where two keys differing only in case are two components. A
	// substring probe makes no such claim about identity.
	KeyContains *string

	// Categories limits the rule to the consumer's own classification of the
	// datapoint, which is not always a Home Assistant platform.
	//
	// [Match.Platforms] is typed to the platform an entity renders as, and a
	// reference table does not always key on that. One measured consumer's
	// 147-rule table has 20 rules keyed on `hub_sensor`, `hub_button`,
	// `hub_binary_sensor` and `schedule_switch` — classifications of what a
	// datapoint *is*, which several platforms can render. Collapsing them to
	// the platform first is not a simplification: a rule meant for a
	// button-shaped action would then also apply to every other entity that
	// happens to render as a button.
	//
	// Matched case-insensitively against [Categorised.Category]. An entity
	// that does not implement that interface matches no category rule.
	Categories []string

	// Postfix matches the trailing segment of the entity's leaf after its
	// last underscore — "_2" of "LEVEL_2".
	//
	// A vendor that numbers repeated parameters gives a rule no other way to
	// say "the second one": the leaf differs per instance, so [Match.Leaves]
	// cannot list them and [Match.KeyContains] would also match "_20".
	// Matched case-insensitively, with or without the leading underscore.
	Postfix *string

	// NameContains is a case-insensitive substring test on the entity's
	// display name, distinct from [Match.KeyContains].
	//
	// The two are different strings and a reference table uses both: a key
	// is the consumer's identifier, a name is what an operator sees and what
	// a vendor's own catalogue is written against. One measured table has 20
	// rules keyed on the name.
	NameContains *string
}

// Categorised is implemented by an entity that carries the consumer's own
// classification of its datapoint, for [Match.Categories].
//
// A separate interface rather than a field on [model.Description], because
// the classification is the consumer's vocabulary and the model has no
// opinion about it — and because an entity that has no such notion should
// not have to carry an empty string saying so.
type Categorised interface {
	Category() string
}

// Overlay is the effect of a rule. Every field is a pointer or a slice, so
// "not set" is distinguishable from "set to the zero value" — a rule that
// clears an icon and a rule that says nothing about icons are different
// things.
type Overlay struct {
	Name *model.Localized
	// NameKey sets [model.Description.NameKey] — the usual way a rule table
	// names an entity, since the table outlives any one language.
	NameKey     *string
	DeviceClass *model.DeviceClass
	StateClass  *hacatalog.StateClass
	Unit        *model.Unit
	Icon        *string
	Category    *hacatalog.EntityCategory
	Enabled     *bool
	Precision   *int
	Min         *float64
	Max         *float64
	Step        *float64
	Options     *model.Enum

	// Multiplier scales the datapoint's value before it is published.
	//
	// It is a description field rather than a value-layer concern because
	// the scale belongs to the *entity*, not to the datapoint: the same raw
	// level is a fraction to one entity and a percentage to another, and a
	// rule table is where that is written down. One measured consumer scales
	// eight of its rules this way — a 0..1 level to 0..100, an operating
	// time to hours.
	//
	// The model does not apply it. It carries it, and the consumer's value
	// layer reads [model.Description.Multiplier] when it renders the value
	// and the bounds — which is the only place that knows whether a given
	// publish is a value at all.
	Multiplier *float64

	// Suppress removes the entity entirely. It is the operator's opt-out, and
	// it uses the same mechanism a composite entity uses to hide the entities
	// it replaces — so there is one suppression concept, not two.
	Suppress *bool

	// Extra merges into the description's escape hatch.
	Extra map[string]any
}

// Rule is a condition and its effect at a priority.
type Rule struct {
	// Priority orders application: lower first, so a higher-priority rule
	// overwrites a lower one. Rules at equal priority apply in slice order.
	Priority int
	Match    Match
	Set      Overlay
}

// Rules is a rule set that is itself an [model.Enricher].
//
// That is the point of the design: catalog defaults and operator overrides are
// the same mechanism at different priorities, rather than two systems that
// have to be kept consistent with each other.
//
// # Porting a first-match-wins table
//
// Rules applies EVERY matching rule, lowest priority first, so a later rule
// layers over an earlier one. A reference table that takes the FIRST match
// and stops is a different machine, and transcribing it rule by rule
// produces a different — usually richer — description.
//
// That is not a corner case. Measured against one consumer's 147-rule table
// over 1,860 witness inputs: 933 of them match more than one rule, and a
// literal transcription diverges on 18 to 21 per cent of the space. A
// faithful port reaches zero divergence, under three conditions that none of
// them is obvious:
//
//  1. Every ported rule's [Overlay] sets EVERY field it has an opinion about
//     — including the ones the source rule leaves empty, as explicit empty
//     values. A first-match-wins table means "this rule decides the whole
//     record"; an Overlay with a nil field means "this rule says nothing
//     about that field". Making every field non-nil turns
//     highest-priority-wins into whole-record replacement, which is what
//     makes stacking equivalent to stopping at the first match.
//
//  2. A per-category default becomes a fully-specified rule at a priority
//     below every other, rather than a fallback consulted when nothing
//     matched. There is no fallback concept here, and there does not need to
//     be: a rule that always matches and is always overlaid is the same
//     thing. It also removes the need for a "nothing matched" signal, which
//     a table that clears fields on a miss would otherwise want.
//
//  3. Rules at EQUAL priority apply in slice order, so the LAST one wins —
//     the opposite of a table scanned top-down for the first hit. Reverse
//     them, or better, give them distinct descending priorities so the order
//     is stated rather than inherited from a line number.
//
// One difference has no mechanical fix and has to be read: [Match.Unit] tests
// the description's unit as it stands when the rule runs, which a lower-
// priority rule may already have set. A table whose unit criterion tests the
// datapoint's wire unit is asking a different question, and the two agree
// only while nothing has rewritten the unit.
type Rules []Rule

var _ model.Enricher = Rules(nil)

// SuppressKey is the description Extra key a suppressing rule sets. The
// enricher cannot remove an entity by itself — it only sees one at a time — so
// it marks it and [Suppressed] collects the marks.
const SuppressKey = "hamqtt:suppress"

// Enrich implements [model.Enricher], applying every matching rule in priority
// order.
func (r Rules) Enrich(dev *model.Device, e model.Entity) error {
	// Indexed rather than ranged by value: a Rule is 256 bytes, and a rule set
	// is walked once per entity per device.
	matching := make([]*Rule, 0, 4)
	for i := range r {
		if r[i].Match.matches(dev, e) {
			matching = append(matching, &r[i])
		}
	}
	// A stable sort keeps slice order meaningful within one priority, so a
	// hand-written rule set reads top to bottom where it does not care.
	sort.SliceStable(matching, func(i, j int) bool {
		return matching[i].Priority < matching[j].Priority
	})

	desc := e.Desc()
	for _, rule := range matching {
		rule.Set.applyTo(desc)
	}
	return nil
}

// Suppressed reports whether a rule marked this entity for removal.
func Suppressed(e model.Entity) bool {
	desc := e.Desc()
	if desc == nil || desc.Extra == nil {
		return false
	}
	v, ok := desc.Extra[SuppressKey].(bool)
	return ok && v
}

// Filter removes entities a rule suppressed and strips the marker from the
// survivors, so it never reaches a payload.
func Filter(entities []model.Entity) []model.Entity {
	out := make([]model.Entity, 0, len(entities))
	for _, e := range entities {
		if Suppressed(e) {
			continue
		}
		if desc := e.Desc(); desc != nil && desc.Extra != nil {
			delete(desc.Extra, SuppressKey)
			if len(desc.Extra) == 0 {
				desc.Extra = nil
			}
		}
		out = append(out, e)
	}
	return out
}

func (m Match) matches(dev *model.Device, e model.Entity) bool {
	if len(m.Platforms) > 0 && !containsPlatform(m.Platforms, e.Platform()) {
		return false
	}
	if len(m.Keys) > 0 && !containsString(m.Keys, e.Key()) {
		return false
	}
	if m.KeyContains != nil && !containsFold(e.Key(), *m.KeyContains) {
		return false
	}
	if m.NameContains != nil {
		desc := e.Desc()
		if desc == nil || !containsFold(desc.Name.Default, *m.NameContains) {
			return false
		}
	}
	if len(m.Categories) > 0 {
		cat, ok := e.(Categorised)
		if !ok || !containsFoldString(m.Categories, cat.Category()) {
			return false
		}
	}
	if len(m.Models) > 0 && !hasModelPrefix(m.Models, dev) {
		return false
	}
	if m.Unit != nil {
		desc := e.Desc()
		if desc == nil || desc.Unit != *m.Unit {
			return false
		}
	}
	if len(m.Leaves) > 0 || len(m.Buckets) > 0 || m.Postfix != nil {
		slot, ok := stateSlot(e)
		if !ok {
			return false
		}
		if len(m.Leaves) > 0 && !containsFoldString(m.Leaves, slot.Leaf()) {
			return false
		}
		if m.Postfix != nil && !hasPostfix(slot.Leaf(), *m.Postfix) {
			return false
		}
		if len(m.Buckets) > 0 && !containsBucket(m.Buckets, slot.Bucket) {
			return false
		}
	}
	return true
}

// stateSlot returns the slot a rule matches against: the state binding if
// there is one, otherwise the first binding. A composite entity has no plain
// state role, and matching it on nothing at all would be worse than matching
// it on its first constituent.
func stateSlot(e model.Entity) (model.Slot, bool) {
	if b, ok := model.Bind(e, model.RoleState); ok {
		return b.Slot, true
	}
	binds := e.Bindings()
	if len(binds) == 0 {
		return model.Slot{}, false
	}
	return binds[0].Slot, true
}

func (o Overlay) applyTo(d *model.Description) {
	if d == nil {
		return
	}
	if o.Name != nil {
		d.Name = *o.Name
	}
	if o.NameKey != nil {
		d.NameKey = *o.NameKey
	}
	if o.DeviceClass != nil {
		d.DeviceClass = *o.DeviceClass
	}
	if o.StateClass != nil {
		d.StateClass = *o.StateClass
	}
	if o.Unit != nil {
		d.Unit = *o.Unit
	}
	if o.Icon != nil {
		d.Icon = *o.Icon
	}
	if o.Category != nil {
		d.Category = *o.Category
	}
	if o.Enabled != nil {
		d.Enabled = o.Enabled
	}
	if o.Precision != nil {
		d.Precision = o.Precision
	}
	if o.Min != nil {
		d.Min = o.Min
	}
	if o.Max != nil {
		d.Max = o.Max
	}
	if o.Step != nil {
		d.Step = o.Step
	}
	if o.Options != nil {
		d.Options = o.Options
	}
	if o.Multiplier != nil {
		d.Multiplier = o.Multiplier
	}
	if o.Suppress != nil {
		if d.Extra == nil {
			d.Extra = map[string]any{}
		}
		d.Extra[SuppressKey] = *o.Suppress
	}
	for k, v := range o.Extra {
		if d.Extra == nil {
			d.Extra = map[string]any{}
		}
		d.Extra[k] = v
	}
}

// Static is a table of entities per device model, and an [model.EntitySource].
//
// The entities are cloned on every call: an EntitySource is asked once per
// device, and handing out the same pointers would let an enricher run for one
// device mutate the description another device is still using.
type Static struct {
	// ByModel maps a device model to its entity templates. The empty key is
	// the fallback for a model with no entry of its own.
	ByModel map[string][]model.Basic
}

var _ model.EntitySource = (*Static)(nil)

// Entities implements [model.EntitySource].
func (s *Static) Entities(_ context.Context, dev *model.Device) ([]model.Entity, error) {
	if s == nil || dev == nil {
		return nil, nil
	}
	templates, ok := s.ByModel[dev.Model]
	if !ok {
		templates = s.ByModel[""]
	}

	out := make([]model.Entity, 0, len(templates))
	for i := range templates {
		clone := templates[i]
		if d := templates[i].Description.Clone(); d != nil {
			clone.Description = *d
		}
		clone.Binds = append([]model.Binding(nil), templates[i].Binds...)
		// Templates carry slots without an address: the table describes a
		// model, not an instance. Binding them to this device is what turns a
		// template into an entity.
		for j := range clone.Binds {
			if clone.Binds[j].Slot.Address == "" {
				clone.Binds[j].Slot.Address = dev.UID()
			}
		}
		out = append(out, &clone)
	}
	return out, nil
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func containsFoldString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if strings.EqualFold(s, needle) {
			return true
		}
	}
	return false
}

// hasPostfix reports whether leaf's trailing underscore-separated segment is
// want. The leading underscore is optional in the rule, because a table
// author writes "_2" as often as "2" and neither reading is surprising.
func hasPostfix(leaf, want string) bool {
	want = strings.TrimPrefix(want, "_")
	if want == "" {
		return false
	}
	i := strings.LastIndexByte(leaf, '_')
	if i < 0 {
		return false
	}
	return strings.EqualFold(leaf[i+1:], want)
}

func containsFold(haystack, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(needle))
}

func containsPlatform(haystack []hacatalog.Platform, needle hacatalog.Platform) bool {
	for _, p := range haystack {
		if p == needle {
			return true
		}
	}
	return false
}

func containsBucket(haystack []model.Bucket, needle model.Bucket) bool {
	for _, b := range haystack {
		if b == needle {
			return true
		}
	}
	return false
}

func hasModelPrefix(prefixes []string, dev *model.Device) bool {
	if dev == nil {
		return false
	}
	lower := strings.ToLower(dev.Model)
	for _, p := range prefixes {
		if strings.HasPrefix(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}
