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
	if len(m.Models) > 0 && !hasModelPrefix(m.Models, dev) {
		return false
	}
	if m.Unit != nil {
		desc := e.Desc()
		if desc == nil || desc.Unit != *m.Unit {
			return false
		}
	}
	if len(m.Leaves) > 0 || len(m.Buckets) > 0 {
		slot, ok := stateSlot(e)
		if !ok {
			return false
		}
		if len(m.Leaves) > 0 && !containsFoldString(m.Leaves, slot.Leaf()) {
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
