// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Package model is the semantic layer every consumer talks to: what a device
// is, what its datapoints are, and what they mean. It knows nothing about MQTT
// and only enough about Home Assistant to name its vocabularies.
//
// The split is deliberate. A bridge describes its devices in these types once;
// the discovery package turns that description into Home Assistant's wire
// format, and the runtime turns it into traffic. Neither of those concepts
// leaks in here, which is what lets a consumer keep a non-Home-Assistant MQTT
// surface on the same model.
//
// Capability interfaces are the extension mechanism throughout: not
// implementing one is the opt-out, so a new capability is additive by
// construction. See [Commander], [Suppressor], [Deriver], [OriginPolicy],
// [EntitySource] and [Enricher].
package model

import (
	"sort"
	"strings"
)

// Identifier is one namespaced name for a device. The namespace exists because
// the six consuming projects identify devices six different ways — a serial, a
// vendor API id, a MAC, an appliance haId — and a bare string would make two
// devices from different bridges collide in Home Assistant's registry the
// moment their identifiers happened to match.
type Identifier struct {
	// Namespace is the kind of identifier: "serial", "unifi:mac",
	// "homeconnect:haId". Conventionally the bridge name, a colon, and the
	// vendor's own term.
	Namespace string
	// Value is the identifier itself.
	Value string
}

// String renders the identifier as Home Assistant sees it in `identifiers`.
//
// An empty namespace renders the value alone, and that is the escape hatch
// rather than an edge case. Home Assistant keys its *device* registry on these
// strings, and it has no migration path for them any more than it has one for
// a unique id: change a device's identifier and the old device stays behind
// with its area, its name override and its place in the hierarchy, while the
// entities move to a new one. A consumer whose devices are already published
// under its own spelling therefore has to be able to keep it verbatim, which a
// hard-coded separator would make impossible.
//
// New consumers should use the namespace. It is what stops two bridges'
// devices from colliding in the registry the moment their identifiers happen
// to match.
func (id Identifier) String() string {
	if id.Namespace == "" {
		return id.Value
	}
	return id.Namespace + ":" + id.Value
}

// IsZero reports whether the identifier names nothing. An identifier with no
// value is not an identifier: Home Assistant would register the device under
// the empty string, where it collides with every other such device.
func (id Identifier) IsZero() bool { return id.Value == "" }

// Connection is a network-level identity Home Assistant can match against
// other integrations — the `connections` block. Type is Home Assistant's own
// vocabulary ("mac", "bluetooth"), Value the address.
type Connection struct {
	Type  string
	Value string
}

// Identity is everything that says which device this is.
//
// The list is ordered and the first entry is primary: it yields [Identity.UID]
// and, through it, the discovery node id. Later entries are alternates that
// still identify the same device — a serial alongside a MAC, say — and are what
// makes [Identity.Equal] able to recognise one device registered twice under
// different primary keys.
type Identity struct {
	IDs         []Identifier
	Connections []Connection
}

// UID is the stable key this device is registered under. It is derived from
// the primary identifier only, so adding an alternate identifier later does
// not rename an already-published device.
//
// An Identity with no identifiers has no UID; callers get an empty string and
// [Identity.Valid] reports false.
func (id Identity) UID() string {
	if len(id.IDs) == 0 || id.IDs[0].IsZero() {
		return ""
	}
	return id.IDs[0].String()
}

// Valid reports whether the identity can address a device at all. Home
// Assistant refuses a device block with no identifier and no connection, so
// this is the same condition, checked before the payload is built rather than
// after the broker has it.
func (id Identity) Valid() bool {
	for _, i := range id.IDs {
		if !i.IsZero() {
			return true
		}
	}
	return len(id.Connections) > 0
}

// Equal reports whether two identities describe the same device: any shared
// identifier is enough.
//
// This is what resolves the shared-hardware case. Two indoor air-conditioning
// units each report the outdoor unit they are attached to; both register it,
// and without this the registry would hold it twice with two node ids and two
// sets of entities. Connections are deliberately *not* compared — a MAC can be
// reassigned, and matching on one would merge two genuinely different devices
// after a DHCP reservation moved.
func (id Identity) Equal(other Identity) bool {
	for _, a := range id.IDs {
		for _, b := range other.IDs {
			if a == b {
				return true
			}
		}
	}
	return false
}

// Merge folds other into id, keeping id's primary identifier. Alternates and
// connections are unioned and deduplicated.
//
// The primary is kept rather than recomputed because it is already published:
// changing it would orphan every entity underneath.
func (id Identity) Merge(other Identity) Identity {
	out := Identity{
		IDs:         append([]Identifier(nil), id.IDs...),
		Connections: append([]Connection(nil), id.Connections...),
	}
	seen := make(map[Identifier]struct{}, len(out.IDs))
	for _, i := range out.IDs {
		seen[i] = struct{}{}
	}
	for _, i := range other.IDs {
		if _, dup := seen[i]; !dup {
			seen[i] = struct{}{}
			out.IDs = append(out.IDs, i)
		}
	}
	conns := make(map[Connection]struct{}, len(out.Connections))
	for _, c := range out.Connections {
		conns[c] = struct{}{}
	}
	for _, c := range other.Connections {
		if _, dup := conns[c]; !dup {
			conns[c] = struct{}{}
			out.Connections = append(out.Connections, c)
		}
	}
	return out
}

// Device is a physical or logical thing that owns entities.
//
// Sub-devices are ordinary Devices with Via set: a battery pack under a
// storage system, an outdoor unit under an indoor one, a switch under a
// gateway. Home Assistant renders that as a device hierarchy; nothing here is
// special-cased for it.
type Device struct {
	Identity Identity

	Name          Localized
	Manufacturer  string `payload:"info"`
	Model         string `payload:"info"`
	ModelID       string `payload:"info,alt=model_id"`
	SWVersion     string `payload:"info,alt=sw_version"`
	HWVersion     string `payload:"info,alt=hw_version"`
	SerialNumber  string `payload:"info,alt=serial_number"`
	SuggestedArea string `payload:"config,alt=suggested_area"`
	ConfigURL     string `payload:"info,alt=configuration_url"`

	// Via names the parent device in the hierarchy. Nil for a root device.
	Via *Identity

	// Extra carries keys the model does not model. It is an escape hatch, not
	// a design surface: a key that every consumer sets belongs in a field.
	Extra map[string]any
}

// UID is shorthand for the device's identity key.
func (d *Device) UID() string {
	if d == nil {
		return ""
	}
	return d.Identity.UID()
}

// Localized is a display string with translations. It is data rather than a
// method so a catalog entry can carry it, a code generator can emit it, and a
// YAML file can declare it — none of which can carry a func.
type Localized struct {
	// Default is used when no translation matches. Conventionally English.
	Default string
	// Lang maps a language tag to its translation: "de" -> "Leistung".
	Lang map[string]string
}

// L is shorthand for a Localized with only a default.
func L(s string) Localized { return Localized{Default: s} }

// In returns the translation for lang, falling back to Default. An empty or
// unknown lang yields Default, so a consumer that does not localise at all can
// ignore the concept entirely.
func (l Localized) In(lang string) string {
	if lang != "" && l.Lang != nil {
		if v, ok := l.Lang[lang]; ok && v != "" {
			return v
		}
		// Tags are compared case- and space-insensitively: a catalog written
		// by hand says "de", a config file may well say "DE".
		if norm := normalizeLang(lang); norm != lang {
			if v, ok := l.Lang[norm]; ok && v != "" {
				return v
			}
		}
	}
	return l.Default
}

// IsZero reports whether the value carries no text at all.
func (l Localized) IsZero() bool { return l.Default == "" && len(l.Lang) == 0 }

// Enum is a closed set of values with display labels — a select's options, or
// an enum sensor's states.
//
// Codes is ordered and that order is what Home Assistant shows, so it is the
// declaration order rather than a sorted one: a mode list reads "off, heat,
// cool", not "cool, heat, off".
type Enum struct {
	Codes  []string
	Labels map[string]Localized
}

// Label returns the display text for code in lang, falling back to the code
// itself. An unlabelled code is normal — most enums need no translation.
func (e *Enum) Label(code, lang string) string {
	if e == nil {
		return code
	}
	if l, ok := e.Labels[code]; ok {
		if s := l.In(lang); s != "" {
			return s
		}
	}
	return code
}

// Options returns the labels in Codes order, which is the form Home Assistant
// wants for a select's `options`.
func (e *Enum) Options(lang string) []string {
	if e == nil {
		return nil
	}
	out := make([]string, 0, len(e.Codes))
	for _, c := range e.Codes {
		out = append(out, e.Label(c, lang))
	}
	return out
}

// Code is the reverse lookup: given a label a user or Home Assistant sent
// back, return the code the device understands.
//
// It matches the code itself first, then every language's label — not just the
// configured one. Home Assistant echoes back whatever string it was given, and
// which language that was depends on when the entity was discovered, not on
// what the bridge is configured for now.
func (e *Enum) Code(label string) (string, bool) {
	if e == nil {
		return "", false
	}
	for _, c := range e.Codes {
		if c == label {
			return c, true
		}
	}
	for _, c := range e.Codes {
		l, ok := e.Labels[c]
		if !ok {
			continue
		}
		if l.Default == label {
			return c, true
		}
		for _, translated := range l.Lang {
			if translated == label {
				return c, true
			}
		}
	}
	return "", false
}

// Languages lists every language tag any label declares, sorted. It exists so
// a consumer can tell which locales its catalog actually covers.
func (e *Enum) Languages() []string {
	if e == nil {
		return nil
	}
	seen := map[string]struct{}{}
	for _, l := range e.Labels {
		for lang := range l.Lang {
			seen[lang] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for lang := range seen {
		out = append(out, lang)
	}
	sort.Strings(out)
	return out
}

// normalizeLang trims and lowercases a language tag so "DE" and "de " both
// find a "de" translation.
func normalizeLang(lang string) string {
	return strings.ToLower(strings.TrimSpace(lang))
}
