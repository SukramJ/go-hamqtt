// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

// Package payload partitions a struct into the three payload kinds a device
// publishes: identity that never changes, capabilities that change rarely, and
// state that changes constantly.
//
// The partition is declared once, on the struct, instead of in three
// hand-written DTOs that drift apart:
//
//	type Device struct {
//		Address      string   `payload:"info,alt=serial_number"`
//		Manufacturer string   `payload:"info"`
//		Modes        []string `payload:"config,alt=operation_modes"`
//		Temperature  float64  `payload:"state"`
//	}
//
//	payload.For(dev, payload.Info) // {"address": …, "manufacturer": …}
//
// The package knows nothing about MQTT or Home Assistant. It is reflection over
// struct tags and nothing else, which is why it sits at the bottom of the
// dependency graph and why a consumer can use it for its own topics without
// adopting anything else in this module.
package payload

import (
	"reflect"
	"strings"
	"sync"
)

// Kind is one of the three payload partitions.
type Kind uint8

const (
	// Info is identity: what the thing is. Stable for the device's lifetime.
	Info Kind = iota + 1
	// Config is capability: what it can do — ranges, modes, units. Changes
	// only when the device itself is reconfigured.
	Config
	// State is the live value. Changes constantly.
	State
)

// String returns the tag spelling of the kind.
func (k Kind) String() string {
	switch k {
	case Info:
		return "info"
	case Config:
		return "config"
	case State:
		return "state"
	default:
		return "unknown"
	}
}

// Extra is the escape hatch for fields reflection cannot see: values behind a
// mutex, computed properties, anything not a plain struct field. Whatever it
// returns is merged over the tagged fields, so it can also override one.
type Extra interface {
	ExtraPayload(k Kind) map[string]any
}

// Options tune the harvest.
type Options struct {
	// IncludeZero keeps fields at their zero value. Off by default: a device
	// that reports 40 capabilities of which 3 are set should publish 3 keys,
	// not 37 nulls. Turn it on where the absence of a key is itself
	// meaningful — a consumer diffing two payloads, say.
	IncludeZero bool
}

// For harvests the fields of obj tagged with kind, keyed by their tag name.
//
// obj may be a struct or a pointer to one; a nil pointer yields an empty map
// rather than panicking, because a partially built device is a normal
// intermediate state, not a programming error.
func For(obj any, k Kind) map[string]any {
	return ForWith(obj, k, Options{})
}

// ForWith is [For] with explicit options.
func ForWith(obj any, k Kind, opts Options) map[string]any {
	out := map[string]any{}
	if obj == nil {
		return out
	}

	v := reflect.ValueOf(obj)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return out
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return out
	}

	for _, f := range fieldsOf(v.Type(), k) {
		fv := v.FieldByIndex(f.index)
		if !opts.IncludeZero && fv.IsZero() {
			continue
		}
		out[f.name] = fv.Interface()
	}

	if extra, ok := obj.(Extra); ok {
		for key, value := range extra.ExtraPayload(k) {
			out[key] = value
		}
	}
	return out
}

// Merge folds src into dst, src winning. It exists so a caller can compose a
// payload from several sources without writing the loop each time; the
// precedence is fixed and one-directional on purpose.
func Merge(dst, src map[string]any) map[string]any {
	if dst == nil {
		dst = make(map[string]any, len(src))
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// ---------------------------------------------------------------------------
// reflection cache
// ---------------------------------------------------------------------------

type field struct {
	index []int
	name  string
}

// cache is keyed by (type, kind). Reflection over a struct's tags is pure and
// its result immutable, so it is computed once per type and reused; a bridge
// publishing at device speed would otherwise re-walk the same tags forever.
var cache sync.Map // map[cacheKey][]field

type cacheKey struct {
	typ  reflect.Type
	kind Kind
}

func fieldsOf(t reflect.Type, k Kind) []field {
	key := cacheKey{typ: t, kind: k}
	if cached, ok := cache.Load(key); ok {
		fields, _ := cached.([]field)
		return fields
	}
	fields := collect(t, k, nil)
	cache.Store(key, fields)
	return fields
}

func collect(t reflect.Type, k Kind, prefix []int) []field {
	var out []field
	want := k.String()

	for i := range t.NumField() {
		sf := t.Field(i)
		index := append(append([]int(nil), prefix...), i)

		// An embedded struct with no tag of its own contributes its fields to
		// the outer payload, which is what embedding means everywhere else in
		// Go. An embedded struct that *is* tagged is treated as a value.
		if sf.Anonymous && sf.Tag.Get("payload") == "" {
			et := sf.Type
			for et.Kind() == reflect.Pointer {
				et = et.Elem()
			}
			if et.Kind() == reflect.Struct {
				out = append(out, collect(et, k, index)...)
				continue
			}
		}

		if !sf.IsExported() {
			continue
		}
		tag := sf.Tag.Get("payload")
		if tag == "" || tag == "-" {
			continue
		}

		kinds, alt := parseTag(tag)
		if !containsString(kinds, want) {
			continue
		}
		name := alt
		if name == "" {
			name = snake(sf.Name)
		}
		out = append(out, field{index: index, name: name})
	}
	return out
}

// parseTag splits `payload:"info,config,alt=serial_number"` into its kinds and
// its rename. Several kinds on one field are allowed and normal: a device's
// name belongs in both info and state payloads.
func parseTag(tag string) (kinds []string, alt string) {
	for part := range strings.SplitSeq(tag, ",") {
		part = strings.TrimSpace(part)
		switch {
		case part == "":
		case strings.HasPrefix(part, "alt="):
			alt = strings.TrimPrefix(part, "alt=")
		default:
			kinds = append(kinds, part)
		}
	}
	return kinds, alt
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// snake converts a Go field name to the snake_case a payload key uses.
// Consecutive capitals are treated as one initialism, so SWVersion becomes
// sw_version rather than s_w_version.
func snake(name string) string {
	var b strings.Builder
	b.Grow(len(name) + 4)
	runes := []rune(name)
	for i, r := range runes {
		if isUpper(r) {
			prevLower := i > 0 && !isUpper(runes[i-1])
			nextLower := i+1 < len(runes) && !isUpper(runes[i+1])
			if i > 0 && (prevLower || nextLower) {
				b.WriteByte('_')
			}
			b.WriteRune(r - 'A' + 'a')
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }
