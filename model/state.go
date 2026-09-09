// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package model

import "time"

// Origin says where a value came from. It matters because several consumers
// read the same datapoint from more than one place — a cloud API and a local
// socket, an integration API and a legacy one — and which reading wins is a
// policy decision, not an accident of arrival order.
//
// The empty Origin means unspecified, which is correct for the majority of
// bridges that have exactly one source.
type Origin string

// State is one datapoint reading.
//
// Availability rides along with the value rather than in a separate topic
// because that is what makes per-datapoint availability expressible at all: a
// device can be reachable while one of its values is stale or unsupported, and
// Home Assistant can only see that if the state payload says so.
type State struct {
	// Value is the reading. Typed as any because the model does not constrain
	// the device's own types; the encoding layer decides how it reaches the
	// wire.
	Value any
	// Available says whether the value can be trusted right now.
	Available bool
	// Origin identifies the source, for [OriginPolicy].
	Origin Origin
	// ModifiedAt is when the value last changed.
	ModifiedAt time.Time
	// RefreshedAt is when it was last confirmed, changed or not. The two
	// differ for a value that is polled often and changes rarely, and Home
	// Assistant's "last updated" means the latter.
	RefreshedAt time.Time
	// Extra carries additional information alongside the value.
	Extra map[string]any
}

// IsZero reports whether the state carries nothing at all.
func (s State) IsZero() bool {
	return s.Value == nil && !s.Available && s.Origin == "" &&
		s.ModifiedAt.IsZero() && s.RefreshedAt.IsZero() && len(s.Extra) == 0
}

// OriginPolicy decides whether an incoming reading replaces the current one.
//
// Not implementing it — the nil policy — means last write wins, which is what
// a single-source bridge wants and what it gets without saying anything.
type OriginPolicy interface {
	// Accept reports whether next should replace cur.
	Accept(cur, next State) bool
}

// OriginPolicyFunc adapts a function to [OriginPolicy].
type OriginPolicyFunc func(cur, next State) bool

// Accept implements [OriginPolicy].
func (f OriginPolicyFunc) Accept(cur, next State) bool { return f(cur, next) }

// Precedence returns a policy that ranks origins, highest first.
//
//	model.Precedence("local", "cloud")
//
// A reading from a higher-ranked origin always wins. A reading from a
// lower-ranked one is accepted only when the current value is unavailable or
// came from an origin at most as high — so a local reading is never overwritten
// by a stale cloud poll, but a local source going away does not freeze the
// entity forever.
//
// An origin not in the list ranks below every listed one. That is deliberate:
// an unexpected source should not silently outrank a configured one.
func Precedence(highestFirst ...Origin) OriginPolicy {
	rank := make(map[Origin]int, len(highestFirst))
	for i, o := range highestFirst {
		rank[o] = len(highestFirst) - i
	}
	return OriginPolicyFunc(func(cur, next State) bool {
		if !cur.Available {
			return true
		}
		return rank[next.Origin] >= rank[cur.Origin]
	})
}

// Accept applies a policy, treating a nil policy as last-write-wins.
func Accept(p OriginPolicy, cur, next State) bool {
	if p == nil {
		return true
	}
	return p.Accept(cur, next)
}
