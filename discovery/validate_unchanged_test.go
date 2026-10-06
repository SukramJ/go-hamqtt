// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

// equivalenceCorpus is every bundle this package's tests judge, plus a
// combinatorial sweep over the keys the validator's rules read.
//
// It is the input half of TestValidateIsUnchanged. Six consumers call
// Validate and some withhold a whole document on any error, so a stricter
// answer there — one more issue, a reworded message, a warning promoted —
// would newly withhold documents in production the moment a consumer bumps
// this module. Everything v0.37.0 adds is therefore new API, and this corpus
// is how that promise is checked rather than asserted.
func equivalenceCorpus() map[string]*discovery.Bundle {
	out := map[string]*discovery.Bundle{
		"validBundle":       validBundle(),
		"parityBundle":      parityBundle(),
		"inspectBundle":     inspectBundle(),
		"homeconnectBundle": homeconnectBundle(),
		"nil components":    {NodeID: "x", Origin: discovery.Origin{Name: "o"}},
		"slash node id":     {NodeID: "a/b c", Device: discovery.DeviceInfo{Connections: [][2]string{{"mac", "aa"}}}},
	}
	for _, tc := range inspectCases {
		b := inspectBundle()
		tc.edit(b)
		out["inspect: "+tc.name] = b
	}
	// The mutations of validate_test.go, validate_ignoring_test.go and
	// availability_check_test.go, applied to their own base bundle.
	for name, edit := range map[string]func(b *discovery.Bundle){
		"no origin":    func(b *discovery.Bundle) { b.Origin.Name = "" },
		"no identity":  func(b *discovery.Bundle) { b.Device.Identifiers = nil },
		"no unique id": func(b *discovery.Bundle) { setV(b, "power", func(c *discovery.Component) { c.UniqueID = "" }) },
		"duplicate":    func(b *discovery.Bundle) { b.Components["power2"] = b.Components["power"] },
		"cross-platform id": func(b *discovery.Bundle) {
			c := b.Components["power"]
			c.Platform = hacatalog.PlatformNumber
			b.Components["n"] = c
		},
		"bad platform": func(b *discovery.Bundle) { setV(b, "power", func(c *discovery.Component) { c.Platform = "sensr" }) },
		"extra key": func(b *discovery.Bundle) {
			setV(b, "power", func(c *discovery.Component) { c.Extra = map[string]any{"object_id": "p"} })
		},
		"bad device class": func(b *discovery.Bundle) { setV(b, "power", func(c *discovery.Component) { c.DeviceClass = "garage" }) },
		"bad state class": func(b *discovery.Bundle) {
			setV(b, "power", func(c *discovery.Component) { c.StateClass = hacatalog.StateClassTotalIncreasing })
		},
		"enum rules": func(b *discovery.Bundle) {
			setV(b, "power", func(c *discovery.Component) { c.Options = []string{"a"}; c.UnitOfMeasure = "W" })
		},
		"micro sign": func(b *discovery.Bundle) {
			setV(b, "power", func(c *discovery.Component) { c.DeviceClass = "pm25"; c.UnitOfMeasure = "µg/m³" })
		},
		"removal": func(b *discovery.Bundle) {
			b.Components["gone"] = discovery.Component{Platform: hacatalog.PlatformSensor}
		},
		"avail no topic": func(b *discovery.Bundle) {
			setV(b, "power", func(c *discovery.Component) { c.Availability = []discovery.AvailabilityEntry{{}} })
		},
		"select": func(b *discovery.Bundle) {
			b.Components["mode"] = discovery.Component{Platform: hacatalog.PlatformSelect, UniqueID: "m", CommandTopic: "c", Options: []string{"a"}}
		},
		"light variant": func(b *discovery.Bundle) {
			b.Components["l"] = discovery.Component{Platform: hacatalog.PlatformLight, UniqueID: "l", CommandTopic: "c", Extra: map[string]any{"schema": "json", "brightness": true}}
		},
		"light no schema": func(b *discovery.Bundle) {
			b.Components["l"] = discovery.Component{Platform: hacatalog.PlatformLight, UniqueID: "l", CommandTopic: "c"}
		},
		"non-finite": func(b *discovery.Bundle) {
			b.Components["n"] = discovery.Component{Platform: hacatalog.PlatformNumber, UniqueID: "n", CommandTopic: "c", Max: new(posInf())}
		},
	} {
		b := validBundle()
		edit(b)
		out["validate: "+name] = b
	}

	// The combinatorial sweep: one sensor, every combination of the keys
	// the sensor rules read, on top of the clean inspectBundle.
	n := 0
	for _, dc := range []string{"", "power", "energy", "timestamp", "enum", "garage"} {
		for _, sc := range []hacatalog.StateClass{"", "measurement", "total", "total_increasing", "measurement_angle"} {
			for _, unit := range []string{"", "W", "kWh", "µg/m³", "s", "°"} {
				for _, opts := range [][]string{nil, {"a", "b"}} {
					for _, cat := range []hacatalog.EntityCategory{"", "config", "diagnostic"} {
						b := inspectBundle()
						b.Components["x"] = discovery.Component{
							Platform: hacatalog.PlatformSensor, UniqueID: "x", StateTopic: "s",
							DeviceClass: dc, StateClass: sc, UnitOfMeasure: unit, Options: opts, EntityCategory: cat,
						}
						out[fmt.Sprintf("sweep %d", n)] = b
						n++
					}
				}
			}
		}
	}
	for _, bounds := range [][3]*float64{
		{nil, nil, nil}, {new(5.0), new(1.0), nil}, {new(150.0), nil, nil}, {nil, nil, new(0.0001)}, {nil, new(-1.0), new(0.5)},
	} {
		b := inspectBundle()
		setV(b, "delay", func(c *discovery.Component) { c.Min, c.Max, c.Step = bounds[0], bounds[1], bounds[2] })
		out[fmt.Sprintf("sweep %d", n)] = b
		n++
	}
	return out
}

func setV(b *discovery.Bundle, key string, edit func(c *discovery.Component)) {
	c := b.Components[key]
	edit(&c)
	b.Components[key] = c
}

func posInf() float64 { var zero float64; return 1 / (zero + 0) }

// sameResult compares two validator results on everything a caller can
// observe: nil-ness, the sentinel each matches, the message, and the
// structured issue and warning lists.
func sameResult(t *testing.T, name string, got, want error) {
	t.Helper()
	if (got == nil) != (want == nil) {
		t.Errorf("%s: got %v, want %v", name, got, want)
		return
	}
	if got == nil {
		return
	}
	if got.Error() != want.Error() {
		t.Errorf("%s: message changed\n got: %s\nwant: %s", name, got, want)
	}
	for _, sentinel := range []error{discovery.ErrInvalidBundle, discovery.ErrAdvisory} {
		if errors.Is(got, sentinel) != errors.Is(want, sentinel) {
			t.Errorf("%s: errors.Is(%v) = %v, want %v", name, sentinel, errors.Is(got, sentinel), errors.Is(want, sentinel))
		}
	}
	var g, w *discovery.ValidationError
	if errors.As(got, &g) != errors.As(want, &w) {
		t.Errorf("%s: *ValidationError-ness differs", name)
		return
	}
	if g != nil && !reflect.DeepEqual(g, w) {
		t.Errorf("%s: ValidationError differs\n got: %+v\nwant: %+v", name, g, w)
	}
}

// TestValidateIsUnchanged runs every bundle of the corpus — and every
// component of it as a per-entity body — through the current Validate,
// ValidateIgnoring, ValidateBody and ValidateBodyIgnoring and through their
// frozen v0.36.0 implementation, and requires identical results. See
// validate_v036_test.go.
func TestValidateIsUnchanged(t *testing.T) {
	t.Parallel()
	corpus := equivalenceCorpus()
	if len(corpus) < 1000 {
		t.Fatalf("corpus shrank to %d bundles", len(corpus))
	}
	corpus["nil"] = nil
	ignores := []map[string]bool{nil, {"translation_key": true}, {"object_id": true, "availability": true}}
	for name, b := range corpus {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sameResult(t, "Validate", discovery.Validate(b), v036ValidateIgnoring(b, nil))
			for _, ignore := range ignores {
				sameResult(t, fmt.Sprintf("ValidateIgnoring %v", ignore),
					discovery.ValidateIgnoring(b, ignore), v036ValidateIgnoring(b, ignore))
			}
			if b == nil {
				return
			}
			for _, key := range b.Keys() {
				body, err := v036componentBody(b.Components[key])
				if err != nil {
					continue
				}
				platform := hacatalog.Platform(v036str(body, "platform"))
				delete(body, "platform")
				sameResult(t, key, discovery.ValidateBody(platform, body), v036ValidateBody(platform, body))
				for _, ignore := range ignores {
					sameResult(t, fmt.Sprintf("%s ignoring %v", key, ignore),
						discovery.ValidateBodyIgnoring(platform, body, ignore), v036ValidateBodyIgnoring(platform, body, ignore))
				}
			}
		})
	}
}

// TestInspectNeverLosesABlockingComponentOfValidate: the new API is allowed
// to judge differently, but not to fall silent. Every component that
// Validate reports an issue for has at least one Inspect finding, except
// for the reclassifications Inspect documents: a unique_id on a non-entity
// platform and a state class on a non-sensor platform.
func TestInspectNeverLosesABlockingComponentOfValidate(t *testing.T) {
	t.Parallel()
	for name, b := range equivalenceCorpus() {
		var verr *discovery.ValidationError
		if !errors.As(discovery.Validate(b), &verr) {
			continue
		}
		findings := discovery.Inspect(b, discovery.InspectOptions{})
		for _, key := range b.Keys() {
			// check_unique_id exempts these two (mqtt/schemas.py:202).
			if p := b.Components[key].Platform; p == hacatalog.PlatformTag || p == hacatalog.PlatformDeviceAutomation {
				continue
			}
			flagged := false
			for _, issue := range verr.Issues {
				if len(issue) > len(key)+1 && issue[:len(key)+1] == key+":" {
					flagged = true
				}
			}
			if flagged && len(findings.ForComponent(key)) == 0 {
				t.Errorf("%s: Validate flags %q, Inspect says nothing", name, key)
			}
		}
	}
}
