// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
)

func payloadOf(t *testing.T, b *discovery.Bundle) map[string]any {
	t.Helper()
	raw, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func componentJSON(t *testing.T, c discovery.Component) string {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestContainPublishesTheHomeconnectDocument is the regression the whole
// release exists for. go-homeconnect2mqtt 0.15.0 withheld every appliance's
// document because Validate refused one component's `state_class` for its
// `device_class: timestamp`. Home Assistant only logs a warning about that
// (sensor/__init__.py:620-645), so containment withholds nothing: every
// component is published, and with StripWarnings the one offending key is
// removed from the one component that carried it — and from nothing else.
func TestContainPublishesTheHomeconnectDocument(t *testing.T) {
	t.Parallel()
	in := homeconnectBundle()
	if err := discovery.Validate(in); !errors.Is(err, discovery.ErrInvalidBundle) {
		t.Fatalf("Validate = %v; the regression this pins is gone", err)
	}

	c := discovery.Contain(in, discovery.ContainOptions{StripWarnings: true})
	if !c.Publishable() || c.Err() != nil {
		t.Fatalf("not publishable: %v", c.Err())
	}
	if len(c.Withheld) != 0 {
		t.Fatalf("withheld %v, want nothing", c.Withheld)
	}
	if want := []discovery.StrippedKey{{Component: "finish", Key: "state_class", Kind: discovery.KindStateClass}}; !slices.Equal(c.Stripped, want) {
		t.Fatalf("stripped %v, want %v", c.Stripped, want)
	}
	if got, want := c.Bundle.Keys(), in.Keys(); !slices.Equal(got, want) {
		t.Fatalf("components %v, want all of %v", got, want)
	}
	for _, key := range in.Keys() {
		if key == "finish" {
			continue
		}
		if a, b := componentJSON(t, c.Bundle.Components[key]), componentJSON(t, in.Components[key]); a != b {
			t.Errorf("%s changed:\n got %s\nwant %s", key, a, b)
		}
	}
	finish := c.Bundle.Components["finish"]
	if strings.Contains(componentJSON(t, finish), "state_class") {
		t.Errorf("finish still carries state_class: %s", componentJSON(t, finish))
	}
	if finish.UniqueID != "hc_dishwasher_1_finish" || finish.DeviceClass != "timestamp" {
		t.Errorf("the rebuilt component lost its typed fields: %+v", finish)
	}
	if got := discovery.Inspect(c.Bundle, discovery.InspectOptions{}); len(got) != 0 {
		t.Errorf("the contained document still has findings:\n%s", dumpFindings(got))
	}
	if in.Components["finish"].StateClass == "" {
		t.Error("Contain modified its input")
	}
}

// TestContainWithholdsOneRefusedComponent is the same document with a
// component Home Assistant really refuses (a timestamp sensor with a unit
// raises on every state, sensor/__init__.py:612-618): that one is withheld
// and reported, the rest are published unchanged.
func TestContainWithholdsOneRefusedComponent(t *testing.T) {
	t.Parallel()
	in := homeconnectBundle()
	set(in, "finish", func(c *discovery.Component) { c.StateClass = ""; c.UnitOfMeasure = "s" })

	c := discovery.Contain(in, discovery.ContainOptions{})
	if !c.Publishable() {
		t.Fatalf("not publishable: %v", c.Err())
	}
	if _, ok := c.Withheld["finish"]; !ok || len(c.Withheld) != 1 {
		t.Fatalf("withheld %v, want finish alone", c.Withheld)
	}
	if _, ok := c.Bundle.Withheld["finish"]; !ok {
		t.Error("the contained bundle does not record what it withheld")
	}
	comps := payloadOf(t, c.Bundle)["components"].(map[string]any)
	if _, present := comps["finish"]; present {
		t.Errorf("finish is in the payload: %v", comps["finish"])
	}
	if len(comps) != len(in.Components)-1 {
		t.Errorf("payload has %d components, want %d", len(comps), len(in.Components)-1)
	}
	if _, still := in.Components["finish"]; !still {
		t.Error("Contain modified its input")
	}
}

// TestContainCuresADocumentScopeFinding: a missing unique_id makes Home
// Assistant refuse the whole document (mqtt/schemas.py:199-204), but it is
// caused by one component, so withholding that component makes the rest
// acceptable again.
func TestContainCuresADocumentScopeFinding(t *testing.T) {
	t.Parallel()
	in := inspectBundle()
	set(in, "door", func(c *discovery.Component) { c.UniqueID = "" })
	c := discovery.Contain(in, discovery.ContainOptions{})
	if !c.Publishable() {
		t.Fatalf("not publishable: %v", c.Err())
	}
	if _, ok := c.Withheld["door"]; !ok {
		t.Fatalf("door not withheld: %v", c.Withheld)
	}
	if got := discovery.Inspect(c.Bundle, discovery.InspectOptions{}); len(got.Errors()) != 0 {
		t.Errorf("the contained document still has errors:\n%s", dumpFindings(got))
	}
}

// TestContainRefusesOnlyForTheDocumentsOwnKeys: an error on the device
// block cannot be cured by withholding anything.
func TestContainRefusesOnlyForTheDocumentsOwnKeys(t *testing.T) {
	t.Parallel()
	in := inspectBundle()
	in.Device.Identifiers = nil
	c := discovery.Contain(in, discovery.ContainOptions{})
	if c.Publishable() {
		t.Fatal("publishable without a device identity")
	}
	err := c.Err()
	if !errors.Is(err, discovery.ErrUnpublishable) || !strings.Contains(err.Error(), "identifier") {
		t.Fatalf("Err = %v", err)
	}

	in = inspectBundle()
	for _, key := range in.Keys() {
		set(in, key, func(c *discovery.Component) { c.UniqueID = "" })
	}
	c = discovery.Contain(in, discovery.ContainOptions{})
	if c.Publishable() || !errors.Is(c.Err(), discovery.ErrUnpublishable) ||
		!strings.Contains(c.Err().Error(), "every component was withheld") {
		t.Fatalf("all withheld: publishable=%v err=%v", c.Publishable(), c.Err())
	}

	if c := discovery.Contain(nil, discovery.ContainOptions{}); c.Publishable() || !errors.Is(c.Err(), discovery.ErrUnpublishable) {
		t.Fatal("a nil bundle is publishable")
	}
}

// TestContainLeavesWarningsAloneUnlessAsked: stripping is opt-in.
func TestContainLeavesWarningsAloneUnlessAsked(t *testing.T) {
	t.Parallel()
	in := homeconnectBundle()
	c := discovery.Contain(in, discovery.ContainOptions{})
	if len(c.Stripped) != 0 || c.Bundle.Components["finish"].StateClass == "" {
		t.Fatalf("stripped without being asked: %v", c.Stripped)
	}
	if c.Bundle.Withheld != nil || c.Withheld != nil {
		t.Errorf("Withheld set with nothing withheld: %v", c.Bundle.Withheld)
	}
}

// TestContainStripsUnknownAndAvailabilityKeys covers the two other
// strippable kinds, a key in Extra and one inside an availability entry,
// and that the rest of the component survives the rebuild byte for byte.
func TestContainStripsUnknownAndAvailabilityKeys(t *testing.T) {
	t.Parallel()
	in := inspectBundle()
	set(in, "door", func(c *discovery.Component) {
		c.Extra = map[string]any{
			"translation_key": "door",
			"availability": []any{map[string]any{
				"topic": "hc/connected", "value_template": "{{ value }}", "availability_template": "x",
			}},
		}
		c.NameNull = true
	})
	c := discovery.Contain(in, discovery.ContainOptions{StripWarnings: true})
	if len(c.Stripped) != 2 {
		t.Fatalf("stripped %v, want 2 keys", c.Stripped)
	}
	got := componentJSON(t, c.Bundle.Components["door"])
	for _, gone := range []string{"translation_key", "availability_template"} {
		if strings.Contains(got, gone) {
			t.Errorf("%s survived: %s", gone, got)
		}
	}
	for _, kept := range []string{`"name":null`, `"unique_id":"hc_dishwasher_1_door"`, `"topic":"hc/connected"`, `"value_template":"{{ value }}"`} {
		if !strings.Contains(got, kept) {
			t.Errorf("%s lost: %s", kept, got)
		}
	}
}

// TestContainingTwiceKeepsWhatWasWithheld: a consumer that contains a
// contained bundle again must not forget the first pass's withheld set.
func TestContainingTwiceKeepsWhatWasWithheld(t *testing.T) {
	t.Parallel()
	in := inspectBundle()
	set(in, "door", func(c *discovery.Component) { c.EntityCategory = hacatalog.EntityCategoryConfig })
	first := discovery.Contain(in, discovery.ContainOptions{})
	second := discovery.Contain(first.Bundle, discovery.ContainOptions{})
	if _, ok := second.Bundle.Withheld["door"]; !ok {
		t.Fatalf("second pass forgot door: %v", second.Bundle.Withheld)
	}
}

// TestAContainedComponentIsNeverTombstoned is the trap containment must not
// set. A tombstone — a platform-only entry — makes Home Assistant delete the
// entity together with its registry entry, and the natural removal diff
// ("in the last document, not in this one") would produce one for every
// withheld component. Driven across successive publishes, in the order a
// consumer naturally writes, and then with the component really leaving.
func TestAContainedComponentIsNeverTombstoned(t *testing.T) {
	t.Parallel()
	gone := func(prev map[string]discovery.Component, now *discovery.Bundle) []string {
		var out []string
		for k := range prev {
			if _, ok := now.Components[k]; !ok {
				out = append(out, k)
			}
		}
		slices.Sort(out)
		return out
	}
	broken := func() *discovery.Bundle {
		b := inspectBundle()
		set(b, "door", func(c *discovery.Component) { c.EntityCategory = hacatalog.EntityCategoryConfig })
		return b
	}

	// Publish 1: everything valid.
	c1 := discovery.Contain(inspectBundle(), discovery.ContainOptions{})
	prev := c1.Bundle.KeepSet()

	// Publishes 2 and 3: door breaks. Contain first, then the diff against
	// what was published — the order that would tombstone door if anything
	// let it.
	for publish := 2; publish <= 3; publish++ {
		c := discovery.Contain(broken(), discovery.ContainOptions{})
		b := c.Bundle
		b.RemoveComponents(prev, gone(prev, b)...)
		b.Remove(map[string]hacatalog.Platform{"door": hacatalog.PlatformBinarySensor}, "door")

		comps := payloadOf(t, b)["components"].(map[string]any)
		if entry, present := comps["door"]; present {
			t.Fatalf("publish %d: door is in the payload as %v — a platform-only entry deletes the entity", publish, entry)
		}
		if _, tomb := b.Tombstones["door"]; tomb {
			t.Fatalf("publish %d: door was tombstoned", publish)
		}
		prev = b.KeepSet()
		if _, kept := prev["door"]; !kept {
			t.Fatalf("publish %d: KeepSet dropped the withheld door", publish)
		}
	}

	// Publish 4: the other order — tombstone the raw render, then contain.
	raw := broken()
	raw.RemoveComponents(prev, gone(prev, raw)...)
	c4 := discovery.Contain(raw, discovery.ContainOptions{})
	if _, tomb := c4.Bundle.Tombstones["door"]; tomb {
		t.Fatal("publish 4: door was tombstoned")
	}
	prev = c4.Bundle.KeepSet()

	// Publish 5: door really leaves the catalogue. Because KeepSet carried
	// it, the diff sees it go and the tombstone is written — the withheld
	// entity does not linger.
	left := inspectBundle()
	delete(left.Components, "door")
	left.RemoveComponents(prev, gone(prev, left)...)
	c5 := discovery.Contain(left, discovery.ContainOptions{})
	comps := payloadOf(t, c5.Bundle)["components"].(map[string]any)
	entry, ok := comps["door"].(map[string]any)
	if !ok || len(entry) != 1 || entry["platform"] != "binary_sensor" {
		t.Fatalf("publish 5: door = %v, want a platform-only tombstone", comps["door"])
	}
	if c5.Bundle.Tombstones["door"].UniqueID != "hc_dishwasher_1_door" {
		t.Errorf("publish 5: tombstone lost the identity: %+v", c5.Bundle.Tombstones["door"])
	}
}
