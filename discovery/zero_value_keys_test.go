// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// The defect class this file guards.
//
// Home Assistant's MQTT discovery schemas are `extra=REMOVE_EXTRA`. A key a
// platform does not declare is dropped during schema validation: nothing on
// the wire says so, nothing in any log says so, and the entity comes up
// looking right. [TestFieldsStructsCarryOnlyLegalKeys] catches a struct that
// *names* such a key. This file catches the other half — a struct that names
// a legal key but serialises it onto a platform that never asked, because the
// field carries no `omitempty` and Go writes its zero value out.
//
// Measured cause: go-unifi2mqtt's own hand-rolled payload builder declares
//
//	StateTopic string `json:"state_topic"`      // internal/hass/discovery.go
//	Optimistic bool   `json:"optimistic"`       // internal/hass/control.go
//
// so every one of its six `button` configs carries `"state_topic": ""` and
// `"optimistic": false`, neither of which `button` declares — 18 refusals
// across 315 configs when that fleet was run through [ValidateBody]. That is a
// consumer struct, not this one; these tests exist so this one never grows the
// same shape.

// TestNoComponentKeySerialisesAZeroValue walks every discovery key this
// package can put on the wire and requires that an unset field emits nothing.
//
// The two ways to satisfy it are the two the generator already applies: a
// string or slice with `omitempty`, or a pointer for a scalar — see
// [discovery.Ptr] — so that a *deliberate* zero survives while an unset field
// stays absent. Both halves matter: dropping `omitempty` leaks a zero onto a
// platform that does not declare the key, and using a bare `bool`/`int`
// *with* `omitempty` makes a deliberate `false`/`0` inexpressible.
func TestNoComponentKeySerialisesAZeroValue(t *testing.T) {
	t.Parallel()

	// The keys that are Required on their enclosing object, where an absent
	// key is the defect and the empty value is the diagnostic. Each is a
	// `vol.Required` marker in `homeassistant/components/mqtt/schemas.py` or
	// `discovery.py`, so omitting it fails Home Assistant's validation just
	// as loudly as an empty one — and `omitempty` would hide the mistake
	// rather than fix it.
	required := map[string]map[string]string{
		"AvailabilityEntry": {"topic": "MQTT_AVAILABILITY_SCHEMA: vol.Required(CONF_TOPIC): valid_subscribe_topic"},
		"Origin":            {"name": "MQTT_ORIGIN_INFO_SCHEMA: vol.Required(CONF_NAME): cv.string"},
		"Bundle": {
			"device":     "DEVICE_DISCOVERY_SCHEMA: vol.Required(CONF_DEVICE)",
			"origin":     "DEVICE_DISCOVERY_SCHEMA: vol.Required(CONF_ORIGIN)",
			"components": "DEVICE_DISCOVERY_SCHEMA: vol.Required(CONF_COMPONENTS)",
		},
	}

	types := map[string]reflect.Type{
		"Component":         reflect.TypeOf(discovery.Component{}),
		"DeviceInfo":        reflect.TypeOf(discovery.DeviceInfo{}),
		"AvailabilityEntry": reflect.TypeOf(discovery.AvailabilityEntry{}),
		"Origin":            reflect.TypeOf(discovery.Origin{}),
		"Bundle":            reflect.TypeOf(discovery.Bundle{}),
	}
	for platform, zero := range discovery.FieldsIndex {
		types[platform] = reflect.TypeOf(zero)
	}

	for owner, typ := range types {
		for i := range typ.NumField() {
			field := typ.Field(i)
			tag := field.Tag.Get("json")
			name, opts, _ := strings.Cut(tag, ",")
			if name == "" || name == "-" {
				continue
			}
			if _, ok := required[owner][name]; ok {
				continue
			}
			if !strings.Contains(","+opts+",", ",omitempty,") {
				t.Errorf("%s.%s (%q) has no omitempty: an unset field would put its zero value on the wire, "+
					"and Home Assistant drops a key the platform does not declare in silence",
					owner, field.Name, name)
			}
			switch field.Type.Kind() {
			case reflect.Bool, reflect.Int, reflect.Int64, reflect.Float64:
				t.Errorf("%s.%s (%q) is a bare %s with omitempty: a deliberate zero is inexpressible — "+
					"use a pointer, as discovery.Ptr does",
					owner, field.Name, name, field.Type.Kind())
			default:
			}
		}
	}
}

// pressButton is the shape the measurement refused: a `button` whose consumer
// both binds a readable datapoint and asks for `optimistic: false` — the two
// worst cases for the keys in question, on the platform that declares neither.
type pressButton struct {
	model.Basic
	methods []string
}

func (p *pressButton) Methods() []string { return p.methods }

// TestButtonRendersNeitherStateTopicNorOptimistic renders that shape through
// this package's own path and looks at the bytes.
//
// Both keys are gated twice over: [RenderComponent] projects `state_topic`
// and `optimistic` only where the catalog says the platform accepts them, and
// [Component] carries both with `omitempty` behind that. The test pins both
// gates — dropping either one alone still leaves the payload clean, so only
// asserting on the encoded bytes proves anything.
func TestButtonRendersNeitherStateTopicNorOptimistic(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	no := false
	e := &pressButton{
		Basic: model.Basic{
			EntityKey:      "restart",
			EntityPlatform: hacatalog.PlatformButton,
			Description: model.Description{
				Name:       model.L("Restart"),
				Optimistic: &no,
			},
			Binds: []model.Binding{
				{Role: model.RoleState, Slot: model.S(dev.UID(), "", model.BucketValues, "RESTART"), Mode: model.Read},
				{Role: model.RoleCommand, Slot: model.S(dev.UID(), "", model.BucketValues, "RESTART"), Mode: model.Write},
			},
		},
		methods: []string{"run"},
	}

	comp, err := discovery.RenderComponent(testContext(), dev, e, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("RenderComponent: %v", err)
	}
	raw, err := comp.EntityJSON()
	if err != nil {
		t.Fatalf("EntityJSON: %v", err)
	}
	body := map[string]any{}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	for _, key := range []string{"state_topic", "optimistic", "value_template"} {
		if v, present := body[key]; present {
			t.Errorf("%q on a button: %#v — platform button declares no such key, "+
				"so Home Assistant drops it with nothing on the wire to show why (bytes: %s)",
				key, v, raw)
		}
	}
	if err := discovery.ValidateBody(hacatalog.PlatformButton, body); err != nil {
		t.Errorf("ValidateBody refused this package's own button: %v\nbytes: %s", err, raw)
	}
}
