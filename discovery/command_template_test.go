// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// notifyCommandTemplate is the constant the measured notify plane sets from a
// [discovery.Builder] written for this one key.
const notifyCommandTemplate = `{{ value | tojson }}`

// TestTheDescriptionProjectsACommandTemplate. The measured need: the
// per-datapoint plane and the notify plane each open a Builder for
// `command_template` alone — a key 16 platforms declare, which is the bar the
// other description-level keys already meet.
func TestTheDescriptionProjectsACommandTemplate(t *testing.T) {
	t.Parallel()

	body := nameNullBody(t, nameNullEntity(hacatalog.PlatformNotify,
		model.Description{CommandTemplate: notifyCommandTemplate}))

	if got := body["command_template"]; got != notifyCommandTemplate {
		t.Errorf("command_template = %#v, want %q", got, notifyCommandTemplate)
	}
}

// TestACommandTemplateIsNotProjectedOntoAPlatformWithoutTheKey. Home
// Assistant's schemas are extra=REMOVE_EXTRA, so a key emitted on the wrong
// platform is dropped with no error on the wire and no log line. climate,
// cover, light and water_heater spell a template per role instead, and only a
// Builder filling their Fields struct can name those.
func TestACommandTemplateIsNotProjectedOntoAPlatformWithoutTheKey(t *testing.T) {
	t.Parallel()

	for _, platform := range []hacatalog.Platform{
		hacatalog.PlatformClimate,
		hacatalog.PlatformCover,
		hacatalog.PlatformLight,
		hacatalog.PlatformSensor,
	} {
		body := nameNullBody(t, nameNullEntity(platform,
			model.Description{CommandTemplate: notifyCommandTemplate}))
		if got, present := body["command_template"]; present {
			t.Errorf("%s: published command_template=%#v although its schema declares none",
				platform, got)
		}
	}
}

// TestABuilderStillWinsOverTheProjectedCommandTemplate. The pipeline has one
// precedence rule — Description, default projection, Builder, Extra, later
// winning — and projecting this key must not take the escape hatch away from
// a plane that computes its template from the Context.
func TestABuilderStillWinsOverTheProjectedCommandTemplate(t *testing.T) {
	t.Parallel()

	e := &commandTemplateBuilder{Basic: *nameNullEntity(hacatalog.PlatformSelect,
		model.Description{CommandTemplate: notifyCommandTemplate})}
	body := nameNullBody(t, e)

	if got := body["command_template"]; got != `{{ value | upper }}` {
		t.Errorf("command_template = %#v, want the builder's own template", got)
	}
}

// commandTemplateBuilder is an entity whose builder overwrites the projected
// template, as a plane computing one from topics would.
type commandTemplateBuilder struct {
	model.Basic
}

func (commandTemplateBuilder) BuildDiscovery(_ discovery.Context, comp *discovery.Component) error {
	comp.CommandTemplate = `{{ value | upper }}`
	return nil
}
