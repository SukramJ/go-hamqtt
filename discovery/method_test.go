// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package discovery_test

import (
	"testing"

	hacatalog "github.com/SukramJ/go-ha-catalog"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
)

// runButton is the shape openccu-loom calls a CCU program's execute role: a
// button whose instruction runs the program rather than writing any of the
// parameters running it happens to touch.
type runButton struct {
	model.Basic
	methods []string
}

func (r *runButton) Methods() []string { return r.methods }

func newRunButton(methods ...string) *runButton {
	return &runButton{
		Basic: model.Basic{
			EntityKey:      "execute",
			EntityPlatform: hacatalog.PlatformButton,
			Description:    model.Description{Name: model.L("Execute")},
		},
		methods: methods,
	}
}

// TestOneMethodBecomesTheCommandTopic. Before methods could be declared, such
// an entity rendered with no command_topic at all — which button requires, so
// the payload was invalid, and Home Assistant would have shown a button that
// does nothing.
func TestOneMethodBecomesTheCommandTopic(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := newRunButton("run")

	bundle, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	comp := bundle.Components["execute"]
	want := "daikin/" + dev.UID() + "/custom/execute/set/run"
	if comp.CommandTopic != want {
		t.Errorf("command_topic = %q, want %q", comp.CommandTopic, want)
	}
	if err := discovery.Validate(bundle); err != nil {
		t.Errorf("a button with a method still failed validation: %v", err)
	}
}

// TestSeveralMethodsAreLeftToTheEntity: Home Assistant names a key per action
// on the platforms that have them, so picking one here would silently make
// the rest unreachable. Those entities fill their own Fields.
func TestSeveralMethodsAreLeftToTheEntity(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := newRunButton("run", "cancel")

	bundle, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got := bundle.Components["execute"].CommandTopic; got != "" {
		t.Errorf("command_topic = %q, want none — the entity has to choose", got)
	}
}

// TestAWritableBindingStillWins: a method is the fallback for an entity that
// has no datapoint to write, never an override of one that has.
func TestAWritableBindingStillWins(t *testing.T) {
	t.Parallel()

	dev := testDevice()
	e := newRunButton("run")
	e.Binds = []model.Binding{{
		Role: model.RoleCommand,
		Slot: model.S(dev.UID(), "", model.BucketValues, "PRESS"),
		Mode: model.Write,
	}}

	bundle, err := discovery.Render(testContext(), dev, []model.Entity{e}, discovery.Origin{Name: "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	want := "daikin/" + dev.UID() + "/values/PRESS/set"
	if got := bundle.Components["execute"].CommandTopic; got != want {
		t.Errorf("command_topic = %q, want the binding's %q", got, want)
	}
}
