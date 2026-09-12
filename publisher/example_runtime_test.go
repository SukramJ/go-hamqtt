// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher_test

import (
	"context"
	"log/slog"

	hacatalog "github.com/SukramJ/go-ha-catalog"
	mqtt "github.com/SukramJ/go-mqtt"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/publisher"
	"github.com/SukramJ/go-hamqtt/publisher/gomqtt"
	"github.com/SukramJ/go-hamqtt/topic"
)

// Example_runtime is the README's runtime section, compiled.
//
// It has no `// Output:` comment on purpose: every call in it needs a broker,
// so `go test` builds it and does not run it. That is the point — the README
// snippet it mirrors is the one place a reader copies from verbatim, and a
// signature that has moved should break the build here rather than in the
// consumer that copied it.
//
// The shape is the composition root every consumer in the family has, and the
// order of the four sections is the measured one. Read the comments as the
// reasons, not the narration: each marks a step whose omission is a defect
// that shipped.
func Example_runtime() {
	ctx := context.Background()

	// The layout is the consumer's topic schema, and it is named ONCE.
	// Everything downstream that has to agree about a topic — the discovery
	// context that writes `state_topic` into every config, the state plane
	// that publishes there, the availability plane, the bridge status topic
	// and the will — derives it from this value. Two layouts in one process,
	// or an operator-configured root read twice, is how an entity ends up
	// permanently `unknown` with a valid config and a valid value on the
	// broker and nothing on the wire naming the mismatch.
	layout := topic.Default{Root: "daikin"}

	dctx := discovery.StdContext{
		Layout:    layout,
		Namespace: "daikin", // a constant of the bridge, never a configurable
		Lang:      "en",
	}

	// ── transport ──────────────────────────────────────────────────────────
	//
	// The runtime never dials. The consumer owns the client, the reconnect
	// loop and the breaker, which is what lets it publish through the breaker
	// and subscribe around it: breaking the subscribe path would only delay
	// resubscription after a reconnect without preventing anything.
	client := mqtt.NewTCPClient(mqtt.TCPConfig{
		BrokerURL: "tcp://broker.local:1883",
		ClientID:  "go-daikin2mqtt",
	})
	breaker := mqtt.NewBreaker(client, mqtt.BreakerConfig{})

	run := publisher.New(gomqtt.Split(breaker, client), publisher.Config{
		// The same string the layout wrote into every config's bridge-level
		// availability entry. Anything else and the will is inert: the broker
		// dutifully writes `offline` on a hard crash and every entity stays
		// available forever, showing the last value it ever saw. Two
		// reference bridges shipped exactly that.
		StatusTopic: layout.Bridge(),
	})
	defer run.Close()

	// The will belongs to CONNECT, so it is returned as data rather than
	// applied. Handing it over as a value is what makes the two halves agree
	// by construction — same topic, same payloads as AnnounceOnline.
	will, err := run.Will()
	if err != nil {
		slog.Error("no status topic", slog.String("err", err.Error()))
		return
	}
	client = mqtt.NewTCPClient(mqtt.TCPConfig{
		BrokerURL: "tcp://broker.local:1883",
		ClientID:  "go-daikin2mqtt",
		Will: &mqtt.Will{
			Topic:   will.Topic,
			Payload: will.Payload,
			QoS:     mqtt.QoS(will.QoS),
			Retain:  will.Retain,
		},
	})

	// ── the three publishing planes ────────────────────────────────────────
	//
	// Separate types because they own different trees and must be able to
	// fail apart. The runtime writes under the discovery prefix, which is
	// Home Assistant's; the other two write in the consumer's own tree.
	state := publisher.StateFor(run, publisher.StateConfig{
		Encoding: dctx.Encoding(), // so the templates read the shape published
	})
	avail := publisher.NewAvailability(gomqtt.Transport(client), publisher.AvailabilityConfig{
		// The same layout again: the availability topic is the one string
		// that has to be identical on both sides of a discovery config.
		Layout: layout,
	})
	router := publisher.NewCommandRouter(gomqtt.Transport(client), publisher.CommandConfig{
		// Process-lifetime, NOT Start's ctx. In the measured consumer Start
		// is reachable from a config reload, whose context ends when the
		// reload returns; handlers derived from it had their downstream
		// writes cancelled the moment the reload finished.
		Lifecycle: ctx,
	})
	// One wildcard, not one filter per writable datapoint. Handlers run on a
	// router worker rather than the transport's read loop, because a command
	// is by definition a write to something outside this process and a
	// handler that answers by publishing would wait on an acknowledgement
	// only the goroutine it is occupying could deliver.
	if err := router.Handle(layout.Root+"/+/+/+/set",
		func(_ context.Context, cmd publisher.Command) {
			_ = cmd.Wildcards // the coordinate, rather than topic arithmetic
		}); err != nil {
		return
	}

	// ── one device ─────────────────────────────────────────────────────────
	dev := &model.Device{
		Identity: model.Identity{IDs: []model.Identifier{{Namespace: "serial", Value: "AC-1"}}},
		Name:     model.L("Living room AC"),
		Model:    "FTXM35",
	}
	power := &model.Basic{
		EntityKey:      "power",
		EntityPlatform: hacatalog.PlatformSensor,
		Description: model.Description{
			Name:        model.L("Power"),
			DeviceClass: model.DeviceClass(hacatalog.SensorDeviceClassPower),
			StateClass:  hacatalog.StateClassMeasurement,
			Unit:        "W",
		},
		Binds: []model.Binding{{
			Role: model.RoleState,
			Slot: model.S(dev.UID(), "", model.BucketValues, "power"),
			Mode: model.Read,
		}},
	}
	entities := []model.Entity{power}

	bundle, err := discovery.Render(dctx, dev, entities,
		discovery.Origin{Name: "go-daikin2mqtt", SW: "1.0.0"})
	if err != nil {
		return
	}
	// Not optional. Home Assistant discards a malformed discovery config in
	// silence, so an invalid payload is indistinguishable from a bridge that
	// never spoke.
	if err := discovery.Validate(bundle); err != nil {
		return
	}

	// ── boot, in this order ────────────────────────────────────────────────
	//
	// Availability before discovery, so no entity is briefly claimed
	// available on stale data. Discovery before state, so the topics a value
	// lands on are already described. The sweep LAST — see below.
	if err := run.AnnounceOnline(ctx); err != nil {
		return
	}
	// The device level is what greys an entity out when its device goes
	// off-bus. The coordinate comes from DeviceSlot rather than the device
	// identity, because the slot inherits the scope and channel of what the
	// entity binds; rebuilding it by hand yields the device root and writes a
	// topic no config names.
	if _, err := avail.Device(ctx, publisher.DeviceSlot(dev, power), true); err != nil {
		return
	}
	if _, err := run.PublishBundle(ctx, bundle); err != nil {
		return
	}
	b, _ := model.Bind(power, model.RoleState)
	if _, err := state.PublishValue(ctx, layout.State(b.Slot), 420, true); err != nil {
		return
	}

	if err := router.Start(ctx); err != nil {
		return
	}
	// Run every topic this process publishes past the router once, at boot,
	// and fail the boot. A broker fans a client's own publishes back to it,
	// so a topic this process writes that a command filter matches is the
	// process commanding itself on every report — which in the measured case
	// ran a program on every boot, every rediscovery and every program in the
	// house, with the programs running as the only signal.
	readable, err := publisher.BundleStateTopics(bundle)
	if err != nil {
		return
	}
	if err := router.CheckDisjoint(append(readable, layout.Bridge())...); err != nil {
		slog.Error("a topic this process publishes is routed back into its own handlers",
			slog.String("err", err.Error()))
		return
	}

	// Replay every config when Home Assistant comes back. Home Assistant
	// keeps the retained configs across its own restart but does not reliably
	// re-read them across every addon reload, and the birth message is the one
	// deterministic signal that it is ready again.
	if err := run.WatchBirth(ctx); err != nil {
		return
	}

	// AFTER the boot snapshot, never before it. The sweep compares against
	// what this process has declared, so a pass that runs while a plane has
	// not published yet judges that plane's entire fleet to be orphans and
	// deletes it — once per boot, with nothing left to re-declare it.
	//
	// Owns is required and has no default: a discovery prefix is shared, and
	// a sweep that judged everything it saw would clear every entity of every
	// other integration on the broker.
	if _, err := run.Sweep(ctx, publisher.SweepRequest{
		Owns: func(t publisher.ConfigTopic) bool {
			return t.NodeID == bundle.NodeID
		},
	}); err != nil {
		return
	}

	// ── reconnect ──────────────────────────────────────────────────────────
	//
	// Start's ctx governs the whole loop, not just the first connect.
	lc := mqtt.NewLifecycle(mqtt.DefaultLifecycle(), client)
	lc.OnConnect(func(ctx context.Context) {
		// All three planes, and they are not interchangeable. The discovery
		// and state planes replay their caches; the availability plane's gate
		// has to be CLEARED first, because it still believes every device is
		// online and would otherwise suppress the republish — after which
		// every entity sits unavailable until the daemon restarts.
		_ = run.AnnounceOnline(ctx)
		_, _ = run.Republish(ctx)
		avail.Reset()
		_, _ = avail.Device(ctx, publisher.DeviceSlot(dev, power), true)
		_, _ = state.Republish(ctx)
		_ = router.Resubscribe(ctx)
	})
	_ = lc.Start(ctx)

	// ── removing a device ──────────────────────────────────────────────────
	//
	// The config first: it is what removes the entity. Clearing availability
	// first only makes the entity unavailable for the moment in between,
	// which an operator watching a restart reads as a fault. And all three
	// trees, because a retained `online` or a retained value behind a
	// retracted config is a ghost Home Assistant reads on every restart.
	_ = run.Retract(ctx, bundle.Topic(run.Prefix()))
	_ = avail.RetractDevice(ctx, publisher.DeviceSlot(dev, power))
	_, _ = state.EvictPrefix(ctx, layout.State(model.Slot{Address: dev.UID()}))

	// Stop drains: no handler starts after it is entered, but one already
	// accepted runs to completion, because abandoning a queued write is how a
	// consumer loses the last command of a session with nothing to say so.
	_ = router.Stop(ctx)
}
