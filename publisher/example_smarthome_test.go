// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher_test

import (
	"context"
	"log/slog"
	"os"

	mqtt "github.com/SukramJ/go-mqtt"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/publisher"
	"github.com/SukramJ/go-hamqtt/publisher/gomqtt"
	"github.com/SukramJ/go-hamqtt/topic"
)

// Example_smartHome is the README's mqtt-smarthome section, compiled. Like
// Example_runtime it has no `// Output:`: every call needs a broker.
func Example_smartHome() {
	ctx := context.Background()
	var level slog.LevelVar

	// The layout is the switch: <name>/status/…, <name>/set/…, and
	// <name>/connected as the bridge topic carrying 0/1/2.
	layout, err := topic.NewSmartHome("daikin")
	if err != nil {
		return // an operator-configured name with / + or #
	}
	dctx := discovery.StdContext{
		Layout:    layout,
		Namespace: "daikin",
		Enc:       discovery.StatusObjectEncoding, // {"val","ts","lc"}, read at value_json.val
	}

	client := mqtt.NewTCPClient(mqtt.TCPConfig{BrokerURL: "tcp://broker.local:1883", ClientID: "go-daikin2mqtt"})
	tr := gomqtt.Transport(client)

	run := publisher.New(tr, publisher.Config{Layout: layout}) // the will writes 0 on daikin/connected
	state := publisher.StateFor(run, publisher.StateConfig{Encoding: dctx.Encoding()})
	router := publisher.NewCommandRouter(tr, publisher.CommandConfig{Lifecycle: ctx})
	inst := publisher.NewInstance(tr, publisher.InstanceConfig{
		Layout:      layout,
		Name:        "go-daikin2mqtt",
		Version:     "1.0.0",
		SetLogLevel: publisher.LevelVarSetter(&level),
		Supervised:  func() bool { return os.Getenv("SUPERVISED") == "1" },
		Shutdown:    func() { /* the daemon's own graceful stop: connected 0, exit 0 */ },
	})

	// set: `{"val": …}` and plain values alike, with the §5.3 conversions.
	// A filter is spelled from the name: the item helpers make every segment
	// topic-safe, wildcards included.
	_ = router.HandleSet(layout.Name()+"/"+topic.FunctionSet+"/+/+/power",
		func(_ context.Context, cmd publisher.Command, v publisher.SetValue) {
			on, err := v.Bool()
			_, _ = on, err
		})
	_ = inst.Register(router) // maintenance/set/loglevel and …/restart
	_ = router.Start(ctx)
	go func() { _ = inst.RunStats(ctx) }()

	// On every (re)connect: the level, info, and the cached status objects.
	_ = run.AnnounceOnline(ctx)
	_ = inst.AnnounceInfo(ctx)
	_, _ = state.Republish(ctx)

	// Upstream reachable: entities become available (connected >= 2).
	_, _ = run.SetConnected(ctx, discovery.ConnectedOperational)

	// A value, deduplicated on val; lc moves only when it changes.
	slot := model.S("uuid-1", "climateControl", model.BucketUnset, "room_temperature")
	_, _ = state.PublishStatus(ctx, layout.State(slot), publisher.Observation{Value: 21.5})
	// The device's own reachability is a status item too.
	_, _ = state.PublishStatus(ctx, layout.Availability(slot), publisher.Observation{Value: true})

	// Graceful stop: the explicit 0 the will would otherwise write.
	_ = run.AnnounceOffline(ctx)
}
