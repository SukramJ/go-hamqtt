# go-hamqtt

The shared data model, Home Assistant MQTT discovery layer and publisher
runtime for the `go-*2mqtt` family: what a device is, what its datapoints
mean, how that becomes a discovery bundle Home Assistant accepts — and how it
gets onto a broker and off it again.

Zero third-party dependencies. Sits on
[go-mqtt](https://github.com/SukramJ/go-mqtt) for transport and
[go-ha-catalog](https://github.com/SukramJ/go-ha-catalog) for Home Assistant's
vocabulary.

Recorded as [ADR 0070](https://github.com/SukramJ/openccu-loom/blob/main/docs/adr/0070-shared-ha-discovery-model-module.md).

> **Status: v0.x.** The API will move while openccu-loom migrates onto it —
> that migration is the design's proof, and finding a wrong signature there is
> the point. Pin an exact version.

## What is here

| Package | |
| --- | --- |
| `model` | `Device`, `Identity`, `Entity`, `Slot`, `Binding`, `State`, `Description` — and the capability interfaces |
| `discovery` | the device-based bundle, the render pipeline, and the validator |
| `topic` | the only place that turns a coordinate into a string |
| `payload` | `payload:"info\|config\|state"` struct-tag partitioning |
| `catalog` | priority rules as an `Enricher`, a static table as an `EntitySource` |
| `publisher` | the runtime: discovery publishing, state, command routing, availability, orphan sweep, birth/LWT |
| `publisher/gomqtt` | the ten lines that adapt a go-mqtt client to the runtime's transport |

## The shape of it

A bridge describes its devices in `model` types. `discovery` turns that into
Home Assistant's wire format. Neither step knows the other's concerns:

```go
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

ctx := discovery.StdContext{
    Layout:    topic.Default{Root: "daikin"},
    Namespace: "daikin",
}
bundle, err := discovery.Render(ctx, dev, []model.Entity{power},
    discovery.Origin{Name: "go-daikin2mqtt", SW: version})
if err != nil {
    return err
}
if err := discovery.Validate(bundle); err != nil {
    return err // nothing is published for this device
}
// publish bundle at bundle.Topic(prefix), retained
```

## Publishing

`publisher` is the other half: the loop each consumer would otherwise write
again, with the ordering Home Assistant enforces baked in.

```go
run := publisher.New(gomqtt.Split(breaker, client), publisher.Config{
    StatusTopic: layout.Bridge(), // the will's topic, and one entities reference
})
defer run.Close()

will, err := run.Will()          // hand this to the client's CONNECT
…
_ = run.AnnounceOnline(ctx)
_ = run.WatchBirth(ctx)          // replay every config when HA comes back

_, err = run.PublishBundle(ctx, bundle) // retract-then-publish, deduped

// after the boot snapshot, never before it
res, err := run.Sweep(ctx, publisher.SweepRequest{
    Owns: func(t publisher.ConfigTopic) bool {
        return strings.HasPrefix(t.NodeID, myNamespace)
    },
})
```

Four facts it encodes, each measured rather than chosen:

- **Retract first, publish second.** Home Assistant refuses a device bundle
  while a per-entity config for the same `unique_id` is still retained, and
  the refusal is a `WARNING` in its log and nothing else. It is symmetric, so
  the rollback needs the same care.
- **The dedup store survives reconnects** and holds the bytes, not a digest —
  the birth resync replays them.
- **The sweep recognises both topic forms.** Matching only
  `<prefix>/<platform>/<node>/<object>/config` makes every
  `<prefix>/device/<node>/config` invisible, and a broker keeps those forever.
- **A will nobody reads is no will at all.** `Will()` returns the same topic
  and payloads `AnnounceOnline`/`AnnounceOffline` use, so the two halves
  cannot drift apart — which is how two reference bridges ended up with an
  inert LWT.

## The runtime layer

`Runtime` publishes discovery. Three more types publish and read everything
else, and they are separate because they own different trees and must be able
to fail apart: `Runtime` writes under the discovery prefix, which is Home
Assistant's; the rest write in the consumer's own.

| | |
| --- | --- |
| `StatePublisher` | retained entity state, deduped against the last value, with an eviction index and an acknowledgement-latency summary |
| `CommandRouter` | the command topics the configs advertise — subscribe, route, dispatch off the read loop, resubscribe |
| `AvailabilityPublisher` | the device and self availability levels `model.Availability` declares and only the bridge level used to publish |

The composition root wires all four onto one client. `topic.Layout` is named
**once**: everything that has to agree about a topic derives it from that one
value, because the alternative is an entity permanently `unknown` with a valid
config and a valid value on the broker and nothing on the wire naming the
mismatch. `publisher/example_runtime_test.go` is this snippet compiled.

```go
layout := topic.Default{Root: "daikin"}
dctx := discovery.StdContext{Layout: layout, Namespace: "daikin", Lang: "en"}

client := mqtt.NewTCPClient(mqtt.TCPConfig{BrokerURL: url, ClientID: "go-daikin2mqtt"})
breaker := mqtt.NewBreaker(client, mqtt.BreakerConfig{}) // publish through it, subscribe around it

run := publisher.New(gomqtt.Split(breaker, client), publisher.Config{
    StatusTopic: layout.Bridge(), // the will's topic, and one entities reference
})
defer run.Close()

state := publisher.StateFor(run, publisher.StateConfig{Encoding: dctx.Encoding()})
avail := publisher.NewAvailability(gomqtt.Transport(client), publisher.AvailabilityConfig{
    Layout: layout,
})
router := publisher.NewCommandRouter(gomqtt.Transport(client), publisher.CommandConfig{
    Lifecycle: ctx, // process-lifetime, NOT Start's ctx — see the field
})
_ = router.Handle("daikin/+/+/+/set", func(ctx context.Context, cmd publisher.Command) {
    // cmd.Wildcards carries the coordinate; a worker goroutine, not the read loop
})
```

Boot, in this order — availability first so nothing is briefly claimed
available on stale data, discovery before state, the sweep last:

```go
_ = run.AnnounceOnline(ctx)
_, _ = avail.Device(ctx, publisher.DeviceSlot(dev, power), true)
_, _ = run.PublishBundle(ctx, bundle)
_, _ = state.PublishValue(ctx, layout.State(stateSlot), 420, true)
_ = router.Start(ctx)
_ = run.WatchBirth(ctx)

// Run every topic this process publishes past the router once, and fail the
// boot. A broker fans a client's own publishes back to it, so a state topic a
// command filter matches is the process commanding itself on every report.
readable, _ := publisher.BundleStateTopics(bundle)
if err := router.CheckDisjoint(append(readable, layout.Bridge())...); err != nil {
    return err
}

// AFTER the boot snapshot, never before it.
_, _ = run.Sweep(ctx, publisher.SweepRequest{Owns: mine})
```

Reconnect restores all four, and they are not interchangeable:

```go
lc.OnConnect(func(ctx context.Context) {
    _ = run.AnnounceOnline(ctx)
    _, _ = run.Republish(ctx)   // cached bytes: HA does not always re-read retained configs
    avail.Reset()               // the gate still believes every device is online
    _, _ = avail.Device(ctx, publisher.DeviceSlot(dev, power), true)
    _, _ = state.Republish(ctx) // a broker back without a persistent retained store
    _ = router.Resubscribe(ctx)
})
```

Removing a device clears all three trees, config first:

```go
_ = run.Retract(ctx, bundle.Topic(run.Prefix()))
_ = avail.RetractDevice(ctx, publisher.DeviceSlot(dev, power))
_, _ = state.EvictPrefix(ctx, layout.State(model.Slot{Address: dev.UID()}))
```

Five more facts it encodes, each measured:

- **`Reset` before the availability republish, or the fleet is lost.** The
  gate suppresses a flip it thinks the broker already has. A broker back
  without a persistent retained store holds nothing, and every entity then
  sits unavailable until the daemon restarts.
- **The config retraction goes first.** Clearing availability first only makes
  the entity unavailable for the moment in between, which an operator watching
  a restart reads as a fault.
- **A retained `online` outliving its device is a ghost.** Home Assistant
  reads it on every restart and the entity behind it stays permanently
  available, showing the last value it ever saw. Retracting the discovery
  config does not clear it — the two live in different trees.
- **Command handlers do not run on the read loop.** `go-mqtt` delivers inline
  on the goroutine that also decodes PUBACK, so a handler that publishes waits
  for an acknowledgement only that goroutine could deliver. The router owns
  workers for exactly this.
- **State is retained, commands are not, and a retained command re-fires.**
  Home Assistant never publishes a command retained; somebody's
  `mosquitto_pub -r` does, and the broker replays it on every resubscribe.
  `CommandConfig.DeliverRetained` is off by default.

## Checking a live deployment

Two tools read a capture rather than a broker — subscribing needs credentials,
a network path and a transport dependency this module does not have.

```sh
mosquitto_sub -h broker -t '#' -v -W 5 | <to-ndjson> > capture.ndjson
hacheck  < capture.ndjson    # one payload at a time, against the platform schemas
hadoctor < capture.ndjson    # the whole capture at once
```

`hacheck` answers "is this payload legal", plus two wiring questions a single
config can answer: an availability topic inside Home Assistant's own discovery
tree (legal, never published, entity unavailable forever) and a command topic
the same config also tells Home Assistant to read.

`hadoctor` answers the questions that only exist between payloads:

| | |
| --- | --- |
| `form-conflict` | one `unique_id` retained in **both** discovery forms — Home Assistant refuses the second with a `WARNING` and nothing else |
| `command-echo` | a command topic something in the capture also publishes |
| `dead-availability` | an availability topic that carries nothing |
| `dead-state` | a state topic that carries nothing |
| `no-availability` | an entity that declares none at all |
| `orphan-availability` | a retained availability marker no config names — the ghost |

Everything but the last sets exit 1. `orphan-availability` is advisory: a
capture cannot decide ownership, and a shared broker legitimately carries
other integrations' unreferenced markers.

## Design rules worth knowing

**The model never builds a topic.** It addresses datapoints by coordinate and
asks a `discovery.Context` for the string. That one indirection is what lets
six projects with six topic schemas share one model — and it is enforced
structurally: `topic` is a package below `discovery`, and nothing in `model`
imports it.

**One precedence rule.** `Description → enricher chain → default projection →
Builder → Extra`, later winning. There is no "frame wins" versus "projection
wins"; there is only "later wins".

**Capability interfaces are the extension mechanism.** Not implementing one is
the opt-out, so adding a capability is additive by construction — which matters
when six consumers pin the module. `Commander`, `Suppressor`, `Deriver`,
`OriginPolicy`, `EntitySource`, `Enricher`, `discovery.Builder`,
`discovery.Dynamic`.

**Validation is not optional.** An invalid bundle returns an error and *nothing*
is published for that device. Home Assistant discards a malformed discovery
config in silence — no error on the wire, nothing in its log naming the cause —
so an invalid payload is indistinguishable from a bridge that never spoke.

## Composite entities

The hard case, and the one the design is measured against: a `climate` entity
consumes seven datapoints and must hide the sensors and selects a catalog also
produced for them. It needs no special case in the pipeline — three capability
interfaces cover it:

```go
func (c *Climate) Suppresses() []string { return []string{"mode", "fan_mode", "power", …} }

func (c *Climate) BuildDiscovery(ctx discovery.Context, comp *discovery.Component) error {
    comp.Fields = discovery.ClimateFields{
        ModeStateTopic:   ctx.StateTopic(c.mode),   // asks; never formats
        ModeCommandTopic: ctx.CommandTopic(c.mode),
        …
    }
    return nil
}

func (c *Climate) Derive(states map[string]model.State) (model.State, bool) { … }
```

Suppression hides the **entity**, not the **datapoint**: the suppressed slots
keep publishing, because the composite's own discovery points at those topics.

## State encoding

State topics carry a JSON envelope by default —
`{"value": …, "available": …, "modified_at": …}` — and components reference it
with a value template. That is the only shape that carries per-datapoint
availability, which is what `model.LevelSelf` resolves against; the alternative
needs a second topic per datapoint.

`RawEncoding` publishes the bare value instead, for a broker other tools read
directly.

## Testing

```sh
make check        # vet + fmt-check + lint + race tests
make cover-check  # per-package coverage gate (COVER_MIN=80)
```

`publisher/conformance*_test.go` is the layer's own contract: one in-memory
broker with discovery, state, commands and availability driven together,
asserting the invariants no single plane can check alone — that the
`state_topic` a config advertises is the topic the state plane writes, that
the `command_topic` is the one the router subscribes, that state and command
topics stay disjoint fleet-wide, that a reconnect restores every tree, and
that a removal leaves no retained ghost. A mismatch in any of them is invisible
to both planes' own tests and produces an entity that never works, with
nothing on the wire naming the cause.

## License

MIT.
