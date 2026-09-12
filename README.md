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
| `publisher` | the publish loop: hash-dedup, the retraction ordering, the orphan sweep, birth/LWT |
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

## License

MIT.
