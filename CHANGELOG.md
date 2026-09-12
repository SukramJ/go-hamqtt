# Changelog

All notable changes to this project are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.23.0] - 2026-09-12

The three measured causes of avoidable escape hatches in the
per-entity form. Measured by migrating five openccu-loom planes onto
`discovery` and counting the hatches each needed: 40 in total, 11 of
them avoidable, clustering on exactly these three.

### Added

- **`Component.EntityJSON`** — the per-entity encoding of a component:
  the same object `MarshalJSON` produces, without `platform`.

- **`DeviceFromInfo`** — the inverse of `NewDeviceInfo`. A consumer
  migrating one plane at a time still harvests its device blocks the
  old way and needs a `model.Device` to render the new way. Two
  measured planes wrote this converter themselves, under different
  names, before it existed.

### Changed

- **`RenderComponent` no longer clears `Platform`** (behavioural
  break). It is the per-entity form's topic segment, so three measured
  consumers set the field again immediately after the call — a hatch
  whose only job was to undo the pipeline. The key still leaves the
  payload, now in `Component.EntityJSON`, where Home Assistant's
  `extra=REMOVE_EXTRA` schemas would otherwise drop it silently.
  A consumer relying on the old behaviour publishes
  `comp.EntityJSON()` instead of marshalling `comp`.

- **A device-level availability slot now carries the channel.** A
  consumer whose availability is per channel rather than per device —
  one measured plane publishes it per alarm zone — could not reach its
  own topic from `LevelDevice` however the slot was filled, short of
  smuggling the segment into `Scope`, which inverts what `Scope`
  means. A `topic.Layout` that does not want it ignores it, as it
  already ignores `Bucket` and `Path`.

## [0.22.0] - 2026-09-12

Everything a measured 147-rule reference table needs that `catalog`
could not state. Measured by resolving 1,860 witness inputs through
both machines and diffing every wire-relevant field.

### Added

- **`Match.Categories`** and the **`Categorised`** interface — a rule
  keyed on the consumer's own classification of a datapoint rather
  than on the platform it renders as. 20 of the 147 rules are keyed
  on `hub_sensor`, `hub_button`, `hub_binary_sensor` or
  `schedule_switch`; `Match.Platforms` is typed to a
  `hacatalog.Platform` and cannot say any of them. Collapsing them to
  the platform first is not a simplification — a rule meant for a
  button-shaped action would then reach every entity that happens to
  render as a button.

- **`Match.Postfix`** — the trailing underscore segment of the leaf.
  A vendor that numbers repeated parameters gives a rule no other way
  to say "the second one": `Leaves` cannot enumerate them and
  `KeyContains("_2")` also matches `_20`.

- **`Match.NameContains`** — a substring test on the display name,
  distinct from `KeyContains`. The two are different strings and the
  table uses both; 20 rules key on the name.

- **`Overlay.Multiplier`** and **`Description.Multiplier`** — the
  scale applied to a datapoint's value before publishing. It belongs
  to the entity rather than to the datapoint (the same raw level is a
  fraction to one entity and a percentage to another) and the rule
  table is where that is written. The model carries it and never
  applies it: only the consumer's value layer knows whether a given
  publish is a value at all.

### Documented

- **How to port a first-match-wins table**, on `Rules`. This is the
  most important part of the release and it is prose rather than
  code, because the hazard is silent: `Rules` applies every matching
  rule and a table that stops at the first is a different machine.
  933 of 1,860 measured inputs match more than one rule, and a
  literal transcription diverges on 18 to 21 per cent of them.

  A faithful port reaches zero divergence under three conditions:
  every ported `Overlay` states every field it has an opinion about
  (including the empty ones, which is what turns stacking into
  whole-record replacement); per-category defaults become
  fully-specified rules below every other priority rather than a
  fallback; and equal-priority rules are reversed, because the last
  one applied wins here and the first one found wins there.

  One difference has no mechanical fix and is called out: `Match.Unit`
  tests the description's unit as it stands when the rule runs, which
  a lower-priority rule may already have set. A table whose unit
  criterion tests the *wire* unit is asking a different question.

## [0.21.0] - 2026-09-11

### Added

- **`ValidateIgnoring` and `ValidateBodyIgnoring`** — validation with a
  set of keys the consumer publishes on purpose and Home Assistant is
  known to drop.

  Measured, not anticipated: the validator was run over one consumer's
  9,996 real retained configs for the first time. It produced 164
  blocking findings and **zero false positives** — and every one of the
  164 was the same key, `translation_key`, which that consumer
  publishes so its cross-stack parity tooling can compare against the
  Python integration it mirrors. Home Assistant declares the key on no
  platform and discards it, so the validator is right.

  The consequence was structural rather than semantic: regrouped into
  device bundles, that one key turned **64 of 398 bundles**
  `Blocking()`. An invalid bundle publishes nothing at all, so a
  consumer gating its publish on the validator would have withheld a
  sixth of its devices.

  The set is a parameter rather than a field on `Bundle`: it is a
  property of the consumer's judgement, not of the document, and the
  same document validated by a tool that did not make that judgement
  should still report the key. `Validate` therefore ignores nothing —
  a caller has to say so deliberately.

  An ignored key is excused from the unknown-key check and nothing
  else. A missing required key is still missing, because an entity
  without it does not work.

## [0.20.0] - 2026-09-11

Phase 3 of openccu-loom's ADR 0070 asks one question: can that daemon's
discovery layer be expressed on this model? All eleven of its discovery
planes were measured, every claimed gap was put through an adversarial
refutation, and the answer is yes — with these four additions. None is a
redesign; the ADR stands.

### Added

- **`Context.NodeID` and `Context.ObjectID`.** All three identity
  strings are now Context methods, for the reason `UniqueID` always
  was: Home Assistant has no migration path for any of them, so a
  consumer with a published fleet owns its spellings — and they need
  not agree with each other. One measured consumer's node id and
  device identifier are deliberately different strings, which made
  deriving both from `Identity.UID` wrong whichever way the identity
  was filled in.

  **An empty `ObjectID` suppresses `default_entity_id` entirely.** A
  consumer whose fleet never carried the key cannot accept one now:
  it seeds the entity id where Home Assistant currently derives its
  own, and Home Assistant does not rename an entity back. Before
  this the only escape was a `Builder` on every entity whose whole
  job was to undo the field the pipeline had just set.

  `StdContext` answers all three from this package's own functions,
  so a consumer with no opinion is unaffected.

- **`RenderComponent` and `NewDeviceInfo`** — the per-entity form as
  a supported output. It renders one entity with the `device` and
  `origin` blocks attached and omits `platform`, which the bundle
  needs as a discriminator and a per-entity topic already states.

  Without it a consumer on that form had to call `Render`, discard
  the bundle, pull the components back out of the map and re-stamp
  the frame — post-processing the pipeline, which is the pattern the
  extraction exists to remove.

- **`Description.ValueTemplate`, with the `model.NoValueTemplate`
  sentinel.** `Context.Encoding` is one answer for a whole consumer,
  and a consumer is rarely uniform: bare datapoint topics,
  composites assembled by a hand-written template, and event
  entities that must carry none. Five of the eleven measured planes
  reported a template added where none is published, or the wrong
  constant. The sentinel exists because an empty string already
  means "no opinion", and "publish none" is a different statement.

### Changed

- **`topic.Layout.Availability` takes a `model.Slot`, not a
  `model.Identity`.** Breaking for anyone implementing `Layout`;
  `Default` is updated.

  An identity carries no scope, and a consumer whose devices sit
  inside containers — a controller and a wire interface, a site —
  publishes availability inside them too. The measurement found one
  such topic **unrenderable by any Layout**: the only item in the
  whole exercise that no escape hatch could reach, because
  `Identity` has no scope field and `model` may not import `topic`
  to acquire one.

- **`Component.Platform` gained `omitempty`**, so the per-entity form
  can drop it. Inside a bundle it is never empty — see below.

- **`Bundle.Remove` skips a key whose platform it does not know**
  instead of writing a component without one. Such an entry marshals
  to `{}`, which Home Assistant ignores, so it was noise in every
  future publish while the entity it was meant to delete stayed on
  screen.

## [0.19.0] - 2026-09-11

### Added

- **`cmd/hadoctor`** — the second tool, and the one that sees what a
  single payload cannot.

  `hacheck` reads one discovery config at a time and asks whether it
  satisfies its platform's schema. It is blind by construction to
  every defect that exists *between* payloads: a topic an entity
  points at that nobody publishes, an availability list no producer
  feeds, an entity that therefore sits unavailable forever with
  nothing anywhere to say why. Those are the defects operators
  actually report.

  Three findings, ordered by how much they hurt:

  - `dead-availability` — the entity names an availability topic the
    capture never saw a payload on. It is unavailable right now and
    nothing says why. Exits 1.
  - `dead-state` — the entity reads from a topic nothing publishes.
    Exits 1.
  - `no-availability` — the entity declares none at all, so it never
    goes unavailable: everything works until the bridge dies, and
    then Home Assistant keeps showing the last value as current.
    Advisory, exits 0, because nothing is broken yet.

  Run against retained configs from a live broker it reports the
  third on a shipped bridge, which is the defect ADR 0070 predicted
  this tool would find.

  A capture holding only `<prefix>/#` is **refused**, not reported:
  every state topic would look unpublished and every entity broken,
  and an operator would act on that. Subscribe to `#`.

  A component inside a device bundle inherits the document's
  availability list, so it is not counted as an entity without one; a
  platform-only tombstone is not counted as an entity at all.

### Changed

- `cmd/hacheck` reads its input through the new `internal/dump`,
  shared with `hadoctor`. No behaviour change.

## [0.18.0] - 2026-09-11

### Added

- **`cmd/hacheck`** — the first of the four tools ADR 0070 calls for:
  validate retained discovery payloads against the platform schemas
  extracted from Home Assistant itself.

  It exists for the one property that makes discovery defects
  expensive. Home Assistant's MQTT schemas are `extra=REMOVE_EXTRA`:
  a key a platform does not declare is dropped on arrival, with no
  error on the wire and no line in any log. The entity works except
  for the one thing that key was for, and nobody notices until
  someone reads the payload and the schema side by side.

  Run against the retained configs of two shipped bridges, it found
  that both publish `object_id` — replaced by `default_entity_id`,
  declared by none of the 32 platforms, and dropped in silence. They
  publish `default_entity_id` too, which is why it went unnoticed:
  the entity id is right, so nothing looks wrong.

  Input is NDJSON (`{"topic": …, "payload": …}`) on stdin, with the
  payload as an object or a string, because a broker dump produces
  one and Home Assistant's own diagnostics the other. Both discovery
  forms are read. A component carrying a platform and nothing else is
  a deletion, not a broken entity, and is not reported.

  Reading a dump rather than a broker is deliberate: subscribing
  needs credentials, a network path and a transport dependency this
  module does not have and should not grow. That is `hadoctor`'s job,
  and it can pipe into this.

  Exit 0 when the only findings are advisories — values Home
  Assistant accepts and rewrites — 1 when a payload carries something
  it would reject or strip, and 2 when the dump itself could not be
  read, because "your input is broken" and "your payloads are" send
  an operator looking in different places.

## [0.17.0] - 2026-09-10

### Added

- **`model.Invoker`** — an entity declares the named actions it
  accepts. 0.13.0 added `Context.MethodTopic` without any way to say a
  method existed, so the topic could be rendered but never advertised,
  and an inbound command on it had nowhere to be routed.

  An entity with exactly one method and no writable binding is the
  common case — a button that runs a program — and the render path
  wires it to `command_topic` on its own. Before this, such an entity
  rendered with no `command_topic` at all, which `Validate` rejects:
  `"command_topic" is required by platform "button"`.

  An entity with several methods is left alone deliberately. Home
  Assistant names a key per action on the platforms that have them
  (`pause_command_topic`, `start_mowing_command_topic`), so choosing
  one here would silently make the rest unreachable. Those entities
  fill their own `Fields` through a `Builder`.

  A writable `RoleCommand` binding still wins. A method is the
  fallback for an entity with no datapoint to write, never an
  override of one that has.

- **`Command.Method`** — the named action an inbound command invoked,
  empty for a command that arrived on a binding's topic. A
  `Commander` tells the two apart by asking rather than by inspecting
  a zero `Slot`.

## [0.16.0] - 2026-09-10

### Fixed

- **`Identifier.String()` forced one spelling on every consumer.** It
  hard-coded `namespace + ":" + value`, so a consumer whose devices
  are already published under its own identifier format could not
  express them without changing them.

  That is the same break the shared `unique_id` turned out to be, one
  layer down and less visible. Home Assistant keys its *device*
  registry on these strings and has no migration path for them: change
  one and the old device stays behind with its area, its name override
  and its place in the hierarchy, while the entities move to a new
  device. The guarantee a consumer needs is not only about entities.

  An empty namespace now renders the value alone, verbatim. A
  namespaced identifier is unchanged, and remains the recommendation
  for a new consumer — it is what stops two bridges' devices from
  colliding in the registry.

- **An empty identifier no longer names a device.** `Identity.Valid()`
  counted `Identifier{}` as an identifier and `UID()` rendered it as
  `":"`, so an identity built from a field that happened to be blank
  registered a device under a string every other such device shares.
  Both now look at the value.

## [0.15.0] - 2026-09-10

### Fixed

- **`LevelSelf` read the wrong field of the right topic.** An entity
  naming a [`RoleAvailability`] binding got
  `{{ value_json.available | lower }}` — the envelope's *reachability*
  flag for that datapoint, not the value the datapoint publishes.
  Those are different questions, and reading the first made the
  explicit binding indistinguishable from the fallback except for
  which topic it pointed at.

  `RoleAvailability` is documented as "a binding whose value says
  whether the entity is available", so the template now reads
  `.value`, as any other state binding does. The new
  `SelfAvailabilityTemplate` is that shape; `AvailabilityTemplate`
  keeps its meaning and stays correct for the fallback, where the
  state datapoint's own `available` flag is the right field and the
  only one there is.

- **`LevelSelf` templated a raw payload.** Under `RawEncoding` an
  explicit availability binding still got a Jinja template applied to
  a bare value. `value_json` renders undefined, which equals neither
  `payload_available` nor `payload_not_available`, and Home Assistant
  silently ignores an availability payload it does not recognise — so
  the entity stayed unavailable forever with nothing on the wire, and
  nothing in any log, to show why. The template is now omitted where
  there is no envelope to reach into.

  The implicit fallback is unchanged: with no binding and no envelope
  there is no flag at all, so the level still resolves to nothing
  rather than to a broken entry.

Neither branch had a single test before this change.

## [0.14.0] - 2026-09-10

### Added

- **`Description.NameKey` and `Overlay.NameKey`** — a catalogue key
  instead of a literal name. 0.13.0 added `Context.Translate` without
  the field it resolves, so nothing on the render path ever called it;
  this is the other half. A rule table names entities by key because
  the table outlives any one language, and the catalogues live with
  the consumer.

  `Name` wins when both are set: a literal is a deliberate override of
  whatever the catalogue says, and the other precedence would make the
  override unreachable. A key with no translator behind it resolves to
  itself, so a consumer with no catalogue still renders something
  readable.

### Changed

- **`Match.Leaves` and `Match.KeyContains` now fold case.** `Models`
  already did, so one rule table matched case-insensitively on the
  device and case-sensitively on the parameter — a split nothing
  justified. A vendor vocabulary has a house style, and a rule author
  writes a parameter the way the vendor prints it.

  `Match.Keys` deliberately stays case-sensitive. An entity key is the
  consumer's own identifier and doubles as the component key inside a
  bundle, where two keys differing only in case are two components. A
  substring probe makes no such claim about identity.

## [0.13.0] - 2026-09-10

### Added

- **`Context.EntityStateTopic`, `Context.MethodTopic` and
  `Context.Translate`** — the three things a composite entity needs
  that the contract could not express.

  `EntityStateTopic` is the entity's *own* aggregate topic, carrying
  the curated document its derived roles are read out of. A climate
  reads its current temperature from a sensor's topic and its
  `hvac_action` from an aggregate no datapoint publishes; only the
  first of those was expressible.

  `MethodTopic` is where an entity listens for a named action. Not
  every command is a write to a datapoint — a cover's stop, a siren's
  turn_on, a port reset — and pointing Home Assistant at one of the
  parameters involved makes the other payloads write nonsense to it.
  The method sits one segment below the aggregate's command topic, so
  one wildcard subscription covers every method an entity declares.

  `Translate` resolves a catalogue key. `Language` alone was not
  enough: the catalogues live with the consumer, so the model could
  ask for a label but not look one up. `StdContext` returns the key
  unchanged without a translator, which keeps a consumer with no
  catalogue rendering something readable and lets a caller tell a
  missing translation from an empty one.

### Changed

- **`Context` gained three methods**, so an implementation that is not
  `StdContext` must add them. `StdContext` implements all three, and
  a consumer embedding it inherits them.

## [0.12.0] - 2026-09-09

### Fixed

- **`Origin` emitted Home Assistant's abbreviations rather than its
  canonical keys** — `sw` and `url` instead of `sw_version` and
  `support_url`. Home Assistant's abbreviation table maps one onto the
  other and it accepts either, so nothing was broken, but every other
  key this package emits is the long form: a payload mixing the two
  reads as though one of them were a different key, and a consumer
  diffing its output against a reference sees a change that is not one.

  This is a wire change for anyone already publishing an `origin`
  block through this type.

## [0.11.0] - 2026-09-09

### Added

- **`Component.Device` and `Component.Origin`**, the per-entity
  discovery form's frame.

  A device bundle carries them once at the top, and `Bundle` is where
  they belong there. But the per-entity form Home Assistant still
  accepts repeats them in every retained config, and that is what five
  of the six consuming projects publish today — Home Assistant
  declares `device` on 31 of the 32 platforms and `origin` on 30, so
  they are ordinary keys there.

  Without them such a consumer cannot express its payload as a
  `Component` at all, which is exactly what kept its discovery frame
  untyped while its entity bodies moved over.

  Both are pointers so a bundle component omits them instead of
  emitting an empty object, which Home Assistant would read as a
  device with no identifiers.

- **`Component.NameNull`** publishes `name: null`, which is how Home
  Assistant is told the entity has no name of its own and should be
  shown as the device's name alone.

  It is not the same as leaving `Name` empty, and the difference is
  visible on every label. `entity.py`'s `_set_entity_name` reads
  `config.get(CONF_NAME, UNDEFINED)`: an explicit null comes back as
  `None` and becomes the entity's name, while an absent key comes back
  `UNDEFINED` and makes Home Assistant derive a default from the
  platform or the device class. `Name string` with `omitempty` can
  only express the absent form, so a consumer that nulls its names had
  no way to say what it meant.

## [0.10.0] - 2026-09-09

### Added

- **`discovery.Ptr`**, the helper every consumer filling a `Fields`
  struct needs. The numeric and boolean discovery keys are pointers so
  a legitimate zero survives `omitempty` — a cover's `position_closed`
  is 0, a siren's `support_duration` is false — and Go cannot take the
  address of a literal, so without this every call site needs a named
  variable, which is how a builder ends up reusing one by accident.

  It lives here rather than in each consumer because the pointers are
  this module's design: a consumer that defines its own copy adds an
  export its dead-code analysis cannot see through, which is exactly
  what happened in the first project to convert its builders.

## [0.9.1] - 2026-09-09

### Dependencies

- **go-ha-catalog v0.2.1**, which v0.9.0's notes already claimed as a
  dependency without the module actually requiring it. The bump is what
  makes that true; the catalog change is a documentation correction and
  carries no data or API change.

## [0.9.0] - 2026-09-09

### Added

- **`discovery.ValidateBody(platform, body)`** checks one
  already-built discovery body — a plain `map[string]any` — against
  the platform's Home Assistant schema.

  It exists for the consumers that still publish the per-entity
  discovery form, one retained config per entity, rather than a device
  bundle. Those bodies are assembled as maps, never pass through
  `Component`, and so never reached `Validate` at all. Five of the six
  consuming projects are in exactly that position.

  The rules are not a second implementation: `Validate` now calls this
  function per component, so a bundle and a raw body get the same
  verdict — pinned by a test.

- **`ValidationError.Warnings` and `ValidationError.Blocking()`**,
  plus **`ErrAdvisory`**, separating what Home Assistant refuses from
  what it accepts and rewrites. `errors.Is(err, ErrInvalidBundle)`
  matches only the former, so existing `if invalid { do not publish }`
  code keeps publishing the payloads Home Assistant is happy with.

### Fixed

- **A dispatching platform's required keys are read from the variant
  the body names**, not from the union of all variants. `light` has
  three sub-schemas selected by its own `schema` key, and the union
  demands the template schema's `command_on_template` of a
  json-schema light — a false alarm on 62 real entities in one
  consumer's corpus. Key *legality* still falls back to the union when
  a body names no variant; required keys are then not checked at all,
  because nothing can say which set applies.

- **The legacy micro sign is a warning, not an error.** Home
  Assistant's `_native_unit_of_measurement_compat` is
  `AMBIGUOUS_UNITS.get(unit, unit)` — it accepts U+00B5 and rewrites
  it to U+03BC rather than discarding the config, which is what this
  module previously claimed in a doc comment and enforced as a
  failure. Publishing the canonical spelling is still right; it is
  advice, not a gate.

### Dependencies

- **go-ha-catalog v0.2.1**, for the corrected `AmbiguousUnits`
  documentation.

## [0.8.0] - 2026-09-09

### Added

- **A typed `Fields` struct for every MQTT platform**, generated from
  the catalog into `discovery/gen_fields.go` — 334 fields across 30
  structs, covering every discovery key Home Assistant accepts.

  Home Assistant's discovery schema is `extra=REMOVE_EXTRA`: a key a
  platform does not declare is dropped without an error on the wire or
  a line in any log, so a typo costs a feature and leaves nothing to
  debug. Until now only `climate` had a struct, hand-written and
  covering 20 of its 59 keys; everything else went through `Extra`,
  which is untyped and unvalidated.

  The structs are generated rather than typed out because the catalog
  already knows the exact key set per platform. `make generate`
  rebuilds them and CI fails when they are stale, so a Home Assistant
  release that adds a key produces a field on the next catalog bump
  rather than a bug report.

  `ClimateFields` is now the generated one. It is a superset of the
  hand-written struct it replaces and keeps every field name, so no
  call site changes.

- **`discovery.FieldsIndex`** maps a platform — `"<platform>"`, or
  `"<platform>/<variant>"` for the two that dispatch on a payload key
  (`light`, `infrared`) — to a zero value of its `Fields` struct, so
  nothing has to hand-maintain a switch from a platform to its type.

- **Thirteen near-universal keys on `Component`**: `command_template`,
  `optimistic`, `entity_picture`, `visible_by_default`, `encoding`,
  `qos`, `retain`, `message_expiry_interval`, `availability_topic`,
  `availability_template`, `payload_available`,
  `payload_not_available`, `group`, and the pair
  `json_attributes_topic` / `json_attributes_template`. Each is
  accepted by at least 16 of the 32 platforms — `group` and `qos` by
  all 32 — which puts them in the same class as `state_topic` (22)
  and `command_topic` (21), already on `Component`.

  `json_attributes_topic` is how a consumer publishes a datapoint's
  descriptor — ranges, value lists, units — alongside its value
  without inventing an entity per field.

### Changed

- **Numeric and boolean discovery fields are pointers.** A minimum of
  `0` and an explicit `false` are exactly the values a consumer sets
  deliberately, and `omitempty` on a bare `int` or `bool` drops both.

- **`make cover-check` exempts `script/`** — build tooling, not
  library code.

### Dependencies

- **go-ha-catalog v0.2.0**, which records each discovery key's value
  type. Without it the generator had to guess whether `min_temp` is an
  integer or a float from a hand-maintained list of key names — and
  the answer is not even constant per key name: `max` is a float on
  `number` and an integer on `cover`, and `group`, which reads like a
  name, is a list.

## [0.7.0] - 2026-09-09

### Added

- **`model.Slot.Scope`** and **`Slot.In(...)`** — the containers a
  device sits in, outermost first. openccu-loom has two (a CCU name
  and a wire interface), go-unifi2mqtt one (a site); the other four
  consumers have none and are unaffected.

  It lives on the slot rather than being looked up from the device so
  that a Slot stays a complete coordinate: `topic.Layout` resolves one
  without further context, and the runtime never has to carry a device
  around to render a topic. That is not a new liberty — `Address` is
  already a device property (it *is* `Identity.UID`), so a Slot
  already embeds enough identity to stand alone.

  `Slot.Key` includes the scope, so two devices with the same address
  under different controllers no longer collide. `Slot.Valid` rejects
  an empty scope segment: it would vanish from the rendered topic and
  move the datapoint one level up, into another device's tree.

## [0.6.0] - 2026-09-09

### Added

- **`model.BucketUnset`** — the zero value is now a declared bucket:
  a datapoint that belongs to no paramset at all. A hub-level value —
  a system variable, a program — is not on a channel and has no
  configuration/runtime distinction to make.

  It renders as the empty string, and `topic.Join` drops empty
  segments, so such a datapoint's topic is one level shallower rather
  than carrying a placeholder nobody can interpret. `Slot.Valid`
  accepts it; refusing it left half of a consumer's tree
  unaddressable.

  `Bucket(99)` still reports `"unknown"` and invalid.

## [0.5.0] - 2026-09-09

### Added

- **`payload.Options.Naming`** — `NamingSnake` (default,
  `sw_version`) or `NamingLower` (`swversion`).

  The field→key mapping was an accidental constant, and two published
  surfaces disagree on it: this module derives `interface_id`, the
  reference implementation publishes `interfaceid` on its own MQTT
  topics today. Neither can adopt the other's spelling without
  renaming a key someone reads. Making the policy explicit lets a
  consumer migrate onto this package without a wire break, and leaves
  new consumers on the better default.

  `alt=` still outranks either policy — it names the key outright.

## [0.4.0] - 2026-09-09

### Changed

- **`payload.Extra` receives the harvest options.**
  `ExtraPayload(Kind)` became `ExtraPayload(Kind, Options)`. Without
  them a contributed property cannot honour `IncludeZero`, so the
  escape hatch emits keys the rest of the payload would have dropped.
  The case is concrete: a field moved behind a mutex to fix a data race
  becomes unreflectable in the same stroke, and contributing it here is
  how it comes back — the reference implementation's device name went
  through exactly that.

## [0.3.0] - 2026-09-09

Two API changes the openccu-loom migration needs (ADR 0070, phase 3,
steps 1 and 3). Both are behaviour changes, not additions.

### Changed

- **`alt=` is now opt-in**, through `payload.Options.UseAltNames`.
  It was applied unconditionally, which cannot serve the requirement
  it exists for: one struct feeds two audiences — a device's own MQTT
  info topic, which wants the model's vocabulary ("address"), and Home
  Assistant's device block, which wants its own ("serial_number"). The
  tag carries both spellings and the caller picks. This matches the
  reference implementation, which had the same two call sites.
- **`topic.Slug` preserves the hyphen.** It folded `-` into `_`, which
  destroys a sub-device id composed as `<parent>-<group>` by collapsing
  it onto a sibling that legitimately contains an underscore. Home
  Assistant's own topic matcher accepts `[a-zA-Z0-9_-]` in a node id,
  so the fold was stricter than necessary and lossy. Entity ids are
  unaffected: Home Assistant slugifies `-` to `_` when it derives one.

## [0.2.0] - 2026-09-09

Four defects found by mapping openccu-loom onto this module before
migrating it — phase 3 of ADR 0070, which sequences the hardest
consumer first precisely so the design is wrong here rather than in
six places. All four would have shipped and then failed on loom's
first day.

### Fixed

- **`Validate` rejected every `select`.** The sensor rule "options
  require `device_class: enum`" ran on every platform, but `select`
  *requires* `options` and declares no `device_class` at all. Since an
  invalid bundle publishes nothing for the whole device, a single enum
  parameter would have silenced every entity of that device. The rule
  is now sensor-only, where Home Assistant's own validator puts it.
- **A `Builder`'s `Extra` was discarded.** `renderComponent` assigned
  `comp.Extra = desc.Extra` *after* running the builder, closing the
  only route to the ~90 platform keys that have no typed field yet —
  exactly where a builder is the only stage holding the `Context`
  needed to compute them. The two now merge, with the description
  winning on a shared key, which is the documented precedence.
- **Bounds landed on illegal keys.** `Min`/`Max`/`Step` were projected
  onto `min`/`max`/`step` for every platform. Those exist only on
  `number` (and `min`/`max` on `text`); `climate` spells them
  `min_temp`/`max_temp`/`temp_step`. Projection is now schema-guarded.
- **So did the plain topics.** Ten platforms declare no `state_topic`
  and eleven no `command_topic` — climate, water_heater and lawn_mower
  name a topic per role, and button, scene and notify are write-only —
  yet the default projection emitted them regardless. Also
  schema-guarded. This one surfaced while fixing the third and was not
  in the migration map.

The common shape: Home Assistant drops a key its platform does not
declare **in silence**, so every one of these produced a payload that
looked fine and quietly lost a feature. The catalog's per-platform
schemas are now consulted by the renderer, not only by the validator.

## [0.1.0] - 2026-09-09

First cut of the shared model. Phase 2 of openccu-loom ADR 0070: types,
the discovery bundle, validation and naming. The publisher runtime is
phase 4 and is not here yet.

The API will move while openccu-loom migrates onto it — that migration
is the design's proof, and finding a wrong signature there is the point.

### Added

- **`model`** — `Identity` with namespaced identifiers and a `Merge`
  that recognises shared hardware; `Slot` with a variadic path and a
  string channel, so a Homematic parameter, a dotted Home Connect
  feature and a named UniFi port all fit; `State` with an origin and an
  `OriginPolicy` for multi-source bridges; one `Description`; and
  `Availability` as a list of levels rather than a topic, so a
  connectivity sensor can outlive the device it reports on.
- **Capability interfaces** — `Commander`, `Suppressor`, `Deriver`,
  `OriginPolicy`, `EntitySource`, `Enricher`. Not implementing one is
  the opt-out.
- **`discovery`** — the device-based `Bundle`, `Render` with a single
  precedence rule, `Context` as the model's only route to a topic
  string, and `Validate`.
- **`topic`** — `Layout`, `Join`, and the two deliberately different
  normalisers `Safe` and `Slug`.
- **`payload`** — struct-tag partitioning into info, config and state.
- **`catalog`** — `Rule` split into `Match` and `Overlay`, a rule set
  that is itself an `Enricher`, and `Static` as an `EntitySource`.

### Notes

- **`object_id` is not published.** It has not been a legal MQTT
  discovery key on any platform since Home Assistant replaced it with
  `default_entity_id`, and Home Assistant drops unknown keys in
  silence — so the six reference implementations that still send it get
  nothing for it. The validator caught this on its first run against
  this module's own renderer.
- State topics carry a JSON envelope by default. It is the only shape
  that carries per-datapoint availability; `RawEncoding` opts out.
- The discovery prefix is configurable and defaults to
  `homeassistant`. Every reference implementation hardcoded it.
