# Changelog

All notable changes to this project are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.32.0] - 2026-09-13

### Fixed

- **A `unique_id` shared by two platforms is no longer refused.**
  `Validate` keyed its duplicate check on the `unique_id` alone. Home
  Assistant does not: its entity registry indexes on a three-part key,
  `(domain, platform, unique_id)` — the entity component, the
  integration, and the id. `entity_registry.py` builds it literally,

      self._index[(entry.domain, entry.platform, entry.unique_id)] = entry.entity_id

  and `async_get_entity_id`, `async_get_or_create`, the
  `deleted_entities` index, the unique-id-change guard that raises
  `Unique id '%s' is already in use by '%s'`, and `entity_platform.py`'s
  runtime check behind `Platform %s does not generate unique IDs` all
  consult that same triple. The developer documentation says it in
  prose: *"An entity is looked up in the registry based on a combination
  of the platform type (for example, `light`), and the integration name
  (domain) (for example, hue) and the unique ID of the entity."*

  Every component in a bundle comes from the one `mqtt` integration, so
  a bundle can only vary the other two. A `sensor` and a `number`
  carrying the same `unique_id` are two distinct registry keys and both
  register. Refusing the pair made this validator stricter than the
  platform it models.

  Nothing in the MQTT integration narrows it further for the
  device-bundle form, which is the newer path and the one that could
  have: `components/mqtt/discovery.py` never mentions `unique_id`;
  `DEVICE_DISCOVERY_SCHEMA`'s validation over `cmps` is `check_unique_id`,
  a presence requirement and nothing more; and the conflict behind
  `Received a conflicting MQTT discovery message` is keyed on
  `(component, discovery_id)` and the discovery topic. The bundled path
  fans `cmps` out into per-component configs that then travel the
  identical per-entity machinery.

  The measured cost of the old rule: one consumer publishes nine
  `unique_id`s under two platforms each — five `number`+`sensor`, three
  `binary_sensor`+`switch`, one `select`+`sensor` — and has done so in
  the per-entity form, in production, for years. Its bundle came back
  with nine blocking issues, `ErrInvalidBundle`, and a runtime that
  validates before publishing would have withheld the whole device: all
  100 entities, not the nine.

  **Two components sharing a platform *and* a `unique_id` are still
  refused blockingly**, because that pair really does collide on the
  registry's own terms. The issue text now names the platform.

### Documentation

- **`Render` and `Validate` disagree on purpose, and now say so.**
  `Render` refuses one thing — two entities with the same
  `model.Entity.Key`, which the components map would otherwise swallow —
  and that is a losslessness guard on the document, not a judgement
  about what Home Assistant accepts. It will build a bundle `Validate`
  then refuses. Folding the check into `Render` was considered and
  rejected: validation is fail-closed, so an invalid bundle publishes
  nothing at all, and making that automatic would cost a device every
  entity over one bad component without the consumer ever asking. The
  choice of what to do with a finding stays with the caller.

- **`Component.UnitOfMeasure` cannot express `"unit_of_measurement": ""`,
  and should not.** A measured consumer publishes the empty string on
  five sensors, reachable today only through `Description.Extra`, which
  raised the question of a `*string` or a sentinel. The answer is no:
  unlike `NameNull`, a blank unit is not a distinction MQTT discovery
  can carry. `components/mqtt/sensor.py` pops it during schema
  validation, before the entity is constructed, and the validator
  wrapping both `PLATFORM_SCHEMA_MODERN` and `DISCOVERY_SCHEMA` —

      if (
          unit_of_measurement := config.get(CONF_UNIT_OF_MEASUREMENT)
      ) is not None and not unit_of_measurement.strip():
          config.pop(CONF_UNIT_OF_MEASUREMENT)

  so publishing it is exactly equivalent to omitting the key. `Extra`
  remains the right route where byte-equality with an already-published
  payload is the goal; the model stays clean. Recorded on the field.

## [0.31.0] - 2026-09-13

### Changed

- **`go.mod` requires `go-mqtt` v1.5.1.** v0.30.0 had to leave this
  open: the fix was on an unmerged pull request, so the release could
  only document the boundary rather than close it. Until now a consumer
  whose own client carried a broad subscription matching a command topic
  could still see a handler run twice per published message — through
  go-mqtt v1.5.0 an identifier-less delivery was matched by topic
  against stamped subscriptions as well, so that subscription's copies
  reached every stamped route.

  v1.5.1 fails closed instead, per MQTT 5.0 §3.3.4: a copy carrying no
  identifier was forwarded for no stamped subscription, so it reaches
  none. The guidance on `CommandRouter` stands unchanged — a consumer
  should still keep a broad subscription off its own command tree, which
  is the subscription-side twin of `CheckDisjoint` — but it is no longer
  the only thing between that mistake and a doubled physical action.

## [0.30.0] - 2026-09-13

An adversarial review of v0.27.0–v0.29.0. Every item below was measured
against the shipped code, and the two that reach a consumer's broker are
the first two.

### Fixed

- **A removed entity kept its old per-entity config forever** when the
  consumer's fleet is on the node-id-less legacy form — which is
  go-zendure2mqtt's wiring, the fleet that form was added for in
  v0.27.0.

  Home Assistant deletes a component from a device document when the
  component's entry carries a platform and nothing else, so
  `discovery.Bundle.Remove` writes exactly that — and a tombstone
  therefore has no `unique_id`. `publisher.LegacyTopicByUniqueID` has
  nothing to key on without one and returned no topic at all, so the
  retained config of the entity being deleted was never retracted. The
  deleted entity goes on being discovered from it: a permanently
  unavailable phantom that returns on every MQTT-integration restart.
  `Runtime.Sweep` cannot reach it either, because the node-id-less form
  parses with an empty `NodeID` and a node-id-scoped `Owns` declines it
  by design. The doc comment on `SupersededTopics` claimed tombstones
  were covered; they were covered for two of the three forms.

  The identity cannot go back into the payload — that would un-remove
  the entity — so it is remembered beside it.
  **`discovery.Bundle.Tombstones`** holds what a removed component was,
  and **`discovery.Bundle.RemoveComponents`** is the call to use when
  the component has left the catalogue and the document being published
  never held it, which is the ordinary case. `Bundle.Remove` fills it
  too, from the entry it overwrites.

  **A consumer that wires `LegacyTopicByUniqueID` and calls `Remove`
  should switch to `RemoveComponents`.** For go-zendure2mqtt the defect
  is latent today — it wires the form and removes nothing yet — and goes
  live the first time an entry leaves its catalogue.

- **A command delivered while the routes were still being subscribed was
  dropped.** The router drops a copy that arrived for a route a more
  specific one outranks, judged against the registered route set — which
  is not the set the broker holds until the last SUBSCRIBE is
  acknowledged. With the general shape registered first, the only
  subscription that existed inside that window was the one whose copies
  get dropped: measured as `generic ran 0, specific ran 0`, one command
  lost, one `Debug` line. That window is the ordinary state of the wire
  during `Start` and during a sequential resubscribe replay, so a button
  pressed in the second after a reconnect did nothing.

  Routes are now subscribed most specific first. That closes the window
  without per-connection liveness bookkeeping, and closes it in the
  direction that cannot double: a copy can only arrive for an outranked
  route once the route that outranks it is already subscribed on the
  same connection. It rests on a transport that replays subscriptions of
  its own replaying them in registration order, which go-mqtt does.

- **A sweep whose context ended discarded everything the window saw.**
  The snapshot reports the caller's context ending as an error even
  after a full window, and the pass mapped any error to a zero
  `SweepResult` — so for a `ReportOnly` pass, whose only output is the
  result, a boot context expiring on the window boundary lost the lot.
  The result now comes back alongside the error.

- **The subscription-identifier counter no longer runs past its
  ceiling.** It reported the range error correctly and kept counting,
  which is harmless until 2³² allocations wrap it back to values that
  pass the range check again. Unreachable in practice; impossible to
  notice if it ever were reached.

### Added

- **`publisher.SweepResult.Unclaimed`** — the owned topics this process
  does not claim, which is what a retracting pass would clear.

  `ReportOnly` could not report: the pass computed exactly this list,
  logged its length at `Debug` and threw it away. The only list it
  handed back was `Owned`, which includes the configs the consumer is
  publishing right now — and `Retract(res.Owned...)` is the composition
  that cleared 29 live configs in a sibling repo. `Owned`'s doc now says
  what it is and what it is not, and drops the claim that a retracting
  pass's `Owned` minus `Retracted` is the claim set: a failed retraction
  warns and the pass continues, so the difference also holds whatever
  the broker refused.

- **`gomqtt.TransportV311` and `gomqtt.SplitV311`** — an adapter for a
  client pinned to `mqtt.ProtocolV311` that claims neither MQTT 5.0
  capability.

  `gomqtt.Transport` implements `AttributingSubscriber` whatever the
  wrapped client is talking, because a Go type cannot carry a value's
  protocol version — so an overlapping route pair was accepted at
  `Handle` on both dialects and a v3.1.1 link was caught only at
  `Start`, as `ErrAttributionUnavailable`. That is correct and it is
  visible, but it is a boot failure where a composition-root error was
  available: a v3.1.1 link has no property block to carry a Subscription
  Identifier, and that is known before the first SUBSCRIBE. A router
  over the new adapter refuses the pair at registration with
  `ErrAmbiguousRoutes`. The README described the refusal that did not
  happen; it now says which constructor reports where.

- **`publisher.Runtime.LegacyForms`**, and a `publisher.legacy_forms`
  line logged once per runtime built. `Config.LegacyEntityTopics`
  replaces the default rather than extending it, so stating one form
  silently stops retracting the other — and nothing anywhere said which
  forms were active. For a fleet spanning releases that line is the
  cheapest evidence there is.

### Changed

- Doc comments corrected where they described behaviour the code does
  not have: `SupersededTopics` on tombstones, `SweepResult.Owned` on
  what the list is for, `ValidateFilter` on a SUBACK failure being
  "deliberately dropped by the transport interface" (the shipped adapter
  surfaces it — go-mqtt returns a `*ReasonError`, and only the granted
  QoS is dropped), and `payload.ParamInt32` on truncation: an
  out-of-**range** integer is an error, while a **fractional** `float64`
  — Home Assistant sends `2.7` for a stepped field — is truncated toward
  zero. Both are now stated, and the fraction is pinned by a test.

### Upgrading

- **Re-read a `SweepRequest.Owns` that does not look at `NodeID`.**
  v0.29.0 widened `ParseConfigTopic` to the three-segment node-id-less
  form, and a widened parser widens what a predicate is asked about: one
  deciding on the platform or the object id alone now judges a class of
  topics it was never shown, and it is a populated class — Tasmota
  publishes exactly that shape into a shared discovery tree. A predicate
  that scopes on the node id is unaffected, because that form parses
  with an empty one and such a predicate declines it. The hazard arrived
  in v0.29.0, whose entry documented the widening without saying what to
  re-check.

- A consumer whose own client carries a broad subscription that also
  matches a command topic wants **go-mqtt v1.5.1 or later**. Through
  v1.5.0 an identifier-less delivery was matched by topic against
  stamped subscriptions too, so that subscription's copies reached every
  stamped route and ran its handler twice per published message. v1.5.1
  fails closed, per MQTT 5.0 §3.3.4. **Closed in v0.31.0**, whose
  `go.mod` requires v1.5.1. Nothing here relied on the permissive
  matching, which is pinned by a test.

## [0.29.0] - 2026-09-12

The inbound half of the payload package: the coercions every consumer
needs to read a service call Home Assistant sent.

### Added

- **`payload.ParamBool`, `ParamFloat64`, `ParamInt32`, `ParamString`**,
  with `ErrMissingParam` and `ErrInvalidParam` — decoders for the body
  of an inbound service call, as JSON already unmarshalled into a
  `map[string]any`.

  They belong here because every consumer needs the same coercions for
  the same reason: Home Assistant's templating decides what type
  reaches the wire and the caller cannot control it. A plain
  `{{ value }}` template sends the string `"42"` where the author meant
  a number, `payload_on` sends whatever the platform's default spelling
  is, and a hand-written automation sends a JSON bool. A consumer that
  accepted only the declared Go type would reject a command Home
  Assistant considers well formed, and the operator would see a control
  that does nothing.

  Three decisions are carried over from the reference implementation
  with their reasons, because each is a boundary where the generous
  answer is the wrong one:
  - A numeric string goes through `strconv.ParseFloat`/`ParseInt`
    rather than a scanning helper, so `"42xyz"` is an error. Reading it
    as 42 would turn a typo in an automation into a command nobody
    wrote.
  - An out-of-range integer is an error, not a truncation. Truncation
    would surprise a caller supplying a 64-bit index, and the surprise
    would arrive as a write to the wrong thing rather than as a
    rejected command.
  - `ParamBool`'s spelling list is exact rather than case-insensitive
    and excludes `"yes"`/`"no"`. The set of spellings Home Assistant
    emits is knowable; a consumer's own coercion of *device* values
    against a parameter descriptor is a different boundary with a
    different set, and the two are deliberately not converged.

  Nothing here touches a published string — these are the read side,
  which is why the reference consumer's golden payload pins correctly
  say nothing about them. ADR 0070's move-up measurement names this as
  the first non-trivial piece of the consumer's `payload` package that
  is genuinely daemon-agnostic: 137 lines the consumer deletes and
  imports back.

## [0.28.0] - 2026-09-12

Overlapping command routes are registerable again — on a transport that
can prove a delivery belongs to one subscription, and only there. v0.27.0
refused every overlap, which was the right fix and a real cost: a
consumer wanting a special case for one parameter name could not state
it, and the measured consumer's own workaround for that collision is a
hand-maintained list of reserved segments inside its general handler.
MQTT 5.0 Subscription Identifiers are the missing information, and
go-mqtt v1.5.0 — cut for this — is the first release that lets a caller
set one.

The MQTT 3.1.1 refusal stands, unchanged and deliberately. That dialect
has no property block, so there is nothing to carry an identifier; a
router that accepted overlaps and then quietly double-ran handlers there
would be strictly worse than the refusal, because the refusal is visible
at the composition root and the doubled write is visible nowhere.

### Added

- **`publisher.AttributingSubscriber`** — the optional `Transport`
  capability that subscribes with a Subscription Identifier
  (§3.8.2.1.2), so the broker stamps every message it forwards for that
  subscription and the client delivers it to that subscription's handler
  alone. `SubscribeAttributed(ctx, filter, qos, id, handler) error`, the
  same optional-interface shape as `NoLocalSubscriber`.

  What it buys: `ccu/+/+/set` together with `ccu/+/PRESS_SHORT/set` is
  accepted, and the more specific route wins the topics it claims. That
  exact pair ran one message's handler twice against Mosquitto 2.1.2 on
  both dialects — a toggle toggling twice, a relay pulse firing twice,
  with nothing in any log — because two fan-outs compose: the broker
  sends one copy per matching subscription (§3.3.4) and a client that
  re-matches each copy against its whole filter list then runs every
  matching handler per copy. Each copy is now attributable, so the
  router drops the copies that arrived for a route a more specific one
  outranks. **Exactly one handler still runs per published message**,
  which is the invariant the type has always promised.

  **A claimed capability is not a demonstrated one**, and the router
  treats the two differently. Implementing the interface is permission
  to *accept* an overlap; `Start` is where it is *proved*. Every route
  goes out through `SubscribeAttributed`, and a transport that cannot
  honour the identifier must return an error — which fails `Start` with
  `ErrAttributionUnavailable` rather than retrying without it. That is
  the case to think hardest about: a v5-capable adapter that turns out
  to be talking MQTT 3.1.1. The shipped adapter behaves that way
  because go-mqtt does, refusing `WithSubscriptionID` on a v3.1.1 link
  instead of dropping it, and surfacing a broker's "Subscription
  Identifiers not supported" as a SUBACK failure. One residual hazard
  cannot be closed from here and is documented on the interface: a
  transport that implements it, returns `nil`, and does not actually
  stamp reproduces the multiplication.

- **`publisher.ErrAttributionUnavailable`** — the sentinel for both
  halves of that story. `Handle` wraps it alongside `ErrAmbiguousRoutes`
  when the transport offers no attribution at all (a composition
  mistake, and the v0.27.0 answer with a reason attached), and `Start`
  returns it when a transport that offered attribution could not
  deliver it.

- **`publisher.MaxSubscriptionID`** (268435455),
  **`CommandRouter.Attributed()`** and
  **`CommandRouter.SubscriptionID(filter)`**.

  Identifiers are **the router's to assign**, never the consumer's, and
  the allocation rule is process-wide rather than per router: a counter
  starting at 1, shared by every router in the binary. Per-router
  numbering is the obvious choice and it is wrong — the identifier space
  belongs to the MQTT session, so two routers over one client (two
  consumers of this library in one process, or a config reload building
  a second router) would both number from 1 and each would then receive
  the other's commands. Exhaustion is an error, not a wrap. The
  accessors exist for the one caveat that rule cannot cover: a consumer
  stamping subscriptions of its own on the same client must stay out of
  the way, which means allocating downward from `MaxSubscriptionID` and
  being able to check its work. `Attributed()` is the boot-log and
  conformance answer to "is this router relying on attribution", which
  is the difference between a `Start` that can fail on dialect grounds
  and one that cannot.

- **`gomqtt.Transport` implements `publisher.AttributingSubscriber`**,
  compile-asserted, passing `mqtt.WithSubscriptionID` together with
  `mqtt.WithNoLocal` — both, because an identifier is v5-only, so a call
  that reaches there is on a link where No Local is available too, and
  the router calls the attributed form *instead of* `SubscribeNoLocal`
  rather than as well as it. Dropping it would silently reopen the echo
  class that option exists to close.

### Changed

- **`Handle` accepts an overlap only when one filter is strictly more
  specific than the other.** `ccu/+/+/set` against
  `ccu/+/PRESS_SHORT/set` qualifies; `a/+/c` against `a/b/+` does not —
  one literal each, in different levels, and no principled winner for
  `a/b/c` — and stays refused with `ErrAmbiguousRoutes` on every
  transport. Attribution says which subscription a copy arrived for; it
  does not say which of two equally-strong claims a consumer meant.

  The ordering is a **dominance test over per-level ranks** (`#` and
  everything after it loosest, then `+`, then a literal, then past the
  last level of a filter that does not end in `#`), not a score: a score
  has to price a literal in one level against a literal in another and
  there is no defensible exchange rate. Dominance is transitive, so the
  routes matching one topic form a chain with one maximum.

  It is deliberately **not** the specificity machinery v0.27.0 deleted.
  That one picked a winner per delivered *copy* and then ran the
  winner's handler for each copy, which is how one command ran its
  handler N times; this one answers "which route owns this topic", once
  per topic, while the *copy* is decided by comparing that winner
  against the subscription the copy actually arrived for. It also fixes
  an inversion the deleted version shipped, which a review had found and
  which is now a regression test: `a/b` lost to `a/b/#`, so an
  exactly-registered route never fired.

- **Accepting one overlap makes every route carry an identifier**,
  including routes that overlap nothing. An unstamped copy carries no
  identifier, so the client falls back to re-matching it against every
  filter it holds — which hands it to the stamped overlapping routes as
  well and restores the multiplication the identifiers were taken out
  for. All-or-nothing per router, and a router that accepted no overlap
  stamps nothing at all: a consumer with disjoint routes is on exactly
  v0.27.0's wire, which also keeps a broker that answers an identifier
  with a SUBACK failure from turning a working `Start` into a failing
  one.

- **`Route` and `Claims` report the route that would actually run** —
  the most specific match, which with disjoint routes is the only match
  and therefore unchanged. Identifiers are replayed with their filters
  on `Resubscribe`, because a broker holds one as part of the
  subscription and forgets it with the session.

- **`go-mqtt` moves to v1.5.0**, which is where `WithSubscriptionID`
  lives.

## [0.27.0] - 2026-09-12

Three gaps the ADR 0070 phase-5 pilot measurement found, all of the
same kind: the library silently imposed openccu-loom's shape on a
consumer that is not openccu-loom. None of the three is a design
conflict with loom — loom remains the tiebreaker where there is one —
and none was visible from the code that contained it. Two were
measured on `go-zendure2mqtt` before any of its code moved
(`docs/adr0070-pilot-measurement.md`, 2026-09-12) and both were
blockers for the pilot; the third was measured by openccu-loom PR #797
trying to use v0.26.0's newest feature and giving up.

### Added

- **`publisher.QoS`** — a configured quality-of-service level, as
  distinct from the byte on the wire, so that **QoS 0 can be stated**.
  All four runtime types read a zero `QoS byte` as *unset* and coerced
  it to 1, which made "unset" and "deliberately QoS 0" one statement.
  The measured consumer publishes and subscribes at **QoS 0
  everywhere** (`discovery.go:78`, `coordinator.go:88,128,138,171,269`)
  and the measurement recorded "cannot be preserved" against the
  column: adopting the runtime would have changed the delivery
  guarantees of a whole installed base on the wire, inside a migration
  step whose purpose was de-duplication, with a broker capture as the
  only evidence.

  `QoSUnset` is the zero value and every existing default is
  unchanged. `QoSAtLeastOnce` and `QoSExactlyOnce` **are** the wire's
  1 and 2, so a v0.26.0 struct literal keeps its meaning;
  `QoSAtMostOnce` sits outside the wire range because 0 was already
  taken by "unset", and resolves to wire 0. An unrecognised value is a
  panic at construction — the third state the old `byte` had no answer
  for — because it is a composition-root mistake and this package no
  longer quietly decides what a consumer meant.

  **Availability keeps QoS 1 as a default and not a floor**, and the
  reasoning is in `AvailabilityConfig.QoS` rather than only here. It
  has the strongest case for a floor in the package: openccu-loom's own
  PR evidence is two availability topics in one daemon with different
  guarantees, and an availability marker lost at QoS 0 leaves an entity
  wrongly available until the next flip, which for a crash is never.
  Three things still point the other way — a floor would be this
  package deciding what a consumer meant, which is the silent
  imposition the type exists to remove; it would make one of the four
  types disagree with the rest, and three answers to one question is a
  defect this package has already fixed once; and the defect the
  evidence records is two topics *differing by accident*, which one
  stated level per publisher makes unreachable. `NewAvailability`
  therefore logs a warning naming the consequence when a consumer
  states QoS 0 there: honoured, and said out loud.

- **`publisher.LegacyTopicFunc`**, `LegacyEntity`,
  `LegacyTopicWithNodeID`, `LegacyTopicByUniqueID`,
  `LegacyTopicByObjectID` and **`Config.LegacyEntityTopics`** — a
  consumer can state which per-entity discovery topic form its
  installed fleet is on.

  `SupersededTopics` rendered exactly one shape,
  `<prefix>/<platform>/<node_id>/<object_id>/config`, so
  `PublishBundle` retracted **nothing at all** for a fleet on the
  four-segment form Home Assistant equally permits. The measured
  consumer's 29 configs are at
  `<prefix>/<platform>/<unique_id>/config`, with no node-id level. The
  consequence is the one measured live on Home Assistant 2026.9 on
  2026-09-10/11: a bundle published while a per-entity config for the
  same `unique_id` is still retained is **refused**, symmetrically,
  with one `WARNING [mqtt.entity] Received a conflicting MQTT
  discovery message` line and nothing else. The entities would simply
  not appear. The measurement called it "the single highest-risk step
  in the whole migration and the one most likely to be missed, because
  everything *looks* right: the bundle publishes, the log is clean,
  and the entities keep their old configs."

  The library cannot guess the form — too narrow misses the
  retractions, too wide retracts a topic belonging to another writer in
  a shared discovery tree — so the consumer states it and a consumer
  that states nothing gets v0.26.0 exactly. Several forms are unioned
  and de-duplicated, for a fleet that spans releases; a component with
  nothing to key on contributes no topic rather than one with a blank
  segment.

- **`SweepRequest.ReportOnly`** and **`SweepResult.Owned`** — the
  sweep can report without acting.

  v0.26.0's `SweepRequest.Inspect` let a consumer judge an orphan on
  its retained payload, but `Inspect` fires only for topics `Owns`
  accepted and every owned-and-unclaimed topic in that same pass was
  retracted: there was no way to look without clearing. openccu-loom
  PR #797 tried and gave up. Its one-off scrub must run *before* the
  first snapshot — the retraction is what makes Home Assistant forget a
  stale `unique_id`, and the snapshot that follows re-announces under
  the corrected one — and at that moment the claim set is empty, so an
  `Owns` wide enough for `Inspect` to see anything makes the pass
  delete the entire retained discovery fleet, which is the hazard
  `Runtime.Sweep`'s own doc comment warns about. Running it afterwards
  finds nothing, because the retained payload is by then already the
  corrected one. So that consumer kept a second hand-rolled broker
  snapshot beside the library's, purely because the library could not
  be asked to report without acting.

  `ReportOnly` returns before the retraction loop rather than
  branching inside it, so "not one message goes out" is a property of
  the control flow and not of five conditions staying in agreement. It
  is the pass that is **safe before the first publish**, which the
  retracting one explicitly is not, and the one mode in which a
  deliberately wide `Owns` costs nothing. `SweepResult.Owned` carries
  the topics the pass judged, in arrival order, on **both** kinds of
  pass: a caller doing its own judging needs the list and not only its
  size, and on a retracting pass Owned minus Retracted is what this
  process still claims.

### Changed

- **BREAKING (types, not values): five config fields change from
  `byte` to `publisher.QoS`** — `Config.QoS`, `StateConfig.QoS`,
  `StateConfig.PulseQoS`, `AvailabilityConfig.QoS` and
  `CommandConfig.QoS`.

  Migration: a struct literal with an untyped constant —
  `Config{QoS: 1}`, `StateConfig{PulseQoS: 0}` — compiles and means
  exactly what it meant before, because `QoSAtLeastOnce` and
  `QoSExactlyOnce` are the wire values. Only code that assigns a
  `byte`-typed *variable* or expression into one of these fields
  breaks, and the fix is one conversion or the named constant:
  `QoS: publisher.QoS(cfgFromFile.QoS)` or, better,
  `QoS: publisher.QoSAtMostOnce`. `Will.QoS` stays a `byte` — it is
  the one place a QoS crosses back out to the consumer's own MQTT
  client, and a client takes a byte.

  A consumer that wants QoS 0 must now say `QoSAtMostOnce`; the
  literal `0` still means "unset", which is still QoS 1 (and still
  QoS 0 for `PulseQoS`, whose default it already was).

- **`SupersededTopics` takes a variadic `...LegacyTopicFunc`.**
  Source-compatible: every existing call site keeps compiling and
  keeps returning the five-segment form.

### Fixed

- **`ParseConfigTopic` rejected the node-id-less per-entity form its
  own doc comment described as handled.** `<prefix>/<platform>/
  <object_id>/config` — three segments, which Home Assistant permits —
  returned `ok=false`, so such a config was invisible to the sweep and
  retained forever: the same defect the device-document form had
  before v0.26.0, and the same class as the four-segment case the
  pilot measurement hit. It now parses, with an empty
  `ConfigTopic.NodeID`, because the topic carries no node id and
  inventing one would be a guess an ownership predicate would then
  trust — so a node-id-scoped `Owns` still declines it, and a consumer
  that knows its own fleet can claim it. `Platform != "" && NodeID ==
  ""` is exactly that form. The doc comment is true again.

## [0.26.0] - 2026-09-12

The rest of the runtime layer ADR 0070 scoped — state publishing,
command routing and entity availability — plus the escape hatches the
twelve migrated discovery planes still had to work *around* the model
instead of *through* it.

The three runtime planes were written against the first full consumer's
measured behaviour by three authors who could not see each other's
files, then merged and adversarially reviewed. **The review is the
reason this release took the shape it did**: it reproduced a
double-dispatch defect against a real Mosquitto, and proved five
doc-comment claims and five tests wrong. Those findings are in
`### Fixed` below, before the release rather than after it.

### Added

- **`publisher.StatePublisher`** — the entity *state* plane, as
  distinct from the retained discovery configs v0.25.0 carries. A
  dedup gate keyed on the topic with the full payload bytes, an
  eviction index (a removed device takes its channel list with it, so
  the index is the only way its retained topics can still be found),
  non-retained QoS-0 pulses (a retained pulse re-fires on every
  reconnect), and the reference implementation's latency probe with
  both of its negative controls — QoS 0 untimed because it measures
  this process's own buffer, failures untimed because they measure the
  failure.

  Two measured defects of the reference are fixed by construction: a
  nil value has no raw rendering rather than becoming an empty
  retained payload, which is MQTT's retraction; and floats render
  through the shortest round-tripping form rather than `%f`-and-trim,
  which capped at six decimals and sent `0.0000001` as `0`.

  `ComponentStateTopic` and `StateTopicFor` exist because
  `topic.Layout.State` is **not** the config's `state_topic` on 10 of
  32 platforms — `climate`, `water_heater`, `camera`, `button` and six
  more render no `state_topic` at all, while the layout happily returns
  one. A consumer deriving the publish topic from the layout writes
  into a topic no config references, and the entity stays "unknown"
  forever with nothing logged.

- **`publisher.CommandRouter`** — the command plane: registration,
  subscription, dispatch, resubscribe, lifecycle, and the
  state-versus-command disjointness guard the reference implementation
  learned the hard way (it mirrored a program's state onto that
  program's own trigger topic, and the echo ran the program on every
  boot, every republish and once per freshly discovered program, with
  nothing in the logs saying so).

  Handlers run on router workers, never on the transport's read loop.
  A command is by definition a write outside the process, and a handler
  that answers by publishing would wait for an ack only the goroutine
  it occupies could deliver — a self-deadlock on the first command, not
  a slow path. The three costs are documented on `CommandHandler`.

- **`publisher.AvailabilityPublisher`** — the publishing side of the
  three availability levels whose declaring side already shipped.
  `LevelBridge` was already covered by the birth/LWT policy; this adds
  the device reachability topic, without which an entity never greys
  out in Home Assistant when its device goes off-bus, and the
  `LevelSelf` payload — which agrees with the declaring side's
  `value` key, its bare boolean under `RawEncoding` and its
  `true`/`false` tokens, rather than with the reference
  implementation's own spelling.

- **`discovery.DeviceSlot`**, **`publisher.ParentSlot`** and
  `AvailabilityPublisher.Parent`/`ParentTopic`/`Bridge` — one
  derivation per availability coordinate, shared by the side that
  declares the topic and the side that writes to it. Two copies of a
  coordinate derivation is exactly how the two sides drift apart, and
  an entity whose availability topic nobody publishes to is one Home
  Assistant greys out forever.

- **`Config.Layout`** — makes `Config.StatusTopic` checkable instead
  of free-form. It is the one string every entity's availability list
  references, and under the default `availability_mode: "all"` a
  single typo greys out the whole fleet with nothing on the wire
  naming the cause. With a layout set, an empty status topic is
  derived from it and a disagreement is refused at the composition
  root.

- **`SweepRequest.Inspect`** — the sweep is the *last* moment an
  orphan's other topics can be found. A config removed while the
  consumer was down is remembered by nobody, and the config body is
  the only place that still names its availability and state topics.
  Discarding it left a retained `online` standing forever, so Home
  Assistant kept a device that no longer exists permanently available,
  showing its last value.

- **`model.Description.NameNull`** — `name: null` as a statement
  distinct from "no name". An empty `Localized` means *no opinion* and
  the pipeline drops the key, which makes Home Assistant derive a name
  from the platform; `name: null` tells it to use the device name
  alone. Three planes set it after rendering.

- **`model.Description.CommandTemplate`** — projected onto the 16 of
  32 platforms whose schema declares it, read out of `go-ha-catalog`
  rather than assumed.

- **`topic.PulseLayout`**, `PulseKind`, `PulseTopic` and
  `Default.Pulse` — the four event/impulse topic kinds a consumer
  publishing pulses needs. Deliberately an **optional capability
  interface rather than a fifth `Layout` method**: this module states
  capability interfaces as its extension mechanism, a method would
  break all six layouts including the ones with nothing to return,
  and an embedded default would be worse than a compile error — a
  pulse topic derived from the state topic is plausible, deterministic,
  and subscribed to by nobody. `PulseTopic` returns `""` for a
  declining layout and invents nothing.

- **`discovery.PresetModeTemplates`** / `EnumTemplates` — the
  `preset_mode` Jinja round-trip pair from a `model.Enum`, whose
  reverse lookup a command template used to re-implement by hand. The
  emitted bytes were verified against the first consumer's pinned
  `climate/thermostat` payload, not by eye.

- **`discovery.RenderFrame`** — a merge-*under* render for a consumer
  that receives a finished component from outside the model. The given
  component always wins; only a key at its Go zero value is taken from
  the frame, the mirror image of `Description.Extra`. It replaces six
  hand-written `if comp.X == ""` lines and the standing risk of
  forgetting one.

- **A conformance suite** over all four planes against one in-memory
  broker, pinning the cross-cutting invariants no single plane can
  check alone: that a config's advertised `state_topic` is the topic
  the state plane writes to, that its `command_topic` is the one the
  command plane subscribes to, that a reconnect restores all four
  planes consistently, and that a removal leaves no retained ghost in
  any tree. **Both gaps it found are fixed in this release**, and both
  were invisible to the planes individually.

- **`hacheck` and `hadoctor` reach the runtime layer**: a `unique_id`
  retained in both discovery forms (which Home Assistant refuses with
  only a `WARNING`, while both payloads validate and both publishes
  report success), a command topic something in the same capture also
  publishes to, an availability topic inside the discovery tree, and
  an advisory pass for retained availability payloads no config
  references.

### Fixed

- **One command ran the handler twice.** Reproduced against Mosquitto
  2.1.2 on both MQTT 3.1.1 and 5.0. The router resolved overlapping
  filters per *delivered copy* rather than per message, and two
  multiplications composed: a broker sends one copy per matching
  subscription, and the client then re-matches each copy against its
  whole local filter list. A `toggle` toggled twice and a relay pulse
  fired twice, with nothing logged.

  Overlapping filters are now **refused at registration**
  (`ErrAmbiguousRoutes`). Specificity cannot fix this in principle: a
  winner is chosen per copy, and no information in the router
  distinguishes N copies of one message from N genuine publishes.
  MQTT 5 Subscription Identifiers would correlate them but are v5-only
  and cannot be set through the client's current API, so a design
  resting on them is silently wrong on a v3.1.1 link. The cost is that
  command shapes must be pairwise disjoint and the discrimination
  moves into the handler — a cost the old scheme only appeared to
  spare, since it double-ran. The specificity machinery is removed
  rather than left unreachable.

- **The availability gate deleted its own index on a failed publish.**
  `last` is simultaneously the dedup gate, the topic list, the
  republish worklist and the sweep's ownership set — so a refused
  `offline` during a broker outage left a retained `online` that
  nothing could find or retract. All three gates now keep the last
  payload the broker accepted and merely decline to record a refused
  one, which is what lets the retry through.

- **A failed `Start` left a live, ungated subscription.** The rollback
  was best-effort and its failure path left the dispatch gate open, so
  commands executed against half-initialised dependencies while the
  consumer saw `Start` fail. A rollback that leaves anything behind now
  closes the router permanently and names every still-live filter.

- **A command could be dropped between the stop check and the
  enqueue.** Confirmed at 1–12 losses per 20,000 deliveries once both
  goroutines were released from one barrier — the earlier review could
  not reproduce it and correctly labelled it a suspicion rather than
  dismissing it.

- **An abandoned router leaked its worker goroutines** (ten discarded
  routers took a process from 2 to 82). The pool starts in `Start` now.

- **`$share/` filters registered cleanly and never fired.** A
  differential enumeration against `go-mqtt`'s fuzzed matcher found
  1,070 disagreements, all of this class. `ValidateFilter` also
  enforces §4.8.2's structure and §1.5.4 now.

- **`BundleStateTopics` omitted the availability list**, so
  `CheckDisjoint` was blind to the collision class it advertises:
  availability is rendered as a list of objects, and the extraction
  walked only the top level.

- **A specificity inversion** made an exactly-registered topic lose to
  `<that topic>/#`; moot now that the machinery is gone, and the
  reproducer is a refused-registration test row.

- **`EvictPrefix` was case-sensitive and reported a typo as success**
  — `(0, nil)` is indistinguishable from "nothing to clear", so a
  mis-cased address left a removed device's whole retained state
  standing.

- **`Reset` then `Republish` published nothing**, because the first
  cleared the index the second walks. `Reset` now opens the gate and
  keeps the index, and both planes share one shape, so one reconnect
  handler needs one idiom.

- **Guard coverage was inconsistent across the three planes** — three
  answers to one question. Every write goes through the guard now.

- **Seven doc comments asserted things the code did not do**, five of
  them found by the review. In a codebase whose comments carry
  measurements, a false one is worse than none: the next author trusts
  it.

- **Five tests could not fail**, proven by mutating the production code
  and watching them pass — including the one standing exactly where the
  availability-gate defect lived, whose sole assertion was
  arithmetically unfalsifiable. All five are replaced and
  mutation-verified, and the fixtures can now inject transport failures,
  which several error paths previously made untestable by construction.

### Changed

- **`gomqtt.Transport` implements `publisher.NoLocalSubscriber`**, so
  the broker does not deliver a consumer's own publishes back to it.
  That removes the self-echo class for the process itself.
  `CheckDisjoint` remains load-bearing: No Local is MQTT 5 only, a
  v3.1.1 link ignores the option silently, and it says nothing about a
  second process publishing into the same tree.


### Added

- **`model.Description.NameNull`** — `name: null` as a statement a
  description can make.

  Home Assistant's `entity.py` reads
  `config.get(CONF_NAME, UNDEFINED)`: an explicit null comes back as
  None and becomes the entity's name, an absent key comes back
  UNDEFINED and makes it derive one from the platform. An empty
  `model.Localized` is the absence of an opinion, so the pipeline
  dropped the key and only the second was reachable — three planes
  (notify, channel aggregate, per-datapoint) set
  `discovery.Component.NameNull` after rendering to get the first,
  two of them by re-deriving it from the component they had just been
  handed.

  It wins over `Name` and `NameKey`, the same precedence
  `Component.NameNull` already has over `Fields` and `Extra`, so an
  enricher can state it after a catalogue default filled in a name.
  The zero value goes on meaning "no opinion", and the null is
  projected only onto the 30 platforms whose schema declares `name` —
  not device_automation, not tag.

- **`model.Description.CommandTemplate`** — projected onto the 16
  platforms that declare the key.

  It existed on `discovery.Component` only, so the per-datapoint plane
  and the notify plane each opened a `discovery.Builder` for this one
  key, on entities that need nothing else platform-specific. climate,
  cover, light and water_heater declare no such key — they spell a
  template per role — and get none. The projection is deliberately not
  gated on a command topic beside it: a `Builder` runs afterwards and
  is often what names that topic, so gating would drop the template
  for exactly the entities that set both.

- **`topic.PulseKind`, `topic.PulseLayout`, `topic.PulseTopic`,
  `topic.Default.Pulse`** — the topics an occurrence is published on:
  a per-datapoint event, a channel event aggregate, an impulse and a
  channel device error.

  `topic.Layout` named state, command, availability and bridge. A
  consumer reaching `publisher.StatePublisher.Pulse` — which takes a
  plain topic string — formatted the other four itself, outside the
  one package that knows its topic tree.

  **A capability interface, not a fifth `Layout` method.** Both forms
  work and the breaking one was on the table; this is the module's own
  extension mechanism, and the only form under which the five
  consumers that publish no pulses need no change at all. A fifth
  method breaks all six layouts at once, including those with nothing
  to return, and an embedded default answers for them with a
  deterministic topic nobody subscribes to — worse than a compile
  error, because it looks like an answer. `PulseTopic` gives the
  declining case one spelling and invents nothing: an empty string
  means this layout names no such topic and must not be published to.
  `topic.Default` implements it, so a consumer with no opinion
  inherits the shapes.

- **`discovery.PresetModeTemplates`, `discovery.EnumTemplates`,
  `discovery.JinjaQuote`** — the `preset_mode` Jinja pair, emitted
  from a `model.Enum`.

  `Enum` already pairs a code the device speaks with the label a
  person reads, `Enum.Options` already emits the `preset_modes` list
  and `Enum.Code` is already the reverse lookup — but the climate
  plane hand-rolls both round-trip dictionaries as string formatting
  beside them. Both are now built from one enum in one pass, so they
  cannot disagree about which label belongs to which code; the
  reference implementation's index-aligned slices could, and a reorder
  of one renamed a preset in one direction only.

  The emitted bytes match that plane's pinned payload exactly — key
  order, quoting, the `is not none` guard, both `m.get` fallbacks —
  verified against the `climate/thermostat` fixture of its byte-pinned
  aggregate golden. That payload is already retained on brokers, so a
  divergence would move a published payload.

- **`discovery.RenderFrame`** — `RenderComponent` merged *under* a
  component whose own keys are authoritative.

  The combined-projection plane receives a finished `Component` built
  outside the model, where every display key is the projection's, and
  needs only the frame: device, origin, state topic, availability,
  unique id. It rendered that frame and gap-filled by hand — six
  `if comp.X == ""` lines, with nothing to catch the seventh when a
  key is added here.

  The precedence is stated once on the function: the given component
  always wins, and only a key it leaves at its zero value is taken
  from the frame. That is the mirror image of `Description.Extra`,
  which is applied last and overrides everything. A nil slice is "no
  opinion", an empty one is a statement and is kept. `Extra` merges
  key by key into a new map, so one key set by the projection loses
  neither the frame's other keys nor the caller's own map. The fill is
  reflective, so a key added to `Component` is carried on the day it
  is added.

### Notes

- Nothing under `publisher/` changed.
- No exported signature moved, and no default changed: every addition
  is reachable only by setting a field or calling a new function.

## [0.25.0] - 2026-09-12

The publisher runtime ADR 0070 scoped from the start and phase 2
left out: "a types-only library would leave the publish loop,
availability policy and orphan sweep duplicated six times." Four
pieces, each taken from what the first full consumer already ships
and each carrying the measurement that decided it.

### Added

- **`publisher.Runtime`** — the retained-discovery state of one
  consumer process, over a `publisher.Transport` (publish retained,
  subscribe, unsubscribe) declared here rather than
  `github.com/SukramJ/go-mqtt` itself.

  The interface is narrow and speaks plain bytes because the
  consumers wrap their client differently — one publishes through a
  circuit breaker and subscribes around it — so a concrete client
  type would fit none of them, and because the whole runtime has to
  be exercisable without a broker. `publisher/gomqtt` adapts a
  go-mqtt client in one call; it is the only package in the module
  that imports the transport.

  Two locks, ordered `sweepMu → mu`, stated on the type the way
  go-mqtt's `TCPClient` states its own. `mu` is never held across a
  transport call: a publish blocks on a broker acknowledgement, and
  holding the claim lock across it would stall every other publisher
  and the sweep behind one slow PUBACK.

- **Hash-dedup retained publish** — `Runtime.Publish`,
  `Runtime.PublishBundle`, `Runtime.PublishComponent`, all reporting
  whether anything reached the broker.

  A steady-state restart re-renders every entity a consumer drives
  and every payload is byte-identical to the one the broker already
  holds — on the measured fleet, nine thousand writes that change
  nothing, each re-read and re-validated by Home Assistant.
  Comparing the bytes turns that into zero.

  The store keeps the bytes rather than a digest of them, because
  the birth resync replays them and a digest would force a consumer
  to re-render its whole fleet to answer a question the runtime
  already knows. It survives reconnects: it is process state, not
  connection state. Only what the broker accepted is recorded —
  caching an attempt that failed would make the next identical
  payload hit the dedup gate, leaving the entity absent until the
  operator restarts the daemon.

- **Retract-then-publish, in both directions** —
  `Runtime.Retract`, `publisher.SupersededTopics`, and the ordering
  built into the two publish paths.

  Measured against a live Home Assistant 2026.9 instance on
  2026-09-10: publishing a device bundle while a per-entity config
  for the same `unique_id` is still retained is refused, and the
  entire signal is `WARNING [mqtt.entity] Received a conflicting
  MQTT discovery message`. The bundle sits retained on the broker,
  the entity keeps its old config, and nothing reports that the
  migration did not happen. The refusal is symmetric — measured
  again on 2026-09-11 with the topics named the other way round — so
  `PublishComponent` retracts the device document first for exactly
  the same reason. `migrate_discovery: true` was tried and did not
  lift the conflict.

  A failed retraction aborts before the publish rather than pressing
  on. Between the two the entity does not exist — absent, not merely
  unavailable — and having lost the old config and then failed to
  write the new one is the one outcome worse than not having
  started.

- **`Runtime.Sweep`** — the orphan pass over the retained discovery
  tree, with `publisher.ParseConfigTopic`, `ConfigTopic`,
  `SweepRequest`, `SweepResult` and `ErrSweepUnscoped`.

  A retained config outlives the build that wrote it: drop an entity
  and the broker keeps handing Home Assistant the old payload
  forever, which re-creates it as a permanently unavailable phantom
  on every integration restart. Operators ran a shell script by
  hand.

  It recognises **both** topic forms — four segments for
  `<prefix>/<platform>/<node>/<object>/config`, three beginning with
  the literal `device` for `<prefix>/device/<node>/config`. `device`
  is not a platform name and the closest ones, `device_automation`
  and `device_tracker`, produce four segments anyway. Matching only
  the per-entity form made every device document invisible: never
  inspected, never cleared, retained by a broker no consumer would
  ever claim it from again.

  `SweepRequest.Owns` is required, and its absence is
  `ErrSweepUnscoped` rather than a default. A discovery prefix is
  shared — a parallel zigbee2mqtt publishes documents into the same
  tree — and a sweep that guessed would clear every other
  integration's entities. `SweepResult.Inspected` is reported beside
  the retractions because zero inspected ("the window saw nothing of
  ours") and zero retracted ("it saw everything and nothing was
  orphaned") are different faults that look identical in a log line
  carrying only the second number.

- **Birth, will and the resync** — `Runtime.Will`,
  `AnnounceOnline`, `AnnounceOffline`, `WatchBirth`, `Republish`,
  `Close`, `BirthTopic`, `BirthPayload`/`DeathPayload` and
  `ErrNoStatusTopic`.

  The will is returned as data, not applied: it belongs to CONNECT
  and therefore to the client the consumer builds. Handing it over
  is what makes the two halves agree by construction — the same
  topic and the same two payloads the announcements use. Two
  reference bridges configure a will no published entity references,
  so a hard crash writes "offline" where nothing reads it and every
  entity stays available forever, showing the last value it ever
  saw. An empty `Config.StatusTopic` is `ErrNoStatusTopic` and
  publishes nothing, rather than quietly reproducing that.

  `WatchBirth` replays every declared config on
  `<prefix>/status: online`, including the retained delivery at
  subscribe time — Home Assistant publishes its status retained, and
  a consumer that connects afterwards gets no other signal. The
  replay runs on a worker, not on the read loop: it is one blocking
  retained publish per declared topic, each waiting on an
  acknowledgement only that same read loop could deliver, so inline
  it is a self-deadlock on the first birth message. A burst
  collapses onto one pending job, since every replay is idempotent.

### Notes

- Additive throughout. Nothing in `discovery`, `model`, `topic` or
  `catalog` changed, and no exported signature moved.
- `github.com/SukramJ/go-mqtt v1.4.0` joins `go-ha-catalog` in
  `go.mod`, imported by `publisher/gomqtt` only.
- One deliberate deviation from the reference implementation: a
  superseded topic is retracted once per process rather than on
  every change of the document. After the first retraction the
  broker holds nothing there, so a device rewritten forty times
  during a boot would otherwise send forty rounds of retractions for
  nothing.
- Not included: batching (`BeginBundleBatch`/`FlushBundles`), which
  is a scheduling policy the consumer owns; the validity counter the
  reference implementation raises per component, which belongs with
  its metrics; and any offline queue — a publish on a disconnected
  client fails, exactly as it does in `go-mqtt`.

## [0.24.0] - 2026-09-12

The four measured model gaps left after migrating openccu-loom's hub
and message planes: every place a plane had to work *around* the model
instead of *through* it, counted as an escape hatch and traced back to
the field that was missing. Twelve hatches, four causes.

### Added

- **`model.LevelNone` and `model.NoAvailability()`** — the explicit
  absence of availability: no `availability` list and no
  `availability_mode`, which is not the same as saying nothing.

  One measured entity needs it. A bridge's daemon-status sensor
  publishes on the bridge LWT itself, so gating it on that topic makes
  it unavailable in exactly the situation it exists to report. Before
  this, `Availability.Resolved` answered every entity with a mode, so
  even an empty level list projected `availability_mode: "all"` beside
  an absent list — and the consumer cleared both fields again in a
  post-render `Builder`, the only hatch of its plane that was not
  platform vocabulary.

  It is a level rather than an explicitly-empty `Levels` slice because
  nil and empty are the same thing at every call site that builds a
  list conditionally, and a rule table that filtered its levels down
  to none would then silently mean "none" where it means "the
  default". `LevelNone` wins over anything listed beside it, so the
  payload does not depend on the order two rules happened to run in.
  The zero `Availability` is unchanged: bridge and device, mode `all`.

- **`model.Description.NameArgs`** — the arguments that fill the
  `{name}` placeholders of the string `NameKey` resolves to, and of a
  literal `Name` that carries any.

  Two measured entity families (install-mode and per-interface
  connectivity) name themselves after an interface id. With no
  parameterised translation the name had to be resolved eagerly and
  handed over as a literal `Description.Name`, which bypasses the
  whole `NameKey` path: the catalogue key never reached the model, so
  nothing downstream could render it in another language.

  A named map rather than positional arguments, because the
  placeholder is written in the catalogue, by a translator, in a
  sentence whose word order is not the source language's — a
  positional `%s` cannot be moved by the person who has to move it.

- **`discovery.Substitute`** — the placeholder substitution
  `StdContext.Translate` applies, exported for a consumer that
  overrides `Translate` and would otherwise write it again. A
  placeholder with no matching argument is left standing rather than
  blanked: "Connectivity {iface}" says where the gap is.

- **`model.Description.Optimistic`** — `optimistic` as a typed
  description field, projected onto the 17 platforms whose schema
  declares it (switch, select, text and number among them; not sensor
  or binary_sensor). It is not platform-specific vocabulary, but it
  was reachable only through a `Builder` or `Extra`, which is why
  eight measured hub entities set it after rendering.

- **`model.Description.JSONAttributesTopic`** and
  **`.JSONAttributesTemplate`** — the attributes pair 30 of the 32
  platforms accept (all but `device_automation` and `tag`), the same
  bar the description's other `Component`-level keys meet. Three
  measured message aggregates published their detail rows through a
  post-render `Builder` for want of it. The template is projected only
  beside the topic: alone it selects a field of a document Home
  Assistant was never told to read.

### Changed

- **`discovery.Context.Translate` takes arguments** (breaking
  interface change):

  ```go
  Translate(key string) string                          // 0.23.0
  Translate(key string, args map[string]string) string  // 0.24.0
  ```

  A custom `Context` adds the parameter and passes it to whatever it
  resolves with; a caller passes `nil` for the unparameterised case,
  which behaves exactly as the one-argument form did.
  `StdContext.Translator` is **unchanged** — it stays a plain
  key -> string lookup, because the catalogue answers with the
  template as authored and the substitution is `StdContext`'s job. A
  consumer whose translator already exists therefore keeps it and
  gains the parameters for free, which is what the measured consumer's
  own pairing of a lookup plus a placeholder helper collapses to.

- **`Description.Clone` copies `NameArgs`**, like every other map an
  enricher may write.

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
