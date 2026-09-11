# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Commands

```sh
make check        # vet + fmt-check + lint + race tests — the gate
make cover-check  # per-package coverage gate (COVER_MIN=80)
go test -run TestName ./...
```

No third-party dependencies. The only requires are `go-ha-catalog` (Home
Assistant's vocabulary) and, once the runtime lands, `go-mqtt`.

## What this repository is

The shared data model and Home Assistant discovery layer for the `go-*2mqtt`
family, extracted per openccu-loom ADR 0070. Phase 2 delivered the model,
the bundle, validation and naming; the publisher runtime is phase 4.

openccu-loom is the architectural source **and** the first full consumer. When
a design question here has no obvious answer, loom's existing implementation is
the tiebreaker — except where it is demonstrably weaker, which is documented
below.

## Layout and the rule it enforces

```
model/      the semantic layer: Device, Identity, Entity, Slot, Binding, State,
            Description, and the capability interfaces
payload/    struct-tag partitioning; no domain knowledge at all
topic/      the ONLY place that turns a Slot into a string
discovery/  the device bundle, the render pipeline, the validator
catalog/    rules as an Enricher, a static table as an EntitySource
cmd/        the tools: hacheck validates payloads one at a time,
            hadoctor reads a whole capture and sees between them
internal/   dump/ reads an NDJSON capture; shared by the tools only
```

Dependencies point strictly downward:
`discovery → {topic, model} → go-ha-catalog`, and `catalog → model`.

**Nothing in `model` imports `topic`.** That is not a style preference — it is
what mechanically prevents the model from formatting a topic, which is what
lets six projects with six topic schemas share one model. A change that makes
`model` depend on `topic` breaks the whole premise.

## Decisions that are settled

- **Device-based discovery only.** No per-entity form. Recorded in ADR 0070.
- **One precedence rule**: `Description → enricher chain → default projection
  → Builder → Extra`, later winning. loom had two builder interfaces with
  *opposite* precedence; collapsing them was a precondition for extraction.
- **One `Description` type.** loom had three overlapping ones with a converter
  between two and a chain applying both table systems in sequence.
- **Envelope encoding by default** on state topics. It is the only shape that
  carries per-datapoint availability, which is what `LevelSelf` resolves
  against.
- **Availability is a list of levels**, and the connectivity-sensor case is
  expressed by *omission* (`BridgeOnly()`), not by an opt-out flag.
- **v0.x** until loom and the first bridge run on this. Go cannot express a
  SemVer major without a `/vN` path change.

## Things that will bite

- **`object_id` is not a legal discovery key.** Home Assistant replaced it with
  `default_entity_id` and drops unknown keys silently. All six reference
  implementations still publish it and get nothing for it. Do not add it back
  because a bridge's code has it.
- **`DeviceClass` is a plain string, deliberately.** Home Assistant declares a
  separate device-class enum *per platform* — "energy" is a sensor class,
  "garage" a cover class — so a single typed field would have to pick one and
  be wrong for the rest. `discovery.Validate` checks it against the entity's
  actual platform.
- **Units are untyped for the same reason** the catalog does not generate them:
  the values are symbols like `°C` and `µg/m³`. Home Assistant silently
  rewrites some spellings (most importantly the micro sign U+00B5 to U+03BC)
  and discards a config that disagrees. The validator catches it.
- **Suppression hides the entity, not the datapoint.** The suppressed slots
  must keep publishing, because the suppressing entity's discovery references
  those topics.
- **`UniqueID`'s namespace must be a constant of the bridge.** One reference
  bridge derived it from the configurable MQTT root, so changing the root
  orphaned every entity at once — and the cleanup sweep could no longer
  recognise the old topics either.

## Conventions

- Every `.go` file starts with:
  ```go
  // SPDX-License-Identifier: MIT
  // Copyright (C) 2026 go-hamqtt authors.
  ```
- gofumpt; `golangci-lint` v2 with the shared config.
- Tests are table-free where a table would obscure the point, stdlib-only, and
  each pins *a fact* with a comment saying why it matters. The composite
  climate entity in `discovery/climate_test.go` is the design's litmus test:
  it implements four capability interfaces and must need no special case
  anywhere in the pipeline.
