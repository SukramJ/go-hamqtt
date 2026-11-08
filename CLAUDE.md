# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Commands

```sh
make check        # vet + fmt-check + lint + race tests — the gate
make cover-check  # per-package coverage gate (COVER_MIN=80)
go test -run TestName ./...
```

No third-party dependencies. The only requires are `go-ha-catalog` (Home
Assistant's vocabulary) and `go-mqtt`, and the latter is imported by
`publisher/gomqtt` alone — the runtime itself takes a transport interface of
its own so it stays testable without a broker.

## What this repository is

The shared data model, Home Assistant discovery layer and publisher runtime
for the `go-*2mqtt` family, extracted per openccu-loom ADR 0070. Phase 2
delivered the model, the bundle, validation and naming; phase 4 added
`publisher`.

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
discovery/  the device bundle, the render pipeline, the validator,
            Inspect (scoped findings) and Contain (what HA would accept)
catalog/    rules as an Enricher, a static table as an EntitySource
publisher/  the publish loop: hash-dedup, retract-then-publish, the orphan
            sweep over both topic forms, birth/LWT availability
cmd/        the tools: hacheck validates payloads one at a time,
            hadoctor reads a whole capture and sees between them
internal/   dump/ reads an NDJSON capture; shared by the tools only
```

Dependencies point strictly downward:
`discovery → {topic, model} → go-ha-catalog`, `catalog → model`, and
`publisher → discovery`.

**Nothing in `model` imports `topic`.** That is not a style preference — it is
what mechanically prevents the model from formatting a topic, which is what
lets six projects with six topic schemas share one model. A change that makes
`model` depend on `topic` breaks the whole premise.

## Decisions that are settled

- **Device-based discovery is the recommendation, not the only output.**
  That decision (ADR 0070) predates this module having a consumer, and the
  first full one publishes the per-entity form and cannot switch: its
  retained configs are on brokers, and Home Assistant refuses a bundle while
  a per-entity config for the same entity is still retained. `Render`
  produces bundles; `RenderComponent` produces one standalone config with
  the frame attached. A new consumer should take the bundle — one retained
  document per device, updated atomically.
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
- **Home Assistant does not refuse a device document for a bad component.**
  It refuses the whole document only over `device`/`origin` and a
  component's `platform`/`unique_id`; everything else costs one entity, and
  unknown keys are stripped. `Validate` is all-or-nothing and FROZEN — a
  stricter answer would newly withhold documents in six consumers;
  `TestValidateIsUnchanged` compares it with the v0.36.0 copy. New rules go
  into `Inspect` (each with a core file:line), and `Contain` is the
  publish-path answer. A withheld component must never become a tombstone:
  a platform-only entry deletes the entity and its registry entry.
- **Suppression hides the entity, not the datapoint.** The suppressed slots
  must keep publishing, because the suppressing entity's discovery references
  those topics.
- **`UniqueID`'s namespace must be a constant of the bridge.** One reference
  bridge derived it from the configurable MQTT root, so changing the root
  orphaned every entity at once — and the cleanup sweep could no longer
  recognise the old topics either.

## Which model does which work

Set by the owner for the go-fabric family on 2026-10-10 (go-fabric's
`CLAUDE.md`); the review split of 2026-11-08 (go-fabric #456) likewise.
Adopted here on 2026-11-08. A statement about Home Assistant MQTT discovery that
lands in this repo is cited later as fact — a comment that names a caller
or a consumer, a docstring that states what the other side does, a
`CHANGELOG.md` line — so the work that produces such statements gets
the strongest model, and the work a guard checks may use a cheaper one. The
agents in [Sub-Agent Delegation](#sub-agent-delegation) carry a default
model each; the rows below decide when a call overrides it (`model:` on the
Agent tool). Implementation defaults to Opus.

| Work | Model |
| --- | --- |
| Analysis, planning, reviews of design, audits; anything written into `CHANGELOG.md`, a settled decision in `CLAUDE.md` or a comment that cites a core line, or a comment that names a Home Assistant core discovery behaviour; the public API of `publisher/`, `discovery/` and `payload/` that every `go-*2mqtt` consumer and openccu-loom compile against; *which* guard gets built and its bite line; rebases and merge conflicts where `main` may have overtaken the change | **Fable** (the default for this repo; also the `hunt` agent) |
| Implementation of a finished plan with guards behind it: a discovery field once the catalogue carries it, a publisher test on an existing pattern, a `CHANGELOG.md` entry for a finished diff | **Opus** (the `impl` default; also what fast mode runs on) |
| **Review of a slice's pull request, split (owner decision 2026-11-08, go-fabric #456).** The code review — correctness, concurrency, error paths, tests, the fixed report shape — is **Opus**. On top of it **Fable** reads, bounded: every new comment or docstring that names a Home Assistant core discovery behaviour against Home Assistant core at the file:line cited, go-ha-catalog at its tag, openccu-loom as the tiebreaker, every line that enters `CHANGELOG.md`, a settled decision in `CLAUDE.md` or a comment that cites a core line, the paths of the first row, and the brief's decisions against the reference — the behavioural gaps a code review does not see. The load-bearing claims of the Opus review are verified before they reach the implementer. | **Opus**, then **Fable** |
| Work a test or generator checks immediately: `make generate` (`discovery/gen_fields.go`), `gofumpt`, a table case; search and first reading of a tree whose result a stronger model or the owner reads again | **Sonnet** (the `guard` default; `impl` with `model: "sonnet"`) |
| Mechanical inventories whose answer is a list the caller can re-grep: which files reference X, which links are dead, which entries lack a counterpart | **Haiku** (`sweep` default) |

Sonnet never writes a claim about a Home Assistant core discovery behaviour into the repo, and
never touches the paths named in the first row. Haiku returns lists, never
verdicts. When in doubt about which row a task is in, it is in the first.

---

## Sub-Agent Delegation

As go-fabric's and openccu-loom's rule (owner, 2026-10-10). The main
conversation owns planning, contract and wire decisions, guard
specifications, sub-agent steering, and final verification. It does not own
typing.

**Delegate what a gate can prove; keep what only a reader can accept.**
Delegate freely: table cases, fakes, scaffolding, fixture updates, doc and
link cleanups, version bumps across the changelog set, inventories and grep
sweeps. Never delegate: the public API, the discovery semantics decided against core, the generator behind `make generate`, and *which* guard gets
built. Locating is delegable; reading is not.

| Agent | Default model | Use for |
|---|---|---|
| `impl` | Opus | scoped implementation with a stated acceptance command |
| `guard` | Sonnet | tests from a caller-written guard spec, plus the bite proof |
| `sweep` | Haiku | read-only inventories and grep sweeps, high fan-out |
| `hunt` | Fable | adversarial read-only defect hunt, ranked candidates |

Each default is one row of
[Which model does which work](#which-model-does-which-work); a call that
falls into another row passes `model:` on the Agent tool — `impl` with
`model: "sonnet"` for work a generator or formatter checks immediately.

**Size CPU-bound fan-out from the host, never from a constant** — the owner
works from a 4-core box and a 14-core box:

```sh
cores=$(nproc); agents=$(( (cores - 1) / share ))   # share = cores per agent
```

Check the 1-minute load first; if it is at or above the core count, run one
CPU-bound agent and queue the rest. Eight concurrent agents is a practical
ceiling whatever the core count. Every CPU-bound agent must **pin** its share
in the command it runs (`GOMAXPROCS=<share> go test -p <share> ./internal/x/...`, `golangci-lint run --concurrency=<share>`) — handing out a number without
pinning it changes nothing. Read-only agents cost no core worth counting;
6–8 is a practical ceiling there.

Two hard rules: **no sub-agent runs the full gate (`make check`, `make cover-check` (COVER_MIN=80)) or a
repo-wide lint** — the full gate runs once, in the main conversation, at the
end of the slice — and **no two writing agents in the same package**
(disjoint file sets, or `isolation: "worktree"`).

Every brief carries five things: the files and public surface plus one style
reference; the acceptance command the agent runs itself; the invariants it
touches; **stop conditions** (new dependency, API or wire change, credential
handling, an ADR-shaped decision — a stop is a successful outcome); and a
report format ≤ 250 words, large artefacts to the scratchpad as a path.

**A report is a claim.** Read the diff, run the gate yourself. For `guard`,
the acceptance artefact is not a green test but the **bite proof**: the named
production line removed, the test observed red with its message, the line
restored, green again.

---

## Token use

Set by the owner on 2026-10-14 ("Wichtig ist mir eine effiziente Nutzung
der Token"), as go-fabric's rule of the same date. A slice costs on
the order of half a million to a million tokens per implementing agent
before the owner's review; the rules below are what keeps that from
doubling.

- **Read slices, not files.** `sed -n a,bp`, `grep -n` with a tight
  pattern and `head`; a whole file only when the task is the file. Never
  re-read what is already in context; cite the earlier read.
- **Delegate first reads.** A source tree's inventory (Home Assistant core, go-ha-catalog, openccu-loom, this repo)
  is a `sweep` or Sonnet read written to the scratchpad with `file:line`
  for every fact; the owner spot-checks the load-bearing lines and verifies
  every fact about Home Assistant MQTT discovery before it enters a brief or
  `CHANGELOG.md`, a settled decision in `CLAUDE.md` or a comment that cites a core line — the agent's result is a map, never a citation.
- **Logs by their summary.** The gate (`make check`, `make cover-check` (COVER_MIN=80)) runs in the
  background with a summary file (one line per gate, `FAIL`/`ok` lines
  only); read the summary, and open a log only on failure, with `grep` for
  the failure markers and the error strings — never `cat`. The same for CI:
  the job log is fetched once to a file and grepped.
- **One turn, every independent call.** Reads, checks and background
  starts that do not depend on each other go out together; a wait is a
  `Monitor` or an `until` loop in the background, not polling by hand.
- **Short artefacts.** A brief names files, decisions and the acceptance
  command, not prose; a report follows the fixed shape of
  [Sub-Agent Delegation](#sub-agent-delegation) (≤ 250 words, large
  artefacts as a scratchpad path); status messages to the owner are short,
  outcome first, in German.
- **Skip what cannot have changed.** No regeneration for a comment change;
  no second gate run for a notes-only commit — say what was skipped and
  why.
- **Edit by script.** Several replacements in several files are one
  scripted call with asserted anchors, not a chain of interactive edits;
  a generated file (`discovery/gen_fields.go`) is regenerated, never patched.
- **Agents read what the brief names.** An implementing agent gets the
  file list and the pinned sources in the brief, reads those, and reports
  in the fixed shape; it does not survey the tree. One agent per brief;
  two agents only on disjoint paths, at most one Opus agent at a time.
- **Push, pull request and tag are the session's own calls.** An
  implementing agent works locally — branch, commits, guards — and
  reports; the owner's session reads the diff, runs the gate, pushes,
  opens the pull request and tags the merge commit; the owner merges. A
  delegated push, pull request or tag is refused by the auto-mode
  classifier ("Create Public Surface", 2026-11-07), and the review in
  [Which model does which work](#which-model-does-which-work) sits
  between the agent's report and the push anyway.

---

## Conventions

- Every `.go` file starts with:
  ```go
  // SPDX-License-Identifier: MIT
  // Copyright (C) 2026 go-hamqtt authors.
  ```
- gofumpt; `golangci-lint` v2 with the shared config.
- **No build artefacts in the tree.** `cmd/` is built with `make tools`
  into the ignored `bin/`. A compiled binary committed at the root ships in
  every `go get` of this module, on every platform, and a 3.6 MB one did
  until it was untracked — see the Unreleased CHANGELOG entry.
- **A CHANGELOG note about a consumer states a capability, not a
  diagnosis.** Never "repository X has defect Y at file F line L" in the
  present tense: this module cannot observe a consumer's tree, so such a
  claim ages the moment they fix it, and two already had to be corrected.
  Say what the guard is, what trap it closes, and how a reader tells whether
  it applies to them; keep measurement evidence in the past tense, dated and
  attributed to the measurement. Full rule at the top of `CHANGELOG.md`.
- Tests are table-free where a table would obscure the point, stdlib-only, and
  each pins *a fact* with a comment saying why it matters. The composite
  climate entity in `discovery/climate_test.go` is the design's litmus test:
  it implements four capability interfaces and must need no special case
  anywhere in the pipeline.
