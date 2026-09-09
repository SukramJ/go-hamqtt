# Changelog

All notable changes to this project are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
