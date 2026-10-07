# Open items

What is known to be unfinished as of `v0.37.0` (2026-10-07), across this
module and its six consumers (openccu-loom, go-mtec2mqtt, go-zendure2mqtt,
go-homeconnect2mqtt, go-daikin2mqtt, go-unifi2mqtt), in one place. Most of it
is left over from the migration of all six to the mqtt-smarthome 2.0
convention ([openccu-loom ADR 0083][adr0083]). Each entry names where the
detail lives; this page does not repeat it. When an item is closed it is
removed here in the same change.

Scope: items that concern this module or more than one consumer, plus the
per-consumer leftovers of the same wave. A statement about a consumer's tree
is a measurement of its `origin/main` on 2026-10-07, not a standing claim
(see "How this file talks about consumers" in [`CHANGELOG.md`](../CHANGELOG.md)).
The non-goals of [ADR 0070][adr0070] and [ADR 0083][adr0083] are not open
items.

## 1. Release wave

| Item | Where | What |
| --- | --- | --- |
| **Acceptance against a running she instance** | [ADR 0083][adr0083], "Migration" ("Order of the wave"); "Not in this PR" of go-mtec2mqtt #65 and go-zendure2mqtt #55 | ADR 0083 requires one bridge to be verified against a running she instance before the wave ships: inventory row, restart (she sends it with an empty payload, amendment item 1), log level, stats, wipe. The wave shipped without it: go-hamqtt 0.36.0/0.37.0, go-mtec2mqtt 2.0.1, go-zendure2mqtt 0.10.1, go-daikin2mqtt 0.14.1, go-unifi2mqtt 2.0.1, go-homeconnect2mqtt 0.15.2; openccu-loom 0.89.0 is in preparation. |
| **she manages a subset of names, and its wipe misses two functions** | [ADR 0083][adr0083], amendment item 3 | she offers only instance names matching `[A-Za-z0-9_.-]+` (a multi-level name is not offered at all). Its wipe clears `<name>/status/…`, `<name>/connected`, `<name>/info` and `<name>/maintenance/…`, not `<name>/meta/…` or `<name>/ha/…`. A she-side limitation; the follow-up for loom's `meta` and `ha` trees is on loom's roadmap. |
| **`lc` across restarts** | [ADR 0083][adr0083], "Status payload" (Known limitation) and amendment item 10 | After a process restart `lc` is the first observation unless the adapter reads its own retained value back first. No consumer and no helper here does that yet. |

## 2. Validation and containment

| Item | Where | What |
| --- | --- | --- |
| **Consumers withhold on `Validate`, not on `Inspect`/`Contain`** | [`CHANGELOG.md`](../CHANGELOG.md) 0.37.0, "Added"; README "Validation and containment" | `discovery.Inspect` and `discovery.Contain` (and `publisher.Config.Contain`) say what Home Assistant would accept; `Validate` is frozen and all-or-nothing. All six consumers pin v0.36.0. go-homeconnect2mqtt contains with its own code (`Discovery.validateBundle` in `internal/hass/discovery.go`, `reconcileSensorClasses` in `internal/hass/payload.go`), which the shared containment replaces. |
| **Hand-kept tables that belong in go-ha-catalog** | `discovery/inspect.go`: `nonEntityPlatforms`, `configCategoryRefused`, `availabilityEntryKeys`, `numberMinStep`; [`CHANGELOG.md`](../CHANGELOG.md) 0.37.0, "Notes" | The non-entity platforms, the platforms refusing `entity_category: config`, the four availability-entry keys and the number step minimum are cited to core and kept here because go-ha-catalog v0.3.0 has no table for them. They move once the catalog carries them. |
| **go-ha-catalog's `device_automation` schema** | [`CHANGELOG.md`](../CHANGELOG.md) 0.37.0, "Notes" | The schema carries only `automation_type`, so `Validate` and `Inspect` both report `topic`, `type` and `subtype` as unknown keys on a `device_automation` component. A catalog fix. |
| **Add-time and state-time rules read, not run** | go-hamqtt #43, "Known limits"; the comments at the top of `discovery/inspect.go` | The rules that are not schema rules — `entity_category` on sensor/binary_sensor, a sensor's numeric expectation, a duplicate `unique_id` — were classified by reading core 2026.10.0b2, not by running it. The schema-level classification was exercised against core's real schemas. |

## 3. Shared helpers not yet adopted

| Item | Where | What |
| --- | --- | --- |
| **`IsReservedFunction` for guards that should include `ha`** | [`CHANGELOG.md`](../CHANGELOG.md) 0.37.0, `topic.FunctionHA`; [ADR 0083][adr0083], amendment item 7 | `topic.IsFunction("ha")` stays false on purpose: go-unifi2mqtt refuses to start on a site segment `IsFunction` reports (`coordinator.CheckTopics`), so widening it would stop an installation whose site is `ha`. A guard that should cover the adapter function calls `IsReservedFunction`. Measured 2026-10-07: no consumer calls it (all pin v0.36.0); the five bridges' migration-sweep and naming guards call `IsFunction`, and openccu-loom checks `ha` locally (`naming.IsFunction`). Whether a given guard needs `ha` depends on whether that project publishes under it — today only loom does. |
| **`StdContext.AvailabilityFrom`** | [`CHANGELOG.md`](../CHANGELOG.md) 0.37.0, "Added" | Added for a tree that keys the `online` item on a bare serial or MAC. Measured 2026-10-07, no consumer uses it: go-zendure2mqtt (`harender.Context.Availability`) and go-unifi2mqtt (`hamqttContext.Availability`) override `Availability` to resolve the device level from the entity's first binding; go-daikin2mqtt appends its device entry in its builder (`smartHomeAvailability` in `internal/hass/hamqtt.go`). |

## 4. Per-consumer leftovers of the wave

| Item | Where | What |
| --- | --- | --- |
| **go-mtec2mqtt: numeric `set` truncates and rejects** | go-mtec2mqtt #65 (2.0.0), "Not in this PR" | Spec §5.3 asks for round-and-clamp; the register catalog defines no ranges to clamp to. |
| **go-zendure2mqtt: battery-pack `set` is not routable** | go-zendure2mqtt `docs/adr0070-pilot-measurement.md`, F3; doc comment of `coordinator.CommandFilter` | Unchanged by 0.10.0: a six-level pack `set` item does not match the command filter. Latent while no pack property is writable. |
| **go-unifi2mqtt: no device-based discovery** | [ADR 0070][adr0070], "Closing (2026-09-14)", "Phase 9 declined its final step" | Two consoles cannot be told apart on one broker; bundles need a per-console identity the bridge does not have. |
| **go-mtec2mqtt, go-zendure2mqtt: catalogs from field logs** | go-mtec2mqtt `changelog.md` 2.0.1; go-zendure2mqtt `changelog.md` 0.10.1 | A code the catalog does not map is now logged once (`coordinator.unmapped_value`). The catalogs are to be extended from what those logs report in the field. |

[adr0070]: https://github.com/SukramJ/openccu-loom/blob/main/docs/adr/0070-shared-ha-discovery-model-module.md
[adr0083]: https://github.com/SukramJ/openccu-loom/blob/main/docs/adr/0083-mqtt-smarthome-topic-convention.md
