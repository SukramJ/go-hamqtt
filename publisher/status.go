// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ErrNilStatusValue is returned when a nil value is published as a status
// object. `{"val":null}` renders as the string "None" through
// [discovery.StatusValueTemplate]; a value that is gone is an empty retained
// payload, which is [StatePublisher.Evict].
var ErrNilStatusValue = errors.New("publisher: nil status value, use Evict to clear an item")

// StatusObject is mqtt-smarthome 2.0's status object (spec §5.2), the payload
// [discovery.StatusObjectEncoding] points Home Assistant at:
//
//	{"val": 21.6, "ts": 1730385720123, "lc": 1730385720123, "hm": {…}}
//
// Exported for a consumer that publishes through something other than
// [StatePublisher] and needs the identical bytes; the publisher renders
// through it.
type StatusObject struct {
	// Val is the value as it would be published plain: a JSON boolean,
	// number, string, or a structured value.
	Val any
	// TS is when the value was observed, LC when it last changed, both in
	// milliseconds since the epoch. The spec requires both, and LC <= TS.
	TS, LC int64
	// ExtKey and Ext are the one project-extension key and its value. An
	// empty key or a nil value omits it.
	ExtKey string
	Ext    any
}

// JSON renders the object with its keys in the spec's order — val, ts, lc,
// then the extension — so two renderings of one object are the same bytes.
func (o StatusObject) JSON() ([]byte, error) {
	val, err := json.Marshal(o.Val)
	if err != nil {
		return nil, fmt.Errorf("publisher: marshal status value: %w", err)
	}
	var b bytes.Buffer
	b.WriteString(`{"val":`)
	b.Write(val)
	b.WriteString(`,"ts":`)
	b.WriteString(strconv.FormatInt(o.TS, 10))
	b.WriteString(`,"lc":`)
	b.WriteString(strconv.FormatInt(o.LC, 10))
	if o.ExtKey != "" && o.Ext != nil {
		key, err := json.Marshal(o.ExtKey)
		if err != nil {
			return nil, fmt.Errorf("publisher: marshal status extension key: %w", err)
		}
		ext, err := json.Marshal(o.Ext)
		if err != nil {
			return nil, fmt.Errorf("publisher: marshal status extension: %w", err)
		}
		b.WriteByte(',')
		b.Write(key)
		b.WriteByte(':')
		b.Write(ext)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// Observation is one reading of a status item, handed to
// [StatePublisher.PublishStatus] and [StatePublisher.PulseStatus].
type Observation struct {
	// Value becomes the object's `val`. Nil is refused with
	// [ErrNilStatusValue].
	Value any
	// At is when the value was observed — the device's own timestamp when
	// it reports one (spec §5.2). Zero means now, from [StateConfig.Clock].
	At time.Time
	// Ext is the value of [StateConfig.ExtensionKey]. Nil omits the key.
	Ext any
}

// statusMemo is what the status-object gate remembers about one topic: the
// rendered `val` and extension it compares, and the `lc` it carries forward.
type statusMemo struct {
	val, ext []byte
	lc       int64
}

// PublishStatus writes one retained status item as a status object and
// reports whether it reached the broker.
//
// The gate compares `val` and the extension, never `ts`: a sensor re-reporting
// 21.5 °C every ten seconds is one publish, not one per poll, which is the
// spec §3.2 rule an adapter "MUST NOT republish unchanged state". The
// extension is compared because it can carry state of its own — a project's
// per-item availability flag — whose change must reach the broker even when
// the value stands still.
//
// `lc` is tracked per topic and moves only when `val` does; `ts` is the
// observation's time. [StatePublisher.Republish] re-sends the cached object
// unchanged, original `ts` included, because the replay is the same
// observation re-delivered rather than a new one. After
// [StatePublisher.Reset] the next publish goes out once even if unchanged,
// with the new observation's `ts` and the remembered `lc`.
//
// The value memory lives with the index, so [StatePublisher.Evict],
// [StatePublisher.EvictPrefix] and [StatePublisher.Forget] drop it; the next
// observation after them starts a new `lc`. So does a process restart, which
// is the known limitation openccu-loom ADR 0083 records.
func (p *StatePublisher) PublishStatus(ctx context.Context, topic string, obs Observation) (bool, error) {
	if topic == "" {
		return false, errors.New("publisher: empty state topic")
	}
	if obs.Value == nil {
		return false, ErrNilStatusValue
	}
	if err := p.guard(topic); err != nil {
		return false, err
	}
	val, ext, err := p.marshalObservation(obs)
	if err != nil {
		return false, err
	}
	ts := p.observedAt(obs)

	p.mu.Lock()
	previous := p.published[topic]
	memo, known := p.status[topic]
	p.mu.Unlock()

	sameVal := known && bytes.Equal(memo.val, val)
	if previous.gated && sameVal && bytes.Equal(memo.ext, ext) {
		return false, nil
	}
	lc := ts
	if sameVal && memo.lc <= ts {
		lc = memo.lc
	}

	payload, err := StatusObject{Val: json.RawMessage(val), TS: ts, LC: lc, ExtKey: p.cfg.ExtensionKey, Ext: rawOrNil(ext)}.JSON()
	if err != nil {
		return false, err
	}
	if err := p.send(ctx, topic, payload, p.qos, true); err != nil {
		return false, fmt.Errorf("publisher: publish state %s: %w", topic, err)
	}

	// Recorded only once the broker accepted it, like every gate here.
	p.mu.Lock()
	p.published[topic] = cachedWrite{payload: payload, gated: true}
	p.status[topic] = statusMemo{val: val, ext: ext, lc: lc}
	p.mu.Unlock()
	return true, nil
}

// PulseStatus writes one occurrence as a status object that is not retained
// — an event, an impulse, a device error (spec §3.2: one-shot events MUST
// NOT be retained). The event type, or whatever the occurrence reports, is
// `val`; `ts` and `lc` are both the observation's time, because every
// occurrence is a change.
//
// Like [StatePublisher.Pulse] it has no gate and no memory: two identical
// keypresses are two events.
func (p *StatePublisher) PulseStatus(ctx context.Context, topic string, obs Observation) error {
	if obs.Value == nil {
		return ErrNilStatusValue
	}
	val, ext, err := p.marshalObservation(obs)
	if err != nil {
		return err
	}
	ts := p.observedAt(obs)
	payload, err := StatusObject{Val: json.RawMessage(val), TS: ts, LC: ts, ExtKey: p.cfg.ExtensionKey, Ext: rawOrNil(ext)}.JSON()
	if err != nil {
		return err
	}
	return p.Pulse(ctx, topic, payload)
}

func (p *StatePublisher) observedAt(obs Observation) int64 {
	if obs.At.IsZero() {
		return p.cfg.Clock().UnixMilli()
	}
	return obs.At.UnixMilli()
}

// marshalObservation renders the two compared halves once, so the gate and
// the payload cannot disagree about what the value was.
func (p *StatePublisher) marshalObservation(obs Observation) (val, ext []byte, err error) {
	val, err = json.Marshal(obs.Value)
	if err != nil {
		return nil, nil, fmt.Errorf("publisher: marshal status value: %w", err)
	}
	if p.cfg.ExtensionKey != "" && obs.Ext != nil {
		ext, err = json.Marshal(obs.Ext)
		if err != nil {
			return nil, nil, fmt.Errorf("publisher: marshal status extension: %w", err)
		}
	}
	return val, ext, nil
}

func rawOrNil(b []byte) any {
	if b == nil {
		return nil
	}
	return json.RawMessage(b)
}
