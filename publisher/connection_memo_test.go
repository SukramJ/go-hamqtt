// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
)

// errBrokerGone is the connection dying under a publish.
var errBrokerGone = errors.New("connection lost")

// generationalFake is a fake transport that can say which connection it is
// on, which is the whole of [Generational].
type generationalFake struct {
	*fakeTransport
	gen atomic.Uint64
}

func newGenerationalFake() *generationalFake {
	f := &generationalFake{fakeTransport: newFake()}
	f.gen.Store(1)
	return f
}

func (f *generationalFake) ConnectionGeneration() uint64 { return f.gen.Load() }

// reconnect is what a consumer's connect hook does to the counter.
func (f *generationalFake) reconnect() { f.gen.Add(1) }

// TestTheRetractionsAreReSentAfterAReconnect is go-mtec2mqtt's shipped defect
// (its PR #54, F1) driven against this package.
//
// The measured numbers there were `retractions re-sent = 0, document
// published = true, configs still retained = 1`: the retractions were flushed
// to a dying socket at QoS 0, memoised as done, and the retry after the
// reconnect published the document into a tree still holding the per-entity
// configs. Home Assistant refuses that with one WARNING in its own log and no
// entities.
//
// Here the first attempt fails at the document, so the runtime holds a
// superseded memo and no declared document — exactly the state the retry
// starts from.
func TestTheRetractionsAreReSentAfterAReconnect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	legacy := "homeassistant/sensor/ccu_abc/temperature/config"
	document := "homeassistant/device/ccu_abc/config"

	f := newGenerationalFake()
	f.seed(legacy, []byte(`{"previous":"build"}`))
	f.failPublish = func(topic string) error {
		if topic == document {
			return errBrokerGone
		}
		return nil
	}
	r := New(f, Config{})

	if _, err := r.PublishBundle(ctx, testBundle()); err == nil {
		t.Fatal("the document publish was supposed to fail")
	}
	if n := len(f.retractions()); n == 0 {
		t.Fatal("the first attempt sent no retractions at all")
	}

	// The connection the memo describes is gone, and the broker never
	// applied what was written to it: the retained config is back.
	f.reconnect()
	f.seed(legacy, []byte(`{"previous":"build"}`))
	f.failPublish = nil
	f.reset()

	if _, err := r.PublishBundle(ctx, testBundle()); err != nil {
		t.Fatalf("retry: %v", err)
	}

	resent := f.retractions()
	if !slices.Contains(resent, legacy) {
		t.Fatalf("retractions re-sent = %v, want %q among them", resent, legacy)
	}
	if f.holds(legacy) {
		t.Fatal("the per-entity config is still retained, so Home Assistant refuses the document")
	}
	if !f.holds(document) {
		t.Fatal("the document was not published")
	}
	// Ordering, not just presence: a retraction after the document is a
	// retraction the document was refused in spite of.
	if idx := slices.Index(f.publishes(), legacy); idx < 0 || idx > slices.Index(f.publishes(), document) {
		t.Fatalf("the retraction must precede the document: %v", f.publishes())
	}
}

// TestAReconnectReopensTheDedupGate is the same contract for the ordinary
// publish path: a config whose bytes reached only a socket must be written
// again on the next connection, because the broker may hold nothing.
func TestAReconnectReopensTheDedupGate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	topic := "homeassistant/sensor/ccu_abc/temperature/config"
	payload := []byte(`{"a":1}`)

	f := newGenerationalFake()
	r := New(f, Config{})
	if sent, err := r.Publish(ctx, topic, payload); err != nil || !sent {
		t.Fatalf("first publish: sent=%v err=%v", sent, err)
	}
	if sent, err := r.Publish(ctx, topic, payload); err != nil || sent {
		t.Fatalf("within one connection the gate must hold: sent=%v err=%v", sent, err)
	}

	f.reconnect()
	if sent, err := r.Publish(ctx, topic, payload); err != nil || !sent {
		t.Fatalf("after a reconnect the same config must go out again: sent=%v err=%v", sent, err)
	}
	// And the gate closes again behind it, or every publish would write.
	if sent, err := r.Publish(ctx, topic, payload); err != nil || sent {
		t.Fatalf("the gate must re-arm after the reconnect write: sent=%v err=%v", sent, err)
	}
	// The payload survived, which is what [Runtime.Republish] and
	// [Runtime.Declared] need.
	if got := r.Declared(); !slices.Equal(got, []string{topic}) {
		t.Fatalf("declared %v, want the topic to survive the reset", got)
	}
}

// TestResetIsTheHandVersionOfTheSameThing pins the escape hatch for a
// transport that cannot implement [Generational]: the consumer calls it from
// its connect hook and gets the same two effects.
func TestResetIsTheHandVersionOfTheSameThing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	topic := "homeassistant/sensor/ccu_abc/temperature/config"
	payload := []byte(`{"a":1}`)

	f := newFake()
	r := New(f, Config{})
	if _, err := r.Publish(ctx, topic, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := r.PublishBundle(ctx, testBundle()); err != nil {
		t.Fatal(err)
	}
	f.reset()

	r.Reset()

	if sent, err := r.Publish(ctx, topic, payload); err != nil || !sent {
		t.Fatalf("Reset must reopen the dedup gate: sent=%v err=%v", sent, err)
	}
	if _, err := r.PublishBundle(ctx, testBundle()); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(f.retractions(), "homeassistant/sensor/ccu_abc/temperature/config") {
		t.Fatalf("Reset must forget the superseded set; retractions were %v", f.retractions())
	}
}

// TestATransportThatCannotSayIsUnchanged is the additive half: a transport
// that does not implement [Generational] keeps every memo for the life of the
// process, exactly as before this existed.
func TestATransportThatCannotSayIsUnchanged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	topic := "homeassistant/sensor/ccu_abc/temperature/config"
	payload := []byte(`{"a":1}`)

	f := newFake()
	r := New(f, Config{})
	if _, err := r.Publish(ctx, topic, payload); err != nil {
		t.Fatal(err)
	}
	if sent, err := r.Publish(ctx, topic, payload); err != nil || sent {
		t.Fatalf("the gate must still hold: sent=%v err=%v", sent, err)
	}
}

// TestOneConnectionRetractsOnce is the other half of the same contract, and
// it is the half that is easy to break while fixing the first: within ONE
// connection the superseded per-entity configs are cleared once, not on every
// rewrite of the document. A boot that rewrites a device forty times would
// otherwise send forty rounds of retractions.
func TestOneConnectionRetractsOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newGenerationalFake()
	r := New(f, Config{})

	b := testBundle()
	if _, err := r.PublishBundle(ctx, b); err != nil {
		t.Fatal(err)
	}
	first := len(f.retractions())
	if first == 0 {
		t.Fatal("the first document must clear the per-entity configs")
	}

	// A changed document on the same connection.
	comp := b.Components["temperature"]
	comp.StateTopic = "x/t2"
	b.Components["temperature"] = comp
	if _, err := r.PublishBundle(ctx, b); err != nil {
		t.Fatal(err)
	}
	if got := len(f.retractions()); got != first {
		t.Fatalf("retractions repeated within one connection: %d then %d", first, got)
	}
}
