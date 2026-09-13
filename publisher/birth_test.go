// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestWillResolvesTheConfiguredQoS pins the one line of [Runtime.Will] a test
// using a configured level that equals its own wire byte cannot reach. The
// v0.27.0–v0.29.0 review measured the gap: replacing `r.qos` with
// `byte(r.cfg.QoS)` survived the suite, because the case in hand was QoS 2,
// where the raw cast is accidentally right.
//
// QoSAtMostOnce is 0x80 precisely so a deliberate QoS 0 cannot be spelled by
// omission, and it is the level go-zendure2mqtt's whole installed base runs
// at — so the raw cast would hand that consumer's client a will at QoS 128,
// which is not a QoS at all.
func TestWillResolvesTheConfiguredQoS(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		cfg  QoS
		want byte
	}{
		"deliberate at most once":   {QoSAtMostOnce, 0},
		"unset means at least once": {QoSUnset, 1},
		"exactly once":              {QoSExactlyOnce, 2},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := New(newFake(), Config{StatusTopic: "bridge/status", QoS: tc.cfg})
			will, err := r.Will()
			if err != nil {
				t.Fatal(err)
			}
			if will.QoS != tc.want {
				t.Fatalf("Will().QoS = %d, want the wire byte %d — a client takes a byte",
					will.QoS, tc.want)
			}
		})
	}
}

// TestWillMatchesTheAnnouncements pins the agreement the whole availability
// policy rests on: the will the consumer configures on CONNECT, the marker
// the runtime sets on connect, and the marker it sets on a clean shutdown
// must name the same topic and the same two payloads.
//
// Two reference bridges configure a will whose topic no published entity
// references, so a hard crash writes "offline" where nothing reads it and
// every entity stays available forever, showing the last value it saw.
func TestWillMatchesTheAnnouncements(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{StatusTopic: "bridge/status", QoS: 2})

	will, err := r.Will()
	if err != nil {
		t.Fatal(err)
	}
	want := Will{Topic: "bridge/status", Payload: []byte(DeathPayload), QoS: 2, Retain: true}
	if !reflect.DeepEqual(will, want) {
		t.Fatalf("will %+v want %+v", will, want)
	}

	ctx := context.Background()
	if err := r.AnnounceOnline(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.AnnounceOffline(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.ops) != 2 {
		t.Fatalf("want two announcements, got %d", len(f.ops))
	}
	for i, want := range []string{BirthPayload, DeathPayload} {
		o := f.ops[i]
		if o.topic != will.Topic || string(o.payload) != want || !o.retain || o.qos != will.QoS {
			t.Fatalf("announcement %d is %+v, want %q on the will topic, retained", i, o, want)
		}
	}
}

// TestAvailabilityRefusesWithoutAStatusTopic pins that the runtime will not
// publish an availability marker to a topic nobody named — the inert-LWT
// defect this package exists to stop reproducing.
func TestAvailabilityRefusesWithoutAStatusTopic(t *testing.T) {
	t.Parallel()
	f := newFake()
	r := New(f, Config{})
	if _, err := r.Will(); !errors.Is(err, ErrNoStatusTopic) {
		t.Fatalf("Will: %v", err)
	}
	if err := r.AnnounceOnline(context.Background()); !errors.Is(err, ErrNoStatusTopic) {
		t.Fatalf("AnnounceOnline: %v", err)
	}
	if err := r.AnnounceOffline(context.Background()); !errors.Is(err, ErrNoStatusTopic) {
		t.Fatalf("AnnounceOffline: %v", err)
	}
	if f.count("publish") != 0 {
		t.Fatal("nothing may reach the broker without a status topic")
	}
}

// TestBirthTriggersAResync pins the reason the birth listener exists: Home
// Assistant keeps the retained configs across its own restart but does not
// reliably re-read them across every addon reload, and the replay on the
// rising edge closes that race.
//
// It also pins that the replay does not run on the delivering goroutine. The
// fake fans a publish out to its subscribers inline, exactly as a broker
// does before Publish returns, so a replay running inline here would
// re-enter the handler — which is the shape of the self-deadlock a real read
// loop suffers.
func TestBirthTriggersAResync(t *testing.T) {
	t.Parallel()
	f := newFake()
	done := make(chan int, 4)
	r := New(f, Config{OnResync: func(n int, err error) {
		if err != nil {
			t.Errorf("resync: %v", err)
		}
		done <- n
	}})
	defer r.Close()

	ctx := context.Background()
	if _, err := r.Publish(ctx, "homeassistant/sensor/n/o/config", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := r.WatchBirth(ctx); err != nil {
		t.Fatal(err)
	}

	// Home Assistant announces itself. The fake delivers it to the
	// subscription synchronously, as a broker does.
	if err := f.Publish(ctx, BirthTopic(""), []byte("online"), 1, true); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("replayed %d configs, want 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no resync after the birth message")
	}
}

// TestBirthIgnoresTheDeathMessage pins that "offline" is not a trigger.
// Home Assistant emits it before its own restart; the configs it will
// re-read are already retained and the replay belongs on the way back up.
func TestBirthIgnoresTheDeathMessage(t *testing.T) {
	t.Parallel()
	f := newFake()
	var resyncs int
	var mu sync.Mutex
	r := New(f, Config{OnResync: func(int, error) {
		mu.Lock()
		resyncs++
		mu.Unlock()
	}})
	defer r.Close()

	ctx := context.Background()
	if err := r.WatchBirth(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.Publish(ctx, BirthTopic(""), []byte("offline"), 1, true); err != nil {
		t.Fatal(err)
	}
	r.Close()
	mu.Lock()
	defer mu.Unlock()
	if resyncs != 0 {
		t.Fatalf("want no resync on the death message, got %d", resyncs)
	}
}

// TestBirthRetainedReplayCounts pins that the retained delivery at subscribe
// time is treated as a real event. Home Assistant publishes its status
// retained, so a consumer that connects after Home Assistant gets no other
// signal that it is up.
func TestBirthRetainedReplayCounts(t *testing.T) {
	t.Parallel()
	f := newFake()
	f.seed(BirthTopic(""), []byte("online"))
	done := make(chan int, 1)
	r := New(f, Config{OnResync: func(n int, _ error) { done <- n }})
	defer r.Close()

	if err := r.WatchBirth(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the retained birth replay must trigger a resync")
	}
}

// TestWatchBirthReportsASubscribeFailure pins that a consumer learns its
// resync will never happen rather than assuming it is armed.
func TestWatchBirthReportsASubscribeFailure(t *testing.T) {
	t.Parallel()
	f := newFake()
	boom := errors.New("refused")
	f.failSubscribe = boom
	r := New(f, Config{})
	if err := r.WatchBirth(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("want the subscribe error, got %v", err)
	}
}

// TestCloseIsSafeTwiceAndWithoutAWatch pins that a shutdown path reached
// from two places is the normal case, not a panic on a closed channel.
func TestCloseIsSafeTwiceAndWithoutAWatch(t *testing.T) {
	t.Parallel()
	r := New(newFake(), Config{})
	r.Close()
	if err := r.WatchBirth(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Close()
	r.Close()
}

// TestDispatcherCollapsesABurst pins the queueing policy. Every job is a
// full idempotent replay, so running the second after the first changes
// nothing — and a bounded queue that filled up would push the blocking back
// onto the read loop the dispatcher exists to keep free.
func TestDispatcherCollapsesABurst(t *testing.T) {
	t.Parallel()
	d := newDispatcher()
	release := make(chan struct{})
	var mu sync.Mutex
	ran := 0
	started := make(chan struct{})
	first := func() {
		close(started)
		<-release
		mu.Lock()
		ran++
		mu.Unlock()
	}
	later := func() {
		mu.Lock()
		ran++
		mu.Unlock()
	}

	d.enqueue(first)
	<-started
	for range 5 {
		d.enqueue(later)
	}
	close(release)
	d.close()

	mu.Lock()
	defer mu.Unlock()
	// The running first job plus exactly one survivor of the five that
	// queued behind it, not six.
	if ran != 2 {
		t.Fatalf("ran %d jobs, want 2", ran)
	}
	// A closed dispatcher accepts nothing more.
	d.enqueue(later)
	if ran != 2 {
		t.Fatalf("ran %d jobs after close, want 2", ran)
	}
}
