// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/SukramJ/go-hamqtt/discovery"
	"github.com/SukramJ/go-hamqtt/model"
	"github.com/SukramJ/go-hamqtt/topic"
)

func smartHomeLayout(t *testing.T, name string) topic.SmartHome {
	t.Helper()
	l, err := topic.NewSmartHome(name)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// sheParseConnected is she's parseConnected (she-services-inventory.js) for
// the plain payloads: a trimmed integer 0, 1 or 2, or nothing.
func sheParseConnected(payload []byte) (int, bool) {
	n, err := strconv.Atoi(string(payload))
	if err != nil || n < 0 || n > 2 {
		return 0, false
	}
	return n, true
}

// TestConnectedWillAndTransitions walks spec §3.1: the will is `0` retained
// on `<name>/connected`, the first announce says 1 until the consumer says
// its upstream is usable, every transition is published, a repeat is not, a
// reconnect republishes the current level, and a graceful stop says 0.
func TestConnectedWillAndTransitions(t *testing.T) {
	t.Parallel()

	f := newFake()
	r := New(f, Config{Layout: smartHomeLayout(t, "mtec"), Logger: discardLogger()})
	ctx := context.Background()

	will, err := r.Will()
	if err != nil {
		t.Fatal(err)
	}
	if will.Topic != "mtec/connected" || string(will.Payload) != "0" || !will.Retain {
		t.Errorf("will = %+v", will)
	}
	if r.BridgeTopic() != "mtec/connected" {
		t.Errorf("BridgeTopic = %q", r.BridgeTopic())
	}

	expect := func(want string) {
		t.Helper()
		got := lastPublish(t, f)
		if got.topic != "mtec/connected" || string(got.payload) != want || !got.retain {
			t.Errorf("published %s=%q retain %v, want %q retained", got.topic, got.payload, got.retain, want)
		}
		if n, ok := sheParseConnected(got.payload); !ok || strconv.Itoa(n) != want {
			t.Errorf("she would not read %q as a connected level", got.payload)
		}
	}

	if err := r.AnnounceOnline(ctx); err != nil {
		t.Fatal(err)
	}
	expect("1")

	changed, err := r.SetConnected(ctx, discovery.ConnectedOperational)
	if err != nil || !changed {
		t.Fatalf("SetConnected(2) = %v, %v", changed, err)
	}
	expect("2")
	before := f.count("publish")
	if changed, _ := r.SetConnected(ctx, discovery.ConnectedOperational); changed || f.count("publish") != before {
		t.Error("a repeat of the current level was published")
	}

	f.reset() // a reconnect
	if err := r.AnnounceOnline(ctx); err != nil {
		t.Fatal(err)
	}
	expect("2")

	if _, err := r.SetConnected(ctx, discovery.ConnectedBroker); err != nil {
		t.Fatal(err)
	}
	expect("1")
	if err := r.AnnounceOffline(ctx); err != nil {
		t.Fatal(err)
	}
	expect("0")
	if r.Connected() != discovery.ConnectedBroker {
		t.Errorf("AnnounceOffline forgot the level: %d", r.Connected())
	}

	for _, bad := range []int{0, 3, -1} {
		if _, err := r.SetConnected(ctx, bad); !errors.Is(err, ErrConnectedLevel) {
			t.Errorf("SetConnected(%d) = %v", bad, err)
		}
	}
}

// TestConnectedNeedsTheConventionLayout: an online/offline runtime has no
// level between its two words, and its will is unchanged.
func TestConnectedNeedsTheConventionLayout(t *testing.T) {
	t.Parallel()

	f := newFake()
	r := New(f, Config{Layout: topic.Default{Root: "mtec"}, Logger: discardLogger()})
	if _, err := r.SetConnected(context.Background(), 2); !errors.Is(err, ErrNotSmartHome) {
		t.Errorf("SetConnected = %v, want ErrNotSmartHome", err)
	}
	will, _ := r.Will()
	if string(will.Payload) != DeathPayload || will.Topic != "mtec/bridge/status" {
		t.Errorf("will = %+v", will)
	}
}

// TestConnectedRefusesADisagreeingWrapper: a wrapper forwarding Connected and
// Bridge to different strings would put the will on one topic and every
// entity's availability template on the other.
func TestConnectedRefusesADisagreeingWrapper(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Error("a layout whose Connected disagrees with Bridge was accepted")
		}
	}()
	New(newFake(), Config{Layout: skewedLayout{smartHomeLayout(t, "x")}, Logger: discardLogger()})
}

type skewedLayout struct{ topic.SmartHome }

func (skewedLayout) Bridge() string { return "x/bridge/status" }

// TestConnectedFailedPublishKeepsTheLevel: the level is the statement; the
// next announce sends it even when the transition's own publish failed.
func TestConnectedFailedPublishKeepsTheLevel(t *testing.T) {
	t.Parallel()

	f := newFake()
	r := New(f, Config{Layout: smartHomeLayout(t, "z"), Logger: discardLogger()})
	f.failPublish = func(string) error { return errors.New("down") }
	if changed, err := r.SetConnected(context.Background(), 2); !changed || err == nil {
		t.Fatalf("SetConnected = %v, %v", changed, err)
	}
	f.failPublish = nil
	if err := r.AnnounceOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := string(lastPublish(t, f).payload); got != "2" {
		t.Errorf("announce = %q, want 2", got)
	}
}

// TestAvailabilityPublisherRefusesStatusItems: under the convention the
// device `online` item and a LevelSelf datapoint are status objects, which
// this publisher's online/offline marker would not match.
func TestAvailabilityPublisherRefusesStatusItems(t *testing.T) {
	t.Parallel()

	f := newFake()
	a := NewAvailability(f, AvailabilityConfig{Layout: smartHomeLayout(t, "d"), Logger: discardLogger()})
	if _, err := a.Device(context.Background(), model.S("dev", "", model.BucketUnset), true); !errors.Is(err, ErrStatusItemAvailability) {
		t.Errorf("Device = %v", err)
	}
	e := &model.Basic{EntityKey: "x", Binds: []model.Binding{{
		Role: model.RoleAvailability, Slot: model.S("dev", "", model.BucketUnset, "ok"), Mode: model.Read,
	}}}
	if _, err := a.Self(context.Background(), e, discovery.StatusObjectEncoding, true); !errors.Is(err, ErrStatusItemAvailability) {
		t.Errorf("Self = %v", err)
	}
	if SelfAvailabilityPayload(discovery.StatusObjectEncoding, true) != nil {
		t.Error("a status object was rendered without timestamps")
	}
	if n := f.count("publish"); n != 0 {
		t.Errorf("published %d messages", n)
	}
}
