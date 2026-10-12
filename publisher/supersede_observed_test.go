// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// observedConfig is a runtime with [Config.SupersedeObserved] and a window
// the fake fills synchronously: it replays the retained set inside Subscribe.
func observedConfig() Config {
	return Config{SupersedeObserved: true, SweepWindow: time.Millisecond}
}

// TestSupersedeObservedRetractsWhatTheBrokerHolds is the migration boot: one
// per-entity config of the document is still retained, the other never was.
// Exactly the held one is cleared, and before the document — the property
// the blind round exists for must survive the narrowing.
func TestSupersedeObservedRetractsWhatTheBrokerHolds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	held := "homeassistant/sensor/ccu_abc/temperature/config"
	document := "homeassistant/device/ccu_abc/config"

	f := newFake()
	f.seed(held, []byte(`{"previous":"build"}`))
	r := New(f, observedConfig())

	if _, err := r.PublishBundle(ctx, testBundle()); err != nil {
		t.Fatal(err)
	}
	if got := f.retractions(); !slices.Equal(got, []string{held}) {
		t.Fatalf("retractions = %v, want only the retained %q", got, held)
	}
	if idx := slices.Index(f.publishes(), held); idx > slices.Index(f.publishes(), document) {
		t.Fatalf("the retraction must precede the document: %v", f.publishes())
	}
}

// TestSupersedeObservedIsSilentOnAMigratedTree is the measured noise: after
// the migration nothing per-entity is retained, and every connection used to
// send one empty message per entity anyway — each logged by Home Assistant
// as a conflicting discovery message. Now a connection costs one snapshot and
// no retraction.
func TestSupersedeObservedIsSilentOnAMigratedTree(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newGenerationalFake()
	r := New(f, observedConfig())

	for range 2 {
		f.reset()
		if _, err := r.PublishBundle(ctx, testBundle()); err != nil {
			t.Fatal(err)
		}
		if got := f.retractions(); len(got) != 0 {
			t.Fatalf("retracted %v on a tree that holds no per-entity config", got)
		}
		if n := f.count("subscribe"); n != 1 {
			t.Fatalf("snapshots on this connection = %d, want 1", n)
		}
		f.reconnect()
	}
}

// TestSupersedeObservedSnapshotsOncePerConnection pins the cost: a second
// device on the same connection reuses the snapshot rather than opening
// another window in the boot path.
func TestSupersedeObservedSnapshotsOncePerConnection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFake()
	r := New(f, observedConfig())

	first, second := testBundle(), testBundle()
	second.NodeID = "ccu_def"
	if _, err := r.PublishBundle(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := r.PublishBundle(ctx, second); err != nil {
		t.Fatal(err)
	}
	if n := f.count("subscribe"); n != 1 {
		t.Fatalf("snapshots = %d, want 1 for the connection", n)
	}
}

// TestSupersedeObservedFallsBackWhenTheSnapshotFails: a broker that refuses
// the subscription must not cost the migration, so the round is blind again.
func TestSupersedeObservedFallsBackWhenTheSnapshotFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newFake()
	f.failSubscribe = errors.New("not authorised")
	r := New(f, observedConfig())

	if _, err := r.PublishBundle(ctx, testBundle()); err != nil {
		t.Fatal(err)
	}
	if n := len(f.retractions()); n != 2 {
		t.Fatalf("retractions = %d, want the blind round over both components", n)
	}
}
