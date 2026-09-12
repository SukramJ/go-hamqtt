// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"context"
	"strings"
	"sync"
)

// op is one transport call, recorded in order.
//
// Order is the point of the whole fixture: the single most expensive fact
// this package encodes — retract first, publish second — is invisible to a
// test that only checks which topics were written.
type op struct {
	kind    string // "publish", "subscribe", "unsubscribe"
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

// fakeTransport is a broker that remembers. Retained holds what a real one
// would replay to a fresh subscriber, which is what the orphan sweep reads.
type fakeTransport struct {
	mu       sync.Mutex
	ops      []op
	retained map[string][]byte
	subs     map[string]Handler

	// failPublish, when set, decides an error per topic.
	failPublish func(topic string) error
	// failSubscribe fails every Subscribe when set.
	failSubscribe error
	// onUnsubscribe runs when the snapshot window is torn down, which is
	// the one moment between the window closing and the retractions
	// starting — where a late claim has to be injected to exercise the
	// second claim check.
	onUnsubscribe func()
}

func newFake() *fakeTransport {
	return &fakeTransport{retained: map[string][]byte{}, subs: map[string]Handler{}}
}

func (f *fakeTransport) Publish(_ context.Context, topic string, payload []byte, qos byte, retain bool) error {
	f.mu.Lock()
	if f.failPublish != nil {
		if err := f.failPublish(topic); err != nil {
			f.mu.Unlock()
			return err
		}
	}
	f.ops = append(f.ops, op{kind: "publish", topic: topic, payload: append([]byte(nil), payload...), qos: qos, retain: retain})
	if retain {
		if len(payload) == 0 {
			delete(f.retained, topic)
		} else {
			f.retained[topic] = append([]byte(nil), payload...)
		}
	}
	handlers := make([]Handler, 0, len(f.subs))
	for filter, h := range f.subs {
		if matches(filter, topic) {
			handlers = append(handlers, h)
		}
	}
	f.mu.Unlock()
	// Fanned out before Publish returns, exactly as a broker does — which
	// is what makes the in-flight claim in Runtime.announced necessary.
	for _, h := range handlers {
		h(topic, payload, retain)
	}
	return nil
}

func (f *fakeTransport) Subscribe(_ context.Context, filter string, qos byte, h Handler) error {
	f.mu.Lock()
	if f.failSubscribe != nil {
		err := f.failSubscribe
		f.mu.Unlock()
		return err
	}
	f.ops = append(f.ops, op{kind: "subscribe", topic: filter, qos: qos})
	f.subs[filter] = h
	replay := map[string][]byte{}
	for t, p := range f.retained {
		if matches(filter, t) {
			replay[t] = p
		}
	}
	f.mu.Unlock()
	for t, p := range replay {
		h(t, p, true)
	}
	return nil
}

func (f *fakeTransport) Unsubscribe(_ context.Context, filter string) error {
	f.mu.Lock()
	f.ops = append(f.ops, op{kind: "unsubscribe", topic: filter})
	delete(f.subs, filter)
	hook := f.onUnsubscribe
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

// matches is the sliver of MQTT filter matching the fixture needs: an exact
// topic or a trailing multi-level wildcard.
func matches(filter, topic string) bool {
	if strings.HasSuffix(filter, "#") {
		return strings.HasPrefix(topic, strings.TrimSuffix(filter, "#"))
	}
	return filter == topic
}

// publishes returns the topics published, in order.
func (f *fakeTransport) publishes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, o := range f.ops {
		if o.kind == "publish" {
			out = append(out, o.topic)
		}
	}
	return out
}

// retractions returns the topics cleared with an empty retained payload.
func (f *fakeTransport) retractions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, o := range f.ops {
		if o.kind == "publish" && len(o.payload) == 0 {
			out = append(out, o.topic)
		}
	}
	return out
}

func (f *fakeTransport) count(kind string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, o := range f.ops {
		if o.kind == kind {
			n++
		}
	}
	return n
}

func (f *fakeTransport) seed(topic string, payload []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retained[topic] = payload
}
