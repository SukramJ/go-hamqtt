// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/SukramJ/go-hamqtt/model"
)

// TestParseSet is spec §5.3's acceptance table: a plain value and its
// `{"val": …}` form arrive identically, other JSON arrives as parameters,
// and empty or broken payloads never reach a handler.
func TestParseSet(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in     string
		text   string
		params string
		err    error
	}{
		{in: "21.5", text: "21.5"},
		{in: " on\n", text: "on"},
		{in: `{"val":21.5}`, text: "21.5"},
		{in: `{"val": true}`, text: "true"},
		{in: `{"val":"eco"}`, text: "eco"},
		{in: `{"val":"say \"hi\""}`, text: `say "hi"`},
		{in: `{"val":{"id":"netflix"}}`, params: `{"id":"netflix"}`},
		{in: `{"val":[1,2]}`, params: `[1,2]`},
		{in: `{"id":"netflix","params":{}}`, params: `{"id":"netflix","params":{}}`},
		{in: `{"minutes": 5}`, params: `{"minutes": 5}`},
		{in: `{}`, params: `{}`},
		{in: `["HDMI_1","HDMI_2"]`, params: `["HDMI_1","HDMI_2"]`},
		{in: "", err: ErrEmptySet},
		{in: "  \n", err: ErrEmptySet},
		{in: `{"val":null}`, err: ErrEmptySet},
		{in: `{"val":`, err: ErrMalformedSet},
		{in: `[1,`, err: ErrMalformedSet},
		{in: `{val: 1}`, err: ErrMalformedSet},
	}
	for _, c := range cases {
		v, err := ParseSet([]byte(c.in))
		if c.err != nil {
			if !errors.Is(err, c.err) {
				t.Errorf("%q: err = %v, want %v", c.in, err, c.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if v.Text != c.text || string(v.Params) != c.params || v.Structured() != (c.params != "") {
			t.Errorf("%q: got text %q params %q", c.in, v.Text, v.Params)
		}
	}
}

func TestSetBool(t *testing.T) {
	t.Parallel()

	for in, want := range map[string]bool{
		"true": true, "TRUE": true, "1": true, "on": true, "On": true, "yes": true,
		"false": false, "0": false, "OFF": false, "no": false,
	} {
		got, err := SetValue{Text: in}.Bool()
		if err != nil || got != want {
			t.Errorf("%q: %v, %v", in, got, err)
		}
	}
	for _, v := range []SetValue{{Text: "2"}, {Text: "maybe"}, {Params: []byte(`{}`)}} {
		if _, err := v.Bool(); !errors.Is(err, ErrSetConversion) {
			t.Errorf("%+v: %v", v, err)
		}
	}
}

func TestSetNumber(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in               string
		lo, hi, step, ok float64
	}{
		{"21.54", 5, 30, 0.5, 21.5},
		{"21.76", 5, 30, 0.5, 22},
		{"0.25", 0, 1, 0.1, 0.3},
		{"99", 5, 30, 0, 30},
		{"-4", 5, 30, 1, 5},
		{"7.6", 0, 10, 1, 8},
		{"1e1", 0, 100, 0, 10},
	}
	for _, c := range cases {
		got, err := SetValue{Text: c.in}.Number(c.lo, c.hi, c.step)
		if err != nil || got != c.ok {
			t.Errorf("%q in [%v,%v] step %v: %v, %v, want %v", c.in, c.lo, c.hi, c.step, got, err, c.ok)
		}
	}
	for _, v := range []SetValue{{Text: "warm"}, {Text: "NaN"}, {Text: "Inf"}, {Params: []byte(`1`)}} {
		if _, err := v.Number(0, 1, 0); !errors.Is(err, ErrSetConversion) {
			t.Errorf("%+v: %v", v, err)
		}
	}
}

func TestSetEnum(t *testing.T) {
	t.Parallel()

	modes := &model.Enum{
		Codes: []string{"HEATING", "cooling"},
		Labels: map[string]model.Localized{
			"HEATING": {Default: "Heating", Lang: map[string]string{"de": "Heizen"}},
		},
	}
	for in, want := range map[string]string{"heating": "HEATING", "Cooling": "cooling", "HEATING": "HEATING"} {
		if got, err := (SetValue{Text: in}).Enum(modes, false); err != nil || got != want {
			t.Errorf("token %q: %q, %v", in, got, err)
		}
	}
	if _, err := (SetValue{Text: "Heizen"}).Enum(modes, false); !errors.Is(err, ErrSetConversion) {
		t.Errorf("a label was accepted without acceptLabels: %v", err)
	}
	for _, in := range []string{"Heizen", "heizen", "Heating"} {
		if got, err := (SetValue{Text: in}).Enum(modes, true); err != nil || got != "HEATING" {
			t.Errorf("label %q: %q, %v", in, got, err)
		}
	}
	if _, err := (SetValue{Text: "fan"}).Enum(modes, true); !errors.Is(err, ErrSetConversion) {
		t.Errorf("unknown: %v", err)
	}
	if _, err := (SetValue{Text: "x"}).Enum(nil, true); !errors.Is(err, ErrSetConversion) {
		t.Errorf("nil enum: %v", err)
	}
}

// syncLog collects log lines across the router's worker goroutines.
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *syncLog) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestRouterNormalizeSet: the router-wide opt-in. A handler that parses plain
// values keeps working when a consumer sends `{"val": …}`, empty payloads and
// broken JSON are dropped — the latter logged at warn with topic and payload
// as spec §3.3 requires — and retained commands stay dropped.
func TestRouterNormalizeSet(t *testing.T) {
	t.Parallel()

	b := newCmdBroker()
	log := &syncLog{}
	r := NewCommandRouter(b, CommandConfig{NormalizeSet: true, Logger: log.logger()})
	var mu sync.Mutex
	var got []string
	if err := r.Handle("n/set/#", func(_ context.Context, cmd Command) {
		mu.Lock()
		got = append(got, string(cmd.Payload))
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	for _, p := range []string{`{"val":"eco"}`, "eco", "", `{"val":null}`, `{"val":`, `{"minutes":5}`} {
		b.fanout("n/set/dev/mode", []byte(p), false)
	}
	b.fanout("n/set/dev/mode", []byte("stale"), true)
	r.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"eco", "eco", `{"minutes":5}`}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("handler saw %q, want %q", got, want)
	}
	logged := log.String()
	if !strings.Contains(logged, "level=WARN msg=publisher.command.rejected topic=n/set/dev/mode") ||
		!strings.Contains(logged, `payload="{\"val\":"`) {
		t.Errorf("malformed payload not logged at warn with topic and payload:\n%s", logged)
	}
}

// TestRouterHandleSet: the per-route opt-in hands the handler a SetValue and
// leaves the payload as received; a router without the opt-in passes bytes
// through untouched, empty ones included.
func TestRouterHandleSet(t *testing.T) {
	t.Parallel()

	b := newCmdBroker()
	r := NewCommandRouter(b, CommandConfig{Logger: discardLogger()})
	var mu sync.Mutex
	var values []SetValue
	var raw []string
	if err := r.HandleSet("n/set/+/temp", func(_ context.Context, cmd Command, v SetValue) {
		mu.Lock()
		values = append(values, v)
		raw = append(raw, string(cmd.Payload))
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	var plain []string
	if err := r.Handle("n/set/+/power", func(_ context.Context, cmd Command) {
		mu.Lock()
		plain = append(plain, string(cmd.Payload))
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.HandleSet("n/set/x", nil); err == nil {
		t.Error("nil SetHandler accepted")
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	b.fanout("n/set/dev/temp", []byte(`{"val":21.5}`), false)
	b.fanout("n/set/dev/temp", nil, false)
	b.fanout("n/set/dev/power", []byte(`{"val":true}`), false)
	b.fanout("n/set/dev/power", nil, false)
	r.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if len(values) != 1 || values[0].Text != "21.5" || raw[0] != `{"val":21.5}` {
		t.Errorf("HandleSet saw %+v / %q", values, raw)
	}
	if strings.Join(plain, "|") != `{"val":true}|` {
		t.Errorf("plain route saw %q, want the bytes untouched", plain)
	}
}
