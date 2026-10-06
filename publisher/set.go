// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/SukramJ/go-hamqtt/model"
)

var (
	// ErrEmptySet is reported for a `set` payload that carries no value: an
	// empty or blank payload, or `{"val": null}`. The convention ignores
	// those (openccu-loom ADR 0083); an empty payload is also what clearing
	// a retained topic looks like.
	ErrEmptySet = errors.New("publisher: empty set payload")
	// ErrMalformedSet is reported for a payload that opens like JSON — `{`
	// or `[` — and is not.
	ErrMalformedSet = errors.New("publisher: malformed set payload")
	// ErrSetConversion is reported by the [SetValue] conversions for a value
	// they cannot read as the requested type.
	ErrSetConversion = errors.New("publisher: set value not convertible")
)

// SetValue is a `set` payload normalised per mqtt-smarthome 2.0 §5.3: a
// plain value and `{"val": …}` arrive as the same Text, and any other JSON
// object or array arrives as structured Params.
type SetValue struct {
	// Text is the plain value: the trimmed payload, or the unwrapped `val`
	// as it would have been published plain — a string without its quotes,
	// a number or boolean as its JSON literal. Empty when Params is set.
	Text string
	// Params is a structured parameter body: a JSON object without `val`,
	// an array, or a `val` that is itself an object or array.
	Params json.RawMessage
}

// Structured reports whether the payload carried parameters rather than a
// plain value.
func (v SetValue) Structured() bool { return v.Params != nil }

// ParseSet normalises one `set` payload. It reports [ErrEmptySet] for a
// payload with no value and [ErrMalformedSet] for broken JSON.
func ParseSet(payload []byte) (SetValue, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return SetValue{}, ErrEmptySet
	}
	if trimmed[0] != '{' && trimmed[0] != '[' {
		return SetValue{Text: string(trimmed)}, nil
	}
	if !json.Valid(trimmed) {
		return SetValue{}, ErrMalformedSet
	}
	if trimmed[0] == '[' {
		return SetValue{Params: json.RawMessage(bytes.Clone(trimmed))}, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return SetValue{}, fmt.Errorf("%w: %w", ErrMalformedSet, err)
	}
	val, ok := fields["val"]
	if !ok {
		return SetValue{Params: json.RawMessage(bytes.Clone(trimmed))}, nil
	}
	switch val[0] {
	case 'n':
		return SetValue{}, ErrEmptySet
	case '{', '[':
		return SetValue{Params: val}, nil
	case '"':
		var text string
		if err := json.Unmarshal(val, &text); err != nil {
			return SetValue{}, fmt.Errorf("%w: %w", ErrMalformedSet, err)
		}
		return SetValue{Text: text}, nil
	default:
		// A number or a boolean: its JSON literal is its plain spelling.
		return SetValue{Text: string(val)}, nil
	}
}

// Bool reads the value as a boolean: true/false, 1/0, on/off, yes/no, in
// any case (spec §5.3).
func (v SetValue) Bool() (bool, error) {
	if !v.Structured() {
		switch strings.ToLower(v.Text) {
		case "true", "1", "on", "yes":
			return true, nil
		case "false", "0", "off", "no":
			return false, nil
		}
	}
	return false, fmt.Errorf("%w: %q is not a boolean", ErrSetConversion, v.Text)
}

// Number reads the value as a decimal number, rounds it to a multiple of
// step when step is positive, and clamps it to [minimum, maximum] (spec
// §5.3). A whole-number item passes a step of 1.
func (v SetValue) Number(minimum, maximum, step float64) (float64, error) {
	if v.Structured() {
		return 0, fmt.Errorf("%w: structured value is not a number", ErrSetConversion)
	}
	x, err := strconv.ParseFloat(v.Text, 64)
	if err != nil || math.IsNaN(x) || math.IsInf(x, 0) {
		return 0, fmt.Errorf("%w: %q is not a number", ErrSetConversion, v.Text)
	}
	if step > 0 {
		x = math.Round(x/step) * step
		// Strip the binary residue a decimal step leaves (0.1 * 3 is not
		// 0.3), so the clamped value is the one an operator would write.
		decimals := 0
		if s := strconv.FormatFloat(step, 'f', -1, 64); strings.Contains(s, ".") {
			decimals = len(s) - strings.IndexByte(s, '.') - 1
		}
		x, _ = strconv.ParseFloat(strconv.FormatFloat(x, 'f', decimals, 64), 64)
	}
	return math.Min(math.Max(x, minimum), maximum), nil
}

// Enum reads the value as one of e's codes, matched without regard to case
// (spec §5.3). With acceptLabels it also accepts a label in any language,
// which a project MAY keep doing for consumers written against its labels.
// The result is always the code, in e's own spelling.
func (v SetValue) Enum(e *model.Enum, acceptLabels bool) (string, error) {
	if e != nil && !v.Structured() {
		for _, c := range e.Codes {
			if strings.EqualFold(c, v.Text) {
				return c, nil
			}
		}
		if acceptLabels {
			if c, ok := e.Code(v.Text); ok {
				return c, nil
			}
			for _, c := range e.Codes {
				l, ok := e.Labels[c]
				if !ok {
					continue
				}
				if strings.EqualFold(l.Default, v.Text) {
					return c, nil
				}
				for _, translated := range l.Lang {
					if strings.EqualFold(translated, v.Text) {
						return c, nil
					}
				}
			}
		}
	}
	return "", fmt.Errorf("%w: %q is not a known token", ErrSetConversion, v.Text)
}

// SetHandler is a [CommandHandler] that receives the normalised payload
// beside the command. See [CommandRouter.HandleSet].
type SetHandler func(ctx context.Context, cmd Command, v SetValue)

// HandleSet registers a route whose handler receives a normalised `set`
// payload, which is the per-route opt-in to spec §5.3; [CommandConfig.NormalizeSet]
// is the router-wide one. cmd.Payload stays as received.
//
// An empty payload never reaches the handler (logged at debug), nor does
// malformed JSON, which is a rejected request and logged at warn with its
// topic and payload as spec §3.3 requires. Everything else about the route —
// registration rules, the retained drop, the worker — is [CommandRouter.Handle].
func (r *CommandRouter) HandleSet(filter string, handler SetHandler) error {
	if handler == nil {
		return fmt.Errorf("%w: nil handler for %q", ErrInvalidFilter, filter)
	}
	return r.handle(filter, func(ctx context.Context, cmd Command) {
		if v, ok := r.parseSet(cmd); ok {
			handler(ctx, cmd, v)
		}
	})
}

// normalizing wraps a plain handler for [CommandConfig.NormalizeSet]: the
// handler sees the plain value — `{"val": 21}` as `21` — or the structured
// body as its Payload, and never sees an empty or malformed one.
func (r *CommandRouter) normalizing(handler CommandHandler) CommandHandler {
	return func(ctx context.Context, cmd Command) {
		v, ok := r.parseSet(cmd)
		if !ok {
			return
		}
		if v.Structured() {
			cmd.Payload = v.Params
		} else {
			cmd.Payload = []byte(v.Text)
		}
		handler(ctx, cmd)
	}
}

func (r *CommandRouter) parseSet(cmd Command) (SetValue, bool) {
	v, err := ParseSet(cmd.Payload)
	switch {
	case err == nil:
		return v, true
	case errors.Is(err, ErrEmptySet):
		r.log.Debug("publisher.command.empty_set", slog.String("topic", cmd.Topic))
	default:
		r.log.Warn("publisher.command.rejected",
			slog.String("topic", cmd.Topic),
			slog.String("payload", string(cmd.Payload)),
			slog.String("err", err.Error()))
	}
	return SetValue{}, false
}
