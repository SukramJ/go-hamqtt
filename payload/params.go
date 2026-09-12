// SPDX-License-Identifier: MIT
// Copyright (C) 2026 go-hamqtt authors.

package payload

import (
	"errors"
	"fmt"
	"strconv"
)

// The decoders below read the *inbound* direction: the body of a service
// call a consumer receives, as JSON already unmarshalled into a
// map[string]any.
//
// They live here because every consumer of this module needs the same
// coercions and for the same reason: Home Assistant's templating decides
// what type reaches the wire, and a caller cannot control it. A
// `{{ value }}` template sends the string "42" where the author meant a
// number, `payload_on` sends whatever the platform's default spelling is,
// and an automation written by hand sends a JSON bool. A consumer that
// accepted only the declared Go type would reject a command that Home
// Assistant considers well formed, and the operator would see a control
// that does nothing.
//
// Nothing here touches a published string. These are the read side, which
// is why the reference consumer's golden payload pins correctly say
// nothing about them.

// ErrMissingParam is returned when a required key is absent from the
// request body. Wrapped with the offending key so a log line names it.
var ErrMissingParam = errors.New("payload: missing required param")

// ErrInvalidParam is returned when a key is present but its value cannot
// be coerced to the expected Go type.
var ErrInvalidParam = errors.New("payload: param has invalid type")

// ParamBool decodes a required bool param.
//
// JSON numbers (1 / 0) coerce to true / false because that is what Home
// Assistant's MQTT layer typically sends through `payload_on` /
// `payload_off` templates, and the listed string spellings are accepted
// for the same reason.
//
// The spelling list is deliberately exact rather than case-insensitive,
// and deliberately excludes "yes"/"no". This decoder coerces north-bound
// service-call JSON, where the set of spellings is whatever Home
// Assistant emits and is therefore knowable; a consumer's own coercion of
// *device* values against a parameter descriptor is a different boundary
// with a different set, and the two are not meant to converge. Widening
// this one to be generous would make a malformed automation look like a
// working one.
func ParamBool(params map[string]any, key string) (bool, error) {
	raw, ok := params[key]
	if !ok {
		return false, fmt.Errorf("%w: %q", ErrMissingParam, key)
	}
	switch v := raw.(type) {
	case bool:
		return v, nil
	case float64:
		return v != 0, nil
	case int:
		return v != 0, nil
	case string:
		switch v {
		case "true", "True", "TRUE", "1", "on", "ON":
			return true, nil
		case "false", "False", "FALSE", "0", "off", "OFF":
			return false, nil
		}
	}
	return false, fmt.Errorf("%w: %q", ErrInvalidParam, key)
}

// ParamFloat64 decodes a required float64 param.
//
// JSON-decoded numbers always arrive as float64; integer types and
// numeric strings are accepted too, which is what lets a plain
// `{{ value }}` template work without an explicit `| float` filter.
//
// A numeric string goes through [strconv.ParseFloat] rather than a
// scanning helper because ParseFloat rejects trailing garbage. "42xyz"
// must be an error: silently reading it as 42 would turn a typo in an
// automation into a command the operator never wrote.
func ParamFloat64(params map[string]any, key string) (float64, error) {
	raw, ok := params[key]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrMissingParam, key)
	}
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
	}
	return 0, fmt.Errorf("%w: %q", ErrInvalidParam, key)
}

// ParamInt32 decodes a required int32 param.
//
// An out-of-range input is an error rather than a truncation. Silent
// truncation would surprise a caller supplying a 64-bit index, and the
// surprise would arrive as a write to the wrong thing rather than as a
// rejected command. Numeric strings go through [strconv.ParseInt] for the
// trailing-garbage reason given on [ParamFloat64].
func ParamInt32(params map[string]any, key string) (int32, error) {
	raw, ok := params[key]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrMissingParam, key)
	}
	const maxI32, minI32 = 1<<31 - 1, -(1 << 31)
	switch v := raw.(type) {
	case int32:
		return v, nil
	case int:
		if v > maxI32 || v < minI32 {
			return 0, fmt.Errorf("%w: %q overflows int32", ErrInvalidParam, key)
		}
		return int32(v), nil
	case int64:
		if v > maxI32 || v < minI32 {
			return 0, fmt.Errorf("%w: %q overflows int32", ErrInvalidParam, key)
		}
		return int32(v), nil
	case float64:
		if v > float64(maxI32) || v < float64(minI32) {
			return 0, fmt.Errorf("%w: %q overflows int32", ErrInvalidParam, key)
		}
		return int32(v), nil
	case string:
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int32(n), nil
		}
	}
	return 0, fmt.Errorf("%w: %q", ErrInvalidParam, key)
}

// ParamString decodes a required string param. A numeric or bool value is
// formatted to its canonical Go string form, so a caller that wants the
// raw token does not have to type-switch for the case where Home
// Assistant sent a number.
func ParamString(params map[string]any, key string) (string, error) {
	raw, ok := params[key]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrMissingParam, key)
	}
	switch v := raw.(type) {
	case string:
		return v, nil
	case bool, int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v), nil
	}
	return "", fmt.Errorf("%w: %q", ErrInvalidParam, key)
}
