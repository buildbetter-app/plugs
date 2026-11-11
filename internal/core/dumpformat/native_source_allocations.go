package dumpformat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeSourcePlanePlanSize counts the existing closed binary plan codec,
// without encoding or detaching the plan. It grants no plan validity/authority.
// Every length, list count and uint64 is eight bytes; optional/bool tags one.
func NativeSourcePlanePlanSize(plan driver.SourcePlanePlanV1) (uint64, error) {
	counter := deltaSizeCounter{}
	var count func(reflect.Value) error
	count = func(value reflect.Value) error {
		switch value.Kind() {
		case reflect.String:
			if err := counter.add(8); err != nil {
				return err
			}
			return counter.add(uint64(value.Len()))
		case reflect.Uint64:
			return counter.add(8)
		case reflect.Bool:
			return counter.add(1)
		case reflect.Pointer:
			if err := counter.add(1); err != nil {
				return err
			}
			if !value.IsNil() {
				return count(value.Elem())
			}
		case reflect.Slice:
			if err := counter.add(8); err != nil {
				return err
			}
			if value.Type().Elem().Kind() == reflect.Uint8 {
				return counter.add(uint64(value.Len()))
			}
			for i := 0; i < value.Len(); i++ {
				if err := count(value.Index(i)); err != nil {
					return err
				}
			}
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if err := count(value.Field(i)); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unsupported source plan accounting type %s", value.Type())
		}
		return nil
	}
	if err := count(reflect.ValueOf(plan)); err != nil {
		return 0, err
	}
	return counter.n, nil
}

// NativeJSONStringBytes counts the UTF-8 bytes encoding/json will allocate
// when decoding one JSON string, without materializing that string. Escaped
// surrogate pairs, replacement runes and lexical escapes match json.Unmarshal.
func NativeJSONStringBytes(raw []byte) (uint64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' || !json.Valid(raw) {
		return 0, fmt.Errorf("expected JSON text for allocation accounting")
	}
	counter := deltaSizeCounter{}
	hex := func(b byte) uint16 {
		if b >= '0' && b <= '9' {
			return uint16(b - '0')
		}
		if b >= 'a' && b <= 'f' {
			return uint16(b - 'a' + 10)
		}
		return uint16(b - 'A' + 10)
	}
	code := func(at int) rune {
		return rune(hex(raw[at])<<12 | hex(raw[at+1])<<8 | hex(raw[at+2])<<4 | hex(raw[at+3]))
	}
	for i := 1; i < len(raw)-1; {
		var n int
		if raw[i] == '\\' {
			if raw[i+1] != 'u' {
				n = 1
				i += 2
			} else {
				r := code(i + 2)
				i += 6
				if r >= 0xd800 && r <= 0xdbff && i+6 <= len(raw)-1 && raw[i] == '\\' && raw[i+1] == 'u' {
					next := code(i + 2)
					if next >= 0xdc00 && next <= 0xdfff {
						r = utf16.DecodeRune(r, next)
						i += 6
					}
				}
				if utf16.IsSurrogate(r) {
					r = utf8.RuneError
				}
				n = utf8.RuneLen(r)
			}
		} else {
			r, size := utf8.DecodeRune(raw[i : len(raw)-1])
			i += size
			n = utf8.RuneLen(r)
		}
		if err := counter.add(uint64(n)); err != nil {
			return 0, err
		}
	}
	return counter.n, nil
}
