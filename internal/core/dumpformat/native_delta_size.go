package dumpformat

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Internal custom encodings may supply a nonallocating size of their exact
// existing wire form. The size includes the structured-value terminal LF;
// arbitrary MarshalJSON implementations are still never invoked to count.
type nativeDeltaJSONSized interface {
	NativeDeltaJSONSize() (uint64, error)
}

// NativeDeltaJSONSize computes encoding/json's compact, HTML-escaped size,
// including one final newline, without encoding an aggregate value into a
// temporary buffer. Only bounded scalar custom marshalers are supported.
func NativeDeltaJSONSize(value any) (uint64, error) {
	c := deltaSizeCounter{}
	if err := c.value(reflect.ValueOf(value)); err != nil {
		return 0, err
	}
	if err := c.add(1); err != nil {
		return 0, err
	}
	return c.n, nil
}

type deltaSizeCounter struct{ n uint64 }

func (c *deltaSizeCounter) add(n uint64) error {
	if n > math.MaxUint64-c.n {
		return fmt.Errorf("decoded size integer overflow")
	}
	c.n += n
	return nil
}
func (c *deltaSizeCounter) text(s string) error {
	if err := c.add(2); err != nil {
		return err
	}
	for i := 0; i < len(s); {
		b := s[i]
		size := uint64(1)
		if b < utf8.RuneSelf {
			i++
			switch b {
			case '"', '\\', '\b', '\f', '\n', '\r', '\t':
				size = 2
			case '<', '>', '&':
				size = 6
			default:
				if b < 0x20 {
					size = 6
				}
			}
		} else {
			r, n := utf8.DecodeRuneInString(s[i:])
			i += n
			size = uint64(n)
			if r == utf8.RuneError && n == 1 || r == '\u2028' || r == '\u2029' {
				size = 6
			}
		}
		if err := c.add(size); err != nil {
			return err
		}
	}
	return nil
}

// RawMessage is compacted by encoding/json and its literal HTML/line-separator
// bytes inside strings are escaped. Already escaped bytes remain exact.
func (c *deltaSizeCounter) raw(raw []byte) error {
	if !json.Valid(raw) {
		return fmt.Errorf("invalid counted raw JSON")
	}
	quoted, escaped := false, false
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if !quoted {
			if b == ' ' || b == '\n' || b == '\r' || b == '\t' {
				continue
			}
			if b == '"' {
				quoted = true
			}
			if err := c.add(1); err != nil {
				return err
			}
			continue
		}
		if escaped {
			escaped = false
			if err := c.add(1); err != nil {
				return err
			}
			continue
		}
		if b == '\\' {
			escaped = true
		}
		if b == '"' {
			quoted = false
		}
		n := uint64(1)
		if b == '<' || b == '>' || b == '&' {
			n = 6
		}
		if b == 0xe2 && i+2 < len(raw) && raw[i+1] == 0x80 && (raw[i+2] == 0xa8 || raw[i+2] == 0xa9) {
			n = 6
			i += 2
		}
		if err := c.add(n); err != nil {
			return err
		}
	}
	return nil
}

func (c *deltaSizeCounter) value(v reflect.Value) error {
	if !v.IsValid() {
		return c.add(4)
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return c.add(4)
		}
		return c.value(v.Elem())
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return c.add(4)
		}
		return c.value(v.Elem())
	}
	if v.CanInterface() {
		if sized, ok := v.Interface().(nativeDeltaJSONSized); ok {
			n, err := sized.NativeDeltaJSONSize()
			if err != nil {
				return err
			}
			if n == 0 {
				return fmt.Errorf("empty custom JSON accounting domain %s", v.Type())
			}
			return c.add(n - 1)
		}
		switch x := v.Interface().(type) {
		case json.RawMessage:
			if x == nil {
				return c.add(4)
			}
			return c.raw(x)
		case json.Number:
			if _, err := strconv.ParseFloat(string(x), 64); err != nil { // Huge exact numbers remain valid JSON.
				if !json.Valid([]byte(x)) {
					return fmt.Errorf("invalid counted number")
				}
			}
			return c.add(uint64(len(x)))
		case model.Natural:
			s, err := x.Canonical()
			if err != nil {
				return err
			}
			return c.add(uint64(len(s)))
		case time.Time:
			// A timestamp is a bounded scalar; never serialize an aggregate to
			// discover its reservation size.
			data, err := x.MarshalJSON()
			if err != nil {
				return err
			}
			return c.add(uint64(len(data)))
		case model.PrincipalRef:
			// MarshalJSON uses this exact underlying struct. Recurse into its
			// fields without allocating a second principal/caveat tree.
			type principalWire model.PrincipalRef
			return c.value(reflect.ValueOf(principalWire(x)))
		case model.Commit:
			// Commit.MarshalJSON validates then marshals this exact alias. Count
			// that wire without invoking aggregate marshal/validation (OpResult
			// validation itself decodes raw records into an allocated map).
			// This counter grants no validity: publication/encoding still validates.
			type commitWire model.Commit
			return c.value(reflect.ValueOf(commitWire(x)))
		case model.OpResult:
			type resultWire model.OpResult
			return c.value(reflect.ValueOf(resultWire(x)))
		case model.Warning:
			type warningWire model.Warning
			return c.value(reflect.ValueOf(warningWire(x)))
		case model.WarningCode:
			if err := x.Validate(); err != nil {
				return err
			}
			return c.text(string(x))
		case model.Violation:
			type violationWire model.Violation
			return c.value(reflect.ValueOf(violationWire(x)))
		case model.Changeset:
			// The native receipt stores the existing durable changeset wire.
			// As above, traverse aliases rather than invoking its marshaler.
			type changesetWire model.Changeset
			return c.value(reflect.ValueOf(changesetWire(x)))
		case model.ChangesetKind:
			if err := x.Validate(); err != nil {
				return err
			}
			return c.text(string(x))
		case model.OpKind:
			if err := x.Validate(); err != nil {
				return err
			}
			return c.text(string(x))
		case model.Ref:
			if err := x.Validate(); err != nil {
				return err
			}
			if x.IsID() {
				return c.value(reflect.ValueOf(struct {
					ID     string        `json:"id"`
					Branch model.Natural `json:"branch,omitempty"`
				}{x.ID, x.Branch}))
			}
			return c.value(reflect.ValueOf(struct {
				Key string `json:"key"`
			}{x.Key}))
		case driver.OfflineAllocator:
			// Mirror only the existing allocator wire selection. No driver
			// hook, detached tree or aggregate serialization is introduced.
			type allocatorWire driver.OfflineAllocator
			if x.Mode == driver.AllocatorRandom {
				if err := x.Validate(); err != nil {
					return err
				}
				return c.value(reflect.ValueOf(struct {
					Prefix         string `json:"prefix"`
					Mode           string `json:"id_mode"`
					RandomBytes    uint32 `json:"random_bytes"`
					SuffixEncoding string `json:"suffix_encoding"`
					TopLevelShape  string `json:"top_level_shape"`
					ChildShape     string `json:"child_shape"`
				}{x.Prefix, x.Mode, x.RandomBytes, x.SuffixEncoding, x.TopLevelShape, x.ChildShape}))
			}
			if x.Mode != driver.AllocatorHash || x.MinHashLength == 0 && x.MaxHashLength == 0 {
				return c.value(reflect.ValueOf(allocatorWire(x)))
			}
			probability := x.MaxCollisionProb
			x.MaxCollisionProb = 0 // the explicit outer field shadows omitempty.
			if err := c.value(reflect.ValueOf(allocatorWire(x))); err != nil {
				return err
			}
			if err := c.add(1); err != nil { // comma after the nonempty base object
				return err
			}
			if err := c.text("max_collision_prob"); err != nil {
				return err
			}
			if err := c.add(1); err != nil {
				return err
			}
			return c.value(reflect.ValueOf(probability))
		}
		if _, ok := v.Interface().(json.Marshaler); ok {
			return fmt.Errorf("unallocated custom JSON accounting type %s", v.Type())
		}
	}
	switch v.Kind() {
	case reflect.String:
		return c.text(v.String())
	case reflect.Bool:
		if v.Bool() {
			return c.add(4)
		}
		return c.add(5)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return c.add(uint64(len(strconv.FormatUint(v.Uint(), 10))))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return c.add(uint64(len(strconv.FormatInt(v.Int(), 10))))
	case reflect.Float32, reflect.Float64:
		// A single finite scalar is bounded independently of aggregate size.
		b, err := json.Marshal(v.Interface())
		if err != nil {
			return err
		}
		return c.add(uint64(len(b)))
	case reflect.Slice:
		if v.IsNil() {
			return c.add(4)
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return c.add(2 + 4*((uint64(v.Len())+2)/3))
		}
		fallthrough
	case reflect.Array:
		if err := c.add(2); err != nil {
			return err
		}
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				if err := c.add(1); err != nil {
					return err
				}
			}
			if err := c.value(v.Index(i)); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		if v.IsNil() {
			return c.add(4)
		}
		if v.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("non-string counted map")
		}
		if err := c.add(2); err != nil {
			return err
		}
		iter := v.MapRange()
		i := 0
		for iter.Next() {
			if i > 0 {
				if err := c.add(1); err != nil {
					return err
				}
			}
			i++
			if err := c.text(iter.Key().String()); err != nil {
				return err
			}
			if err := c.add(1); err != nil {
				return err
			}
			if err := c.value(iter.Value()); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		if err := c.add(2); err != nil {
			return err
		}
		count := 0
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if f.PkgPath != "" {
				continue
			}
			tag := f.Tag.Get("json")
			name := strings.Split(tag, ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			item := v.Field(i)
			if strings.Contains(tag, ",omitempty") && deltaJSONEmpty(item) {
				continue
			}
			if count > 0 {
				if err := c.add(1); err != nil {
					return err
				}
			}
			count++
			if err := c.text(name); err != nil {
				return err
			}
			if err := c.add(1); err != nil {
				return err
			}
			if err := c.value(item); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unsupported counted JSON type %s", v.Type())
}
func deltaJSONEmpty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}
