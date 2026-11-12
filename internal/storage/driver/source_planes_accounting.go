package driver

import (
	"bytes"
	"io"
	"reflect"
)

// The encoder interface permits a canonical equality check without materializing
// the encoded plan again. EncodeSourcePlanePlanV1 still uses its original buffer.
type sourcePlaneEncoder interface {
	Len() int
	Write([]byte) (int, error)
	WriteString(string) (int, error)
	WriteByte(byte) error
}
type sourcePlaneCanonicalCheck struct {
	data   []byte
	offset int
	equal  bool
}

func (c *sourcePlaneCanonicalCheck) Len() int { return c.offset }
func (c *sourcePlaneCanonicalCheck) Write(p []byte) (int, error) {
	if c.offset > len(c.data) || len(p) > len(c.data)-c.offset || !bytes.Equal(c.data[c.offset:c.offset+len(p)], p) {
		c.equal = false
	}
	c.offset += len(p)
	return len(p), nil
}
func (c *sourcePlaneCanonicalCheck) WriteString(s string) (int, error) {
	if c.offset > len(c.data) || len(s) > len(c.data)-c.offset || string(c.data[c.offset:c.offset+len(s)]) != s {
		c.equal = false
	}
	c.offset += len(s)
	return len(s), nil
}
func (c *sourcePlaneCanonicalCheck) WriteByte(b byte) error {
	if c.offset >= len(c.data) || c.data[c.offset] != b {
		c.equal = false
	}
	c.offset++
	return nil
}

// Compute the backing arrays, strings and pointed-to values that the closed
// decoder will allocate. It borrows the wire, makes no decoded candidates, and
// applies the same input/count bounds before multiplication or allocation.
func sourcePlaneAllocationSize(r *bytes.Reader, t reflect.Type) (uint64, error) {
	switch t.Kind() {
	case reflect.String, reflect.Slice:
		n, err := readSourcePlaneSize(r)
		if err != nil {
			return 0, err
		}
		if t.Kind() == reflect.String || t.Elem().Kind() == reflect.Uint8 {
			if n > uint64(r.Len()) {
				return 0, sourcePlaneError("truncated canonical byte field")
			}
			_, _ = r.Seek(int64(n), io.SeekCurrent)
			return n, nil
		}
		minimum := sourcePlaneMinimumSize(t.Elem())
		if minimum == 0 || n > SourcePlaneMaxListEntries || n > uint64(r.Len())/minimum {
			return 0, sourcePlaneError("canonical list count exceeds input")
		}
		total := n * uint64(t.Elem().Size())
		for i := uint64(0); i < n; i++ {
			size, err := sourcePlaneAllocationSize(r, t.Elem())
			if err != nil {
				return 0, err
			}
			total += size
		}
		return total, nil
	case reflect.Uint64:
		var b [8]byte
		_, err := io.ReadFull(r, b[:])
		return 0, err
	case reflect.Bool, reflect.Pointer:
		b, err := r.ReadByte()
		if err != nil || b > 1 {
			return 0, sourcePlaneError("invalid presence/boolean tag")
		}
		if t.Kind() == reflect.Pointer && b == 1 {
			size, err := sourcePlaneAllocationSize(r, t.Elem())
			return size + uint64(t.Elem().Size()), err
		}
		return 0, nil
	case reflect.Struct:
		var total uint64
		for i := 0; i < t.NumField(); i++ {
			size, err := sourcePlaneAllocationSize(r, t.Field(i).Type)
			if err != nil {
				return 0, err
			}
			total += size
		}
		return total, nil
	default:
		return 0, sourcePlaneError("unsupported canonical schema type")
	}
}

// NewSourcePlaneImportBindingWithAccounting uses the same canonical encoder and
// fixed output capacity. The sizing pass holds no serialized candidate.
func NewSourcePlaneImportBindingWithAccounting(plan SourcePlanePlanV1, accounting RestoreImportProfileAccounting) (SourcePlaneImportBindingV1, func(), error) {
	if err := plan.validateWithAccounting(accounting); err != nil {
		return SourcePlaneImportBindingV1{}, nil, err
	}
	count := sourcePlaneEncodedSize{}
	if err := encodeSourcePlaneValue(&count, reflect.ValueOf(plan)); err != nil {
		return SourcePlaneImportBindingV1{}, nil, err
	}
	release, err := accounting.reserveBytes("source_plan_encoded_output", uint64(count.Len()))
	if err != nil {
		return SourcePlaneImportBindingV1{}, nil, err
	}
	out := bytes.NewBuffer(make([]byte, 0, count.Len()))
	if err := encodeSourcePlaneValue(out, reflect.ValueOf(plan)); err != nil {
		release()
		return SourcePlaneImportBindingV1{}, nil, err
	}
	data := out.Bytes()
	return SourcePlaneImportBindingV1{Version: SourcePlanePlanVersionV1, CanonicalPlan: data, JointPlanDigest: sourcePlanePlanDigest(data)}, release, nil
}

type sourcePlaneEncodedSize struct{ n int }

func (c *sourcePlaneEncodedSize) Len() int                    { return c.n }
func (c *sourcePlaneEncodedSize) Write(p []byte) (int, error) { c.n += len(p); return len(p), nil }
func (c *sourcePlaneEncodedSize) WriteString(s string) (int, error) {
	c.n += len(s)
	return len(s), nil
}
func (c *sourcePlaneEncodedSize) WriteByte(byte) error { c.n++; return nil }

// Map element domains are admitted before capacity allocation. Runtime bucket
// metadata and allocator rounding are Go overhead, not another payload copy.
func sourcePlanValidationMap[K comparable, V any](accounting RestoreImportProfileAccounting, phase string, count int) (map[K]V, func(), error) {
	n := uint64(count) * uint64(reflect.TypeFor[K]().Size()+reflect.TypeFor[V]().Size())
	release, err := accounting.reserveBytes("source_plan_validation_"+phase, n)
	if err != nil {
		return nil, nil, err
	}
	return make(map[K]V, count), release, nil
}
