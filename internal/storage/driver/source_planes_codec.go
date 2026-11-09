package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"unicode/utf8"
)

const (
	SourcePlaneMaxPlanBytes   = 64 << 20
	SourcePlaneMaxListEntries = 1 << 20
	sourcePlaneDigestDomain   = "plug.restore.source-planes.joint-plan/1"
)

// SourcePlaneMirrorIDV1 identifies a retained auxiliary source record using
// all physical identity components. It does not change the source address.
// Restore and public readers share this framing, including an empty project.
func SourcePlaneMirrorIDV1(kind, project, table, id string) string {
	key, _ := json.Marshal([]string{kind, project, table, id})
	sum := sha256.Sum256(key)
	return "source/" + kind + "/" + hex.EncodeToString(sum[:])
}

// EncodeSourcePlanePlanV1 encodes only the closed SourcePlanePlanV1 schema,
// in declaration order. Strings are valid UTF-8, not normalized. Byte strings
// are length-delimited and every list count is uint64 big endian. Optional
// pointers and booleans have an explicit 0/1 tag. Set order is checked, never
// silently repaired. A changed schema requires a new protocol version.
func EncodeSourcePlanePlanV1(plan SourcePlanePlanV1) ([]byte, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := encodeSourcePlaneValue(&out, reflect.ValueOf(plan)); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// DecodeSourcePlanePlanV1 bounds every allocation against both the complete
// input and the minimum encoded element size. Neither a count nor a field
// length can request an allocation larger than the admitted input budget.
func DecodeSourcePlanePlanV1(data []byte) (SourcePlanePlanV1, error) {
	if len(data) == 0 || len(data) > SourcePlaneMaxPlanBytes {
		return SourcePlanePlanV1{}, sourcePlaneError("plan byte limit")
	}
	r := bytes.NewReader(data)
	var plan SourcePlanePlanV1
	if err := decodeSourcePlaneValue(r, reflect.ValueOf(&plan).Elem()); err != nil {
		return SourcePlanePlanV1{}, err
	}
	if r.Len() != 0 {
		return SourcePlanePlanV1{}, sourcePlaneError("trailing canonical plan data")
	}
	if err := plan.Validate(); err != nil {
		return SourcePlanePlanV1{}, err
	}
	encoded, err := EncodeSourcePlanePlanV1(plan)
	if err != nil || !bytes.Equal(encoded, data) {
		return SourcePlanePlanV1{}, sourcePlaneError("noncanonical plan")
	}
	return plan, nil
}

func sourcePlaneError(reason string) error {
	return fmt.Errorf("%w: source planes: %s", ErrUnsupported, reason)
}

func writeSourcePlaneBytes(out *bytes.Buffer, data []byte) error {
	if len(data) > SourcePlaneMaxPlanBytes || out.Len() > SourcePlaneMaxPlanBytes-8-len(data) {
		return sourcePlaneError("plan byte limit")
	}
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(data)))
	out.Write(size[:])
	out.Write(data)
	return nil
}

// Reflection is private and starts at one concrete, closed schema. No caller
// can select a type, insert a map/interface, or change the field order.
func encodeSourcePlaneValue(out *bytes.Buffer, v reflect.Value) error {
	if out.Len() > SourcePlaneMaxPlanBytes-8 {
		return sourcePlaneError("plan byte limit")
	}
	switch v.Kind() {
	case reflect.String:
		if !utf8.ValidString(v.String()) {
			return sourcePlaneError("invalid UTF-8")
		}
		return writeSourcePlaneBytes(out, []byte(v.String()))
	case reflect.Uint64:
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], v.Uint())
		out.Write(n[:])
	case reflect.Bool:
		if v.Bool() {
			out.WriteByte(1)
		} else {
			out.WriteByte(0)
		}
	case reflect.Pointer:
		if v.IsNil() {
			out.WriteByte(0)
		} else {
			out.WriteByte(1)
			return encodeSourcePlaneValue(out, v.Elem())
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return writeSourcePlaneBytes(out, v.Bytes())
		}
		if v.Len() > SourcePlaneMaxListEntries {
			return sourcePlaneError("list count limit")
		}
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(v.Len()))
		out.Write(n[:])
		for i := 0; i < v.Len(); i++ {
			if err := encodeSourcePlaneValue(out, v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := encodeSourcePlaneValue(out, v.Field(i)); err != nil {
				return err
			}
		}
	default:
		return sourcePlaneError("unsupported canonical schema type")
	}
	return nil
}

func readSourcePlaneSize(r *bytes.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, sourcePlaneError("truncated canonical plan")
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func sourcePlaneMinimumSize(t reflect.Type) uint64 {
	switch t.Kind() {
	case reflect.Bool, reflect.Pointer:
		return 1
	case reflect.String, reflect.Uint64, reflect.Slice:
		return 8
	case reflect.Struct:
		var n uint64
		for i := 0; i < t.NumField(); i++ {
			n += sourcePlaneMinimumSize(t.Field(i).Type)
		}
		return n
	default:
		return 0
	}
}

func decodeSourcePlaneValue(r *bytes.Reader, v reflect.Value) error {
	switch v.Kind() {
	case reflect.String, reflect.Slice:
		n, err := readSourcePlaneSize(r)
		if err != nil {
			return err
		}
		if v.Kind() == reflect.String || v.Type().Elem().Kind() == reflect.Uint8 {
			if n > uint64(r.Len()) {
				return sourcePlaneError("truncated canonical byte field")
			}
			data := make([]byte, int(n))
			if _, err := io.ReadFull(r, data); err != nil {
				return err
			}
			if v.Kind() == reflect.String {
				if !utf8.Valid(data) {
					return sourcePlaneError("invalid UTF-8")
				}
				v.SetString(string(data))
			} else {
				v.SetBytes(data)
			}
			return nil
		}
		min := sourcePlaneMinimumSize(v.Type().Elem())
		if min == 0 || n > SourcePlaneMaxListEntries || n > uint64(r.Len())/min {
			return sourcePlaneError("canonical list count exceeds input")
		}
		v.Set(reflect.MakeSlice(v.Type(), int(n), int(n)))
		for i := 0; i < int(n); i++ {
			if err := decodeSourcePlaneValue(r, v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Uint64:
		n, err := readSourcePlaneSize(r)
		if err != nil {
			return err
		}
		v.SetUint(n)
	case reflect.Bool, reflect.Pointer:
		b, err := r.ReadByte()
		if err != nil || b > 1 {
			return sourcePlaneError("invalid presence/boolean tag")
		}
		if v.Kind() == reflect.Bool {
			v.SetBool(b == 1)
		} else if b == 1 {
			v.Set(reflect.New(v.Type().Elem()))
			return decodeSourcePlaneValue(r, v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := decodeSourcePlaneValue(r, v.Field(i)); err != nil {
				return err
			}
		}
	default:
		return sourcePlaneError("unsupported canonical schema type")
	}
	return nil
}

func sourcePlanePlanDigest(data []byte) string {
	var domain bytes.Buffer
	_ = writeSourcePlaneBytes(&domain, []byte(sourcePlaneDigestDomain))
	// Hash the length and bytes directly: a valid maximum-size plan need not
	// fit in a second size-limited canonical buffer together with the domain.
	h := sha256.New()
	h.Write(domain.Bytes())
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(data)))
	h.Write(size[:])
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func NewSourcePlaneImportBindingV1(plan SourcePlanePlanV1) (SourcePlaneImportBindingV1, error) {
	data, err := EncodeSourcePlanePlanV1(plan)
	if err != nil {
		return SourcePlaneImportBindingV1{}, err
	}
	return SourcePlaneImportBindingV1{Version: SourcePlanePlanVersionV1, CanonicalPlan: data, JointPlanDigest: sourcePlanePlanDigest(data)}, nil
}

func (b SourcePlaneImportBindingV1) Detached() (SourcePlaneImportBindingV1, error) {
	if b.Version != SourcePlanePlanVersionV1 || len(b.CanonicalPlan) == 0 || len(b.CanonicalPlan) > SourcePlaneMaxPlanBytes || !importDigest(b.JointPlanDigest) || sourcePlanePlanDigest(b.CanonicalPlan) != b.JointPlanDigest {
		return SourcePlaneImportBindingV1{}, sourcePlaneError("invalid joint plan binding")
	}
	if _, err := DecodeSourcePlanePlanV1(b.CanonicalPlan); err != nil {
		return SourcePlaneImportBindingV1{}, err
	}
	b.CanonicalPlan = bytes.Clone(b.CanonicalPlan)
	return b, nil
}

func (b SourcePlaneImportBindingV1) Plan() (SourcePlanePlanV1, error) {
	detached, err := b.Detached()
	if err != nil {
		return SourcePlanePlanV1{}, err
	}
	return DecodeSourcePlanePlanV1(detached.CanonicalPlan)
}

// UnmarshalJSON refuses partial/unknown/duplicate bindings, including null.
func (b *SourcePlaneImportBindingV1) UnmarshalJSON(data []byte) error {
	type wire SourcePlaneImportBindingV1
	if err := sourcePlaneJSONObject(data, map[string]bool{"version": true, "canonicalPlan": true, "jointPlanDigest": true}, true); err != nil {
		return err
	}
	var value wire
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&value); err != nil {
		return err
	}
	detached, err := SourcePlaneImportBindingV1(value).Detached()
	if err != nil {
		return err
	}
	*b = detached
	return nil
}

func sourcePlaneJSONObject(data []byte, fields map[string]bool, requireAll bool) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return sourcePlaneError("expected binding object")
	}
	seen := make(map[string]bool)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || seen[key] || (fields != nil && !fields[key]) {
			return sourcePlaneError("duplicate/unknown binding field")
		}
		seen[key] = true
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return err
		}
		if key == "sourcePlanes" && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return sourcePlaneError("explicit null sourcePlanes")
		}
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	if requireAll && len(seen) != len(fields) {
		return sourcePlaneError("partial binding")
	}
	if _, err := d.Token(); err != io.EOF {
		return sourcePlaneError("trailing JSON binding data")
	}
	return nil
}
