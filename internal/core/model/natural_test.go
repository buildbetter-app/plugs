package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNaturalLosslessIntegralDomain(t *testing.T) {
	cases := map[string]string{
		`0`: "0", `-0`: "0", `-0.00e+999`: "0", `1.000`: "1", `1e0`: "1", `100e-2`: "1", `12.30e1`: "123",
		`9007199254740993`: "9007199254740993", `9223372036854775807`: "9223372036854775807",
		`18446744073709551615`: "18446744073709551615", `18446744073709551616`: "18446744073709551616",
		`1000000000000000000000000000000000000000000000000000000000000000000000`: "1e69",
		`1e1000000000`: "1e1000000000", `1e9999999999999999999999999999999999999`: "1e9999999999999999999999999999999999999",
	}
	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			var value Natural
			if err := json.Unmarshal([]byte(input), &value); err != nil {
				t.Fatal(err)
			}
			if value.String() != want {
				t.Fatalf("got %s, want %s", value.String(), want)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != want {
				t.Fatalf("roundtrip got %s, want %s", encoded, want)
			}
			var revision Revision
			if err := json.Unmarshal(encoded, &revision); err != nil || revision.String() != want {
				t.Fatal("revision roundtrip failed", err)
			}
		})
	}
	for _, input := range []string{`-1`, `-1e100`, `0.1`, `1e-1`, `1e-999999999999999999999`, `1.00001`, `"1"`, `null`, `true`, `[]`, `{}`, `01`, `1 2`} {
		var value Natural
		if err := json.Unmarshal([]byte(input), &value); err == nil {
			t.Fatalf("accepted invalid natural %s", input)
		}
	}
	for _, input := range []Natural{"-1", "nope", "1.1", "null"} {
		if _, err := json.Marshal(input); err == nil {
			t.Fatalf("marshaled invalid direct natural %q", input)
		}
	}
	var zero Natural
	if !zero.IsZero() || zero.String() != "0" {
		t.Fatal("zero value is not zero")
	}
	if Natural("invalid").IsZero() {
		t.Fatal("invalid value reported zero")
	}
	if !strings.Contains(Natural("invalid").String(), "invalid") {
		t.Fatal("invalid string lost")
	}
}

func TestNaturalCheckedConversions(t *testing.T) {
	for _, input := range []Natural{"18446744073709551616", "1e1000000000", "-1"} {
		if _, err := input.Uint64(); err == nil {
			t.Fatalf("narrowed %s", input)
		}
	}
	if got, err := Natural("18446744073709551615").Uint64(); err != nil || got != ^uint64(0) {
		t.Fatal(got, err)
	}
	for _, input := range []Natural{"9223372036854775808", "1e1000000000", "-1"} {
		if _, err := input.Int64(); err == nil {
			t.Fatalf("narrowed int64 %s", input)
		}
	}
	if got, err := Natural("9223372036854775807").Int64(); err != nil || got != 1<<63-1 {
		t.Fatal(got, err)
	}
	if got, err := Natural("1.0e2").Uint64(); err != nil || got != 100 {
		t.Fatal(got, err)
	}
}

func TestRefArmsAgainstOpenRPC(t *testing.T) {
	for _, raw := range []string{`{"id":""}`, `{"key":""}`, `{"id":"a","branch":0.0}`, `{"id":"a","branch":184467440737095516160}`, `{"id":"a","branch":1e30}`} {
		var ref Ref
		roundtripComponent(t, "Ref", raw, &ref)
		if ref.IsID() != strings.Contains(raw, `"id"`) {
			t.Fatal("selected ref arm lost")
		}
	}
	for _, raw := range []string{`{}`, `null`, `{"id":"","key":""}`, `{"key":"","branch":0}`, `{"id":"a","branch":-1}`, `{"id":null}`, `{"key":1}`, `{"ID":"a"}`, `{"id":"a","branch":null}`, `{"id":"a","extra":true}`} {
		rejectComponent(t, "Ref", raw, &Ref{})
	}
	for _, r := range []Ref{RefByID("", Natural64(0)), RefByKey(""), {ID: "x"}, {Key: "x"}} {
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkComponent(t, "Ref", raw); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range []Ref{{}, {ID: "a", Key: "b"}, {Key: "b", Branch: Natural64(1)}, {ID: "a", Branch: Natural("-1")}} {
		if err := r.Validate(); err == nil {
			t.Fatalf("invalid ref validated: %#v", r)
		}
	}
}
