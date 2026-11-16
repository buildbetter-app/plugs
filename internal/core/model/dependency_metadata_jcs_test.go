package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/gowebpki/jcs"
)

// JCS is a test-only oracle. The production model and the offline source
// plugin helper must compile without it or any additional module download.
func TestDependencyMetadataDifferentialJCS(t *testing.T) {
	corpus := []string{
		`null`, `true`, `false`, `0`, `-0`, `0.0`, `1`, `1.0`, `1e0`, `1 `,
		`1e-9999`, `0e999999999`, `1e9999`, `10e9998`, `9007199254740993`,
		`9007199254740992.0`, `9007199254740994`, `1e-6`, `0.000001`,
		`1e21`, `1000000000000000000000`, `5e-324`, `4.9406564584124654e-324`,
		`"x"`, `"\u0078"`, `"α"`, `"\u03b1"`, `"𝄞"`, `"\ud834\udd1e"`,
		`"a/b"`, `"a\/b"`, `"\n\t\b\f\r\\\""`, `"é"`, `"e\u0301"`,
		`{}`, ` { } `, `[]`, `[1,2]`, `[2,1]`, `{"a":1,"b":[true,null,"x"]}`,
		` { "b":[true,null,"\u0078"], "a":1e0 } `,
		`{"n":-0.0}`, `{"n":0e999999999}`, `{"n":1e9999}`,
		`{"n":1e-9999}`, `{"n":0}`, `{"x":1,"x":2}`, `{"x":2}`,
		`{"x":1,"\u0078":2}`, `{"a":{"x":1,"x":2}}`,
		`{"α":1,"𝄞":2}`, `{"\ud834\udd1e":2.0,"\u03b1":1e0}`,
		`not json`, ``, ` `, `[1 2]`, `01`, `[01]`, `{"a":1,}`, `1 2`, `{}[]`,
		`{"n":`, `"\q"`, `[true false]`, `{"a":null}`, `{"a":{}}`,
	}
	checked, invalid, nativeFallback := 0, 0, 0
	compare := func(left, right string) {
		t.Helper()
		a, _ := json.Marshal(left)
		b, _ := json.Marshal(right)
		got := dependencyMetadataTokenEqual(string(a), string(b))
		if !json.Valid([]byte(left)) || !json.Valid([]byte(right)) {
			invalid++
			if got != (left == right) {
				t.Fatalf("invalid JSON did not use exact bytes: %q / %q", left, right)
			}
			return
		}
		canonicalA, errA := jcs.Transform([]byte(left))
		canonicalB, errB := jcs.Transform([]byte(right))
		if errA != nil || errB != nil {
			// Pinned JCS rejects valid top-level number whitespace. That documented
			// difference is tested separately; duplicate/overflow cases still use
			// byte fallback in both implementations.
			if bytes.Equal(bytes.TrimSpace([]byte(left)), []byte(left)) && bytes.Equal(bytes.TrimSpace([]byte(right)), []byte(right)) {
				nativeFallback++
				if got != (left == right) {
					t.Fatalf("native fallback differs: %q / %q", left, right)
				}
			}
			return
		}
		checked++
		if want := bytes.Equal(canonicalA, canonicalB); got != want {
			t.Fatalf("valid JSON equality differs from pinned JCS: %q / %q got=%v want=%v", left, right, got, want)
		}
	}
	for _, left := range corpus {
		for _, right := range corpus {
			compare(left, right)
		}
	}
	// Deterministic generated pairs cross key ordering, nesting, escaping and
	// independently spelled IEEE-754 values; unequal variants change one leaf.
	random := rand.New(rand.NewSource(110712))
	for index := 0; index < 1024; index++ {
		number := random.Float64()*2e20 - 1e20
		decimal := strconv.FormatFloat(number, 'f', -1, 64)
		exponent := strconv.FormatFloat(number, 'e', -1, 64)
		left := fmt.Sprintf(`{"n":%s,"array":[%d,{"text":"α𝄞x","null":null}]}`, decimal, index)
		right := fmt.Sprintf(` {"array":[%d,{"null":null,"text":"\u03b1\ud834\udd1e\u0078"}],"n":%s} `, index, exponent)
		compare(left, right)
		compare(left, fmt.Sprintf(`{"n":%s,"array":[%d,{"text":"α𝄞y","null":null}]}`, decimal, index))
		compare(left, left+` {}`)
	}
	if checked < 4000 || invalid == 0 || nativeFallback == 0 {
		t.Fatalf("insufficient differential coverage: valid=%d invalid=%d fallback=%d", checked, invalid, nativeFallback)
	}
	t.Logf("pinned JCS valid pairs=%d; strict-invalid pairs=%d; native error fallback pairs=%d; generated pairs=3072", checked, invalid, nativeFallback)
}

func TestDependencyMetadataDocumentedJCSParserDivergences(t *testing.T) {
	for _, test := range []struct {
		name, left, right string
		plug, native      bool
	}{
		// jcs.go:169-174 parses the full top-level primitive via ParseFloat.
		{"top level whitespace", `1`, `1 `, true, false},
		// jcs.go:266-288 drops whitespace while collecting simple tokens.
		{"invalid number whitespace", `[1 2]`, `[12]`, false, true},
		// jcs.go:217-241 uses one DecodeRune replacement for an invalid pair;
		// encoding/json replaces each unpaired surrogate independently.
		{"invalid surrogate pair", `"\ud800\ud800"`, `"\ufffd"`, false, true},
		{"unpaired surrogate replacement", `"\ud800"`, `"\ufffd"`, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			left, _ := json.Marshal(test.left)
			right, _ := json.Marshal(test.right)
			if got := dependencyMetadataTokenEqual(string(left), string(right)); got != test.plug {
				t.Fatalf("Plug equality=%v want=%v", got, test.plug)
			}
			a, errA := jcs.Transform([]byte(test.left))
			b, errB := jcs.Transform([]byte(test.right))
			native := test.left == test.right
			if errA == nil && errB == nil {
				native = bytes.Equal(a, b)
			}
			if native != test.native {
				t.Fatalf("pinned JCS equality=%v want=%v errors=%v/%v", native, test.native, errA, errB)
			}
		})
	}
}
