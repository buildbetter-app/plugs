package dumpformat

import (
	"encoding/json"
	"testing"
)

func TestNativeJSONStringAllocationCountPreservesLexicalAndSurrogateInputs(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte(`""`), []byte(`"plain"`), []byte(`"<>&\n\t\\\""`), []byte(`"\u0041\u2028"`),
		[]byte(`"\ud83d\ude80"`), []byte(`"\ud800alone\udc00"`), []byte(`"世界"`), {'"', 0xff, '"'},
	} {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			t.Fatal(err)
		}
		n, err := NativeJSONStringBytes(raw)
		if err != nil || n != uint64(len(text)) {
			t.Fatalf("%q count=%d expected=%d err=%v", raw, n, len(text), err)
		}
	}
	for _, raw := range []string{`null`, `2`, `"unterminated`, `"\uZZZZ"`} {
		if _, err := NativeJSONStringBytes([]byte(raw)); err == nil {
			t.Fatal("invalid allocation input accepted", raw)
		}
	}
}
