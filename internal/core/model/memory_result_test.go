package model

import (
	"encoding/json"
	"testing"
)

func TestMemoryReplacementWireDistinguishesAbsentFalseTrue(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag *bool
		want string
	}{
		{"other-operation-absent", nil, `{"ref":{"id":"public","branch":0},"revision":1}`},
		{"absent-key-false", func() *bool { v := false; return &v }(), `{"ref":{"id":"public","branch":0},"revision":1,"replaced":false}`},
		{"present-key-true", func() *bool { v := true; return &v }(), `{"ref":{"id":"public","branch":0},"revision":1,"replaced":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(OpResult{Ref: RefByID("public", Natural64(0)), Revision: Natural64(1), Replaced: tc.flag})
			if err != nil || string(raw) != tc.want {
				t.Fatalf("wire got=%s want=%s err=%v", raw, tc.want, err)
			}
			var result OpResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			if (result.Replaced == nil) != (tc.flag == nil) || tc.flag != nil && *result.Replaced != *tc.flag {
				t.Fatal("optional replacement flag did not round trip")
			}
			if err := checkComponent(t, "OpResult", raw); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, invalid := range []string{`null`, `"true"`, `0`} {
		var result OpResult
		if err := json.Unmarshal([]byte(`{"ref":{"id":"public","branch":0},"revision":1,"replaced":`+invalid+`}`), &result); err == nil {
			t.Fatalf("accepted nonboolean replaced=%s", invalid)
		}
	}
}
