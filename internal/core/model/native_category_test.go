package model

import (
	"encoding/json"
	"testing"
)

func TestNativeCategoryConditionDecisionsRoundTripExactly(t *testing.T) {
	for _, category := range []string{"active", "wip", "done", "frozen"} {
		want := ConditionDecision{Status: "native_status", Category: category, Terminal: category == "done"}
		if want.Terminal {
			want.Outcome = "success"
		}
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got ConditionDecision
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("native decision changed: %+v != %+v", got, want)
		}
		bad := want
		bad.Terminal = !bad.Terminal
		if bad.Validate() == nil {
			t.Fatalf("inconsistent native decision accepted: %+v", bad)
		}
	}
	if err := (ConditionDecision{Status: "native_status", Category: "unknown"}).Validate(); err == nil {
		t.Fatal("unknown category accepted")
	}
}
