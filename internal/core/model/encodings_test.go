package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPrincipalWarningAndErrorEncodings(t *testing.T) {
	for _, kind := range []string{"anonymous", "user", "agent", "service", "plugin", "system"} {
		roundtripComponent(t, "Principal", `{"kind":"`+kind+`","id":"","sponsor":null,"caveats":{"large":184467440737095516160}}`, &PrincipalRef{})
	}
	roundtripComponent(t, "Principal", `{"kind":"agent","id":"a","sponsor":{"kind":"user","id":"u","sponsor":{"kind":"system","id":""}}}`, &PrincipalRef{})
	for _, raw := range []string{`{}`, `{"kind":"unknown","id":"x"}`, `{"kind":"user","id":null}`, `{"kind":"user","id":"x","caveats":null}`, `{"kind":"user","id":"x","sponsor":{}}`, `{"kind":"user","id":"x","extra":true}`} {
		rejectComponent(t, "Principal", raw, &PrincipalRef{})
	}
	schemas := componentSchemas(t)
	for _, code := range schemas["WarningCode"].(map[string]any)["enum"].([]any) {
		roundtripComponent(t, "Warning", `{"code":"`+code.(string)+`","message":"","details":{"n":9007199254740993}}`, &Warning{})
	}
	for _, raw := range []string{`{"code":"not_found","message":"x"}`, `{"code":"subscriber_skipped"}`, `{"code":"subscriber_skipped","message":null}`, `{"code":"subscriber_skipped","message":"x","details":null}`} {
		rejectComponent(t, "Warning", raw, &Warning{})
	}
	for _, code := range schemas["ErrorCode"].(map[string]any)["enum"].([]any) {
		roundtripComponent(t, "ErrorData", `{"code":"`+code.(string)+`","retryable":false,"retryAfterMs":184467440737095516160,"hint":"","traceId":"","details":{}}`, &ErrorData{})
	}
	for _, raw := range []string{`{"code":"future","retryable":false}`, `{"code":"not_found"}`, `{"code":"not_found","retryable":null}`, `{"code":"not_found","retryable":false,"details":null}`} {
		rejectComponent(t, "ErrorData", raw, &ErrorData{})
	}
	cause := &Error{Message: "missing", Data: ErrorData{Code: "not_found"}}
	var typed *Error
	if !errors.As(fmt.Errorf("read: %w", cause), &typed) || typed.Data.Code != "not_found" || typed.Error() != "missing" {
		t.Fatal("typed error wrapping lost")
	}
}

func TestRecordCommitAndViolationAgainstOpenRPC(t *testing.T) {
	roundtripComponent(t, "Record", `{"ref":{"id":""},"kind":"","revision":1e30,"body":{"n":9007199254740993}}`, &Record{})
	for _, raw := range []string{`{}`, `{"ref":{"id":"a"},"kind":"task","revision":0,"body":null}`, `{"ref":{"id":"a"},"kind":"task","revision":0,"body":{},"extra":true}`} {
		rejectComponent(t, "Record", raw, &Record{})
	}
	roundtripComponent(t, "Commit", `{"revision":1e30,"results":[{"ref":{"key":""},"revision":0,"record":{"n":9007199254740993},"setBy":{"title":"plugin"}}],"warnings":[],"wouldCommit":false,"violations":[],"leaseExpiresAt":"2026-10-29T00:00:00Z","leaseTtlMs":1.0}`, &Commit{})
	for _, raw := range []string{`{}`, `{"revision":0,"results":null,"warnings":[]}`, `{"revision":0,"results":[],"warnings":null}`, `{"revision":0,"results":[],"warnings":[],"leaseTtlMs":0}`, `{"revision":0,"results":[],"warnings":[],"leaseExpiresAt":null}`, `{"revision":0,"results":[],"warnings":[],"unexpected":true}`, `{"revision":0,"results":[{"ref":{"id":"a"},"revision":0,"record":null}],"warnings":[]}`} {
		rejectComponent(t, "Commit", raw, &Commit{})
	}
	roundtripComponent(t, "Violation", `{"guard":"","message":"","details":{"n":9007199254740993}}`, &Violation{})
	rejectComponent(t, "Violation", `{"guard":"x"}`, &Violation{})
}

func durableJSON(kind string) string {
	return `{"category":"durable","cursor":"","incarnation":"","revision":184467440737095516160,"index":1.0,"type":"` + kind + `","resource":"","changed":[],"before":{},"after":{"n":9007199254740993},"principal":{"kind":"anonymous","id":""},"chain":[],"setBy":{},"cause":{"root":"","depth":8.0,"chain":[]},"derived":true}`
}

func controlJSON(kind, details, extra string) string {
	return `{"category":"control","cursor":"","incarnation":"","revision":0,"index":0,"type":"` + kind + `","details":` + details + `,"derived":false` + extra + `}`
}

func TestEveryEventKindAgainstOpenRPC(t *testing.T) {
	schemas := componentSchemas(t)
	durable := schemas["OrdinaryDurableEvent"].(map[string]any)["properties"].(map[string]any)["type"].(map[string]any)["enum"].([]any)
	for _, kind := range durable {
		raw := durableJSON(kind.(string))
		roundtripComponent(t, "Event", raw, &Event{})
		roundtripComponent(t, "DurableEvent", raw, &DurableEvent{})
	}
	controls := []struct{ kind, details, extra string }{
		{"event.gap", `{"fromIncarnation":"","toIncarnation":"","floor":1e30,"reason":"restore"}`, ""},
		{"store.restored", `{"fromIncarnation":"","toIncarnation":"","reason":"restore"}`, ""},
		{"watch.delta", `{"subscriptionId":"","fromCursor":"","toCursor":"","events":[` + durableJSON("bead.created") + `]}`, ""},
		{"watch.reset", `{"subscriptionId":"","reason":"overflow","snapshotCursor":""}`, ""},
		{"visibility.lost", `{"reason":"redacted"}`, `,"resource":""`},
		{"rpc.rejected", `{"requestId":-184467440737095516160.0,"reason":"notification_no_result"}`, ""},
	}
	for _, c := range controls {
		t.Run(c.kind, func(t *testing.T) {
			raw := controlJSON(c.kind, c.details, c.extra)
			roundtripComponent(t, "Event", raw, &Event{})
			roundtripComponent(t, "ControlEvent", raw, &ControlEvent{})
		})
	}
	for _, id := range []string{`""`, `1e30`, `-1.0`, `-1e30`} {
		roundtripComponent(t, "Event", controlJSON("rpc.rejected", `{"requestId":`+id+`,"reason":"notification_no_result"}`, ""), &Event{})
	}
}

func TestEventRejectsWrongArmsAndBounds(t *testing.T) {
	valid := parseJSON(t, []byte(durableJSON("bead.created"))).(map[string]any)
	for _, mutation := range []func(map[string]any){
		func(v map[string]any) { v["category"] = "future" }, func(v map[string]any) { v["type"] = "event.gap" },
		func(v map[string]any) { v["cause"].(map[string]any)["depth"] = json.Number("9") },
		func(v map[string]any) { v["changed"] = []any{nil} }, func(v map[string]any) { v["setBy"] = map[string]any{"x": nil} },
		func(v map[string]any) { v["after"] = nil }, func(v map[string]any) { v["extra"] = true },
	} {
		raw, _ := json.Marshal(valid)
		v := parseJSON(t, raw).(map[string]any)
		mutation(v)
		bad, _ := json.Marshal(v)
		rejectComponent(t, "Event", string(bad), &Event{})
	}
	for _, raw := range []string{
		controlJSON("event.gap", `{"fromIncarnation":"","toIncarnation":"","floor":0,"reason":"future"}`, ""),
		controlJSON("watch.reset", `{"reason":"overflow"}`, ""),
		controlJSON("visibility.lost", `{"reason":"redacted"}`, ""),
		controlJSON("rpc.rejected", `{"requestId":null,"reason":"notification_no_result"}`, ""),
		controlJSON("rpc.rejected", `{"requestId":1.5,"reason":"notification_no_result"}`, ""),
		controlJSON("store.restored", `{"fromIncarnation":"","toIncarnation":"","reason":"failover"}`, ""),
		controlJSON("watch.delta", `{"subscriptionId":"","fromCursor":"","toCursor":"","events":null}`, ""),
		controlJSON("event.gap", `{"fromIncarnation":"","toIncarnation":"","floor":0,"reason":"restore"}`, `,"resource":""`),
	} {
		rejectComponent(t, "Event", raw, &Event{})
	}
	if _, err := json.Marshal(Event{Durable: &DurableEvent{}, Control: &ControlEvent{}}); err == nil {
		t.Fatal("mixed event arms marshaled")
	}
	if _, err := json.Marshal(Event{Category: EventControl, Durable: &DurableEvent{}}); err == nil {
		t.Fatal("category mismatch marshaled")
	}
}

func TestNativeInvalidEncodingsFailExplicitly(t *testing.T) {
	p := &PrincipalRef{Kind: "agent", ID: "a"}
	p.Sponsor = p
	if _, err := json.Marshal(p); err == nil {
		t.Fatal("cyclic principal accepted")
	}
	invalid := []any{
		ErrorCode("future"), WarningCode("not_found"), OpKind("future"), ChangesetKind("future"),
		PrincipalRef{Kind: "future"}, Warning{Code: "not_found"},
		EventCause{Depth: Natural64(9), Chain: []string{}},
		EventCause{Depth: Natural("1e1000000000"), Chain: []string{}},
		ControlEvent{Type: EventGap, Details: WatchResetDetails{Reason: "overflow"}},
		ControlEvent{Type: EventWatchReset, Resource: "extra", Details: WatchResetDetails{Reason: "overflow"}},
		Record{Ref: RefByID("a", Natural64(0))},
		OpResult{Ref: RefByID("a", Natural64(0)), Record: json.RawMessage(`null`)},
		Commit{Results: []OpResult{}, Warnings: []Warning{}, LeaseTtlMs: func() *Natural { v := Natural64(0); return &v }()},
	}
	for _, v := range invalid {
		if _, err := json.Marshal(v); err == nil {
			t.Fatalf("invalid native value marshaled: %#v", v)
		}
	}
	for _, kind := range []ChangesetKind{ChangesetWrite, ChangesetDerived, ChangesetRevert, ChangesetMigration, ChangesetRedact, ChangesetRestore} {
		v := Changeset{Revision: Natural("1e30"), Branch: Natural64(0), Incarnation: "store", Kind: kind, Time: time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC), Principal: PrincipalRef{Kind: "user", ID: ""}, Chain: []string{}, Changes: []ChangeRecord{{Ref: RefByID("a", Natural64(0)), Op: OpPatch, FromRevision: Natural64(0), ChangedPaths: []string{"title"}}}}
		encoded, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkComponent(t, "Changeset", encoded); err != nil {
			t.Fatal(err)
		}
		var decoded Changeset
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Kind != kind || decoded.Changes[0].Op != OpPatch {
			t.Fatal("changeset header roundtrip failed", err)
		}
	}
}
