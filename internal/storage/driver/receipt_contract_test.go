package driver

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Consumer signature checks use real method expressions, without a runtime
// driver substitute. Existing finalizer, no-op and cleanup capabilities must
// remain source-compatible while receipt metadata gains explicit state.
var (
	_ func(ResponseFinalizer, context.Context, ActiveLease, string, string, string, string, model.Revision, []model.Warning) error = ResponseFinalizer.FinalizeResponse
	_ func(NoopCommitter, context.Context, ChangesetHeader) (model.Commit, error)                                                  = NoopCommitter.CommitNoop
	_ func(Reader, context.Context, string, string, string, string) (*IdempotencyRecord, error)                                    = Reader.IdempotencyByKey
	_ func(Reader) error                                                                                                           = Reader.Close
	_ func(Store) error                                                                                                            = Store.Close
)

func TestReceiptPendingStateContract(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[ChangesetHeader](), reflect.TypeFor[IdempotencyRecord]()} {
		field, ok := typ.FieldByName("ResponsePending")
		if !ok || !field.IsExported() || field.Type != reflect.TypeFor[bool]() {
			t.Fatalf("%s must expose ResponsePending bool", typ.Name())
		}
		if reflect.Zero(typ).FieldByIndex(field.Index).Bool() {
			t.Fatalf("%s zero value must represent a final response", typ.Name())
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[ResponseFinalizer](), reflect.TypeFor[NoopCommitter]()} {
		if typ.NumMethod() != 1 {
			t.Fatalf("%s must remain an independent optional capability", typ.Name())
		}
	}
}

func TestReceiptStateStaysOutsideCommitWireContract(t *testing.T) {
	// A receipt may be pending internally, but public Commit still has the
	// closed wire shape and lossless per-result revisions. Neither state is a
	// new RPC success shape. Runtime gating belongs to pipeline conformance.
	receipt := IdempotencyRecord{
		RequestHash: "request-digest",
		Commit: model.Commit{
			Revision: model.Natural("18446744073709551617"),
			Results: []model.OpResult{{
				Ref:      model.RefByID("bead", model.Natural64(0)),
				Revision: model.Natural("9007199254740993"),
			}},
			Warnings: []model.Warning{},
		},
		ResponsePending: true,
	}
	want := `{"revision":18446744073709551617,"results":[{"ref":{"id":"bead","branch":0},"revision":9007199254740993}],"warnings":[]}`
	for _, pending := range []bool{true, false} {
		receipt.ResponsePending = pending
		encoded, err := json.Marshal(receipt.Commit)
		if err != nil || string(encoded) != want {
			t.Fatalf("pending=%t changed Commit wire contract: %s, %v", pending, encoded, err)
		}
	}
	var commit model.Commit
	if err := json.Unmarshal([]byte(`{"revision":0,"results":[],"warnings":[],"responsePending":true}`), &commit); err == nil {
		t.Fatal("internal receipt state entered the public Commit schema")
	}
}
