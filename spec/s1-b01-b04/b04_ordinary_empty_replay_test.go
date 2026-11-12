package s1b01b04

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/api/wire"
)

// These are authored request/outcome expectations, not a recorded response.
// The original native empty-front observation has a different CLI envelope.
//
//go:embed b04_ordinary_empty_replay_inputs.json
var ordinaryB04EmptyReplayInputs []byte

func (o *ordinaryB04) replayNeverPopulatedEmpty(stage string) {
	t := o.f.t
	t.Helper()
	if sha256Hex(ordinaryB04EmptyReplayInputs) != "368dd9e3b83677c26952d1eb2d9c64c11f187ea35d0299fab12450ad5ef84679" {
		t.Fatal("ordinary empty replay input bytes changed")
	}
	var input struct {
		Format          string          `json:"format"`
		Provenance      json.RawMessage `json:"provenance"`
		ReceiptLabel    string          `json:"receiptLabel"`
		Method          string          `json:"method"`
		Agent           string          `json:"agent"`
		Params          map[string]any  `json:"params"`
		LiveReadyFilter map[string]any  `json:"liveReadyFilter"`
		LiveReadyIDs    []string        `json:"liveReadyIDs"`
		Expected        struct {
			Result             json.RawMessage `json:"result"`
			Replayed           bool            `json:"replayed"`
			HeadDelta          int             `json:"headDelta"`
			ChangesetDelta     int             `json:"changesetDelta"`
			DomainVersionDelta int             `json:"domainVersionDelta"`
			ReceiptDelta       int             `json:"receiptDelta"`
		} `json:"expected"`
		StateConservation []string `json:"stateConservation"`
		ReplayStages      []string `json:"replayStages"`
		NativeBoundary    string   `json:"nativeBoundary"`
	}
	decoder := json.NewDecoder(bytes.NewReader(ordinaryB04EmptyReplayInputs))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	if input.Format != "plug.s1.b04.ordinary-empty-replay-inputs/1" || input.Method != "bead.claimNext" || input.Agent != owner || len(input.Params) != 0 || !bytes.Equal(input.Expected.Result, []byte("null")) || !input.Expected.Replayed || input.Expected.HeadDelta != 0 || input.Expected.ChangesetDelta != 0 || input.Expected.DomainVersionDelta != 0 || input.Expected.ReceiptDelta != 0 {
		t.Fatal("ordinary empty replay contract differs")
	}
	admittedStage := false
	for _, name := range input.ReplayStages {
		admittedStage = admittedStage || name == stage
	}
	if !admittedStage {
		t.Fatalf("undeclared ordinary empty replay stage %q", stage)
	}
	prior, ok := o.receipts[input.ReceiptLabel]
	if !ok || prior == nil || len(prior.Commit.Results) != 0 || len(prior.Changeset.Changes) != 0 {
		t.Fatal("replay requires the original independently checked empty receipt")
	}
	before := o.state(stage + "-empty-replay-before")
	if got := o.readyIDs(stage+"-live-ready", input.LiveReadyFilter); !reflect.DeepEqual(got, input.LiveReadyIDs) {
		t.Fatalf("empty replay needs the exact later ready row: got=%v want=%v", got, input.LiveReadyIDs)
	}
	// call constructs the same method/params/project/agent/key as the initial
	// empty request. Only this new transport request's JSON-RPC ID differs.
	raw := o.call(input.ReceiptLabel, input.Method, input.Agent, input.Params)
	want := map[string]any{"jsonrpc": "2.0", "id": o.seq, "result": nil, "_meta": map[string]any{"replayed": true}}
	wantBytes, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	gotFrame, gotErr := wire.DecodeJSON(raw)
	wantFrame, wantErr := wire.DecodeJSON(wantBytes)
	if gotErr != nil || wantErr != nil || !reflect.DeepEqual(gotFrame, wantFrame) {
		t.Fatalf("stored empty result must replay its exact public envelope: got=%s want=%s decode=%v/%v", raw, wantBytes, gotErr, wantErr)
	}
	o.equalState(stage+"-empty-replay-after", before)
	o.checkReceipts()
	if got := o.receipt(input.ReceiptLabel, input.Method, input.Agent); !reflect.DeepEqual(got, prior) {
		t.Fatal("replayed empty changed its original durable receipt")
	}
	o.retain(stage+"-empty-replay-oracle", map[string]any{"inputs": json.RawMessage(ordinaryB04EmptyReplayInputs), "expectedFrame": json.RawMessage(wantBytes), "originalReceiptRevision": prior.Commit.Revision})
	t.Logf("B04_ORDINARY_EMPTY_REPLAY backend=%s stage=%s live_ready=%s exact_null=true replayed=true rows_head_history_versions_receipts_unchanged=true", o.f.backend, stage, fmt.Sprint(input.LiveReadyIDs))
}
