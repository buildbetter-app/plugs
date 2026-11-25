package dumpformat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNativePreparedCaptureFileLedgerBindsOrderedRootsAndParts(t *testing.T) {
	archive, _ := preparedDeltaFixture(t)
	prepared, err := archive.PrepareNativeDelta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := prepared.NativeCaptureFileLedger(prepared.budget.parent)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger) != 3 || ledger[0].Name != "capture-set.json" || ledger[1].Name != "head.tar" || ledger[2].Name != "parts/00000000000000000001.part" {
		t.Fatalf("unexpected native capture ledger: %#v", ledger)
	}
	for _, file := range ledger {
		if !validDeltaSHA256(file.SHA256) {
			t.Fatalf("invalid native capture ledger entry: %#v", file)
		}
	}
	if ledger[0].SHA256 != digest([]byte("synthetic root")) {
		t.Fatal("capture root commitment changed")
	}
}

func TestNativePreparedCaptureFileLedgerChargesBeforeReturningCandidate(t *testing.T) {
	archive, _ := preparedDeltaFixture(t)
	prepared, err := archive.PrepareNativeDelta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	owner := prepared.budget.parent.Scope()
	defer owner.Release()
	beforeRoot := owner.Usage().CurrentAccountedDecodedBytes
	if ledger, err := prepared.NativeCaptureFileLedger(owner.root()); ledger != nil || err == nil {
		t.Fatal("root owner accepted a ledger with no release lifecycle")
	}
	if owner.Usage().CurrentAccountedDecodedBytes != beforeRoot {
		t.Fatal("refused root owner retained ledger bytes")
	}
	need, err := nativeCaptureFileLedgerJSONSize(prepared.files)
	if err != nil {
		t.Fatal(err)
	}
	root := owner.root()
	originalLimit, held := root.limit, root.used
	defer func() { root.limit = originalLimit }()
	root.limit = held + need - 1
	ledger, err := prepared.NativeCaptureFileLedger(owner)
	var limit *NativeDeltaDecodedLimitError
	if ledger != nil || !errors.As(err, &limit) || limit.Phase != "native_capture_file_ledger" || limit.RejectedChargeBytes != need {
		t.Fatalf("ledger escaped pre-admission limit: %#v %v", ledger, err)
	}
	if got := owner.Usage().CurrentAccountedDecodedBytes; got != held {
		t.Fatalf("refused ledger retained decoded bytes: %d", got)
	}
	root.limit = held + need
	ledger, err = prepared.NativeCaptureFileLedger(owner)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(ledger)
	if err != nil || uint64(len(encoded)+1) != need || owner.ReservedBytes("native_capture_file_ledger") != need {
		t.Fatalf("ledger charge differs from encoded payload: need=%d bytes=%d error=%v", need, len(encoded)+1, err)
	}
	prepared.Close()
	if owner.ReservedBytes("native_capture_file_ledger") != need || len(ledger) != 3 {
		t.Fatal("source close released surviving ledger")
	}
	ledger = nil
	owner.Release()
	if owner.Usage().CurrentCategories["native_capture_file_ledger"] != 0 {
		t.Fatal("discarded ledger retained reservation")
	}
}

func TestNativeStoredCredentialRefusalIsTypedAndCountsOnly(t *testing.T) {
	table := NativeDeltaTableSchema{Name: "config", Columns: []NativeDeltaColumn{{Name: "key", SQLType: "text", PrimaryKeyOrdinal: deltaTestUint(0)}, {Name: "value", SQLType: "text"}}}
	const logicalKey = "api_key_REVIEW_MARKER"
	cells := []NativeDeltaCell{{Kind: "string", Value: logicalKey}, {Kind: "string", Value: "do-not-leak-this"}}
	key, err := NativeDeltaPrimaryKey(table, cells)
	if err != nil {
		t.Fatal(err)
	}
	rowBytes, err := json.Marshal(Row{"key": logicalKey, "value": "do-not-leak-this"})
	if err != nil {
		t.Fatal(err)
	}
	row := NativeDeltaRow{Type: "row", Op: "insert", Key: base64.StdEncoding.EncodeToString(key), Cells: cells, NormalizedJSON: base64.StdEncoding.EncodeToString(append(rowBytes, '\n'))}
	_, _, err = validateNativeDeltaRow(table, row, nil)
	var refusal *NativeStoredCredentialError
	if !errors.As(err, &refusal) || refusal.Code != "native_stored_credential" || refusal.Family != "config" || refusal.Field != "value" || refusal.Count != 1 {
		t.Fatalf("missing typed credential refusal: %T %v", err, err)
	}
	encoded, marshalErr := json.Marshal(refusal)
	if marshalErr != nil || strings.Contains(string(encoded), logicalKey) || strings.Contains(string(encoded), "do-not-leak-this") || strings.Contains(err.Error(), "do-not-leak-this") || strings.Contains(err.Error(), logicalKey) {
		t.Fatal("credential value escaped typed refusal")
	}
	if err := validateSecretReferences(map[string]any{"api_key": "do-not-leak-this"}); err == nil || errors.As(err, &refusal) {
		t.Fatal("portable credential validator changed or became native typed error")
	}
	nestedMarker := "nested-token-REVIEW-MARKER"
	err = validateNativeDeltaSecretReferences("config", map[string]any{
		"key":   "ordinary-setting",
		"value": map[string]any{nestedMarker: "nested-secret"},
	})
	if !errors.As(err, &refusal) || refusal.Field != "value" || strings.Contains(err.Error(), nestedMarker) || strings.Contains(err.Error(), "nested-secret") {
		t.Fatalf("nested credential diagnostic leaked source path or value: %T %v", err, err)
	}
	encoded, marshalErr = json.Marshal(refusal)
	if marshalErr != nil || strings.Contains(string(encoded), nestedMarker) || strings.Contains(string(encoded), "nested-secret") {
		t.Fatal("nested credential diagnostic JSON leaked source path or value")
	}
}

func TestNativeBlankPeerCredentialStillPasses(t *testing.T) {
	for _, value := range []any{nil, "", map[string]any{"$bytes": ""}} {
		if err := validateNativeDeltaSecretReferences("federation_peers", map[string]any{"password_encrypted": value}); err != nil {
			t.Fatalf("blank peer credential refused: %#v: %v", value, err)
		}
	}
}

func TestReverseHeadAdmitsWideJSONWithinMemoryBudget(t *testing.T) {
	wrapping := `{"items":[`
	wide := []byte(wrapping + strings.Repeat("0,", 100_000) + "0]}")
	if got, err := DecodeReverseOriginFields(wide); err != nil || got != 100_001 {
		t.Fatalf("wide retained origin fields=%d err=%v", got, err)
	}
	record := []byte(`{"kind":"bead","ref":{"id":"wide","branch":0},"revision":1,"body":` + string(wide) + "}")
	if err := ValidateNativeHeadRecord(record); err != nil {
		t.Fatalf("wide material record was refused: %v", err)
	}
	var object strings.Builder
	object.WriteByte('{')
	for i := range 80_000 {
		fmt.Fprintf(&object, "%q:1,", strconv.Itoa(i))
	}
	object.WriteString(`"end":1}`)
	if got, err := DecodeReverseOriginFields([]byte(object.String())); err != nil || got != 80_001 {
		t.Fatalf("wide object fields=%d err=%v", got, err)
	}
	body := []byte(`{"escaped":"` + strings.Repeat(`\"{:,[`, 100_000) + `","nested":[{}, [], null, 1]}`)
	if fields, err := DecodeReverseOriginFields(body); err != nil || fields != 5 {
		t.Fatalf("quoted delimiters or empty containers changed the count: fields=%d, err=%v", fields, err)
	}
	if _, err := DecodeReverseOriginFields([]byte(`{"same":1,"same":2}`)); err == nil {
		t.Fatal("duplicate origin keys were admitted")
	}
}

func TestReverseHeadHistoryMarkerChecksEachFrame(t *testing.T) {
	const prefix = `{"kind":"beads-archive","ref":{"id":"plug-restore-history","branch":0},"revision":1,"body":`
	letters := strings.Repeat(`{"frame":{"sequence":1}},`, 17000)
	marker := `{"sourceFrames":[` + letters + `{"frame":{"sequence":1}}],"mapping":[]}`
	if err := ValidateNativeHeadRecord([]byte(prefix + marker + "}")); err != nil {
		t.Fatalf("wide marker with bounded frames was refused: %v", err)
	}
	if count, err := CountReverseHistoryMarkerFields([]byte(marker)); err != nil || count != 17002 {
		t.Fatalf("wide marker field count=%d, err=%v", count, err)
	}
	wideFrame := `{"sourceFrames":[{"value":[` + strings.Repeat("0,", 100_000) + `0]}],"mapping":[]}`
	if err := ValidateNativeHeadRecord([]byte(prefix + wideFrame + "}")); err != nil {
		t.Fatalf("wide individual frame was refused: %v", err)
	}
	overBudget := `{"sourceFrames":[{"value":[` + strings.Repeat("0,", 700_000) + `0]}],"mapping":[]}`
	var limit *driver.ReverseJSONLimitError
	if err := ValidateNativeHeadRecord([]byte(prefix + overBudget + "}")); !errors.As(err, &limit) ||
		limit.Resource != "decoded_bytes" || limit.Measured <= limit.Limit || limit.Limit != driver.NativeHeadMaterialBytes {
		t.Fatalf("individual frame escaped its decoded byte budget: %v", err)
	}
}

func TestReverseHeadRequiresExactEnvelopeKeys(t *testing.T) {
	const canonical = `{"kind":"bead","ref":{"id":"public","branch":0},"revision":1,"body":{}}`
	if err := ValidateNativeHeadRecord([]byte(canonical)); err != nil {
		t.Fatalf("canonical native envelope was refused: %v", err)
	}
	for _, raw := range []string{
		strings.Replace(canonical, `"kind"`, `"Kind"`, 1),
		strings.Replace(canonical, `"kind":"bead"`, `"kind":"unknown","Kind":"bead"`, 1),
	} {
		if err := ValidateNativeHeadRecord([]byte(raw)); err == nil {
			t.Fatal("case-aliased native envelope was admitted")
		}
	}
}

func TestRetainedHeadHistoryStreamsLargeCommitCensus(t *testing.T) {
	body := strings.Repeat("x", 9<<20)
	raw := []byte(`{"format":"beads-dolt-images/1","commits":[{"id":"first","parents":[],"metadata":{"note":"` +
		body + `"}},{"id":"second","parents":["first"],"metadata":{"note":"` + body +
		`"}}]}`)
	var history SourceHistory
	if _, err := decodeRetainedHeadJSON(raw, SourceHistoryMember, NewNativeDeltaBudget(0), &history); err != nil {
		t.Fatal(err)
	}
	if len(history.Commits) != 2 || history.Commits[0].Metadata["note"] != body ||
		history.Commits[1].Metadata["note"] != body || history.Commits[1].Parents[0] != "first" {
		t.Fatal("history commit census changed")
	}
}

func TestRetainedHeadManifestRedactsMapKeyOnDepthRefusal(t *testing.T) {
	const depth = 10_001
	raw := []byte(`{"files":{"private-entry-name":` + strings.Repeat("[", depth) +
		"0" + strings.Repeat("]", depth) + "}}")
	var manifest Manifest
	_, err := decodeRetainedHeadJSON(raw, "manifest.json", NewNativeDeltaBudget(0), &manifest)
	var limit *driver.ReverseJSONLimitError
	if !errors.As(err, &limit) || limit.Resource != "nesting_depth" || limit.Member != "manifest.json" ||
		limit.JSONPath != "$.files.<id>" || limit.ElementIndex == nil || *limit.ElementIndex != 0 ||
		strings.Contains(err.Error(), "private-entry-name") {
		t.Fatalf("expected located redacted depth error, got %v", err)
	}
}

func TestRetainedHeadHistoryRejectsMalformedContainers(t *testing.T) {
	for _, raw := range []string{
		`{"format":"beads-dolt-images/1",}`,
		`{"format":"beads-dolt-images/1","commits":[{"id":"one","parents":[],"metadata":{}},]}`,
		`{"format":"beads-dolt-images/1","commits":[`,
		`{"format":"beads-dolt-images/1","\u0066ormat":"duplicate"}`,
		"{\"format\":\"beads-dolt-images/1\"}\u00a0",
	} {
		var history SourceHistory
		if _, err := decodeRetainedHeadJSON([]byte(raw), SourceHistoryMember, NewNativeDeltaBudget(0), &history); err == nil {
			t.Fatalf("malformed history was accepted: %q", raw)
		}
	}
}
