package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNativeReverseBlobStreamsPublishAtomically(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "public-blob-stream.sqlite")
	rawStore, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	store := rawStore.(*Store)
	defer store.Close()
	hash := func(raw []byte) driver.Hash {
		sum := sha256.Sum256(raw)
		return driver.Hash(hex.EncodeToString(sum[:]))
	}
	legacy := bytes.Repeat([]byte{0, 0xff, 17}, blobChunkBytes)
	legacyHash := hash(legacy)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO blobs(hash,rev,data) VALUES(?,?,?)`, legacyHash, revKey(0), legacy); err != nil {
		t.Fatal(err)
	}
	chunked := bytes.Repeat([]byte{0xf0, 0, 3, 9}, blobChunkBytes/2+1)
	chunkedHash := hash(chunked)
	publish := func(expected driver.Hash) error {
		return store.immediate(ctx, nil, func(c *sql.Conn) error {
			return publishNativeRestoreBlobs(ctx, c, revKey(0), func(put func(driver.Hash, int64, io.Reader) error) error {
				return put(expected, int64(len(chunked)), bytes.NewReader(chunked))
			})
		})
	}
	wrong := hash([]byte("different bytes"))
	if err := publish(wrong); err == nil {
		t.Fatal("expected failed digest to roll back the blob and its chunks")
	}
	var occupied bool
	if err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM blobs WHERE hash=?)
		OR EXISTS(SELECT 1 FROM sqlite_master WHERE name='blob_chunks')`, wrong).Scan(&occupied); err != nil || occupied {
		t.Fatalf("failed publication escaped its transaction: occupied=%t, err=%v", occupied, err)
	}
	if err := publish(chunkedHash); err != nil {
		t.Fatal(err)
	}
	var peak, count, sum int
	if err := store.db.QueryRowContext(ctx, `SELECT MAX(length(data)), COUNT(*), SUM(length(data))
		FROM blob_chunks WHERE hash=?`, chunkedHash).Scan(&peak, &count, &sum); err != nil {
		t.Fatal(err)
	}
	if peak != blobChunkBytes || count != 3 || sum != len(chunked) {
		t.Fatalf("storage chunk inventory differs: peak=%d count=%d bytes=%d", peak, count, sum)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := (Driver{}).Open(ctx, driver.Config{DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	meta, err := reopened.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reopened.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	buffer := make([]byte, blobChunkBytes+17)
	for _, fixture := range [][]byte{legacy, chunked} {
		want := hash(fixture)
		input, size, err := snapshot.(driver.BlobStreamReader).OpenBlob(ctx, want)
		if err != nil || size != int64(len(fixture)) {
			t.Fatalf("open bytes=%d err=%v", size, err)
		}
		h := sha256.New()
		for {
			n, readErr := input.Read(buffer)
			if n > blobChunkBytes {
				t.Fatalf("unbounded storage read of %d bytes", n)
			}
			_, _ = h.Write(buffer[:n])
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
		}
		if err := input.Close(); err != nil || hex.EncodeToString(h.Sum(nil)) != string(want) {
			t.Fatalf("streamed readback differs: %v", err)
		}
		actual, err := snapshot.Blob(ctx, want)
		if err != nil || !bytes.Equal(actual, fixture) {
			t.Fatalf("ordinary blob readback differs: %v", err)
		}
	}
	legacyInput, _, err := snapshot.(driver.BlobStreamReader).OpenBlob(ctx, legacyHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := legacyInput.Read(buffer); !errors.Is(err, driver.ErrTxClosed) {
		t.Fatalf("pinned reader did not close the legacy blob: %v", err)
	}
}

func TestNativeReverseCheckpointInventoryAdmitsWorkflowEnvelopeSeparately(t *testing.T) {
	ctx := driver.WithNativeReverseRead(context.Background())
	rawStore, err := (Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "public-checkpoint.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	store := rawStore.(*Store)
	defer store.Close()
	workflow := driver.SourcePlaneWorkflowNamespaceV1
	const expanded = driver.SourcePlaneMaxPlanBytes + 1
	_, err = store.db.ExecContext(ctx, `INSERT INTO checkpoints(key,generation,payload)
		VALUES('public-nil','1',NULL),(?,'1',zeroblob(?))`, workflow, expanded)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	view := driver.View{Incarnation: meta.Incarnation}
	inventory, err := store.ReadCheckpointInventory(ctx, view)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := inventory.CheckpointKeys()
	if err != nil || len(keys) != 2 {
		t.Fatalf("checkpoint inventory changed: count=%d, err=%v", len(keys), err)
	}
	checkpoint, err := inventory.SourcePlaneCheckpoint("public-nil")
	if err != nil || checkpoint.Generation != 1 || checkpoint.Payload != nil {
		t.Fatalf("nil checkpoint changed: generation=%d, err=%v", checkpoint.Generation, err)
	}
	checkpoint, err = inventory.SourcePlaneCheckpoint(workflow)
	if err != nil || len(checkpoint.Payload) != expanded {
		t.Fatalf("expanded workflow checkpoint changed: bytes=%d, err=%v", len(checkpoint.Payload), err)
	}
	if err := inventory.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = store.db.ExecContext(ctx, `INSERT INTO checkpoints(key,generation,payload)
		VALUES('public-oversize','1',zeroblob(?))`, expanded)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ReadCheckpointInventory(ctx, view)
	var limit *driver.ReverseJSONLimitError
	if !errors.As(err, &limit) || limit.Resource != "checkpoint_bytes" ||
		limit.Measured != expanded || limit.Limit != driver.SourcePlaneMaxPlanBytes ||
		limit.ElementIndex == nil || *limit.ElementIndex != 2 {
		t.Fatalf("checkpoint limit error = %v", err)
	}
}

func TestRetainedSourceStartupAdmitsEncodedPlanAboveTransportRecordLimit(t *testing.T) {
	raw := bytes.Repeat([]byte("x"), driver.NativeHeadRecordBytes+1)
	ref := driver.Ref{}
	if err := checkReverseMetadata(driver.WithRetainedSourceRead(context.Background()), ref, "instance_config", raw, 0); err != nil {
		t.Fatal("startup refused metadata within the source-plan wire range", err)
	}
	err := checkReverseMetadata(driver.WithNativeReverseRead(context.Background()), ref, "instance_config", raw, 0)
	var limit *driver.ReverseJSONLimitError
	if !errors.As(err, &limit) || limit.Resource != "value_bytes" || limit.Limit != driver.NativeHeadRecordBytes {
		t.Fatalf("transport record bound changed: %v", err)
	}
}

func TestRetainedSourceStartupStreamsLargeReceiptChangeset(t *testing.T) {
	ctx := context.Background()
	rawStore, err := (Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "public-receipt.sqlite")})
	if err != nil {
		t.Fatal(err)
	}
	store := rawStore.(*Store)
	defer store.Close()
	const change = `{"ref":{"id":"pl-public","branch":0},"op":"create","fromRevision":0,"changedPaths":["public-field"]}`
	count := driver.NativeHeadRecordBytes/len(change) + 1
	entry := `{"revision":0,"branch":0,"incarnation":"public","kind":"restore","time":"2037-01-01T00:00:00Z","principal":{"kind":"system","id":"public"},"chain":null,"idempotencyKey":"public","mutators":[],"changes":[` +
		strings.Repeat(change+",", count-1) + change + `]}`
	if _, err := store.db.ExecContext(ctx, `INSERT INTO replays(scope,created,rev,request_hash,response,changeset)
		VALUES(?,?,?,?,?,?)`, scopeKey("public", "", "restore", "public"), time.Now().UnixNano(),
		revKey(0), "public-hash", []byte(`{"revision":0,"results":[],"warnings":[]}`), []byte(entry)); err != nil {
		t.Fatal(err)
	}
	info, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Read(ctx, driver.View{Incarnation: info.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	offline := driver.WithNativeReverseRead(ctx)
	startup := driver.WithRetainedSourceRead(offline)
	receipt, err := driver.ReadNativeReverseReceipt(startup, reader, "public", "", "restore", "public", nil)
	if err != nil || receipt == nil {
		t.Fatalf("startup receipt: %v", err)
	}
	got := receipt.Changeset
	if got.Kind != model.ChangesetRestore || got.Incarnation != "public" || got.IdempotencyKey != "public" ||
		got.Time.IsZero() || got.Principal.ID != "public" || !got.Branch.IsZero() || !got.Revision.IsZero() ||
		got.Chain != nil || got.Mutators == nil || len(got.Mutators) != 0 || len(got.Changes) != count {
		t.Fatalf("startup changeset header or census differs: %d changes", len(got.Changes))
	}
	for i, row := range got.Changes {
		if row.Op != model.OpCreate || !row.FromRevision.IsZero() || row.Ref.ID != "pl-public" ||
			len(row.ChangedPaths) != 1 || row.ChangedPaths[0] != "public-field" {
			t.Fatalf("startup change %d differs: %+v", i, row)
		}
	}
	_, err = driver.ReadNativeReverseReceipt(offline, reader, "public", "", "restore", "public", nil)
	var limit *driver.ReverseJSONLimitError
	if !errors.As(err, &limit) || limit.Member != "receipt/changeset.json" || limit.JSONPath != "$" ||
		limit.Limit != driver.NativeHeadRecordBytes || limit.Measured != uint64(len(entry)) {
		t.Fatalf("standalone native reverse changed its changeset bound: %v", err)
	}
}

func TestRetainedSourceStartupPreservesReceiptFramingErrors(t *testing.T) {
	for _, prefix := range []string{`{"revis`, `{"results":[`, `{"warnings":[]`, `{"results":[]}`} {
		source := io.MultiReader(strings.NewReader(prefix), readerFunc(func([]byte) (int, error) {
			return 0, context.Canceled
		}))
		err := readReverseReceipt(context.Background(), source, &driver.IdempotencyRecord{}, nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("receipt read after %q lost cancellation: %v", prefix, err)
		}
	}
	err := readReverseReceipt(context.Background(),
		strings.NewReader(`{"warnings":["`+strings.Repeat("x", reverseReceiptMetadataBytes)+`"]}`),
		&driver.IdempotencyRecord{}, nil)
	var limit *driver.ReverseJSONLimitError
	if !errors.As(err, &limit) || limit.Member != "receipt/commit.json" || limit.JSONPath != "$.warnings" ||
		limit.Resource != "value_bytes" || limit.ElementIndex == nil || *limit.ElementIndex != 0 {
		t.Fatalf("receipt metadata lost structural location: %v", err)
	}
}

func TestRetainedSourceStartupPreservesChangesetWireChecks(t *testing.T) {
	ref := model.RefByID("pl-public", model.Natural64(0))
	fixture := model.Changeset{
		Revision: model.Natural64(1), Branch: model.Natural64(0), Incarnation: "public", Kind: model.ChangesetRestore,
		Time: time.Date(2037, 1, 1, 0, 0, 0, 0, time.UTC), Principal: model.PrincipalRef{Kind: "system", ID: "public"},
		Chain: []string{"public"}, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)}},
		Events: []model.EventDescriptor{{
			Index: model.Natural64(0), ChangeIndex: model.Natural64(0), Type: model.EventBeadCreated,
			Resource: model.AuditResource{Kind: "bead", Ref: ref},
			Provenance: model.EventProvenance{Method: "restore", SetBy: map[string]string{}, Chain: []string{"public"},
				Cause: model.EventCause{Root: "public", Depth: model.Natural64(0), Chain: []string{}}},
		}},
	}
	wire, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var got model.Changeset
	if err := readSourceRecoveryChangeset(bytes.NewReader(wire), &got); err != nil || !reflect.DeepEqual(got, fixture) {
		t.Fatalf("streamed changeset differs: %+v, %v", got, err)
	}
	for _, tc := range []struct{ name, from, to string }{
		{"missing field", `"revision":1,`, ""},
		{"duplicate field", `"revision":1,`, `"revision":1,"revision":1,`},
		{"case variant", `"revision":1,`, `"Revision":1,`},
		{"null scalar", `"incarnation":"public"`, `"incarnation":null`},
		{"unknown nested field", `"changedPaths":null`, `"unknown":true,"changedPaths":null`},
		{"null nested element", `"changedPaths":null`, `"changedPaths":[null]`},
		{"nonnullable array", `"chain":["public"],"changes"`, `"mutators":null,"chain":["public"],"changes"`},
		{"unanchored event", `"changeIndex":0`, `"changeIndex":2`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := strings.Replace(string(wire), tc.from, tc.to, 1)
			if bad == string(wire) {
				t.Fatal("fixture mutation did not alter wire")
			}
			if err := readSourceRecoveryChangeset(strings.NewReader(bad), &got); err == nil {
				t.Fatal("invalid persisted changeset accepted")
			}
			if !reflect.DeepEqual(got, fixture) {
				t.Fatal("failed decode changed the output changeset")
			}
		})
	}
}

func TestRetainedSourceStartupLocatesReceiptDepthBeyondJSONScanner(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, suffix, member, path string
		index                              uint64
		decode                             func(io.Reader) error
	}{
		{"change", `{"changes":[{},`, `]}`, "receipt/changeset.json", "$.changes[]", 1, func(r io.Reader) error {
			return readSourceRecoveryChangeset(r, &model.Changeset{})
		}},
		{"warning", `{"warnings":`, `}`, "receipt/commit.json", "$.warnings", 0, func(r io.Reader) error {
			return readReverseReceipt(context.Background(), r, &driver.IdempotencyRecord{}, nil)
		}},
		{"result", `{"results":[`, `]}`, "receipt/commit.json", "$.results[]", 0, func(r io.Reader) error {
			return readReverseReceipt(context.Background(), r, &driver.IdempotencyRecord{}, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, nesting := range []int{driver.NativeHeadJSONDepth + 1, 10001} {
				value := strings.Repeat("[", nesting) + "0" + strings.Repeat("]", nesting)
				err := tc.decode(strings.NewReader(tc.prefix + value + tc.suffix))
				var limit *driver.ReverseJSONLimitError
				if !errors.As(err, &limit) || limit.Resource != "nesting_depth" || limit.Member != tc.member ||
					limit.JSONPath != tc.path || limit.ElementIndex == nil || *limit.ElementIndex != tc.index ||
					limit.Limit != driver.NativeHeadJSONDepth || limit.Measured <= limit.Limit {
					t.Fatalf("depth %d lost its typed receipt location: %v", nesting, err)
				}
			}
		})
	}
}
