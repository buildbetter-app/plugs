package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func lifecycleRecord(t *testing.T, h *harness, ref model.Ref) driver.Record {
	t.Helper()
	rows, err := h.read(t).Get(context.Background(), []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("stored lifecycle row=%+v err=%v", rows, err)
	}
	return rows[0]
}

func TestLifecycleCreationInstantIsPersistedAndImmutable(t *testing.T) {
	h := setup(t)
	w := h.writer()
	createdAt := h.now
	created := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(810))
	ref := created.Results[0].Ref
	stored := lifecycleRecord(t, h, ref)
	if !bytes.Equal(stored.Body, created.Results[0].Record) {
		t.Fatal("create response and persisted bytes differ")
	}
	doc, err := DecodeDocument(stored.Body)
	if err != nil {
		t.Fatal(err)
	}
	instant, known := doc.CreationTime()
	if !known || !instant.Equal(createdAt) || doc.StartedAt != nil || doc.ClosedAt != nil {
		t.Fatalf("creation instant=%s known=%t document=%+v", instant, known, doc)
	}
	h.now = h.now.Add(time.Minute)
	apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "later"})}}, meta(811))
	doc, err = DecodeDocument(lifecycleRecord(t, h, ref).Body)
	if err != nil || doc.CreatedAt == nil || !doc.CreatedAt.Equal(createdAt) {
		t.Fatalf("edit rewrote creation instant: %+v err=%v", doc, err)
	}
	versions, err := h.read(t).Versions(context.Background(), ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 2 || !versions.Items[1].CreatedAt.Equal(h.now) || versions.Items[1].CreatedAt.Equal(createdAt) {
		t.Fatalf("version time was confused with bead creation: %+v err=%v", versions, err)
	}
	before := lifecycleRecord(t, h, ref)
	h.now = h.now.Add(time.Minute)
	noop := apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "later"})}}, meta(812))
	if noop.Revision != before.Revision || !bytes.Equal(lifecycleRecord(t, h, ref).Body, before.Body) {
		t.Fatal("ordinary no-op refreshed the creation instant")
	}
}

func TestLifecycleLegacyCreationStaysUnknownAfterEditsAndClaim(t *testing.T) {
	h := setup(t)
	ref := model.RefByID("pl-legacy-time", model.Natural64(0))
	body := []byte(`{"id":"pl-legacy-time","type":"issue","fields":{"title":"legacy","status":"todo"}}`)
	if _, err := conformance.Seed(context.Background(), h.store, h.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	h.now = h.now.Add(time.Minute)
	w := h.writer()
	apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "edited legacy"})}}, meta(820))
	m := meta(821)
	m.Method = "bead.claim"
	claimedAt := h.now
	claim := apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{})}}, m)
	before := lifecycleRecord(t, h, ref)
	doc, err := DecodeDocument(before.Body)
	if err != nil {
		t.Fatal(err)
	}
	if at, known := doc.CreationTime(); known || !at.IsZero() || doc.CreatedAt != nil || doc.StartedAt == nil || !doc.StartedAt.Equal(claimedAt) {
		t.Fatalf("legacy creation was fabricated or claim time missing: %+v", doc)
	}
	h.now = h.now.Add(time.Minute)
	m.IdempotencyKey = meta(822).IdempotencyKey
	same := apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{})}}, m)
	if same.Revision != claim.Revision || !bytes.Equal(lifecycleRecord(t, h, ref).Body, before.Body) {
		t.Fatal("same-holder claim rewrote a legacy body or started instant")
	}
}

func TestLifecycleCoreTimesCannotBePatchedByAMutator(t *testing.T) {
	h := setup(t)
	ref := apply(t, h.writer(), model.ChangeSet{Ops: []model.Op{create("")}}, meta(830)).Results[0].Ref
	before := lifecycleRecord(t, h, ref)
	w := h.writer(WithMutators(MutatorBinding{ID: "malicious-clock", Owns: []string{"fields.title"}, Mutator: mutatorFunc(func(_ context.Context, p port.ProposedChange, _ port.SnapshotReader) ([]port.Patch, error) {
		return []port.Patch{{Resource: p.Changes[0].Resource, Paths: map[string]json.RawMessage{"createdAt": json.RawMessage(`"2099-01-01T00:00:00Z"`)}}}, nil
	})}))
	_, err := w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "attempt"})}}, meta(831))
	code(t, err, "plugin_patch_out_of_scope")
	if after := lifecycleRecord(t, h, ref); after.Revision != before.Revision || !bytes.Equal(after.Body, before.Body) {
		t.Fatal("refused core-time patch changed persisted bytes")
	}
	principal, _ := driver.PrincipalKey(by)
	receipt, err := h.read(t).IdempotencyByKey(context.Background(), principal, "", "tx.apply", meta(831).IdempotencyKey)
	if err != nil || receipt != nil {
		t.Fatalf("refused core-time patch persisted receipt=%+v err=%v", receipt, err)
	}
}

func TestLifecycleDocumentRejectsMalformedOrZeroInstants(t *testing.T) {
	for _, name := range []string{"createdAt", "startedAt", "closedAt"} {
		for _, value := range []string{`null`, `"not-a-time"`, `"0001-01-01T00:00:00Z"`, `42`, `{}`} {
			t.Run(name+"/"+value, func(t *testing.T) {
				body := []byte(`{"id":"pl-malformed","type":"issue","fields":{},"` + name + `":` + value + `}`)
				if _, err := DecodeDocument(body); err == nil {
					t.Fatalf("malformed lifecycle instant accepted: %s", body)
				}
			})
		}
	}
}
