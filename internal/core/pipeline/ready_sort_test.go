package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This fixture exercises only Writer ownership of an already-classified rank.
// Canonical bucket/priority comparison belongs to B01's ontology/service lane.
type sortTestOntology struct {
	testOntology
	mutate bool
}

type unknownSortOntology struct{ testOntology }

func (unknownSortOntology) ReadySortKey(Document, time.Time) (int, int, bool, error) {
	return 0, 0, false, nil
}

func (o sortTestOntology) ReadySortKey(doc Document, _ time.Time) (int, int, bool, error) {
	if o.mutate {
		doc.Fields["title"] = "classifier cannot edit body"
		if doc.CreatedAt != nil {
			*doc.CreatedAt = doc.CreatedAt.Add(100 * time.Hour)
		}
	}
	return 0, 2, true, nil
}

func TestReadySortClassifierCannotEditThePendingBody(t *testing.T) {
	h := setup(t)
	w := h.writer(WithOntology(sortTestOntology{mutate: true}))
	createdAt := h.now
	commit := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(840))
	ref := commit.Results[0].Ref
	stored := lifecycleRecord(t, h, ref)
	doc, err := DecodeDocument(stored.Body)
	if err != nil || doc.Fields["title"] != "initial" || doc.CreatedAt == nil || !doc.CreatedAt.Equal(createdAt) {
		t.Fatalf("classifier changed bead body=%+v err=%v", doc, err)
	}
	rows, err := h.read(t).DerivedRows(context.Background(), driver.DerivedSortKey, "ready/")
	if err != nil || len(rows) != 1 {
		t.Fatalf("sort rows=%+v err=%v", rows, err)
	}
	id, key, err := DecodeReadySortKey(rows[0])
	if err != nil || id != ref.ID || key.Revision != commit.Revision || !key.CreatedAt.Equal(createdAt) || !key.RankedAt.Equal(createdAt) {
		t.Fatalf("sort key identity=%s value=%+v err=%v", id, key, err)
	}
	if !bytes.Equal(stored.Body, commit.Results[0].Record) {
		t.Fatal("rank publication changed the committed response image")
	}
}

func TestReadySortSourceOnlyRestampKeepsUnselectedCacheRow(t *testing.T) {
	h := setup(t)
	w := h.writer(WithOntology(sortTestOntology{}))
	created := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(853))
	ref := created.Results[0].Ref
	readRows := func() []driver.DerivedRow {
		t.Helper()
		rows, err := h.read(t).DerivedRows(context.Background(), driver.DerivedSortKey, "ready/")
		if err != nil || len(rows) != 1 {
			t.Fatalf("ready sort rows=%+v err=%v", rows, err)
		}
		return rows
	}
	before := readRows()
	changed := apply(t, w, model.ChangeSet{Ops: []model.Op{{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: "note", Value: json.RawMessage(`"source-only change"`)}}}}, meta(854))
	if changed.Revision.String() == created.Revision.String() {
		t.Fatal("source-only write did not publish a new source revision")
	}
	after := readRows()
	if !bytes.Equal(before[0].Value, after[0].Value) || before[0].Key != after[0].Key || before[0].Index != nil || after[0].Index != nil {
		t.Fatalf("same semantic sort rewrote the unselected cache: before=%+v after=%+v", before[0], after[0])
	}
	_, key, err := DecodeReadySortKey(after[0])
	if err != nil || key.Revision.String() != created.Revision.String() {
		t.Fatalf("source-only sort cache must retain its old revision: %+v %v", key, err)
	}
	h.now = h.now.Add(time.Second)
	updated := apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "ordinary update"})}}, meta(855))
	_, refreshed, err := DecodeReadySortKey(readRows()[0])
	if err != nil || refreshed.Revision.String() != updated.Revision.String() || !refreshed.RankedAt.Equal(h.now) {
		t.Fatalf("ordinary title update did not publish a fresh rank: %+v %v", refreshed, err)
	}
}

func TestReadySortCannotInventLegacyCreationEvenForDryRun(t *testing.T) {
	h := setup(t)
	ref := model.RefByID("pl-sort-legacy", model.Natural64(0))
	body := []byte(`{"id":"pl-sort-legacy","type":"issue","fields":{"title":"legacy","status":"todo"}}`)
	if _, err := conformance.Seed(context.Background(), h.store, h.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	before := lifecycleRecord(t, h, ref)
	for i, dry := range []bool{false, true} {
		m := meta(850 + i)
		m.DryRun = dry
		_, err := h.writer(WithOntology(sortTestOntology{})).Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "attempt"})}}, m)
		code(t, err, "invalid_params")
		after := lifecycleRecord(t, h, ref)
		if after.Revision != before.Revision || !bytes.Equal(after.Body, before.Body) {
			t.Fatal("refused legacy rank changed the bead")
		}
		rows, err := h.read(t).DerivedRows(context.Background(), driver.DerivedSortKey, "ready/")
		if err != nil || len(rows) != 0 {
			t.Fatalf("refused legacy rank created derived row=%+v err=%v", rows, err)
		}
		principal, _ := driver.PrincipalKey(by)
		receipt, err := h.read(t).IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
		if err != nil || receipt != nil {
			t.Fatalf("refused legacy rank created receipt=%+v err=%v", receipt, err)
		}
	}
	// A classifier that explicitly reports unavailable can still allow a real
	// legacy edit. It publishes no invented creation instant or live sort row.
	commit := apply(t, h.writer(WithOntology(unknownSortOntology{})), model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "known unavailable"})}}, meta(852))
	doc, err := DecodeDocument(commit.Results[0].Record)
	if err != nil || doc.CreatedAt != nil {
		t.Fatalf("known=false backfilled legacy creation: %+v err=%v", doc, err)
	}
	rows, err := h.read(t).DerivedRows(context.Background(), driver.DerivedSortKey, "ready/")
	if err != nil || len(rows) != 0 {
		t.Fatalf("known=false created live sort row=%+v err=%v", rows, err)
	}
}

func TestReadySortCodecRefusesMalformedFacts(t *testing.T) {
	key, err := ReadySortKeyRowKey("pl-rank")
	if err != nil {
		t.Fatal(err)
	}
	valid := map[string]any{"bucket": 0, "rank": 2, "createdAt": "2026-11-03T00:00:00Z", "rankedAt": "2026-11-03T00:00:01Z", "revision": 1}
	for _, field := range []string{"bucket", "rank", "createdAt", "rankedAt", "revision"} {
		for _, value := range []any{nil, "malformed"} {
			body := map[string]any{}
			for name, original := range valid {
				body[name] = original
			}
			body[field] = value
			raw, _ := json.Marshal(body)
			if _, _, err := DecodeReadySortKey(driver.DerivedRow{Key: key, Value: raw}); err == nil {
				t.Fatalf("malformed %s accepted: %s", field, raw)
			}
			delete(body, field)
			raw, _ = json.Marshal(body)
			if _, _, err := DecodeReadySortKey(driver.DerivedRow{Key: key, Value: raw}); err == nil {
				t.Fatalf("missing %s accepted: %s", field, raw)
			}
		}
	}
	for _, patch := range []map[string]any{{"bucket": -1}, {"rank": -1}, {"revision": 0}, {"unexpected": true}, {"createdAt": "0001-01-01T00:00:00Z"}, {"rankedAt": "0001-01-01T00:00:00Z"}} {
		body := map[string]any{}
		for name, original := range valid {
			body[name] = original
		}
		for name, value := range patch {
			body[name] = value
		}
		raw, _ := json.Marshal(body)
		if _, _, err := DecodeReadySortKey(driver.DerivedRow{Key: key, Value: raw}); err == nil {
			t.Fatalf("invalid rank fact accepted: %s", raw)
		}
	}
}
