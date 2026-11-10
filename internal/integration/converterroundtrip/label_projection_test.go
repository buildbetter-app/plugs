package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const importedLabelID = "demo-42v"
const importedOldLabel = "public-source-label"
const importedNewLabel = "public-source-renamed"

// This logical census is deliberately bounded and fails on incomplete pages.
// It observes imported bytes/versions, not a reconstruction by the lowerer.
type labelImportState struct {
	Meta          driver.Meta
	Records       []driver.Record
	Versions      map[string][]driver.Version
	Log           []model.Changeset
	Derived       map[driver.DerivedTable][]driver.DerivedRow
	Blobs         map[string][]byte
	ImportReceipt *driver.IdempotencyRecord
	WriteReceipt  *driver.IdempotencyRecord
}

func savedLabelImport(t *testing.T, store driver.Store, archive *dumpformat.Archive, p driver.RestoreImportProfile, writeKey string) labelImportState {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || rows.NextCursor != nil || len(rows.Items) == 1000 {
		t.Fatalf("incomplete imported-label record census: %v", err)
	}
	state := labelImportState{Meta: meta, Records: rows.Items, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Blobs: map[string][]byte{}}
	for _, row := range rows.Items {
		versions, err := r.Versions(ctx, row.Ref, driver.PageReq{Limit: 1000})
		if err != nil || versions.NextCursor != nil || len(versions.Items) == 1000 {
			t.Fatalf("incomplete imported-label versions for %s: %v", row.Ref.ID, err)
		}
		state.Versions[row.Ref.ID] = versions.Items
	}
	// Snapshot imports retain their floor changeset in the import receipt;
	// the public log starts strictly after the floor. Full imports have floor0.
	var cursor *driver.Cursor
	if !meta.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor}
	}
	state.Log, err = r.Changesets(ctx, cursor, 1000)
	if err != nil || len(state.Log) == 1000 {
		t.Fatalf("incomplete imported-label log: %v", err)
	}
	for _, table := range []driver.DerivedTable{driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins, driver.DerivedReadyState, driver.DerivedReferenceIndex, driver.DerivedSortKey} {
		state.Derived[table], err = r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	hashes := map[string]bool{archive.SHA256(): true}
	for _, file := range archive.Manifest().Files {
		hashes[file.SHA256] = true
	}
	for hash := range hashes {
		state.Blobs[hash], err = r.Blob(ctx, driver.Hash(hash))
		if err != nil || hashBytes(state.Blobs[hash]) != hash {
			t.Fatalf("imported source blob changed: %s %v", hash, err)
		}
	}
	principal, err := driver.PrincipalKey(p.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	state.ImportReceipt, err = r.IdempotencyByKey(ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	requestHash, hashErr := p.RequestHash()
	if err != nil || hashErr != nil || state.ImportReceipt == nil || state.ImportReceipt.ResponsePending || state.ImportReceipt.RequestHash != requestHash {
		t.Fatalf("imported-label receipt lost binding: %v %v", err, hashErr)
	}
	if writeKey != "" {
		state.WriteReceipt, err = r.IdempotencyByKey(ctx, principal, "", "tx.apply", writeKey)
		if err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func projectionLabels(t *testing.T, raw any) []string {
	t.Helper()
	data, err := json.Marshal(raw)
	var labels []string
	if err != nil || json.Unmarshal(data, &labels) != nil {
		t.Fatalf("label projection is not strings: %s %v", data, err)
	}
	return labels
}

func requireBothLabelProjections(t *testing.T, store driver.Store, lease driver.ActiveLease, actor model.PrincipalRef, id string, at model.Revision, want []string) driver.Record {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	rows, readErr := r.Get(ctx, []model.Ref{model.RefByID(id, model.Natural64(0))})
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(rows) != 1 {
		t.Fatalf("missing raw imported bead at%s: %v %v", at, readErr, closeErr)
	}
	// Decode the stored envelope independently of pipeline.DecodeDocument and
	// public projection code, so read-time normalization cannot satisfy this.
	var body struct {
		Fields map[string]json.RawMessage `json:"fields"`
		Labels []string                   `json:"labels"`
	}
	if err := json.Unmarshal(rows[0].Body, &body); err != nil || body.Fields == nil || !slices.Equal(body.Labels, want) {
		t.Fatalf("raw labels at%s: %s want=%v err=%v", at, rows[0].Body, want, err)
	}
	field, present := body.Fields["labels"]
	if len(want) == 0 {
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(rows[0].Body, &envelope); err != nil {
			t.Fatal(err)
		}
		_, topPresent := envelope["labels"]
		if present || topPresent {
			t.Fatalf("empty imported labels changed the historical envelope shape at%s: %s", at, rows[0].Body)
		}
	} else {
		var typed []string
		if !present || json.Unmarshal(field, &typed) != nil || !slices.Equal(typed, want) {
			t.Fatalf("raw typed labels disagree at%s: %s want=%v", at, field, want)
		}
	}
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	domain := service.New(store, lease, writer, service.WithOntology(ontology))
	got, err := domain.Get(ctx, actor, service.GetRequest{ID: rows[0].Ref, AsOf: &service.AsOf{Revision: at, Incarnation: meta.Incarnation}})
	if err != nil || !slices.Equal(got.Bead.Labels, want) || !slices.Equal(projectionLabels(t, got.Bead.Fields["labels"]), want) {
		t.Fatalf("public dual labels disagree at%s: %+v want=%v err=%v", at, got.Bead, want, err)
	}
	return rows[0]
}

func requireLabelViews(t *testing.T, store driver.Store, lease driver.ActiveLease, actor model.PrincipalRef, at model.Revision, oldPresent, newPresent bool) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	views, err := viewops.New(store, lease, viewops.Config{OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		query string
		match bool
	}{{"label=" + importedOldLabel, oldPresent}, {"NOT label=" + importedOldLabel, !oldPresent}, {"label=" + importedNewLabel, newPresent}} {
		budget := model.Natural64(1)
		request := viewops.RunRequest{Project: "demo", AsOf: &viewops.AsOf{Revision: at, Incarnation: meta.Incarnation}, Definition: &viewops.Definition{
			Filter: &viewops.Filter{Query: test.query, Resources: []string{importedLabelID}, Types: []string{"issue"}}, Fields: []string{"id"}, Sort: []string{"id"}, BudgetTokens: &budget,
		}}
		list, err := views.Run(ctx, actor, request)
		want := 0
		if test.match {
			want = 1
		}
		if err != nil || len(list.Items) != want || list.Count != nil || list.NextCursor != nil || list.AsOf != *request.AsOf || want == 1 && list.Items[0].ID != importedLabelID {
			t.Fatalf("label list at%s query=%q: %+v want=%d err=%v", at, test.query, list, want, err)
		}
		request.CountOnly = true
		count, err := views.Run(ctx, actor, request)
		if err != nil || count.Count == nil || *count.Count != want || len(count.Items) != 0 || count.NextCursor != nil || count.AsOf != *request.AsOf {
			t.Fatalf("label count at%s query=%q: %+v want=%d err=%v", at, test.query, count, want, err)
		}
	}
}

func requireReverseLabelValue(t *testing.T, store driver.Store, want []string) LiveReverse {
	t.Helper()
	got, err := ReverseRestored(context.Background(), store)
	if err != nil || len(got.Result.JSONL) == 0 || got.Result.Digest == "" {
		t.Fatalf("labelled live reverse refused: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(got.Result.JSONL))
	found := 0
	for {
		var row struct {
			ID     string   `json:"id"`
			Labels []string `json:"labels"`
		}
		if err := decoder.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if row.ID == importedLabelID {
			found++
			if !slices.Equal(row.Labels, want) {
				t.Fatalf("reverse emitted stale/normalized labels: got=%v want=%v", row.Labels, want)
			}
		}
	}
	if found != 1 {
		t.Fatalf("reverse lost the imported labelled bead: %d", found)
	}
	return got
}

func withoutLabelProjections(t *testing.T, raw []byte) []byte {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var body map[string]any
	if err := decoder.Decode(&body); err != nil {
		t.Fatal(err)
	}
	fields, ok := body["fields"].(map[string]any)
	if !ok {
		t.Fatal("bead lost typed fields")
	}
	delete(body, "labels")
	delete(fields, "labels")
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func requireOnlyImportedLabelWrite(t *testing.T, before, after labelImportState, commit model.Commit) {
	t.Helper()
	wantMeta := before.Meta
	wantMeta.Head = commit.Revision
	if !reflect.DeepEqual(after.Meta, wantMeta) || len(after.Records) != len(before.Records) || len(after.Log) != len(before.Log)+1 || !reflect.DeepEqual(before.Log, after.Log[:len(before.Log)]) {
		t.Fatal("label replacement changed imported identity, record census or log prefix")
	}
	changed := 0
	for i, original := range before.Records {
		got := after.Records[i]
		oldVersions, versions := before.Versions[original.Ref.ID], after.Versions[original.Ref.ID]
		if original.Ref.ID == importedLabelID {
			changed++
			if got.Ref != original.Ref || got.Kind != original.Kind || got.Revision != commit.Revision || !bytes.Equal(withoutLabelProjections(t, original.Body), withoutLabelProjections(t, got.Body)) || len(versions) != len(oldVersions)+1 || !reflect.DeepEqual(oldVersions, versions[:len(oldVersions)]) {
				t.Fatal("label replacement changed provenance or rewrote imported versions")
			}
		} else if !reflect.DeepEqual(original, got) || !reflect.DeepEqual(oldVersions, versions) {
			t.Fatalf("label replacement changed unrelated source record %s", original.Ref.ID)
		}
	}
	last := after.Log[len(after.Log)-1]
	if changed != 1 || last.Revision != commit.Revision || len(last.Changes) != 1 || last.Changes[0].Ref.ID != importedLabelID || !reflect.DeepEqual(before.Blobs, after.Blobs) || !reflect.DeepEqual(before.ImportReceipt, after.ImportReceipt) || before.WriteReceipt != nil || after.WriteReceipt == nil || after.WriteReceipt.ResponsePending || !reflect.DeepEqual(after.WriteReceipt.Commit, commit) {
		t.Fatal("label replacement lost exact source/log/blob/receipt conservation")
	}
}

func TestImportedLabelProjectionsAcrossHistoryAndSQLReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { exerciseImportedLabelProjections(t, engine, false) })
	}
}

func TestImportedSnapshotLabelProjectionRemainsReversibleAcrossDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { exerciseImportedLabelProjections(t, engine, true) })
	}
}

func exerciseImportedLabelProjections(t *testing.T, engine string, snapshot bool) {
	t.Helper()
	ctx := context.Background()
	data, source := publicSourceFixture(t, true)
	p, _ := admittedImportProfile(t, data)
	head, floor := uint64(1), uint64(1)
	historical := []uint64{1}
	if !snapshot {
		p.HistoryMode = driver.RestoreHistoryModeFull
		var err error
		p, err = BindHistoryImport(ctx, bytes.NewReader(data), p, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		head, floor = 8, 0
		historical = []uint64{5, 6, 7, 8}
	}
	store, lease, reopen := openIdentityTarget(t, engine)
	empty, err := store.Meta(ctx)
	if err != nil || !empty.Head.IsZero() {
		t.Fatalf("label import requires a fresh target: %+v %v", empty, err)
	}
	report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
	if err != nil || report.Restored.StoreID != p.SourceStoreID || report.Restored.Incarnation == empty.Incarnation || report.Restored.Head != model.Natural64(head) || report.Restored.HistoryFloor != model.Natural64(floor) {
		t.Fatalf("label import boundary: %+v %v", report.Restored, err)
	}
	key := hashBytes([]byte("imported-label-replacement"))
	retained := map[uint64]driver.Record{}
	check := func(renamed bool) {
		t.Helper()
		before := savedLabelImport(t, store, source, p, key)
		for _, revision := range historical {
			want := []string{importedOldLabel}
			if !snapshot && revision == 5 {
				want = nil
			}
			row := requireBothLabelProjections(t, store, lease, p.Admission.Actor, importedLabelID, model.Natural64(revision), want)
			if original, present := retained[revision]; present && !reflect.DeepEqual(original, row) {
				t.Fatalf("historical raw body/provenance changed at%d", revision)
			}
			retained[revision] = row
			requireLabelViews(t, store, lease, p.Admission.Actor, model.Natural64(revision), len(want) != 0, false)
		}
		want := []string{importedOldLabel}
		if renamed {
			want = []string{importedNewLabel}
		}
		requireBothLabelProjections(t, store, lease, p.Admission.Actor, importedLabelID, before.Meta.Head, want)
		requireLabelViews(t, store, lease, p.Admission.Actor, before.Meta.Head, !renamed, renamed)
		requireReverseLabelValue(t, store, want)
		if after := savedLabelImport(t, store, source, p, key); !reflect.DeepEqual(before, after) {
			t.Fatal("label projections/filter/count/reverse mutated imported state")
		}
	}
	check(false)
	beforeReopen := savedLabelImport(t, store, source, p, key)
	store, lease = reopen()
	if !reflect.DeepEqual(beforeReopen, savedLabelImport(t, store, source, p, key)) {
		t.Fatal("first SQL reopen or memory refresh changed imported bytes")
	}
	check(false)
	before := savedLabelImport(t, store, source, p, key)
	current := requireBothLabelProjections(t, store, lease, p.Admission.Actor, importedLabelID, before.Meta.Head, []string{importedOldLabel})
	// Main has the ordinary public Writer, not S1's strict rename planner.
	// Its independently composed acceptance test must prove that planner too.
	ontology := service.NewOntology(nil)
	writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
	revision := current.Revision
	commit, err := writer.Apply(ctx, p.Admission.Actor, model.ChangeSet{
		Expect: []model.Precondition{{Ref: current.Ref, Revision: &revision}},
		Ops:    []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: current.Ref, Fields: map[string]any{"labels": []string{importedNewLabel}}}}},
	}, model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: key})
	if err != nil || commit.Revision != model.Natural64(head+1) || len(commit.Results) != 1 || commit.Results[0].Ref != current.Ref {
		t.Fatalf("ordinary public label replacement failed: %+v %v", commit, err)
	}
	after := savedLabelImport(t, store, source, p, key)
	requireOnlyImportedLabelWrite(t, before, after, commit)
	check(true)
	store, lease = reopen()
	if !reflect.DeepEqual(after, savedLabelImport(t, store, source, p, key)) {
		t.Fatal("second SQL reopen or memory refresh changed labels/history")
	}
	check(true)
	lifecycle := "two-physical-SQL-reopens"
	if engine == "memory" {
		lifecycle = "same-store-lease-refreshes"
	}
	t.Logf("IMPORTED_LABEL_PROJECTIONS engine=%s lifecycle=%s mode=%s imported_head=%d live_head=%s archive=%s", engine, lifecycle, p.HistoryMode, head, commit.Revision, source.SHA256())
}

// These head-only test archives do not claim source history. All non-target
// labels are empty, and the source census/checksums are rebuilt for each input.
func labelSnapshotBytes(t *testing.T, labels any) []byte {
	t.Helper()
	return supportedBytes(t, func(members map[string][]byte) {
		var output bytes.Buffer
		for _, line := range bytes.Split(bytes.TrimSpace(members["records/bead.ndjson"]), []byte{'\n'}) {
			var record map[string]any
			decode(t, line, &record)
			body(record)["labels"] = []any{}
			if record["id"] == idA {
				body(record)["labels"] = labels
			}
			output.Write(encode(t, record))
		}
		members["records/bead.ndjson"] = output.Bytes()
		count := 0
		if values, ok := labels.([]any); ok {
			count = len(values)
		}
		var manifest map[string]any
		decode(t, members["manifest.json"], &manifest)
		for _, raw := range manifest["sources"].([]any) {
			row := raw.(map[string]any)
			if row["family"] == "labels" {
				row["rows"] = count
			}
		}
		members["manifest.json"] = encode(t, manifest)
	})
}

type labelWriteCounter struct {
	driver.Store
	writes int
}

func (s *labelWriteCounter) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.writes++
	return s.Store.Write(ctx, lease)
}

func TestImportedLabelContractRefusesBeforeWrite(t *testing.T) {
	// Construct admission from an independent valid control. The overlong
	// archive must enter RestoreImport; InspectImport would already refuse it.
	control, _ := admittedImportProfile(t, supportedBytes(t, nil))
	for _, test := range []struct {
		name   string
		labels any
		parser bool
	}{
		{"null", nil, true},
		{"scalar", "label", true},
		{"mixed", []any{"a", 1}, true},
		{"empty-value", []any{""}, true},
		{"duplicate", []any{"a", "a"}, true},
		{"unordered", []any{"b", "a"}, true},
		{"256-unicode-scalars", []any{strings.Repeat("界", 256)}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := labelSnapshotBytes(t, test.labels)
			_, readErr := dumpformat.Read(bytes.NewReader(data))
			if (readErr != nil) != test.parser {
				t.Fatalf("source shape guard did not reach the intended boundary: %v", readErr)
			}
			p := control
			p.ArchiveDigest = hashBytes(data)
			store, lease, cleanup := openTarget(t, "memory")
			defer cleanup()
			before := reverseState(t, store)
			counted := &labelWriteCounter{Store: store}
			_, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), counted, lease, p)
			if err == nil || counted.writes != 0 || !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatalf("malformed/unwritable labels reached a write: err=%v writes=%d", err, counted.writes)
			}
			if test.parser {
				if err.Error() != readErr.Error() {
					t.Fatalf("restore changed the existing source parser error: got=%v want=%v", err, readErr)
				}
			} else if !errors.Is(err, ErrRestoreUnavailable) {
				t.Fatalf("overlong target label lost representability refusal: %v", err)
			}
		})
	}
	for _, test := range []struct {
		name string
		want []string
	}{
		{"255-unicode-scalars", []string{strings.Repeat("界", 255)}},
		{"exact-identities", []string{" Label", "LABEL", "cafe\u0301", "café", "𠀀"}},
		{"empty-set", []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			labels := []any{}
			for _, label := range test.want {
				labels = append(labels, label)
			}
			data := labelSnapshotBytes(t, labels)
			if _, err := dumpformat.Read(bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			p := control
			p.ArchiveDigest = hashBytes(data)
			store, lease, cleanup := openTarget(t, "memory")
			defer cleanup()
			if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, p); err != nil {
				t.Fatal(err)
			}
			requireBothLabelProjections(t, store, lease, p.Admission.Actor, idA, model.Natural64(1), test.want)
		})
	}
}

type labelProjectionStore struct {
	driver.Store
	change func(map[string]any)
}

func (s labelProjectionStore) ReadCheckpointInventory(ctx context.Context, view driver.View) (driver.CheckpointInventory, error) {
	capability, ok := s.Store.(driver.CheckpointInventoryStore)
	if !ok {
		return nil, driver.ErrUnsupported
	}
	r, err := capability.ReadCheckpointInventory(ctx, view)
	if err != nil {
		return nil, err
	}
	return labelProjectionInventory{CheckpointInventory: r, change: s.change}, nil
}

type labelProjectionInventory struct {
	driver.CheckpointInventory
	change func(map[string]any)
}

func (r labelProjectionInventory) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	return (labelProjectionReader{Reader: r.CheckpointInventory, change: r.change}).Query(ctx, q)
}

func (s labelProjectionStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return labelProjectionReader{Reader: r, change: s.change}, nil
}

type labelProjectionReader struct {
	driver.Reader
	change func(map[string]any)
}

func (r labelProjectionReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, q)
	if err != nil {
		return page, err
	}
	for i, row := range page.Items {
		if row.Ref.ID != importedLabelID {
			continue
		}
		var doc map[string]any
		decoder := json.NewDecoder(bytes.NewReader(row.Body))
		decoder.UseNumber()
		if err := decoder.Decode(&doc); err != nil {
			return page, err
		}
		r.change(doc)
		page.Items[i].Body, err = json.Marshal(doc)
		if err != nil {
			return page, err
		}
	}
	return page, nil
}

func TestLiveReverseRefusesConflictingTypedLabelsWithoutOutput(t *testing.T) {
	data, _ := publicSourceFixture(t, true)
	p, _ := admittedImportProfile(t, data)
	store, lease, cleanup := openTarget(t, "memory")
	defer cleanup()
	if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, p); err != nil {
		t.Fatal(err)
	}
	before := reverseState(t, store)
	baseline := requireReverseLabelValue(t, store, []string{importedOldLabel})
	for _, test := range []struct {
		name string
		raw  any
	}{
		{"null", nil}, {"scalar", importedOldLabel}, {"mixed", []any{importedOldLabel, 1}},
		{"empty-value", []any{""}}, {"duplicate", []any{importedOldLabel, importedOldLabel}},
		{"unordered", []any{"z", "a"}}, {"overlong", []any{strings.Repeat("界", 256)}},
		{"mismatch", []any{"different-label"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			hook := labelProjectionStore{Store: store, change: func(doc map[string]any) { doc["fields"].(map[string]any)["labels"] = test.raw }}
			got, err := ReverseRestored(context.Background(), hook)
			var refused ReverseRefusal
			if !errors.As(err, &refused) || refused.Reason != "inconsistent_live_labels" || len(got.Result.JSONL) != 0 || got.Result.Digest != "" || !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatalf("contradictory typed labels leaked reverse output/state: %+v %v", got, err)
			}
		})
	}
	legacy := labelProjectionStore{Store: store, change: func(doc map[string]any) { delete(doc["fields"].(map[string]any), "labels") }}
	if got := requireReverseLabelValue(t, legacy, []string{importedOldLabel}); !bytes.Equal(got.Result.JSONL, baseline.Result.JSONL) || got.Result.Digest != baseline.Result.Digest {
		t.Fatal("legacy absent-field compatibility changed reverse bytes")
	}
	empty := labelProjectionStore{Store: store, change: func(doc map[string]any) {
		doc["fields"].(map[string]any)["labels"] = []any{}
		delete(doc, "labels")
	}}
	requireReverseLabelValue(t, empty, nil)
	unknown := labelProjectionStore{Store: store, change: func(doc map[string]any) { doc["fields"].(map[string]any)["unrepresentable"] = true }}
	got, err := ReverseRestored(context.Background(), unknown)
	var refused ReverseRefusal
	if !errors.As(err, &refused) || refused.Reason != "unsupported_live_field" || len(got.Result.JSONL) != 0 || got.Result.Digest != "" || !reflect.DeepEqual(before, reverseState(t, store)) {
		t.Fatalf("unknown-field refusal or reverse read-only boundary changed: %+v %v", got, err)
	}
}
