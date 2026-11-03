package b16sourcehistory_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const archiveSHA256 = "a2d933a7a54b53303d33a20d5b96eaac0a85c079c5b08cae553aa2a654d820b5"
const historySHA256 = "049b7602c5360e6be2b69ad25a5fae0cb77b7a1fc2af2b18da9a41eb71242b62"
const sourceID = "demo-42v"
const sourceLabel = "public-source-label"
const sourceLabelEventID = "1cea927e-0758-53e6-847b-cf53140d1962"
const renamedLabel = "public-source-renamed"

type sourceEvent struct {
	ID        string `json:"id"`
	IssueID   string `json:"issue_id"`
	EventType string `json:"event_type"`
	Comment   string `json:"comment"`
}

type sourceFrame struct {
	Sequence uint64 `json:"sequence"`
	CommitID string `json:"commit_id"`
	Records  []struct {
		Kind, ID, Project string
		Body              struct {
			Labels []string `json:"labels"`
		} `json:"body"`
	} `json:"records"`
	Events []sourceEvent `json:"events"`
}

type capturedHistory struct {
	Format  string            `json:"format"`
	Commits []json.RawMessage `json:"commits"`
	Range   struct {
		Status         string `json:"status"`
		Complete       bool   `json:"complete"`
		First, Head    uint64
		Baseline       []json.RawMessage `json:"baseline"`
		BaselineEvents []json.RawMessage `json:"baseline_events"`
		Gaps           []json.RawMessage `json:"gaps"`
		Frames         []sourceFrame     `json:"frames"`
	} `json:"range"`
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// This oracle reads the retained producer bytes using only standard tar/JSON
// decoding. It never calls the importer, HistoryImage, label matcher or rename
// planner to calculate expected source membership or event/revision identity.
func capturedSource(t *testing.T) ([]byte, capturedHistory) {
	t.Helper()
	data, err := os.ReadFile("../converterroundtrip/testdata/public-complete-root-990eb800.tar")
	if err != nil || len(data) != 78848 || digest(data) != archiveSHA256 {
		t.Fatalf("retained public producer archive changed: %v", err)
	}
	var history []byte
	reader := tar.NewReader(bytes.NewReader(data))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name != "history/source.json" {
			continue
		}
		if history != nil || header.Typeflag != tar.TypeReg {
			t.Fatal("history must be one retained regular archive member")
		}
		history, err = io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
	}
	if digest(history) != historySHA256 {
		t.Fatal("retained raw history member changed")
	}
	var source capturedHistory
	if err := json.Unmarshal(history, &source); err != nil {
		t.Fatal(err)
	}
	r := source.Range
	if source.Format != "beads-dolt-images/1" || r.Status != "verified" || !r.Complete || r.First != 1 || r.Head != 7 || len(source.Commits) != 7 || len(r.Frames) != 7 || len(r.Baseline)+len(r.BaselineEvents)+len(r.Gaps) != 0 {
		t.Fatal("retained complete-root source census changed")
	}
	for i, frame := range r.Frames {
		if frame.Sequence != uint64(i+1) || frame.CommitID == "" || len(frame.Events) != []int{0, 0, 0, 1, 2, 3, 4}[i] {
			t.Fatalf("source frame/event order changed: %+v", frame)
		}
		labelEvents := 0
		for _, event := range frame.Events {
			if event.EventType == "label_added" {
				labelEvents++
				if event.ID != sourceLabelEventID || event.IssueID != sourceID || event.Comment != "Added label: "+sourceLabel {
					t.Fatalf("actual label event changed: %+v", event)
				}
			}
		}
		wantEvents := 0
		if frame.Sequence >= 6 {
			wantEvents = 1
		}
		if labelEvents != wantEvents {
			t.Fatalf("source label event appears at wrong boundary: sequence=%d events=%d", frame.Sequence, labelEvents)
		}
	}
	if len(source.labels(t, 5)) != 0 || !reflect.DeepEqual(source.labels(t, 6), []string{sourceLabel}) || !reflect.DeepEqual(source.labels(t, 7), []string{sourceLabel}) {
		t.Fatal("actual source before/add/retained label boundary changed")
	}
	return data, source
}

func (source capturedHistory) labels(t *testing.T, sequence uint64) []string {
	t.Helper()
	if sequence < 1 || sequence > uint64(len(source.Range.Frames)) {
		t.Fatalf("source sequence out of range: %d", sequence)
	}
	var labels []string
	found := 0
	for _, record := range source.Range.Frames[sequence-1].Records {
		if record.Kind != "bead" {
			continue
		}
		if record.ID != sourceID || record.Project != project || record.Body.Labels == nil {
			t.Fatalf("unexpected source bead: %+v", record)
		}
		labels, found = append([]string{}, record.Body.Labels...), found+1
	}
	if found != 1 {
		t.Fatalf("source sequence %d must retain exactly one real bead, got %d", sequence, found)
	}
	return labels
}

func requireMapping(t *testing.T, state durableState, source capturedHistory) {
	t.Helper()
	var evidence driver.HistoryImportEvidence
	found := 0
	for _, record := range state.Records {
		if record.Ref.ID == driver.OfflineHistoryEvidenceID {
			found++
			if err := json.Unmarshal(record.Body, &evidence); err != nil {
				t.Fatal(err)
			}
		}
	}
	if found != 1 || evidence.Source != source.Format || evidence.Mode != driver.RestoreHistoryModeFull || !evidence.Complete || evidence.First != source.Range.First || evidence.Head != source.Range.Head || !evidence.Floor.IsZero() || len(evidence.Gaps) != 0 || len(evidence.Mapping) != len(source.Range.Frames) {
		t.Fatalf("imported full source mapping changed: %+v", evidence)
	}
	seen := map[string]bool{}
	for i, frame := range source.Range.Frames {
		newEvents := []string{}
		for _, event := range frame.Events {
			if event.ID == "" {
				t.Fatal("raw event identity missing")
			}
			if !seen[event.ID] {
				newEvents = append(newEvents, event.ID)
			}
			seen[event.ID] = true
		}
		sort.Strings(newEvents)
		want := driver.SourceImportRevision{Sequence: frame.Sequence, Revision: model.Natural64(frame.Sequence), CommitIDs: []string{frame.CommitID}, EventIDs: newEvents}
		if !reflect.DeepEqual(evidence.Mapping[i], want) {
			t.Fatalf("raw event/commit assigned to wrong public revision: got=%+v want=%+v", evidence.Mapping[i], want)
		}
	}
}

type publicBead struct {
	ID, Project string
	Revision    model.Revision
	Labels      []string
}

type snapshot struct {
	Revision    model.Revision `json:"revision"`
	Incarnation string         `json:"incarnation"`
}

func (f *fixture) publicLabels(at *snapshot, want []string) publicBead {
	f.t.Helper()
	params := map[string]any{"id": ref(sourceID)}
	if at != nil {
		params["asOf"] = at
	}
	var result struct {
		Bead publicBead `json:"bead"`
	}
	raw := f.request("bead.get", params, "", "").Result
	if err := json.Unmarshal(raw, &result); err != nil || result.Bead.ID != sourceID || result.Bead.Project != project || result.Bead.Revision.IsZero() || !reflect.DeepEqual(append([]string{}, result.Bead.Labels...), want) {
		f.t.Fatalf("public source bead lost exact raw labels: got=%s want=%v err=%v", raw, want, err)
	}
	return result.Bead
}

func (f *fixture) publicFilter(at snapshot, query string, want []string) {
	f.t.Helper()
	definition := map[string]any{
		"filter": map[string]any{"query": query, "resources": []string{sourceID}, "types": []string{"issue"}},
		"sort":   []string{"id"}, "fields": []string{"id"}, "budgetTokens": 1,
	}
	type result struct {
		Items []struct{ ID string } `json:"items"`
		Count *model.Natural        `json:"count"`
		AsOf  snapshot              `json:"asOf"`
		Next  json.RawMessage       `json:"nextCursor"`
	}
	var list, count result
	raw := f.request("view.run", map[string]any{"definition": definition, "asOf": at}, "", "").Result
	if err := json.Unmarshal(raw, &list); err != nil {
		f.t.Fatal(err)
	}
	ids := []string{}
	for _, item := range list.Items {
		ids = append(ids, item.ID)
	}
	if !reflect.DeepEqual(ids, want) || list.Count != nil || list.AsOf != at || len(list.Next) != 0 {
		f.t.Fatalf("historical public filter lost source boundary: query=%s revision=%s got=%s want=%v", query, at.Revision, raw, want)
	}
	raw = f.request("view.run", map[string]any{"definition": definition, "asOf": at, "countOnly": true}, "", "").Result
	if err := json.Unmarshal(raw, &count); err != nil || count.Count == nil || *count.Count != model.Natural64(uint64(len(want))) || len(count.Items) != 0 || count.AsOf != at || len(count.Next) != 0 {
		f.t.Fatalf("historical public count lost source boundary: query=%s revision=%s got=%s want=%d err=%v", query, at.Revision, raw, len(want), err)
	}
	f.t.Logf("B16_SOURCE_FILTER engine=%s revision=%s query=%q ids=%v count=%s incarnation=%s", f.engine, at.Revision, query, ids, count.Count, at.Incarnation)
}

func (f *fixture) sourceSnapshots(source capturedHistory, migrated bool) {
	f.t.Helper()
	before := f.state()
	requireMapping(f.t, before, source)
	for _, sequence := range []uint64{5, 6, 7, 8} {
		observed := sequence
		if observed == 8 { // the import completion revision retains source head7
			observed = source.Range.Head
		}
		labels := source.labels(f.t, observed)
		at := snapshot{model.Natural64(sequence), before.Meta.Incarnation}
		f.publicLabels(&at, labels)
		present, absent := []string{}, []string{sourceID}
		if len(labels) != 0 {
			present, absent = absent, present
		}
		f.publicFilter(at, "label="+sourceLabel, present)
		// The complement proves an absent label is not a missing imported bead.
		f.publicFilter(at, "NOT label="+sourceLabel, absent)
		f.publicFilter(at, "label="+renamedLabel, []string{})
	}
	labels := source.labels(f.t, source.Range.Head)
	oldIDs, newIDs := []string{sourceID}, []string{}
	if migrated {
		labels, oldIDs, newIDs = []string{renamedLabel}, newIDs, oldIDs
	}
	f.publicLabels(nil, labels)
	at := snapshot{before.Meta.Head, before.Meta.Incarnation}
	f.publicFilter(at, "label="+sourceLabel, oldIDs)
	f.publicFilter(at, "label="+renamedLabel, newIDs)
	requireState(f.t, before, f.state())
}

func withoutLabels(t *testing.T, body []byte) []byte {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		t.Fatal(err)
	}
	fields, ok := object["fields"].(map[string]any)
	if !ok {
		t.Fatal("changed bead lost typed fields")
	}
	delete(object, "labels")
	delete(fields, "labels")
	return encoded(t, object)
}

func requireOnlyLiveRename(t *testing.T, before, after durableState, changed model.Commit) {
	t.Helper()
	wantMeta := before.Meta
	wantMeta.Head = changed.Revision
	if !reflect.DeepEqual(wantMeta, after.Meta) || len(after.Records) != len(before.Records) || len(after.Log) != len(before.Log)+1 || !reflect.DeepEqual(before.Log, after.Log[:len(before.Log)]) {
		t.Fatal("live imported rename changed identity, imported log, or record census")
	}
	changedRecords := 0
	for i, original := range before.Records {
		got := after.Records[i]
		oldVersions, versions := before.Versions[original.Ref.ID], after.Versions[original.Ref.ID]
		if original.Ref.ID == sourceID {
			changedRecords++
			if original.Ref != got.Ref || original.Kind != got.Kind || got.Revision != changed.Revision || !bytes.Equal(withoutLabels(t, original.Body), withoutLabels(t, got.Body)) || len(versions) != len(oldVersions)+1 || !reflect.DeepEqual(oldVersions, versions[:len(oldVersions)]) {
				t.Fatal("live rename changed source metadata or rewrote imported versions")
			}
		} else if !reflect.DeepEqual(original, got) || !reflect.DeepEqual(oldVersions, versions) {
			t.Fatalf("live rename changed unrelated imported state: %s", original.Ref.ID)
		}
	}
	entry := after.Log[len(after.Log)-1]
	if changedRecords != 1 || len(entry.Changes) != 1 || entry.Changes[0].Ref.ID != sourceID || entry.Revision != changed.Revision || len(after.Receipts) != len(before.Receipts)+1 {
		t.Fatal("live imported rename did not produce exactly one changed record/revision/receipt")
	}
	for identity, receipt := range before.Receipts {
		if !reflect.DeepEqual(receipt, after.Receipts[identity]) {
			t.Fatal("live rename changed the original import receipt")
		}
	}
}

func TestB16CapturedLabelEventMapsToHistoricalPublicFilterAndRenameAfterSQLReopen(t *testing.T) {
	data, source := capturedSource(t)
	eachEngine(t, func(t *testing.T, f *fixture) {
		ctx := context.Background()
		inspection, err := converterroundtrip.InspectImport(ctx, bytes.NewReader(data))
		if err != nil || inspection.ArchiveSHA256 != archiveSHA256 {
			t.Fatalf("actual public archive inspection failed: %v", err)
		}
		// This is an explicit trusted test-host admission. Archive authors,
		// events and source timestamps grant no Plug authority.
		profile := driver.RestoreImportProfile{
			Admission: driver.RestoreImportAdmission{
				Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b16-source-restore-admin"},
				Authority: "restore-admin", AdmissionDigest: digest([]byte("b16-source-history trusted test-host admission")),
			},
			SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import",
			IdempotencyKey: "b16-source-history-import-0001", ArchiveDigest: inspection.ArchiveSHA256,
			DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeFull,
		}
		profile, err = converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), profile, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		old := f.meta()
		restored, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), f.store, f.lease, profile)
		if err != nil || restored.Restored.Head != model.Natural64(8) || !restored.Restored.HistoryFloor.IsZero() || restored.Restored.Incarnation == old.Incarnation || restored.Restored.StoreID != inspection.SourceStoreID {
			t.Fatalf("actual full-history import boundary failed: %+v %v", restored, err)
		}
		identity := f.track(profile.Admission.Actor, profile.AgentLabel(), profile.Method, profile.IdempotencyKey)
		requestHash, err := profile.RequestHash()
		receipt := f.state().Receipts[identity]
		if err != nil || receipt == nil || receipt.ResponsePending || receipt.RequestHash != requestHash || receipt.Commit.Revision != restored.Restored.Head {
			t.Fatal("actual full-history import receipt missing or mismatched")
		}
		f.host()
		f.sourceSnapshots(source, false)
		if f.engine != "memory" {
			// Reopen the imported SQL state before attempting the new seam, so
			// a planner refusal cannot masquerade as SQL persistence evidence.
			f.reopen()
			f.sourceSnapshots(source, false)
		}
		before := f.state()
		current := f.publicLabels(nil, source.labels(t, source.Range.Head))
		plan, err := rpc.PlanLabelRenameFromResults(project, sourceLabel, renamedLabel, []json.RawMessage{f.get(sourceID)})
		requireState(t, before, f.state())
		if err != nil {
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "invalid_params" || plan.Renamed != 0 || plan.Merged != 0 || len(plan.ChangeSet.Ops)+len(plan.ChangeSet.Expect) != 0 {
				t.Fatalf("imported-label planner failure lost typed atomic refusal: plan=%+v err=%v", plan, err)
			}
			t.Fatalf("valid retained source labels must reach ordinary public rename without fixture repair; planner refused with %s: %v", typed.Data.Code, err)
		}
		if plan.Renamed != 1 || plan.Merged != 0 || len(plan.ChangeSet.Ops) != 1 || len(plan.ChangeSet.Expect) != 1 {
			t.Fatalf("imported source rename plan lost exact selected count: %+v", plan)
		}
		op, guard := plan.ChangeSet.Ops[0], plan.ChangeSet.Expect[0]
		if op.Kind != model.OpPatch || op.Patch == nil || op.Patch.Target != ref(sourceID) || guard.Ref != ref(sourceID) || guard.Revision == nil || *guard.Revision != current.Revision || !bytes.Equal(encoded(t, op.Patch.Fields), encoded(t, map[string]any{"labels": []string{renamedLabel}})) {
			t.Fatalf("imported source rename lost exact typed patch/current revision CAS: %+v", plan)
		}
		key := f.nextKey()
		changed := commit(t, f.request("tx.apply", map[string]any{"changeSet": plan.ChangeSet}, key, ""))
		if changed.Revision != model.Natural64(9) || len(changed.Results) != 1 || changed.Results[0].Ref != ref(sourceID) || changed.Results[0].Revision != changed.Revision {
			t.Fatalf("live rename did not follow the seven source units and import completion: %+v", changed)
		}
		after := f.state()
		writeIdentity := f.track(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, agent, "tx.apply", key)
		writeReceipt := after.Receipts[writeIdentity]
		if writeReceipt == nil || writeReceipt.ResponsePending || writeReceipt.Commit.Revision != changed.Revision || writeReceipt.Changeset.Revision != changed.Revision {
			t.Fatal("actual live public rename receipt missing or mismatched")
		}
		requireOnlyLiveRename(t, before, after, changed)
		f.sourceSnapshots(source, true)
		reopens := 0
		if f.engine != "memory" {
			f.reopen()
			f.sourceSnapshots(source, true)
			reopens = 2
		}
		t.Logf("B16_SOURCE_CONNECTED engine=%s archive_sha256=%s history_sha256=%s label_event=%s source_before=5 source_added=6 source_head=7 import_head=8 live_rename=9 renamed=%d merged=%d sql_reopens=%d", f.engine, archiveSHA256, historySHA256, sourceLabelEventID, plan.Renamed, plan.Merged, reopens)
	})
}
