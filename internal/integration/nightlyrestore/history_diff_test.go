package nightlyrestore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var historyFixtureSHA = map[string]string{
	"full":    "a96f0134097751e981ec6c46b7d412fb0dd92938a04df0838564b71c1064a212",
	"partial": "ab25df11463f68dbfa1ee1ce781a252cfc1b802abac8bb2f1cbc7bed5932cf9c",
}

func sameJSON(t *testing.T, family string, got, want any) {
	t.Helper()
	decode := func(value any) any {
		dec := json.NewDecoder(bytes.NewReader(jsonBytes(t, value)))
		dec.UseNumber()
		var result any
		if err := dec.Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	same(t, family, decode(got), decode(want))
}

func publicHistoryFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("internal", "integration", "offlinehistory", "testdata", name+".tar")
	paths := []string{filepath.Join("..", "offlinehistory", "testdata", name+".tar")}
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		paths = append(paths, filepath.Join(root, os.Getenv("TEST_WORKSPACE"), path))
	}
	for _, path := range paths {
		if data, err := os.ReadFile(path); err == nil {
			if digest(data) != historyFixtureSHA[name] {
				t.Fatal("public history fixture changed; review source images before repinning")
			}
			return data
		}
	}
	t.Fatal("declared public history fixture is absent")
	return nil
}

type versionSample struct {
	Revision model.Revision `json:"revision"`
	Base     bool           `json:"base"`
	Deleted  bool           `json:"deleted"`
	Image    string         `json:"image_sha256,omitempty"`
}

type historyCensus struct {
	Head                         model.Revision                `json:"head"`
	Floor                        model.Revision                `json:"floor"`
	Current                      census                        `json:"current"`
	AsOf                         map[string]map[string]string  `json:"as_of_record_sha256"`
	Versions                     map[string][]versionSample    `json:"versions"`
	Evidence                     *driver.HistoryImportEvidence `json:"history_evidence,omitempty"`
	Changesets                   []model.Changeset             `json:"changesets_after_floor"`
	RestoreReceipt               model.Changeset               `json:"restore_receipt"`
	ChangesetsUnavailableThrough model.Revision                `json:"changesets_unavailable_through"`
}

func portableKind(kind string) bool {
	switch kind {
	case "bead", "edge", "comment", "memory", "config":
		return true
	}
	return false
}

func sourceTargetID(row dumpformat.Record) string {
	if row.Kind == "bead" {
		return row.ID
	}
	return "source/" + row.Kind + "/" + row.ID
}

func indexRows(t *testing.T, rows []driver.Record) map[string]driver.Record {
	t.Helper()
	indexed := map[string]driver.Record{}
	for _, row := range rows {
		if !row.Ref.IsID() || !row.Ref.Branch.IsZero() || indexed[row.Ref.ID].Kind != "" {
			t.Fatal("invalid or duplicate imported identity")
		}
		indexed[row.Ref.ID] = row
	}
	return indexed
}

// The source oracle reads checked archive images, not another driver's output
// or the converter's private lowering/plan. Wide numbers and nulls remain exact.
func checkSourceImage(t *testing.T, rows map[string]driver.Record, a *dumpformat.Archive, p driver.RestoreImportProfile, when time.Time, historical bool) {
	t.Helper()
	wantIDs, gotIDs := []string{}, []string{}
	for _, row := range rows {
		if portableKind(row.Kind) || historical {
			gotIDs = append(gotIDs, row.Ref.ID)
		}
	}
	for _, kind := range a.Manifest().Kinds {
		if !portableKind(kind.Name) {
			continue
		}
		for _, source := range a.Records(kind.Name) {
			id := sourceTargetID(source)
			wantIDs = append(wantIDs, id)
			row, ok := rows[id]
			if !ok || row.Kind != kind.Name {
				t.Fatalf("source image record missing: %s/%s", kind.Name, id)
			}
			if kind.Name != "bead" {
				want := map[string]any{}
				for key, value := range source.Body {
					want[key] = value
				}
				want["source_id"], want["source_table"], want["source"] = source.ID, source.Source.Table, source.Source.Fields
				if kind.Name == "memory" {
					// The live memory API requires content and identity alongside
					// the exact retained source value. Derive them independently
					// from this checked archive image, including JSON numbers/null.
					value, text := source.Body["value"].(string)
					encoding := "text"
					if !text {
						value, encoding = string(jsonBytes(t, source.Body["value"])), "json"
					}
					want["id"], want["content"] = id, value
					want["metadata"] = map[string]any{"beads.memory.value_encoding": encoding, "beads.classification": source.Body["classification"]}
				}
				sameJSON(t, "portable source row "+id, json.RawMessage(row.Body), want)
				continue
			}
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(err)
			}
			if doc.ID != source.ID || doc.Project != source.Project || doc.Type != source.Body["type"] || doc.Lease != nil || len(doc.Attachments) != 0 {
				t.Fatalf("source bead identity/type/owned fields changed: %s", id)
			}
			fields := map[string]any{"status": map[string]string{"open": "todo", "closed": "done", "in_progress": "in_progress"}[fmt.Sprint(source.Source.Fields["status"])]}
			metadata := map[string]json.RawMessage{}
			if user, ok := source.Body["metadata"].(map[string]any); ok {
				for key, value := range user {
					metadata[key] = jsonBytes(t, value)
				}
			}
			for key, value := range source.Body {
				switch key {
				case "title", "description", "priority":
					if value != nil {
						fields[key] = value
					}
				case "metadata", "labels":
				default:
					metadata["beads.source."+key] = jsonBytes(t, value)
				}
			}
			if labels := source.Body["labels"].([]any); len(labels) != 0 {
				fields["labels"] = append([]any(nil), labels...)
			}
			metadata["beads.source.status"] = jsonBytes(t, source.Source.Fields["status"])
			metadata["beads.source_fields"] = jsonBytes(t, source.Source.Fields)
			for _, key := range dumpformat.OpaqueIssueFields() {
				if value, present := source.Source.Fields[key]; present {
					metadata["beads."+key] = jsonBytes(t, value)
				}
			}
			sameJSON(t, "source bead fields "+id, doc.Fields, fields)
			sameJSON(t, "source bead metadata/opaque fields "+id, doc.Metadata, metadata)
			sameJSON(t, "source labels "+id, append([]string{}, doc.Labels...), source.Body["labels"])
			links := []pipeline.Link{}
			for _, edge := range a.Records("edge") {
				if edge.Body["from"] == source.ID {
					relation := fmt.Sprint(edge.Body["type"])
					if relation == "related" {
						relation = "relates-to"
					}
					links = append(links, pipeline.Link{To: model.RefByID(fmt.Sprint(edge.Body["to"]), model.Natural64(0)), Kind: relation})
				}
			}
			sameJSON(t, "source dependencies "+id, append([]pipeline.Link{}, doc.Links...), links)
			comments := []pipeline.Comment{}
			for _, sourceComment := range a.Records("comment") {
				if sourceComment.Body["issue_id"] != source.ID {
					continue
				}
				sourceTime, err := time.Parse(time.RFC3339Nano, fmt.Sprint(sourceComment.Body["created_at"]))
				if err != nil {
					t.Fatal(err)
				}
				comments = append(comments, pipeline.Comment{Body: fmt.Sprint(sourceComment.Body["text"]), Principal: p.Admission.Actor, Time: when, Origin: "import", SourceAuthor: strings.TrimSpace(fmt.Sprint(sourceComment.Body["author"])), SourceTime: &sourceTime})
			}
			sameJSON(t, "source comments/attribution/time "+id, append([]pipeline.Comment{}, doc.Comments...), comments)
		}
	}
	sort.Strings(gotIDs)
	sort.Strings(wantIDs)
	sameJSON(t, "complete source image identities", gotIDs, wantIDs)
}

// Cross-run normalization follows exact binding verification, never precedes
// it. Only execution-bound fields vary; source authors, times and data remain.
func normalizedImportBody(t *testing.T, row driver.Record, p driver.RestoreImportProfile) []byte {
	t.Helper()
	if row.Kind != "instance_config" {
		return canonicalBody(t, row, p.Admission.Actor)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row.Body, &fields); err != nil {
		t.Fatal(err)
	}
	key := "importProfile"
	if row.Ref.ID == "instance_config/restore-import/binding" {
		key = "profile"
		hash, err := p.RequestHash()
		if err != nil {
			t.Fatal(err)
		}
		sameJSON(t, "stored admission request hash", fields["request_hash"], hash)
		fields["request_hash"] = jsonBytes(t, "validated execution binding")
	}
	sameJSON(t, "stored admitted profile", fields[key], p)
	p.HistoryDigest, p.HistoryTime = "", ""
	fields[key] = jsonBytes(t, p)
	return jsonBytes(t, fields)
}

func imageDigests(t *testing.T, rows map[string]driver.Record, p driver.RestoreImportProfile) map[string]string {
	t.Helper()
	result := map[string]string{}
	for id, row := range rows {
		result[id] = digest(normalizedImportBody(t, row, p))
	}
	return result
}

func currentHistoryCensus(t *testing.T, r driver.Reader, rows map[string]driver.Record, a *dumpformat.Archive, p driver.RestoreImportProfile) census {
	t.Helper()
	ctx := context.Background()
	got := census{Counts: map[string]int{}, Records: imageDigests(t, rows, p), Versions: map[string]string{}, Source: a.Manifest().Projects, Target: []driver.OfflineAllocator{}, Members: map[string]string{}, Dependencies: []string{}, Archive: a.SHA256()}
	for id, row := range rows {
		got.IDs = append(got.IDs, id)
		got.Counts[row.Kind]++
		if row.Kind == "bead" {
			doc, err := pipeline.DecodeDocument(row.Body)
			if err != nil {
				t.Fatal(err)
			}
			for _, link := range doc.Links {
				got.Dependencies = append(got.Dependencies, id+"/"+link.Kind+"/"+link.To.ID)
			}
		}
	}
	wantCounts := map[string]int{"restore-archive": 1, "instance_config": len(got.Source) + 1}
	wantIDs := []string{"restore/archive", "instance_config/restore-import/binding"}
	for _, kind := range a.Manifest().Kinds {
		wantCounts[kind.Name] = kind.Count
		for _, source := range a.Records(kind.Name) {
			id := sourceTargetID(source)
			wantIDs = append(wantIDs, id)
			if !portableKind(kind.Name) {
				want := map[string]any{"source_id": source.ID, "source_table": source.Source.Table, "source": source.Source.Fields}
				for key, value := range source.Body {
					want[key] = value
				}
				want["read_only"], want["redactable"] = true, true
				if rows[id].Kind != kind.Name {
					t.Fatalf("source archive row absent: %s", id)
				}
				sameJSON(t, "retained archival row "+id, json.RawMessage(rows[id].Body), want)
			}
		}
	}
	if p.HistoryMode != driver.RestoreHistoryModeSnapshot {
		wantCounts["beads-archive"]++
		wantIDs = append(wantIDs, driver.OfflineHistoryEvidenceID)
	}
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		t.Fatal("restored allocator readback missing")
	}
	wantProjects := []map[string]any{}
	for _, source := range got.Source {
		id := "instance_config/restore-import/project/" + source.Prefix
		wantIDs = append(wantIDs, id)
		actual, err := allocators.LoadAllocator(ctx, source.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		want := driver.NewHashAllocator(source.Prefix, 3, 8, 0.25)
		want.LastID, want.Children = source.LastID, source.ChildCounters
		sameJSON(t, "target allocator policy", actual, want)
		sameJSON(t, "retained source counters and target allocator", json.RawMessage(rows[id].Body), map[string]any{
			"project": source.Prefix, "importProfile": p, "metadata": map[string]any{"plug.restore.sourceAllocator": source}, "targetAllocator": actual,
		})
		wantProjects = append(wantProjects, map[string]any{"sourceAllocator": source, "targetAllocator": actual})
		got.Target = append(got.Target, actual)
	}
	var binding map[string]json.RawMessage
	if err := json.Unmarshal(rows["instance_config/restore-import/binding"].Body, &binding); err != nil {
		t.Fatal(err)
	}
	sameJSON(t, "complete project binding", binding["projects"], wantProjects)
	sort.Strings(wantIDs)
	sort.Strings(got.IDs)
	sort.Strings(got.Dependencies)
	sameJSON(t, "complete current-state counts", got.Counts, wantCounts)
	sameJSON(t, "complete current-state IDs", got.IDs, wantIDs)
	var evidence struct {
		Archive string                     `json:"archive_sha256"`
		Derived string                     `json:"derived_sha256"`
		Members map[string]dumpformat.File `json:"members"`
		Profile string                     `json:"profile"`
		Source  string                     `json:"source_store"`
	}
	if err := json.Unmarshal(rows["restore/archive"].Body, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.Archive != a.SHA256() || evidence.Derived != p.DerivedDigest || evidence.Profile != "builtin-head/1" || evidence.Source != string(p.SourceStoreID) {
		t.Fatal("retained archive digest/profile/identity mismatch")
	}
	wantMembers := a.Manifest().Files
	manifest, _ := a.Member("manifest.json")
	wantMembers["manifest.json"] = dumpformat.File{SHA256: digest(manifest), Size: int64(len(manifest))}
	sameJSON(t, "complete member inventory", evidence.Members, wantMembers)
	for name, expected := range wantMembers {
		body, err := r.Blob(ctx, driver.Hash(expected.SHA256))
		source, _ := a.Member(name)
		if err != nil || int64(len(body)) != expected.Size || digest(body) != expected.SHA256 || !bytes.Equal(body, source) {
			t.Fatalf("retained archive blob differs: %s", name)
		}
		got.Members[name] = digest(body)
	}
	return got
}

func expectedHistoryEvidence(t *testing.T, a *dumpformat.Archive, p driver.RestoreImportProfile) driver.HistoryImportEvidence {
	t.Helper()
	source, ok := a.SourceHistory()
	if !ok || source.Range.Status != "verified" || source.Range.Head != 4 {
		t.Fatal("pinned fixture lacks its verified source range")
	}
	want := driver.HistoryImportEvidence{Source: dumpformat.SourceHistoryFormat, Mode: p.HistoryMode, Complete: source.Range.Complete, First: source.Range.First, Head: source.Range.Head, Floor: model.Natural64(0), Gaps: []driver.SourceHistoryGap{}, Mapping: []driver.SourceImportRevision{}}
	previous := map[string]bool{}
	for _, event := range source.Range.BaselineEvents {
		previous[dumpformat.SourceEventID(event)] = true
	}
	var revision uint64
	if source.Range.First > 1 {
		revision = 1
		want.Floor = model.Natural64(1)
		sameJSON(t, "explicit partial source gaps", source.Range.Gaps, []dumpformat.HistoryGap{{First: 1, Last: 3, Reason: "not-retained"}})
	}
	for _, gap := range source.Range.Gaps {
		want.Gaps = append(want.Gaps, driver.SourceHistoryGap{First: gap.First, Last: gap.Last, Reason: gap.Reason})
	}
	for _, frame := range source.Range.Frames {
		revision++
		events := []string{}
		for _, event := range frame.Events {
			id := dumpformat.SourceEventID(event)
			if !previous[id] {
				events = append(events, id)
			}
			previous[id] = true
		}
		sort.Strings(events)
		want.Mapping = append(want.Mapping, driver.SourceImportRevision{Sequence: frame.Sequence, Revision: model.Natural64(revision), EventIDs: events, CommitIDs: []string{frame.CommitID}})
	}
	return want
}

// Once every as-of state has passed the source oracle, its transitions supply
// independent expectations for native versions and complete change records.
func checkNativeVersions(t *testing.T, r driver.Reader, states []map[string]driver.Record, p driver.RestoreImportProfile, when time.Time) (map[string][]versionSample, map[model.Revision][]model.ChangeRecord) {
	t.Helper()
	expected := map[string][]driver.Version{}
	changes := map[model.Revision][]model.ChangeRecord{}
	lastRevision := map[string]model.Revision{}
	kinds := map[string]string{}
	for revision := 1; revision < len(states); revision++ {
		rev := model.Natural64(uint64(revision))
		before, after := states[revision-1], states[revision]
		ids := map[string]bool{}
		for id := range before {
			ids[id] = true
		}
		for id := range after {
			ids[id] = true
		}
		changes[rev] = []model.ChangeRecord{}
		for id := range ids {
			left, wasPresent := before[id]
			right, isPresent := after[id]
			if wasPresent && isPresent && left.Kind == right.Kind && bytes.Equal(left.Body, right.Body) {
				if right.Revision != lastRevision[id] {
					t.Fatalf("unchanged record has a fabricated revision: %s at %s", id, rev)
				}
				continue
			}
			row := right
			op := model.OpPatch
			if !wasPresent {
				op = model.OpCreate
			}
			if !isPresent {
				row, op = left, model.OpDelete
			} else if right.Revision != rev {
				t.Fatalf("source transition revision mismatch: %s at %s", id, rev)
			}
			if old, seen := kinds[id]; seen && old != row.Kind {
				t.Fatal("imported identity changed kind")
			}
			kinds[id] = row.Kind
			previous := expected[id]
			base := len(previous)%driver.VersionBaseEvery == 0 || len(previous) > 0 && previous[len(previous)-1].Deleted
			expected[id] = append(previous, driver.Version{Ref: row.Ref, Revision: rev, Base: base, Deleted: !isPresent, Body: right.Body})
			from, seen := lastRevision[id]
			if !seen {
				from = model.Natural64(0)
			}
			root := ""
			if p.HistoryMode == driver.RestoreHistoryModeSnapshot {
				root = "/"
			}
			changes[rev] = append(changes[rev], model.ChangeRecord{Ref: row.Ref, Op: op, FromRevision: from, ChangedPaths: []string{root}})
			lastRevision[id] = rev
		}
		sort.Slice(changes[rev], func(i, j int) bool { return changes[rev][i].Ref.ID < changes[rev][j].Ref.ID })
	}
	result := map[string][]versionSample{}
	for id, want := range expected {
		var cursor *driver.Cursor
		versions := []driver.Version{}
		for {
			page, err := r.Versions(context.Background(), want[0].Ref, driver.PageReq{Limit: 2, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			versions = append(versions, page.Items...)
			if page.NextCursor == nil {
				break
			}
			if len(page.Items) == 0 || len(versions) > 100 || reflect.DeepEqual(cursor, page.NextCursor) {
				t.Fatal("version pagination did not advance")
			}
			cursor = page.NextCursor
		}
		if len(versions) != len(want) {
			t.Fatalf("complete version census %s: got %d want %d", id, len(versions), len(want))
		}
		for i, version := range versions {
			w := want[i]
			if version.Ref != w.Ref || version.Revision != w.Revision || version.Base != w.Base || version.Deleted != w.Deleted || !version.CreatedAt.Equal(when) || !reflect.DeepEqual(version.Principal, p.Admission.Actor) {
				t.Fatalf("native version metadata/actor/restore time differs: %s at %s", id, w.Revision)
			}
			sample := versionSample{Revision: version.Revision, Base: version.Base, Deleted: version.Deleted}
			if version.Deleted {
				if len(version.Body) != 0 || len(version.Patch) != 0 {
					t.Fatal("tombstone retained a live image or patch")
				}
			} else {
				body := version.Body
				if version.Base {
					if len(version.Patch) != 0 {
						t.Fatal("base version contains a forward patch")
					}
				} else {
					var patch []struct {
						Op    string          `json:"op"`
						Path  string          `json:"path"`
						Value json.RawMessage `json:"value"`
					}
					if err := json.Unmarshal(version.Patch, &patch); err != nil || len(version.Body) != 0 || len(patch) != 1 || patch[0].Op != "replace" || patch[0].Path != "" {
						t.Fatalf("invalid native forward patch: %s at %s", id, w.Revision)
					}
					body = patch[0].Value
				}
				sameJSON(t, "version image versus source-checked as-of state", json.RawMessage(body), json.RawMessage(w.Body))
				sample.Image = digest(normalizedImportBody(t, driver.Record{Ref: version.Ref, Kind: kinds[id], Body: body}, p))
			}
			result[id] = append(result[id], sample)
		}
	}
	return result, changes
}

func checkedChangeset(t *testing.T, entry model.Changeset, expected []model.ChangeRecord, meta driver.Meta, p driver.RestoreImportProfile, when time.Time) model.Changeset {
	t.Helper()
	if !entry.Branch.IsZero() || entry.Incarnation != meta.Incarnation || entry.Kind != model.ChangesetRestore || !entry.Time.Equal(when) || !reflect.DeepEqual(entry.Principal, p.Admission.Actor) || len(entry.Chain) != 0 || len(entry.Events) != 0 || len(entry.Mutators) != 0 {
		t.Fatal("changeset invented source authority, time, branch or events")
	}
	key := ""
	if entry.Revision == meta.Head {
		key = p.IdempotencyKey
	}
	if entry.IdempotencyKey != key {
		t.Fatal("imported changeset recovery key differs")
	}
	sort.Slice(entry.Changes, func(i, j int) bool { return entry.Changes[i].Ref.ID < entry.Changes[j].Ref.ID })
	sameJSON(t, "complete native changeset versus source transitions", entry.Changes, expected)
	entry.Time, entry.Incarnation = time.Time{}, ""
	return entry
}

func checkNativeChangesets(t *testing.T, r driver.Reader, tail model.Changeset, expected map[model.Revision][]model.ChangeRecord, meta driver.Meta, p driver.RestoreImportProfile, when time.Time) ([]model.Changeset, model.Changeset) {
	t.Helper()
	ctx := context.Background()
	floor, _ := meta.HistoryFloor.Uint64()
	head, _ := meta.Head.Uint64()
	var cursor *driver.Cursor
	if floor != 0 {
		_, err := r.Changesets(ctx, nil, 2)
		var truncated driver.ErrTruncated
		if !errors.As(err, &truncated) || truncated.Floor != meta.HistoryFloor {
			t.Fatalf("changesets below floor did not return exact typed truncation: %v", err)
		}
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor, Index: model.Natural64(0), Method: "changesets"}
	}
	result := []model.Changeset{}
	next := floor + 1
	for {
		page, err := r.Changesets(ctx, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, entry := range page {
			if entry.Revision != model.Natural64(next) || next > head {
				t.Fatal("changeset census has a duplicate, gap or nonadvancing page")
			}
			result = append(result, checkedChangeset(t, entry, expected[entry.Revision], meta, p, when))
			next++
		}
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: page[len(page)-1].Revision, Index: model.Natural64(0), Method: "changesets"}
	}
	if next != head+1 || tail.Revision != meta.Head {
		t.Fatal("incomplete changeset or keyed receipt census")
	}
	return result, checkedChangeset(t, tail, expected[meta.Head], meta, p, when)
}

func collectHistory(t *testing.T, store driver.Store, a *dumpformat.Archive, p driver.RestoreImportProfile, start, end time.Time) historyCensus {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantHead, wantFloor := model.Natural64(5), model.Natural64(0)
	if p.HistoryMode == driver.RestoreHistoryModePartial {
		wantHead, wantFloor = model.Natural64(3), model.Natural64(1)
	} else if p.HistoryMode == driver.RestoreHistoryModeSnapshot {
		wantHead, wantFloor = model.Natural64(1), model.Natural64(1)
	}
	if meta.StoreID != p.SourceStoreID || meta.Head != wantHead || meta.HistoryFloor != wantFloor {
		t.Fatalf("imported identity/head/floor mismatch: %+v", meta)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	principal, err := driver.PrincipalKey(p.Admission.Actor)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := r.ChangesetByKey(ctx, principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	if err != nil || tail == nil || tail.Revision != meta.Head || tail.Time.IsZero() {
		t.Fatal("actual keyed restore changeset is absent", err)
	}
	when := tail.Time
	if p.HistoryMode != driver.RestoreHistoryModeSnapshot {
		boundTime, err := time.Parse(time.RFC3339Nano, p.HistoryTime)
		if err != nil || !boundTime.Equal(when) {
			t.Fatal("restore execution time differs from admitted history time")
		}
	} else if when.Before(start) || when.After(end) {
		t.Fatal("snapshot restore time lies outside actual execution")
	}
	rows := indexRows(t, allRows(t, r))
	checkSourceImage(t, rows, a, p, when, false)
	got := historyCensus{Head: meta.Head, Floor: meta.HistoryFloor, Current: currentHistoryCensus(t, r, rows, a, p), AsOf: map[string]map[string]string{}, ChangesetsUnavailableThrough: meta.HistoryFloor}
	head, _ := meta.Head.Uint64()
	states := make([]map[string]driver.Record, head+1)
	states[0] = map[string]driver.Record{}
	states[head] = rows
	got.AsOf[meta.Head.String()] = imageDigests(t, rows, p)
	if p.HistoryMode != driver.RestoreHistoryModeSnapshot {
		want := expectedHistoryEvidence(t, a, p)
		var actual driver.HistoryImportEvidence
		marker, ok := rows[driver.OfflineHistoryEvidenceID]
		if !ok || marker.Kind != "beads-archive" || json.Unmarshal(marker.Body, &actual) != nil {
			t.Fatal("native history provenance marker missing")
		}
		sameJSON(t, "exact source provenance mapping/gaps/floor", actual, want)
		got.Evidence = &actual
		source, _ := a.SourceHistory()
		for revision := uint64(1); revision < head; revision++ {
			sequence := revision
			if source.Range.First > 1 {
				sequence = source.Range.First + revision - 2
			}
			image, err := a.HistoryImage(sequence)
			if err != nil {
				t.Fatal(err)
			}
			at := model.Natural64(revision)
			historical, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
			if err != nil {
				t.Fatal(err)
			}
			state := indexRows(t, allRows(t, historical))
			if err := historical.Close(); err != nil {
				t.Fatal(err)
			}
			checkSourceImage(t, state, image, p, when, true)
			states[revision] = state
			got.AsOf[at.String()] = imageDigests(t, state, p)
		}
	}
	zero := model.Natural64(0)
	initial, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero})
	if meta.HistoryFloor.IsZero() {
		if err != nil {
			t.Fatal(err)
		}
		if len(allRows(t, initial)) != 0 {
			t.Fatal("full-history revision zero is not empty")
		}
		if err := initial.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		if err == nil {
			initial.Close()
			t.Fatal("below-floor as-of read unexpectedly succeeded")
		}
		var truncated driver.ErrTruncated
		if !errors.As(err, &truncated) || truncated.Floor != meta.HistoryFloor {
			t.Fatalf("as-of below floor did not return exact typed truncation: %v", err)
		}
	}
	var changes map[model.Revision][]model.ChangeRecord
	got.Versions, changes = checkNativeVersions(t, r, states, p, when)
	got.Changesets, got.RestoreReceipt = checkNativeChangesets(t, r, *tail, changes, meta, p, when)
	receipt, err := r.IdempotencyByKey(context.Background(), principal, p.AgentLabel(), p.Method, p.IdempotencyKey)
	hash, hashErr := p.RequestHash()
	if err != nil || hashErr != nil || receipt == nil || receipt.ResponsePending || receipt.RequestHash != hash || receipt.Commit.Revision != meta.Head {
		t.Fatal("final durable restore receipt binding differs", err)
	}
	recovered, err := converterroundtrip.RecoverImport(context.Background(), store, p, converterroundtrip.Verification{ArchiveSHA256: a.SHA256(), DerivedSHA256: p.DerivedDigest})
	if err != nil || !recovered.Recovered || recovered.Restored != meta {
		t.Fatal("actual receipt recovery failed", err)
	}
	after, err := store.Meta(context.Background())
	if err != nil || after != meta {
		t.Fatal("receipt recovery changed the restored store", err)
	}
	return got
}

func writeNightlyEvidence(t *testing.T, name string, report any) {
	t.Helper()
	encoded := append(jsonBytes(t, report), '\n')
	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
		if err := os.WriteFile(filepath.Join(dir, name), encoded, 0600); err != nil {
			t.Error(err)
		}
	}
	t.Logf("public comparison evidence %s sha256=%s bytes=%d", name, digest(encoded), len(encoded))
}

func TestNightlyHistoryFreshStoreDiffs(t *testing.T) {
	ctx := context.Background()
	type result struct {
		Case        string        `json:"case"`
		Driver      string        `json:"driver"`
		Restore     int           `json:"fresh_restore"`
		Incarnation string        `json:"incarnation"`
		Census      historyCensus `json:"census"`
	}
	results := []result{}
	t.Cleanup(func() {
		status := "bounded_public_history"
		if t.Failed() {
			status = "failed"
		}
		writeNightlyEvidence(t, "nightly-history-restore-diffs.json", map[string]any{
			"schema": "plug-nightly-history-restore-diffs/v1", "status": status, "fixtures": historyFixtureSHA, "results": results,
			"boundaries": []string{"checked_in_public_synthetic_images_not_source_exporter_or_real_ledger_history", "partial_baseline_changeset_not_enumerable_at_floor", "snapshot_versions_are_restore_bases_not_source_history", "old_storage_incarnation_refused", "memory.list_rpc_continuation_not_qualified", "not_complete_paired_or_private_census", "scheduled_main_execution_not_proven_by_manual_gate", "one_dispatch_is_not_a_three_night_streak"},
		})
	})
	cases := []struct {
		name, fixture string
		mode          driver.RestoreHistoryMode
	}{
		{"strict-default-full", "full", ""},
		{"explicit-full", "full", driver.RestoreHistoryModeFull},
		{"explicit-partial", "partial", driver.RestoreHistoryModePartial},
		{"snapshot-full", "full", driver.RestoreHistoryModeSnapshot},
		{"snapshot-partial", "partial", driver.RestoreHistoryModeSnapshot},
	}
	incarnations := map[string]bool{}
	for _, tc := range cases {
		data := publicHistoryFixture(t, tc.fixture)
		a := readArchive(t, data)
		var baseline *historyCensus
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			for iteration := 1; iteration <= 2; iteration++ {
				t.Run(fmt.Sprintf("%s/%s/fresh-%d", tc.name, engine, iteration), func(t *testing.T) {
					store, reopen := freshStore(t, engine)
					before, err := store.Meta(ctx)
					if err != nil || !before.Head.IsZero() || !before.HistoryFloor.IsZero() {
						t.Fatal("history target is not fresh", err)
					}
					unchanged(t, store, before)
					p := profile(t, data)
					p.HistoryMode = tc.mode
					if tc.mode != driver.RestoreHistoryModeSnapshot {
						p, err = converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), p, time.Now().UTC())
						if err != nil {
							t.Fatal(err)
						}
						if len(p.HistoryDigest) != 64 || p.HistoryTime == "" {
							t.Fatal("actual history binding is absent")
						}
						unchanged(t, store, before)
					}
					p, err = p.Detached()
					if err != nil {
						t.Fatal(err)
					}
					start := time.Now().UTC()
					supplied := p
					if tc.mode == "" {
						supplied.HistoryMode = ""
					}
					restored, err := converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, supplied)
					end := time.Now().UTC()
					if err != nil {
						t.Fatal("real driver history/snapshot import failed", err)
					}
					if restored.Restored.Incarnation == before.Incarnation || incarnations[restored.Restored.Incarnation] {
						t.Fatal("independent restore reused an incarnation")
					}
					incarnations[restored.Restored.Incarnation] = true
					stale, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
					if err == nil {
						stale.Close()
						t.Fatal("old incarnation unexpectedly opened a reader")
					}
					var changed driver.ErrIncarnationChanged
					if !errors.As(err, &changed) {
						t.Fatalf("old incarnation did not refuse with its typed reset: %v", err)
					}
					got := collectHistory(t, store, a, p, start, end)
					if baseline == nil {
						baseline = &got
					} else {
						sameJSON(t, "independent fresh driver source/history census", got, *baseline)
					}
					if engine != "memory" {
						if err := store.Close(); err != nil {
							t.Fatal(err)
						}
						sameJSON(t, "durable reopened source/history census", collectHistory(t, reopen(), a, p, start, end), got)
					}
					results = append(results, result{Case: tc.name, Driver: engine, Restore: iteration, Incarnation: restored.Restored.Incarnation, Census: got})
				})
			}
		}
	}
	if len(results) != len(cases)*3*2 {
		t.Fatalf("history fresh-store matrix incomplete: got %d want %d", len(results), len(cases)*3*2)
	}
}

func TestNightlyHistoryRefusalsNoWrites(t *testing.T) {
	ctx := context.Background()
	passed := []string{}
	t.Cleanup(func() {
		status := "pass"
		if t.Failed() {
			status = "failed"
		}
		writeNightlyEvidence(t, "nightly-history-refusals.json", map[string]any{"schema": "plug-nightly-history-refusals/v1", "status": status, "typed_no_write_controls": passed})
	})
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, scenario := range []string{"default-refuses-partial", "full-refuses-partial", "missing-binding", "changed-digest", "changed-time"} {
			t.Run(engine+"/"+scenario, func(t *testing.T) {
				fixture := "full"
				wantReason := driver.RestoreHistoryReasonUnmappable
				if strings.HasSuffix(scenario, "refuses-partial") {
					fixture, wantReason = "partial", driver.RestoreHistoryReasonNonContiguous
				}
				data := publicHistoryFixture(t, fixture)
				p := profile(t, data)
				p.HistoryMode = driver.RestoreHistoryModeFull
				if scenario == "default-refuses-partial" {
					p.HistoryMode = ""
				}
				if strings.HasSuffix(scenario, "refuses-partial") {
					_, err := converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), p, time.Now().UTC())
					var refused driver.ErrRestoreHistoryRefused
					if !errors.As(err, &refused) || refused.Reason != driver.RestoreHistoryReasonNonContiguous {
						t.Fatalf("incomplete range binding did not return typed non-contiguous refusal: %v", err)
					}
				}
				if strings.HasPrefix(scenario, "changed-") {
					var err error
					p, err = converterroundtrip.BindHistoryImport(ctx, bytes.NewReader(data), p, time.Now().UTC())
					if err != nil {
						t.Fatal(err)
					}
					if scenario == "changed-digest" {
						p.HistoryDigest = strings.Repeat("e", 64)
					} else {
						p.HistoryTime = time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
					}
				}
				store, _ := freshStore(t, engine)
				before, err := store.Meta(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_, err = converterroundtrip.RestoreImport(ctx, bytes.NewReader(data), store, p)
				var refused driver.ErrRestoreHistoryRefused
				if !errors.As(err, &refused) || refused.Reason != wantReason {
					t.Fatalf("incorrect typed refusal: got %T %v want %s", err, err, wantReason)
				}
				unchanged(t, store, before)
				checkNoArchiveWrites(t, store, readArchive(t, data))
				passed = append(passed, engine+"/"+scenario+"/"+string(refused.Reason))
			})
		}
	}
	if len(passed) != 15 {
		t.Fatal("typed no-write matrix incomplete")
	}
}

func checkNoArchiveWrites(t *testing.T, store driver.Store, a *dumpformat.Archive) {
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
	members := a.Manifest().Files
	manifest, _ := a.Member("manifest.json")
	members["manifest.json"] = dumpformat.File{SHA256: digest(manifest)}
	for name, file := range members {
		if _, err := r.Blob(ctx, driver.Hash(file.SHA256)); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("refused import published archive blob %s: %v", name, err)
		}
	}
	allocators, ok := r.(driver.AllocatorReader)
	if !ok {
		t.Fatal("allocator no-write inspection capability absent")
	}
	for _, project := range a.Manifest().Projects {
		if _, err := allocators.LoadAllocator(ctx, project.Prefix); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("refused import published allocator %s: %v", project.Prefix, err)
		}
	}
}
