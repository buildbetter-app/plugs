package cardb20

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// These expectations are authored independently of the captured archive.
// SQL NULL is different from both an absent field and a timestamp string.
type b24TimerOracle struct {
	ID, Project, Title, Status string
	Due, Defer                 *time.Time
}

func b24TimerValue(t *testing.T, field string, value any, want *time.Time) {
	t.Helper()
	if want == nil {
		if value != nil {
			t.Fatalf("%s lost SQL NULL: %#v", field, value)
		}
		return
	}
	text, ok := value.(string)
	if !ok {
		t.Fatalf("%s timestamp is not text: %#v", field, value)
	}
	var observed time.Time
	var err error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999"} {
		observed, err = time.Parse(layout, text)
		if err == nil {
			break
		}
	}
	if err != nil || !observed.Equal(*want) {
		t.Fatalf("independent %s oracle: raw=%q actual=%s want=%s err=%v", field, text, observed, want, err)
	}
}

func b24TimerSource(t *testing.T, data []byte, want b24TimerOracle) dumpformat.Record {
	t.Helper()
	a, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range a.Records("bead") {
		if row.ID != want.ID || row.Project != want.Project {
			continue
		}
		if row.Body["title"] != want.Title {
			t.Fatalf("native title update lost: %#v", row.Body["title"])
		}
		for name, expected := range map[string]*time.Time{"due_at": want.Due, "defer_until": want.Defer} {
			physical, present := row.Source.Fields[name]
			portable, bodyPresent := row.Body[name]
			if !present || !bodyPresent || !reflect.DeepEqual(physical, portable) {
				t.Fatalf("actual physical/portable %s missing or unequal: %#v/%#v", name, physical, portable)
			}
			b24TimerValue(t, name, physical, expected)
		}
		return row
	}
	t.Fatalf("actual timer source omitted %s/%s", want.Project, want.ID)
	return dumpformat.Record{}
}

func b24RunTimerImport(t *testing.T, engine string, data []byte, want b24TimerOracle) {
	t.Helper()
	source := b24TimerSource(t, data, want)
	f := newFixture(t, engine)
	inspection, err := converterroundtrip.InspectImport(f.ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	// Trusted disposable test-host admission; the archive supplies no authority.
	profile := driver.RestoreImportProfile{
		Admission:     driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b24-timer-import-admin"}, Authority: "restore-admin", AdmissionDigest: key("b24-timer-import-admission")},
		SourceStoreID: inspection.SourceStoreID, Method: "offline.restore.import", IdempotencyKey: key("b24-timer-import"),
		ArchiveDigest: inspection.ArchiveSHA256, DerivedDigest: inspection.DerivedSHA256, HistoryMode: driver.RestoreHistoryModeSnapshot,
	}
	restored, err := converterroundtrip.RestoreImportWithLease(f.ctx, bytes.NewReader(data), f.store, f.lease, profile)
	if err != nil || restored.Restored.Head != model.Natural64(1) || restored.Restored.HistoryFloor != model.Natural64(1) {
		t.Fatalf("actual snapshot import: %+v %v", restored, err)
	}
	// Bind callers to the new incarnation using the existing owned lease.
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.bind(t)
	ref := model.RefByID(want.ID, model.Natural64(0))
	params := func(extra map[string]any) map[string]any {
		out := map[string]any{"id": ref, "_meta": map[string]any{"project": want.Project}}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	read := func(phase, title, status string, comments int) {
		t.Helper()
		b24ConservedRead(t, f, phase, func() {
			got := f.call("bead.get", phase, params(nil))
			if got.Error != nil {
				t.Fatalf("public imported get: %+v", got.Error)
			}
			var result struct {
				Bead struct {
					Status   string                     `json:"status"`
					Fields   map[string]any             `json:"fields"`
					Metadata map[string]json.RawMessage `json:"metadata"`
				} `json:"bead"`
				Comments []struct {
					Body string `json:"body"`
				} `json:"comments"`
			}
			if err := json.Unmarshal(got.Result, &result); err != nil {
				t.Fatal(err)
			}
			if result.Bead.Fields["title"] != title || result.Bead.Status != status || len(result.Comments) != comments {
				t.Fatalf("public imported state mismatch: %s", got.Result)
			}
			for name, expected := range map[string]any{"due_at": source.Body["due_at"], "defer_until": source.Body["defer_until"], "source_fields": source.Source.Fields} {
				metadataKey := "beads.source." + name
				if name == "source_fields" {
					metadataKey = "beads.source_fields"
				}
				encoded, err := json.Marshal(expected)
				var observed, expectedJSON any
				decoder := json.NewDecoder(bytes.NewReader(result.Bead.Metadata[metadataKey]))
				decoder.UseNumber()
				if err != nil || decoder.Decode(&observed) != nil {
					t.Fatalf("missing exact imported %s", metadataKey)
				}
				decoder = json.NewDecoder(bytes.NewReader(encoded))
				decoder.UseNumber()
				if decoder.Decode(&expectedJSON) != nil || !reflect.DeepEqual(observed, expectedJSON) {
					t.Fatalf("imported %s changed: %s want=%s", metadataKey, result.Bead.Metadata[metadataKey], encoded)
				}
				if _, installed := result.Bead.Fields[name]; installed {
					t.Fatalf("source %s silently became a live timer field", name)
				}
			}
			if comments != 0 && result.Comments[0].Body != "waiting for timer input" {
				t.Fatal("imported defer reason changed")
			}
		})
	}
	// The independent native date stays opaque. The application clock controls
	// stale reads; the durable clock stays in 2026 throughout this matrix.
	target := time.Date(2099, 6, 15, 0, 0, 0, 0, time.UTC)
	application := testclock.New(t, target.Add(-24*time.Hour))
	b24BindApplicationClock(t, f, application)
	read("imported-initial", want.Title, want.Status, 0)
	check := converterroundtrip.Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest}
	recovered, err := converterroundtrip.RecoverImport(f.ctx, f.store, profile, check)
	if err != nil || recovered.Restored.Incarnation != restored.Restored.Incarnation {
		t.Fatalf("import receipt recovery: %+v %v", recovered, err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: restored.Restored.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Changesets(f.ctx, nil, 10000)
	if closeErr := r.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor != model.Natural64(1) {
		t.Fatalf("snapshot floor was hidden: %v", err)
	}
	before := b24ReadSnapshot(t, f)
	b24ReopenClocks(t, f, application)
	if !reflect.DeepEqual(before, b24ReadSnapshot(t, f)) {
		t.Fatal("initial SQL reopen changed imported physical state")
	}
	if _, err := converterroundtrip.RecoverImport(f.ctx, f.store, profile, check); err != nil {
		t.Fatalf("reopened import receipt: %v", err)
	}
	read("imported-reopened", want.Title, want.Status, 0)
	updated := "timer fields survive unrelated Plug update"
	commit(t, f.call("bead.update", "imported-title-update", params(map[string]any{"fields": map[string]any{"title": updated}})))
	read("imported-title-updated", updated, want.Status, 0)
	b24ConservedRead(t, f, "imported-empty-reason", func() {
		got := f.call("bead.defer", "imported-empty-reason", params(map[string]any{"reason": " "}))
		if got.Error == nil || got.Error.Data.Code != "invalid_params" {
			t.Fatalf("blank defer reason accepted: %+v", got)
		}
	})
	prior := f.state(t)
	commit(t, f.call("bead.defer", "imported-defer", params(map[string]any{"reason": "waiting for timer input"})))
	deferred := f.state(t)
	if len(deferred.Versions[ref.ID]) != len(prior.Versions[ref.ID])+1 || len(deferred.Changesets) != len(prior.Changesets)+1 {
		t.Fatal("Plug defer did not use one ordinary version/ChangeSet")
	}
	read("imported-deferred", updated, "backlog", 1)
	tick := time.Nanosecond
	if engine == "postgres" {
		tick = time.Microsecond
	}
	for _, boundary := range []struct {
		name string
		at   time.Time
	}{
		{"before", target.Add(-tick)}, {"at", target}, {"after", target.Add(tick)},
	} {
		b24StepClocks(t, f, application, boundary.at.Sub(application.Now()), 0)
		b24ObserveClocks(t, f, application, "imported-target-"+boundary.name)
		read("imported-target-"+boundary.name, updated, "backlog", 1)
		b24ConservedRead(t, f, "imported-stale-"+boundary.name, func() {
			query := map[string]any{
				"definition": staleDefinition("status=backlog"), "stale": map[string]any{"days": 1},
				"_meta": map[string]any{"project": want.Project},
			}
			ids, next := staleIDs(t, f.call("view.run", "imported-stale-"+boundary.name, query))
			count := staleCount(t, f.call("view.count", "imported-stale-count-"+boundary.name, query))
			wanted := []string{}
			if boundary.name == "after" {
				wanted = []string{want.ID}
			}
			if next != "" || len(ids) != len(wanted) || count != len(wanted) || len(wanted) != 0 && ids[0] != want.ID {
				t.Fatalf("imported strict stale boundary %s ids=%v count=%d next=%q want=%v", boundary.name, ids, count, next, wanted)
			}
		})
		b24ConservedRead(t, f, "imported-ready-"+boundary.name, func() {
			got := f.call("bead.ready", "imported-ready-"+boundary.name, map[string]any{"limit": 100, "_meta": map[string]any{"project": want.Project}})
			if got.Error != nil {
				t.Fatalf("imported ready: %+v", got.Error)
			}
			var page struct {
				Items []struct{ ID string } `json:"items"`
			}
			if json.Unmarshal(got.Result, &page) != nil {
				t.Fatal("invalid ready page")
			}
			for _, item := range page.Items {
				if item.ID == want.ID {
					t.Fatal("opaque imported timestamp automatically woke untimed Plug defer")
				}
			}
		})
		before := b24ReadSnapshot(t, f)
		b24ReopenClocks(t, f, application)
		if !reflect.DeepEqual(before, b24ReadSnapshot(t, f)) {
			t.Fatal("boundary SQL reopen changed imported state")
		}
		read("imported-target-"+boundary.name+"-reopened", updated, "backlog", 1)
	}
	commit(t, f.call("bead.reopen", "imported-explicit-reopen", params(nil)))
	read("imported-explicit-reopened", updated, "todo", 1)
	before = b24ReadSnapshot(t, f)
	b24ReopenClocks(t, f, application)
	if !reflect.DeepEqual(before, b24ReadSnapshot(t, f)) {
		t.Fatal("final SQL reopen changed timer provenance")
	}
	read("imported-final-durable", updated, "todo", 1)
	b24ConservedRead(t, f, "imported-final-ready", func() {
		got := f.call("bead.ready", "imported-final-ready", map[string]any{"limit": 100, "_meta": map[string]any{"project": want.Project}})
		if got.Error != nil {
			t.Fatalf("reopened ready: %+v", got.Error)
		}
		var page struct {
			Items []struct{ ID string } `json:"items"`
		}
		if json.Unmarshal(got.Result, &page) != nil {
			t.Fatal("invalid reopened ready page")
		}
		found := false
		for _, item := range page.Items {
			found = found || item.ID == want.ID
		}
		if !found {
			t.Fatal("explicit reopen did not restore actual imported derived readiness")
		}
	})
	persistence := "SAME_STORE_MEMORY_ONLY"
	if engine != "memory" {
		persistence = "ACTUAL_SQL_CLOSE_OPEN"
	}
	b24Retain(t, fmt.Sprintf("%s-%s-timer-import.json", want.ID, engine), map[string]any{
		"oracle": want, "archive_sha256": inspection.ArchiveSHA256, "source_fields": source.Source.Fields,
		"restore": restored.Restored, "persistence": persistence, "target": target,
		"import_mapping":              "beads.source.due_at / beads.source.defer_until / beads.source_fields",
		"adopted_semantics":           "UNTIMED_DEFER_EXPLICIT_REOPEN_ALWAYS_ON_CHANGESETS",
		"native_exact_clock_equality": "NOT_MEASURED", "native_version_read": "NOT_MEASURED",
	})
	t.Logf("B24_TIMER_IMPORT engine=%s id=%s archive=%s persistence=%s source_due=%v source_defer=%v", engine, want.ID, inspection.ArchiveSHA256, persistence, source.Body["due_at"], source.Body["defer_until"])
}

func TestB24PublicImportedNullTimersDurable(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "integration", "converterroundtrip", "testdata", "public-complete-root-990eb800.tar"))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != "a2d933a7a54b53303d33a20d5b96eaac0a85c079c5b08cae553aa2a654d820b5" {
		t.Fatal("actual NULL-timer archive changed")
	}
	// This public source has no due/defer values, distinct from the native
	// non-NULL fixture created by the separately selected manual target.
	want := b24TimerOracle{ID: "demo-42v", Project: "demo", Title: "Public source updated through Beads", Status: "todo"}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b24RunTimerImport(t, engine, data, want) })
	}
}
