package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Test-only exports cross the existing external composition-test boundary.
// All expectations still come from the original detached native observations.
type B27NativeReferenceForTest struct {
	Data    []byte
	Archive *dumpformat.Archive
	inputs  []b27NativeInput
}

func B27RichReferenceForTest(t *testing.T) *B27NativeReferenceForTest {
	t.Helper()
	r := &B27NativeReferenceForTest{inputs: b27NativeInputs(t)}
	var archives []*dumpformat.Archive
	for _, input := range r.inputs {
		a, err := dumpformat.Read(bytes.NewReader(b27NativeBytes(t, filepath.Join(input.dir, "legacy", "capture.tar"))))
		if err != nil || a.Manifest().StoreID != input.StoreID || !a.HasNativeSourceHistories() {
			t.Fatal("original portable native history required", input.Project, err)
		}
		t.Cleanup(a.ReleaseNativeDecoded)
		b27NativeEqual(t, "original project identity/profile", a.Manifest().Projects, input.Projects)
		archives = append(archives, a)
	}
	var err error
	r.Data, err = dumpformat.MergeArchives(archives...)
	if err != nil {
		t.Fatal(err)
	}
	r.Archive, err = dumpformat.Read(bytes.NewReader(r.Data))
	if err != nil || len(r.Archive.Manifest().Projects) != 2 {
		t.Fatal("actual two-source portable merge", err)
	}
	t.Cleanup(r.Archive.ReleaseNativeDecoded)
	return r
}

// HistoryEvidence binds the exact lowered history from the original archive.
// Use the later of its two independent source HEAD commit times as the
// deterministic restore clock.
func (r *B27NativeReferenceForTest) HistoryEvidence(t *testing.T, store driver.Store, lease driver.ActiveLease) (digest, historyTime string) {
	t.Helper()
	ctx := context.Background()
	var when time.Time
	if err := r.Archive.VisitNativeSourceHistories(ctx, func(history dumpformat.NativeHistory) error {
		for _, commit := range history.Commits {
			if commit.ID != history.Head {
				continue
			}
			date, _ := commit.Metadata["date"].(string)
			headTime, err := time.Parse(time.RFC3339Nano, date)
			if err != nil || headTime.IsZero() {
				return fmt.Errorf("invalid archived HEAD time for %s: %q", history.StoreID, date)
			}
			if headTime.After(when) {
				when = headTime
			}
			return nil
		}
		return fmt.Errorf("missing archived HEAD commit for %s", history.StoreID)
	}); err != nil || when.IsZero() {
		t.Fatal("rich archive history clock", err)
	}
	profile := driver.RestoreImportProfile{
		Admission: driver.RestoreImportAdmission{
			Mode: "anonymous-local", Actor: model.PrincipalRef{Kind: "anonymous", ID: "anonymous"},
			Authority: "trusted-human-presence", AdmissionDigest: strings.Repeat("0", 64),
		},
		SourceStoreID:  driver.StoreID(r.Archive.Manifest().StoreID),
		Method:         "offline.restore.import",
		IdempotencyKey: strings.Repeat("0", 32),
		ArchiveDigest:  r.Archive.SHA256(),
		HistoryMode:    driver.RestoreHistoryModeFull,
		HistoryTime:    when.UTC().Format(time.RFC3339Nano),
	}
	bound, err := BindSourceImport(ctx, bytes.NewReader(r.Data), profile, r.Plan(t, store, lease))
	if err != nil || bound.HistoryDigest == "" || bound.HistoryTime != profile.HistoryTime {
		t.Fatal("bind archived full-history evidence", err)
	}
	return bound.HistoryDigest, bound.HistoryTime
}

func (r *B27NativeReferenceForTest) Plan(t *testing.T, store driver.Store, lease driver.ActiveLease) driver.SourcePlanePlanV1 {
	t.Helper()
	return b27NativePlan(t, r.Archive, store, lease)
}

// B27PlanArchiveForTest keeps the public composition harness on the same
// authenticated source-disposition path when it consumes a separately
// captured single-project archive.
func B27PlanArchiveForTest(t *testing.T, archive *dumpformat.Archive, store driver.Store, lease driver.ActiveLease) driver.SourcePlanePlanV1 {
	t.Helper()
	return b27NativePlan(t, archive, store, lease)
}

func (r *B27NativeReferenceForTest) IDs() map[string]string {
	ids := map[string]string{}
	for _, input := range r.inputs {
		for _, row := range input.current["issues"].Rows {
			ids[row["id"].(string)] = input.Project
		}
	}
	return ids
}

func (r *B27NativeReferenceForTest) AssertPublic(t *testing.T, bodies map[string]json.RawMessage) {
	t.Helper()
	rows := map[string]driver.Record{}
	for id, body := range bodies {
		rows[id] = driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Body: body}
	}
	b27NativeDocumentRows(t, rows, r.Archive, r.inputs)
}

func (r *B27NativeReferenceForTest) AssertImported(t *testing.T, store driver.Store, digest, historyTime string) {
	t.Helper()
	profile, report, found, err := RecoverBoundSourceImport(context.Background(), store)
	if err != nil || !found || report.Receipt == nil {
		t.Fatal("public import's original durable binding/receipt", err)
	}
	if report.Archive != nil {
		defer report.Archive.ReleaseNativeDecoded()
	}
	if profile.HistoryMode != driver.RestoreHistoryModeFull || profile.HistoryDigest != digest || profile.HistoryTime != historyTime {
		t.Fatal("public import did not retain the requested full-history binding")
	}
	b27NativeCurrent(t, store, r.Archive, r.inputs, report)
	b27NativeDocuments(t, store, r.Archive, r.inputs)
	b27NativeFullHistory(t, store, r.Archive, r.inputs, report)
}

// The reference reads actual SQL declarations and the captured YAML's narrow
// fixture grammar independently of the converter's catalog parser. It retains
// per-origin declarations even when names overlap. Catalog types remain source
// declarations; explicit archive mappings decide their installed target type.
func (r *B27NativeReferenceForTest) Catalog(t *testing.T, project string) (map[string]string, map[string][]string) {
	t.Helper()
	statuses := map[string]string{"open": "active", "in_progress": "wip", "blocked": "wip", "deferred": "frozen", "closed": "done", "pinned": "frozen"}
	origins := map[string][]string{}
	add := func(origin, kind, text string) {
		for _, part := range strings.Split(strings.Trim(text, "[]"), ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				t.Fatal("empty actual catalog declaration")
			}
			name := part
			if kind == "status" {
				var category string
				var ok bool
				name, category, ok = strings.Cut(part, ":")
				if !ok || category == "" || (statuses[name] != "" && statuses[name] != category) {
					t.Fatal("inconsistent independent native catalog", project, name)
				}
				statuses[name] = category
			}
			origins[origin+"/"+kind] = append(origins[origin+"/"+kind], name)
		}
	}
	found := false
	for _, input := range r.inputs {
		if input.Project != project {
			continue
		}
		found = true
		for _, row := range input.current["config"].Rows {
			switch row["key"] {
			case "status.custom":
				add("config", "status", row["value"].(string))
			case "types.custom":
				add("config", "type", row["value"].(string))
			}
		}
		for _, row := range input.current["custom_statuses"].Rows {
			add("custom_statuses", "status", row["name"].(string)+":"+row["category"].(string))
		}
		for _, row := range input.current["custom_types"].Rows {
			add("custom_types", "type", row["name"].(string))
		}
		section := ""
		seen := map[string]bool{}
		for _, line := range strings.Split(string(b27NativeBytes(t, filepath.Join(input.dir, "config.yaml"))), "\n") {
			if line == "status:" || line == "types:" {
				section = strings.TrimSuffix(line, ":")
			} else if strings.HasPrefix(line, "  custom: ") && section != "" {
				if seen[section] {
					t.Fatal("repeated captured YAML custom declaration")
				}
				seen[section] = true
				kind := "type"
				if section == "status" {
					kind = "status"
				}
				add("project-config-yaml", kind, strings.TrimPrefix(line, "  custom: "))
			} else if line != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "#") {
				section = ""
			}
		}
		if len(seen) != 2 {
			t.Fatal("both independently captured YAML catalogs required")
		}
		// The original complete catalog independently supplies the remaining
		// builtin name. Custom declarations above must still match their own
		// SQL and YAML inputs; initializing from the entire catalog would hide
		// an omitted custom declaration.
		if len(input.Projects) != 1 || input.Projects[0].Prefix != project {
			t.Fatal("independent complete project catalog missing", project)
		}
		remaining := 0
		captured := map[string]bool{}
		for _, declaration := range input.Projects[0].Statuses {
			if captured[declaration.Name] {
				t.Fatal("independent complete catalog repeats status", project, declaration.Name)
			}
			captured[declaration.Name] = true
			if category, exists := statuses[declaration.Name]; exists {
				if category != declaration.Category {
					t.Fatal("independent complete catalog category differs", project, declaration.Name)
				}
				continue
			}
			if declaration.Category != "wip" {
				t.Fatal("remaining original builtin category differs", project)
			}
			remaining++
			statuses[declaration.Name] = declaration.Category
		}
		if remaining != 1 || len(statuses) != len(captured) {
			t.Fatal("independent complete catalog census differs", project)
		}
	}
	if !found {
		t.Fatal("unknown native project", project)
	}
	for _, names := range origins {
		sort.Strings(names)
	}
	return statuses, origins
}
