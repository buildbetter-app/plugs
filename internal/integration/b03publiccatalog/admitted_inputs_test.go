package cardb20

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

func b03SafeInputPath(name string) bool {
	return name != "" && !filepath.IsAbs(name) && filepath.Clean(name) == name && name != "." && name != ".." && !strings.HasPrefix(name, "../") && !strings.Contains(name, "\\")
}

// A transport package of byte-identical immutable inputs, not a native source
// builder. Every inner file is separately bound to its delivered original.
func b03ReadNativeInputBundle(t *testing.T, inputs b03AdmittedInputs) map[string][]byte {
	t.Helper()
	if filepath.Base(inputs.Bundle) != inputs.Bundle || inputs.Bundle == "" {
		t.Fatal("bound input bundle name required")
	}
	data, err := os.ReadFile(filepath.Join("testdata", inputs.Bundle))
	if err != nil || b03Digest(data) != inputs.BundleSHA256 {
		t.Fatal("immutable input transport bundle changed", err)
	}
	z, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	r := tar.NewReader(z)
	files := map[string][]byte{}
	total := int64(0)
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !b03SafeInputPath(h.Name) || h.Typeflag != tar.TypeReg || h.Size < 0 || h.Size > 32<<20 || files[h.Name] != nil {
			t.Fatal("invalid duplicate/escaping/nonregular input bundle member")
		}
		total += h.Size
		if total > 64<<20 {
			t.Fatal("public input transport exceeds its fixed bound")
		}
		body, err := io.ReadAll(r)
		if err != nil || int64(len(body)) != h.Size {
			t.Fatal("incomplete original input bytes", err)
		}
		files[h.Name] = body
	}
	if _, err := io.Copy(io.Discard, z); err != nil {
		t.Fatal("incomplete input transport gzip checksum/EOF", err)
	}
	if len(files) != len(inputs.Evidence) {
		t.Fatal("input transport contains missing or undeclared evidence members")
	}
	return files
}

type b03IndependentMilestone struct {
	CommitID       string           `json:"commit_id"`
	Config         []dumpformat.Row `json:"config"`
	CustomStatuses []dumpformat.Row `json:"custom_statuses"`
	Issues         []dumpformat.Row `json:"issues"`
}

type b03IndependentProject struct {
	Catalog struct {
		ManifestProjects []dumpformat.Project `json:"manifest_projects"`
		StoredConfig     []dumpformat.Row     `json:"stored_config"`
		StoredStatuses   []dumpformat.Row     `json:"stored_custom_statuses"`
		YAML             map[string]string    `json:"yaml_status_declarations"`
		YAMLSHA256       string               `json:"physical_yaml_sha256"`
	} `json:"catalog"`
	Milestones        map[string]b03IndependentMilestone `json:"milestones"`
	RawBusinessTables map[string]dumpformat.NativeTable  `json:"raw_business_tables"`
	Ready             []struct {
		IDs  []string `json:"ids"`
		Exit int      `json:"exit"`
	} `json:"ready"`
	Eligibility []struct {
		Status        string `json:"status"`
		ID            string `json:"id"`
		Exit          int    `json:"exit"`
		Accepted      bool   `json:"expected_success"`
		Conserved     bool   `json:"complete_raw_state_equal"`
		Before, After dumpformat.Row
	} `json:"eligibility"`
}

type b03IndependentLedger struct {
	Head, Tree, Reviewer string
	Projects             map[string]b03IndependentProject
}

func b03IndependentNativeInputs(t *testing.T, inputs b03AdmittedInputs) b03IndependentLedger {
	t.Helper()
	if b03Digest(inputs.files["handoff/original-command.txt"]) != inputs.BuildRequestSHA256 || b03Digest(inputs.files["handoff/original-bazel.log"]) != inputs.RawOutputSHA256 {
		t.Fatal("original native request/output binding differs")
	}
	for _, project := range inputs.Sources {
		var binary struct{ SHA256 string }
		if err := json.Unmarshal(inputs.files[project.Project+"/binary.json"], &binary); err != nil || binary.SHA256 != inputs.CaptureELFSHA256 {
			t.Fatal("original source-built capture ELF identity differs", err)
		}
	}
	var delivery struct {
		Source           struct{ Commit, Tree string }
		CommittedFixture struct {
			Commit string `json:"payload_commit"`
			Tree   string `json:"payload_tree"`
		} `json:"committed_fixture"`
		Focus struct {
			RunID              json.Number `json:"run_id"`
			Conclusion, Status string
		}
	}
	if err := json.Unmarshal(inputs.files["handoff/source-delivery.json"], &delivery); err != nil || delivery.Source.Commit != inputs.NativeSourceCommit || delivery.Source.Tree != inputs.NativeSourceTree || delivery.CommittedFixture.Commit != inputs.FixtureCommit || delivery.CommittedFixture.Tree != inputs.FixtureTree || delivery.Focus.RunID.String() != inputs.OriginalRun || delivery.Focus.Conclusion != "success" || delivery.Focus.Status != "completed" || string(inputs.files["handoff/original-exit_code"]) != "0\n" || string(inputs.files["handoff/original-evidence_status"]) != "ok\n" {
		t.Fatal("original native source/fixture/run identity differs", err)
	}
	// Expectations are projected from the native author's pre-producer SQL
	// snapshots and CLI outcomes. The independent review binds these same r5
	// originals; neither the receiver nor any Plug result supplies an oracle.
	ledger := b03IndependentLedger{Head: inputs.NativeSourceCommit, Tree: inputs.NativeSourceTree, Reviewer: "8234fa9e-c8a3-461e-898a-79f6ef128ece", Projects: map[string]b03IndependentProject{}}
	for _, source := range inputs.Sources {
		var identity struct {
			Project         string
			StoreID         string             `json:"store_id"`
			Head            string             `json:"source_head"`
			RootSHA256      string             `json:"capture_set_json_sha256"`
			ProjectIdentity dumpformat.Project `json:"project_identity"`
		}
		if err := json.Unmarshal(inputs.files[source.Project+"/identity-summary.json"], &identity); err != nil || identity.Project != source.Project || identity.StoreID != source.StoreID || identity.Head != source.Head || identity.RootSHA256 != source.SHA256 || identity.ProjectIdentity.Prefix != source.Project {
			t.Fatal("original native identity summary differs", err)
		}
		var expected struct {
			Project     string
			Children    []string
			CategoryIDs map[string]string `json:"category_ids"`
			Milestones  map[string]struct {
				CommitID string `json:"commit_id"`
				Tables   map[string]json.RawMessage
			} `json:"source_milestones"`
		}
		b03DecodeJSON(t, inputs.files[source.Project+"/expected.json"], new(map[string]json.RawMessage))
		if err := json.Unmarshal(inputs.files[source.Project+"/expected.json"], &expected); err != nil || expected.Project != source.Project || len(expected.CategoryIDs) != 6 || len(expected.Children) != 2 {
			t.Fatal("native pre-producer expectation vector missing", err)
		}
		independent := b03IndependentProject{Milestones: map[string]b03IndependentMilestone{}, RawBusinessTables: b03SourceBusinessTables(t, inputs.files[source.Project+"/raw-current.json"])}
		independent.Catalog.ManifestProjects = []dumpformat.Project{identity.ProjectIdentity}
		independent.Catalog.StoredStatuses = independent.RawBusinessTables["custom_statuses"].Rows
		independent.Catalog.StoredConfig = independent.RawBusinessTables["config"].Rows
		independent.Catalog.YAML = map[string]string{b03Active: "active", "yaml_only": "frozen"}
		independent.Catalog.YAMLSHA256 = b03Digest(inputs.files[source.YAML])
		for name, milestone := range expected.Milestones {
			var tables map[string]dumpformat.NativeTable = map[string]dumpformat.NativeTable{}
			for _, family := range []string{"config", "custom_statuses", "issues"} {
				var table dumpformat.NativeTable
				b03DecodeJSON(t, milestone.Tables[family], &table)
				tables[family] = table
			}
			independent.Milestones[name] = b03IndependentMilestone{CommitID: milestone.CommitID, Config: tables["config"].Rows, CustomStatuses: tables["custom_statuses"].Rows, Issues: tables["issues"].Rows}
		}
		b03CheckNativeLifecycle(t, expected.Children[1], independent.Milestones)
		for _, status := range []string{"open", b03Active, b03WIP, b03Done, b03Frozen} {
			var control struct {
				Before, After map[string]json.RawMessage
				Command       int
				Accepted      bool `json:"expected_success"`
				ID            string
				Status        string `json:"source_status"`
			}
			b03DecodeJSON(t, inputs.files[source.Project+"/eligibility/"+status+".json"], &control)
			prefix := source.Project + "/commands/" + fmt.Sprintf("%03d", control.Command)
			var command struct {
				Args []string `json:"argv"`
				Exit int
			}
			if err := json.Unmarshal(inputs.files[prefix+".json"], &command); err != nil || control.ID != expected.CategoryIDs[status] || control.Status != status || !reflect.DeepEqual(command.Args, []string{"update", control.ID, "--claim", "--actor", "rich-fixture", "--json"}) || control.Accepted != (command.Exit == 0) {
				t.Fatal("independently recorded ordinary native claim command/outcome differs", err)
			}
			before, after := map[string]dumpformat.NativeTable{}, map[string]dumpformat.NativeTable{}
			// Machine-local metadata values are never decoded or compared.
			for family, raw := range control.Before {
				if family == "local_metadata" || family == "leases" || family == "repo_mtimes" {
					continue
				}
				var left, right dumpformat.NativeTable
				b03DecodeJSON(t, raw, &left)
				b03DecodeJSON(t, control.After[family], &right)
				before[family], after[family] = left, right
			}
			if len(before) != len(control.After)-3 {
				t.Fatal("native eligibility before/after family membership changed")
			}
			if !control.Accepted && !strings.Contains(string(inputs.files[prefix+".stderr"]), "issue not claimable: status "+status) {
				t.Fatal("literal native non-active refusal diagnostic changed")
			}
			independent.Eligibility = append(independent.Eligibility, struct {
				Status        string `json:"status"`
				ID            string `json:"id"`
				Exit          int    `json:"exit"`
				Accepted      bool   `json:"expected_success"`
				Conserved     bool   `json:"complete_raw_state_equal"`
				Before, After dumpformat.Row
			}{status, control.ID, command.Exit, control.Accepted, reflect.DeepEqual(before, after), b03IssueByID(t, before["issues"].Rows, control.ID), b03IssueByID(t, after["issues"].Rows, control.ID)})
		}
		readyCount := 0
		for name, raw := range inputs.files {
			if !strings.HasPrefix(name, source.Project+"/commands/") || !strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".clock.json") || strings.HasSuffix(name, ".sql.json") {
				continue
			}
			var command struct {
				Args []string `json:"argv"`
				Exit int
			}
			if err := json.Unmarshal(raw, &command); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(command.Args, []string{"ready", "--label", "category-carrier", "--limit", "1000", "--json"}) {
				continue
			}
			var rows []struct {
				ID              string          `json:"id"`
				Title           string          `json:"title"`
				Status          string          `json:"status"`
				Priority        json.RawMessage `json:"priority"`
				IssueType       string          `json:"issue_type"`
				Owner           string          `json:"owner"`
				CreatedAt       string          `json:"created_at"`
				CreatedBy       string          `json:"created_by"`
				UpdatedAt       string          `json:"updated_at"`
				Labels          []string        `json:"labels"`
				DependencyCount json.RawMessage `json:"dependency_count"`
				DependentCount  json.RawMessage `json:"dependent_count"`
				CommentCount    json.RawMessage `json:"comment_count"`
			}
			b03DecodeJSON(t, inputs.files[strings.TrimSuffix(name, ".json")+".stdout"], &rows)
			ids := []string{}
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			independent.Ready = append(independent.Ready, struct {
				IDs  []string `json:"ids"`
				Exit int      `json:"exit"`
			}{ids, command.Exit})
			readyCount++
		}
		if readyCount != 1 {
			t.Fatal("exact original native default ready observation missing or duplicated")
		}
		ledger.Projects[source.Project] = independent
	}
	return ledger
}

// Only these B03 families are decoded from source snapshots. Other families
// remain byte-bound opaque provenance, including all machine-local values.
func b03SourceBusinessTables(t *testing.T, raw []byte) map[string]dumpformat.NativeTable {
	t.Helper()
	var encoded map[string]json.RawMessage
	b03DecodeJSON(t, raw, &encoded)
	result := map[string]dumpformat.NativeTable{}
	for _, family := range []string{"config", "custom_statuses", "issues"} {
		var table dumpformat.NativeTable
		b03DecodeJSON(t, encoded[family], &table)
		result[family] = table
	}
	return result
}

func b03IssueByID(t *testing.T, rows []dumpformat.Row, id string) dumpformat.Row {
	t.Helper()
	var found dumpformat.Row
	for _, row := range rows {
		if row["id"] == id {
			if found != nil {
				t.Fatal("independent native issue identity is duplicated")
			}
			found = row
		}
	}
	if found == nil {
		t.Fatal("independently named native issue is absent", id)
	}
	return found
}

func b03CheckNativeLifecycle(t *testing.T, id string, milestones map[string]b03IndependentMilestone) {
	t.Helper()
	rows := map[string]dumpformat.Row{}
	points := map[string]b03AdmittedRow{}
	for _, name := range []string{"created", "started", "closed", "repeated-close", "reopened"} {
		point, present := milestones[name]
		if !present || point.CommitID == "" {
			t.Fatal("native lifecycle lacks an observed source commit", name)
		}
		rows[name] = b03IssueByID(t, point.Issues, id)
		points[name] = b03NativeExpectedRow(t, rows[name])
	}
	created, started, closed, repeated, reopened := points["created"], points["started"], points["closed"], points["repeated-close"], points["reopened"]
	if created.Status != "open" || created.Started != nil || created.Closed != nil || started.Status != "in_progress" || started.Started == nil || started.Started.Before(created.Created) || started.Closed != nil || closed.Status != "closed" || closed.Closed == nil || closed.Closed.Before(*started.Started) || repeated.Status != "closed" || !reflect.DeepEqual(closed.Closed, repeated.Closed) || reopened.Status != "open" || reopened.Closed != nil {
		t.Fatal("actual native create/start/close/reclose/reopen timestamp relations changed")
	}
	for _, point := range []b03AdmittedRow{started, closed, repeated, reopened} {
		if !point.Created.Equal(created.Created) || !reflect.DeepEqual(point.Started, started.Started) {
			t.Fatal("native lifecycle rewrote creation or actual start time")
		}
	}
	if rows["closed"]["close_reason"] != "public close 雪\nexact reason" || !reflect.DeepEqual(rows["closed"], rows["repeated-close"]) || milestones["closed"].CommitID != milestones["repeated-close"].CommitID || rows["reopened"]["close_reason"] != "" || rows["reopened"]["defer_until"] != nil {
		t.Fatal("native repeat-close reason/row/commit conservation or reopen clearing changed")
	}
}

func b03NativeBusinessFamily(family string) bool {
	return family == "issues" || family == "config" || family == "custom_statuses"
}

func b03CaptureNativeMilestones(t *testing.T, source *b03AdmittedSource) {
	t.Helper()
	wanted := map[string]bool{}
	for _, milestone := range source.History {
		wanted[milestone.CommitID] = true
	}
	source.snapshots = map[string]dumpformat.NativeHistoryImage{}
	// These are candidates until the existing reader verifies terminal EOF.
	// The callback copies only the three B03 business tables; it never retains
	// the borrowed rolling maps or decodes machine-local metadata values.
	if err := source.archive.VisitNativeDeltaImages(context.Background(), func(image dumpformat.NativeHistoryImage, _ []string) error {
		if !wanted[image.CommitID] {
			return nil
		}
		if _, duplicate := source.snapshots[image.CommitID]; duplicate {
			return fmt.Errorf("native milestone image repeated")
		}
		copy := dumpformat.NativeHistoryImage{Sequence: image.Sequence, CommitID: image.CommitID, Tables: map[string]dumpformat.NativeTable{}}
		for _, family := range []string{"issues", "config", "custom_statuses"} {
			table, present := image.Tables[family]
			if !present {
				return fmt.Errorf("B03 native historical family absent: %s", family)
			}
			encoded, err := json.Marshal(table)
			if err != nil {
				return err
			}
			var detached dumpformat.NativeTable
			b03DecodeJSON(t, encoded, &detached)
			copy.Tables[family] = detached
		}
		source.snapshots[image.CommitID] = copy
		return nil
	}); err != nil {
		t.Fatal("complete original native milestone stream failed authentication", err)
	}
	if len(wanted) != len(source.snapshots) {
		t.Fatal("independently named native milestone is absent from the authenticated stream")
	}
	for _, milestone := range source.History {
		image := source.snapshots[milestone.CommitID]
		if !reflect.DeepEqual(b03SQLCatalog(t, image.Tables["custom_statuses"].Rows), milestone.SQLCatalog) || !reflect.DeepEqual(b03NativeRows(t, image.Tables["config"].Rows), b03NativeRows(t, milestone.Config)) || len(image.Tables["issues"].Rows) != len(milestone.Rows) {
			t.Fatal("authenticated native milestone differs from independent pre-producer SQL expectations")
		}
		for _, want := range milestone.Rows {
			got := b03NativeExpectedRow(t, b03IssueByID(t, image.Tables["issues"].Rows, want.ID))
			if !reflect.DeepEqual(got, want) {
				t.Fatal("authenticated native milestone issue/timestamps differ from independent observations")
			}
		}
	}
}

func b03SQLCatalog(t *testing.T, rows []dumpformat.Row) map[string]string {
	t.Helper()
	result := map[string]string{}
	for _, row := range rows {
		name, ok := row["name"].(string)
		category, valid := row["category"].(string)
		if !ok || !valid || name == "" || result[name] != "" {
			t.Fatal("independent SQL catalog has an invalid or duplicate name")
		}
		result[name] = category
	}
	return result
}

// Native normalized rows take precedence over the legacy status.custom cell.
// Filesystem YAML is intentionally absent here: a current file is not evidence
// of its contents at a retained SQL commit.
func b03EffectiveStoredCatalog(t *testing.T, normalized map[string]string, config []dumpformat.Row) map[string]string {
	t.Helper()
	result := map[string]string{"open": "active", "in_progress": "wip", "blocked": "wip", "closed": "done", "deferred": "frozen", "pinned": "frozen", "hooked": "wip"}
	custom := normalized
	if len(custom) == 0 {
		custom = map[string]string{}
		for _, row := range config {
			if row["key"] != "status.custom" {
				continue
			}
			value, ok := row["value"].(string)
			if !ok {
				t.Fatal("native fallback status config is not a string")
			}
			for _, field := range strings.Split(value, ",") {
				if strings.TrimSpace(field) == "" {
					continue
				}
				parts := strings.Split(strings.TrimSpace(field), ":")
				if len(parts) != 2 || custom[parts[0]] != "" {
					t.Fatal("recorded catalog fallback is malformed/duplicate")
				}
				custom[parts[0]] = parts[1]
			}
		}
	}
	for name, category := range custom {
		if _, exists := result[name]; exists {
			t.Fatal("recorded native catalog collides with a builtin")
		}
		switch category {
		case "active", "wip", "done", "frozen":
		default:
			t.Fatal("independently observed native catalog category is unknown")
		}
		result[name] = category
	}
	return result
}

func b03NativeExpectedRow(t *testing.T, row dumpformat.Row) b03AdmittedRow {
	t.Helper()
	id, idOK := row["id"].(string)
	status, statusOK := row["status"].(string)
	instant := func(name string, optional bool) *time.Time {
		value, present := row[name]
		if !present {
			t.Fatalf("independent native row omits %s", name)
		}
		if value == nil && optional {
			return nil
		}
		text, ok := value.(string)
		if !ok {
			t.Fatalf("independent native %s is not an observed instant", name)
		}
		parsed, err := time.Parse(time.RFC3339Nano, text)
		if err != nil || parsed.IsZero() {
			t.Fatal("invalid independent native instant", err)
		}
		return &parsed
	}
	if !idOK || !statusOK || id == "" || status == "" {
		t.Fatal("independent issue identity/status missing")
	}
	return b03AdmittedRow{ID: id, Status: status, Created: *instant("created_at", false), Started: instant("started_at", true), Closed: instant("closed_at", true)}
}

func b03AdoptIndependentExpectations(t *testing.T, source *b03AdmittedSource, independent b03IndependentProject) {
	t.Helper()
	if len(independent.Catalog.ManifestProjects) != 1 || independent.Catalog.ManifestProjects[0].Prefix != source.Project || len(independent.Milestones) == 0 {
		t.Fatal("independent project catalog/history expectation missing")
	}
	source.Catalog = map[string]string{}
	for _, entry := range independent.Catalog.ManifestProjects[0].Statuses {
		if source.Catalog[entry.Name] != "" {
			t.Fatal("independent manifest repeats a status")
		}
		source.Catalog[entry.Name] = entry.Category
	}
	file := source.yamlBytes
	if len(file) == 0 || b03Digest(file) != independent.Catalog.YAMLSHA256 {
		t.Fatal("actual physical native YAML changed")
	}
	// This exact producer file has one literal plain scalar. Its independently
	// verified full hash and declarations are fixture provenance, not a new
	// general-purpose YAML parser or a historical filesystem reconstruction.
	parts := strings.Split(string(file), "\nstatus:\n  custom: ")
	if len(parts) != 2 {
		t.Fatal("physical fixture status scalar is absent or duplicated")
	}
	value, _, found := strings.Cut(parts[1], "\n")
	if !found {
		t.Fatal("physical fixture status scalar is unterminated")
	}
	yamlCatalog := map[string]string{}
	for _, declaration := range strings.Split(value, ",") {
		parts := strings.Split(strings.TrimSpace(declaration), ":")
		if len(parts) != 2 || yamlCatalog[parts[0]] != "" {
			t.Fatal("actual physical status declarations are invalid/duplicate")
		}
		yamlCatalog[parts[0]] = parts[1]
	}
	if !reflect.DeepEqual(yamlCatalog, independent.Catalog.YAML) {
		t.Fatal("physical YAML differs from independent pre-export provenance")
	}
	currentCatalog := b03EffectiveStoredCatalog(t, b03SQLCatalog(t, independent.Catalog.StoredStatuses), independent.Catalog.StoredConfig)
	for name, category := range yamlCatalog {
		if old, exists := currentCatalog[name]; exists && old != category {
			t.Fatal("fixture has an unqualified conflicting catalog declaration")
		}
		currentCatalog[name] = category
	}
	if !reflect.DeepEqual(currentCatalog, source.Catalog) {
		t.Fatal("exact current catalog union differs from independent SQL/config/YAML expectations")
	}
	for _, row := range independent.RawBusinessTables["issues"].Rows {
		source.Current = append(source.Current, b03NativeExpectedRow(t, row))
	}
	if len(source.Current) == 0 {
		t.Fatal("independent current native issues are empty")
	}
	for _, name := range []string{"catalog-initial", "catalog-changed", "created", "started", "closed", "repeated-close", "reopened"} {
		point, exists := independent.Milestones[name]
		if !exists {
			if name == "catalog-changed" && source.Project == "review-project" {
				continue
			}
			t.Fatal("independent native milestone missing", name)
		}
		sql := b03SQLCatalog(t, point.CustomStatuses)
		milestone := b03AdmittedMilestone{Name: name, CommitID: point.CommitID, SQLCatalog: sql, Catalog: b03EffectiveStoredCatalog(t, sql, point.Config), Config: point.Config}
		for _, row := range point.Issues {
			milestone.Rows = append(milestone.Rows, b03NativeExpectedRow(t, row))
		}
		source.History = append(source.History, milestone)
	}
	source.Eligibility, source.CategoryIDs = map[string]bool{}, map[string]bool{}
	for _, control := range independent.Eligibility {
		if _, duplicate := source.Eligibility[control.Status]; duplicate || control.ID == "" || control.Accepted != (control.Exit == 0) || !control.Accepted && (!control.Conserved || !reflect.DeepEqual(control.Before, control.After)) {
			t.Fatal("independent native sequential eligibility outcome/conservation is invalid")
		}
		before, after := b03NativeExpectedRow(t, control.Before), b03NativeExpectedRow(t, control.After)
		if before.Status != control.Status || !after.Created.Equal(before.Created) || control.Accepted && (after.Status != "in_progress" || after.Started == nil || after.Started.Before(before.Created) || control.After["assignee"] != "rich-fixture") {
			t.Fatal("actual native category eligibility did not reach its literal status/timestamp outcome")
		}
		source.Eligibility[control.Status] = control.Accepted
		source.CategoryIDs[control.ID] = true
	}
	for _, row := range source.Current {
		if row.Status == "project_local" {
			source.CategoryIDs[row.ID] = true
		}
	}
	if len(independent.Ready) != 1 || independent.Ready[0].Exit != 0 {
		t.Fatal("independent category-carrier ready observation missing")
	}
	source.ReadyIDs = append([]string{}, independent.Ready[0].IDs...)
	sort.Strings(source.ReadyIDs)
}

func b03OrderNativeMilestones(t *testing.T, source *b03AdmittedSource) {
	t.Helper()
	sequence := map[string]uint64{}
	for _, image := range source.snapshots {
		sequence[image.CommitID] = image.Sequence
	}
	for _, point := range source.History {
		if _, present := sequence[point.CommitID]; !present {
			t.Fatal("independent milestone is not in the actual native source graph", point.Name)
		}
	}
	sort.SliceStable(source.History, func(i, j int) bool {
		return sequence[source.History[i].CommitID] < sequence[source.History[j].CommitID]
	})
}

// The native capture-set admission does not weaken the portable contract.
// Both the preserved r4 TARs and the r5 author's separate legacy controls are
// supplied byte-for-byte. Their native-only archive dispositions must refuse
// even though the three affected families contain zero rows.
func TestB03LegacyPortableNativeOnlyFamiliesRefuse(t *testing.T) {
	for _, name := range []string{"b03-rich-catalog.inputs.json", "b03-rich-r5-catalog.inputs.json"} {
		t.Run(name, func(t *testing.T) {
			var inputs b03AdmittedInputs
			raw, err := os.ReadFile(filepath.Join("testdata", name))
			if err != nil {
				t.Fatal(err)
			}
			b03DecodeJSON(t, raw, &inputs)
			files := b03ReadNativeInputBundle(t, inputs)
			for _, evidence := range inputs.Evidence {
				if !b03SafeInputPath(evidence.Path) || b03Digest(files[evidence.Path]) != evidence.SHA256 {
					t.Fatal("immutable portable refusal control changed", evidence.Path)
				}
			}
			for _, source := range inputs.Sources {
				data := files[source.Archive]
				if len(data) == 0 {
					t.Fatal("original legacy control is missing")
				}
				before := b03Digest(data)
				var manifest dumpformat.Manifest
				seen := false
				reader := tar.NewReader(bytes.NewReader(data))
				for {
					header, err := reader.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if header.Name != "manifest.json" {
						continue
					}
					if seen {
						t.Fatal("original legacy manifest is duplicated")
					}
					seen = true
					body, err := io.ReadAll(reader)
					if err != nil {
						t.Fatal(err)
					}
					b03DecodeJSON(t, body, &manifest)
				}
				if !seen || manifest.Format != dumpformat.Format || len(manifest.Projects) != 1 || manifest.Projects[0].Prefix != source.Project {
					t.Fatal("original portable source identity changed")
				}
				families := map[string]bool{}
				for _, disposition := range manifest.Sources {
					if disposition.Family != "routes" && disposition.Family != "interactions" && disposition.Family != "federation_peers" {
						continue
					}
					if families[disposition.Family] || disposition.Action != "archive" || disposition.Rows != 0 {
						t.Fatal("original native-only portable refusal disposition changed")
					}
					families[disposition.Family] = true
				}
				if len(families) != 3 {
					t.Fatal("original native-only family controls are incomplete")
				}
				archive, err := dumpformat.Read(bytes.NewReader(data))
				if archive != nil {
					archive.ReleaseNativeDecoded()
				}
				if err == nil || err.Error() != "source disposition/action/count mismatch" || b03Digest(data) != before {
					t.Fatal("unchanged legacy native-only input did not retain its exact portable refusal", err)
				}
				b03Write(t, "b03-portable-refusal-"+name+"-"+source.Project, map[string]any{"input_sha256": before, "project": source.Project, "native_only_families": families, "diagnostic": err.Error(), "input_bytes_unchanged": true, "target_opened": false})
			}
		})
	}
}
