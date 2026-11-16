package cardb20

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontologyops"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/inprocess"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// These inputs are supplied by the independent public native fixture owner.
// They name original capture bytes and native SQL/CLI expectations. No archive,
// source sequence, catalog or historical row is manufactured by this consumer.
type b03AdmittedRow struct {
	ID, Status string
	Created    time.Time
	Started    *time.Time
	Closed     *time.Time
	Ready      bool
}

type b03AdmittedMilestone struct {
	Name, CommitID string
	Catalog        map[string]string
	SQLCatalog     map[string]string
	Config         []dumpformat.Row
	Rows           []b03AdmittedRow
}

type b03AdmittedSource struct {
	Archive, SHA256, StoreID, Project string
	Directory, Head                   string
	VerifiedCommits                   uint64
	YAML                              string
	Catalog                           map[string]string      `json:"-"`
	Current                           []b03AdmittedRow       `json:"-"`
	History                           []b03AdmittedMilestone `json:"-"`
	Eligibility                       map[string]bool        `json:"-"`
	CategoryIDs                       map[string]bool        `json:"-"`
	ReadyIDs                          []string               `json:"-"`
	native                            dumpformat.NativeHistory
	original                          map[string]dumpformat.Row
	snapshots                         map[string]dumpformat.NativeHistoryImage
	archive                           *dumpformat.Archive
	directory                         string
	yamlBytes                         []byte
}

type b03AdmittedInputs struct {
	Format                                                string
	Sources                                               []b03AdmittedSource
	Oracle                                                string
	Bundle, BundleSHA256                                  string
	NativeSourceCommit, NativeSourceTree                  string
	FixtureCommit, FixtureTree, OriginalRun               string
	CaptureELFSHA256, BuildRequestSHA256, RawOutputSHA256 string
	Evidence                                              []b03AdmittedEvidence
	files                                                 map[string][]byte
}

type b03AdmittedEvidence struct {
	Path, SHA256 string
}

func b03Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func b03AdmittedArchive(t *testing.T) b03AdmittedInputs {
	t.Helper()
	var inputs b03AdmittedInputs
	raw, err := os.ReadFile("testdata/b03-rich-r5-catalog.inputs.json")
	if err != nil {
		t.Fatal("genuine shared native fixture inputs are required", err)
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&inputs); err != nil || inputs.Format != "b03-public-native-catalog-inputs/2" || len(inputs.Sources) != 2 {
		t.Fatalf("invalid independently bound native inputs: %v", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		t.Fatal("native input binding contains trailing JSON", err)
	}
	for _, id := range []string{inputs.NativeSourceCommit, inputs.NativeSourceTree, inputs.FixtureCommit, inputs.FixtureTree} {
		if value, err := hex.DecodeString(id); err != nil || len(value) != 20 {
			t.Fatal("actual original native/fixture source identities are required")
		}
	}
	if inputs.OriginalRun == "" || len(inputs.Evidence) == 0 {
		t.Fatal("original native request/output evidence must accompany the captures")
	}
	for _, digest := range []string{inputs.CaptureELFSHA256, inputs.BuildRequestSHA256, inputs.RawOutputSHA256} {
		if value, err := hex.DecodeString(digest); err != nil || len(value) != 32 {
			t.Fatal("actual native ELF, request and raw-output hashes are required")
		}
	}
	seenEvidence := map[string]bool{}
	inputs.files = b03ReadNativeInputBundle(t, inputs)
	for _, evidence := range inputs.Evidence {
		if !b03SafeInputPath(evidence.Path) || seenEvidence[evidence.Path] {
			t.Fatal("original native evidence path is empty, duplicate or escapes testdata")
		}
		seenEvidence[evidence.Path] = true
		data, present := inputs.files[evidence.Path]
		if !present || b03Digest(data) != evidence.SHA256 {
			t.Fatal("immutable original native evidence binding changed", evidence.Path)
		}
	}
	oracle := b03IndependentNativeInputs(t, inputs)
	projects := map[string]bool{}
	root := t.TempDir()
	for i := range inputs.Sources {
		source := &inputs.Sources[i]
		if !b03SafeInputPath(source.Archive) || source.Directory != source.Project+"/capture-set" || source.Project == "" || projects[source.Project] || source.StoreID == "" {
			t.Fatal("two distinct original projects and populated independent history expectations required")
		}
		projects[source.Project] = true
		source.yamlBytes = inputs.files[source.YAML]
		b03AdoptIndependentExpectations(t, source, oracle.Projects[source.Project])
		for name, category := range b03Categories {
			if source.Catalog[name] != string(category) {
				t.Fatal("independent native catalog lost a required literal name/category")
			}
		}
		for _, status := range []string{"open", b03Active, b03WIP, b03Done, b03Frozen} {
			accepted, observed := source.Eligibility[status]
			if !observed || accepted != (status == "open" || status == b03Active) {
				t.Fatal("independent ordinary native claim outcomes must reach all five literal category controls")
			}
		}
		// Materialize only the complete authenticated set's declared files.
		// Every byte and each original single-project identity is retained.
		source.directory = filepath.Join(root, source.Directory)
		for _, name := range []string{"capture-set.json", "head.tar", "parts/00000000000000000001.part"} {
			data, present := inputs.files[source.Directory+"/"+name]
			if !present {
				t.Fatal("complete original capture-set member missing", name)
			}
			path := filepath.Join(source.directory, name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if b03Digest(inputs.files[source.Directory+"/capture-set.json"]) != source.SHA256 {
			t.Fatal("authenticated source root commitment changed")
		}
		a, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), source.directory, dumpformat.NativeCaptureSetOptions{})
		if err != nil {
			t.Fatal("actual complete native capture-set reader", err)
		}
		source.archive = a
		t.Cleanup(a.ReleaseNativeDecoded)
		if !a.HasNativeDelta() || a.SHA256() != source.SHA256 || a.Manifest().StoreID != source.StoreID || len(a.Manifest().Projects) != 1 || a.Manifest().Projects[0].Prefix != source.Project || a.Manifest().StatusCategoryContract != dumpformat.StatusCategoryContract {
			t.Fatalf("original source/project/status contract changed: %s %v", source.Project, err)
		}
		actual := map[string]string{}
		for _, status := range a.Manifest().Projects[0].Statuses {
			actual[status.Name] = status.Category
		}
		if !reflect.DeepEqual(actual, source.Catalog) {
			t.Fatal("native manifest differs from independent pre-export catalog expectation")
		}
		histories := a.NativeSourceHistories()
		if len(histories) != 1 || histories[0].StoreID != source.StoreID || histories[0].Head != source.Head || !reflect.DeepEqual(histories[0].Projects, []string{source.Project}) || len(histories[0].Images) != 1 {
			t.Fatal("one original native project history with its HEAD checkpoint is required")
		}
		if !histories[0].Complete || len(histories[0].Gaps) != 0 || histories[0].VerifiedImageCount() != source.VerifiedCommits || uint64(len(histories[0].Commits)) != source.VerifiedCommits {
			t.Fatal("original complete native history must retain every actual source commit without gaps")
		}
		source.native = histories[0]
		// Machine-local unversioned rows are outside B03's business oracle.
		source.native.Current, source.native.Images = nil, nil
		source.original = map[string]dumpformat.Row{}
		for _, row := range a.Records("bead") {
			source.original[row.ID] = row.Source.Fields
		}
		if len(source.original) != len(oracle.Projects[source.Project].RawBusinessTables["issues"].Rows) {
			t.Fatal("authenticated current issue census differs from independent native SQL")
		}
		for _, row := range oracle.Projects[source.Project].RawBusinessTables["issues"].Rows {
			id, ok := row["id"].(string)
			// These two native implementation fields have the existing
			// manifest/rebuild disposition. Their raw historical values remain
			// in native evidence, not in a fabricated portable source field.
			portable := dumpformat.Row{}
			for name, value := range row {
				if name != "row_lock" && name != "is_blocked" {
					portable[name] = value
				}
			}
			if !ok || !reflect.DeepEqual(b03NativeRows(t, []dumpformat.Row{portable}), b03NativeRows(t, []dumpformat.Row{source.original[id]})) {
				t.Fatal("authenticated current issue fields differ from independent pre-producer SQL")
			}
		}
		b03CaptureNativeMilestones(t, source)
		b03OrderNativeMilestones(t, source)
	}
	if inputs.Sources[0].Catalog["project_local"] != "active" || inputs.Sources[1].Catalog["project_local"] != "done" {
		t.Fatal("same literal catalog key must have independently recorded different project categories")
	}
	return inputs
}

// Request construction remains in the single existing receiver executable.
// This is explicit disposable host policy, not a parallel source planner.
func b03AdmittedPlan(t *testing.T, ctx context.Context, engine string, source b03AdmittedSource) driver.SourcePlanePlanV1 {
	t.Helper()
	a := source.archive
	projects, sources := []string{}, []dumpformat.SourceIdentity{}
	for _, project := range a.Manifest().Projects {
		projects = append(projects, project.Prefix)
	}
	for _, source := range a.NativeSourceHistories() {
		if len(source.Projects) != 1 {
			t.Fatal("native source must retain its one original project")
		}
		sources = append(sources, dumpformat.SourceIdentity{StoreID: source.StoreID, Project: source.Projects[0]})
	}
	lifetime := driver.CheckpointLifetimeProcess
	if engine != "memory" {
		lifetime = driver.CheckpointLifetimeRestart
	}
	principal, _ := json.Marshal(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	fields := []driver.SourcePlaneStringPairV1{{From: "id", To: "id"}, {From: "title", To: "title"}, {From: "issue_type", To: "type"}, {From: "status", To: "status"}, {From: "metadata", To: "metadata"}, {From: "created_at", To: "created_at"}, {From: "priority", To: "priority"}, {From: "description", To: "description"}}
	policy := map[string]any{
		"format": "plug-native-receiving-policy/1", "archive_sha256": a.SHA256(), "source_store_id": a.Manifest().StoreID, "projects": projects, "sources": sources,
		"admission": b03AdmittedAdmission(), "idempotency_key": "b03-public-native-catalog-000001",
		"principal":     driver.SourcePlanePrincipalV1{Key: "owner", Source: "B03 explicit public host policy", Authority: "trusted-explicit/1", PrincipalJSON: principal},
		"provider":      driver.SourcePlaneProviderV1{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: lifetime, OriginAdapter: "beads-workflow-origins/1"},
		"entity_policy": "durable-bead-identity", "tuple_policy": "copy-observed-exact", "status_policy": "source-name-identity", "target_id_policy": "source-id", "route_policy": "active-first", "field_mappings": fields, "unmapped_fields": "opaque-metadata", "other_records": "inert-origin", "sidecars": "retain-captured-origins", "dependency_policy": "exact-typed",
	}
	root := t.TempDir()
	policyPath := filepath.Join(root, "public-policy.json")
	policyBytes, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policyPath, policyBytes, 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("B03_NATIVE_RECEIVE_BINARY"))
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal("existing source-built receiver executable required", err)
	}
	cmd := exec.CommandContext(ctx, binary, "prepare-request", "--driver", engine, "--capture-set", source.directory, "--policy", policyPath, "--expected-binary-sha256", b03Digest(binaryBytes))
	cmd.Env = append(os.Environ(), "PATH=/tmp/plug-swarm/bin/worker-safe:/usr/local/bin:"+os.Getenv("PATH"))
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	b03Write(t, "b03-admitted-prepare-"+engine+"-"+source.Project, map[string]any{"argv": cmd.Args, "binary_sha256": b03Digest(binaryBytes), "policy": policy, "stdout": stdout.String(), "stderr": stderr.String(), "error": fmt.Sprint(err)})
	if err != nil {
		t.Fatalf("single receiver's explicit policy construction refused: %v %s", err, stderr.Bytes())
	}
	var request struct {
		ArchiveSHA256 string                   `json:"archive_sha256"`
		SourcePlan    driver.SourcePlanePlanV1 `json:"source_plan"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &request); err != nil || request.ArchiveSHA256 != a.SHA256() || request.SourcePlan.Target.StoreID != "" || request.SourcePlan.Target.Incarnation != "" || !request.SourcePlan.Target.RequireEmpty || request.SourcePlan.Target.Lifetime != lifetime {
		t.Fatalf("existing request constructor lost exact source or target boundary: %v", err)
	}
	return request.SourcePlan
}

func b03AdmittedCatalog(t *testing.T, f *b03Public, project string, want map[string]string) ontologyops.Result {
	t.Helper()
	var result ontologyops.Result
	f.call(t, "ontology.get", map[string]any{"project": project}, false, "", &result)
	if result.Drifted || result.Document == nil || result.Revision.IsZero() {
		t.Fatal("admitted installed source is absent or drifted")
	}
	if len(result.Document.Statuses) != len(want) {
		t.Fatal("admitted catalog added aliases or lost native status keys")
	}
	for name, category := range want {
		status, exists := result.Document.Statuses[name]
		if !exists || string(status.Category) != category {
			t.Fatalf("project %s native name %s category differs: %+v want %s", project, name, status, category)
		}
	}
	return result
}

func b03AdmittedRows(t *testing.T, f *b03Public, project string, rows []b03AdmittedRow, at *model.Revision, native ...map[string]dumpformat.Row) {
	t.Helper()
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, want := range rows {
		if want.ID == "" || want.Status == "" || want.Created.IsZero() || want.Started != nil && want.Started.IsZero() || want.Closed != nil && want.Closed.IsZero() {
			t.Fatal("independent native row must retain exact identities and nonzero present timestamps")
		}
		params := map[string]any{"id": model.Ref{ID: want.ID}, "_meta": b03ProjectMeta(project)}
		if at != nil {
			params["asOf"] = map[string]any{"incarnation": meta.Incarnation, "revision": *at}
		}
		var got struct {
			Bead struct {
				ID, Project, Status string
				Metadata            map[string]json.RawMessage
			}
		}
		f.call(t, "bead.get", params, false, "", &got)
		records, err := r.Get(f.ctx, []model.Ref{{ID: want.ID}})
		if err != nil || len(records) != 1 {
			t.Fatalf("admitted native row missing: %s %v", want.ID, err)
		}
		doc, err := pipeline.DecodeDocument(records[0].Body)
		if err != nil || got.Bead.ID != want.ID || got.Bead.Project != project || got.Bead.Status != want.Status || doc.Project != project || doc.Fields["status"] != want.Status || doc.CreatedAt == nil || !doc.CreatedAt.Equal(want.Created) {
			t.Fatalf("admitted public/committed native row differs from independent expectation: %s at=%v %+v %v", want.ID, at, doc, err)
		}
		if at != nil {
			if !reflect.DeepEqual(doc.StartedAt, want.Started) || !reflect.DeepEqual(doc.ClosedAt, want.Closed) {
				t.Fatal("new ordinary Plug lifecycle timestamps changed at their actual committed revision")
			}
			continue
		}
		// The frozen source mapping admits created_at as a core timestamp.
		// Native started_at/closed_at remain exact source fields, rather than
		// fabricating historical Plug lifecycle operations at import time.
		if doc.StartedAt != nil || doc.ClosedAt != nil {
			t.Fatal("import fabricated a core start or close timestamp outside the admitted field mapping")
		}
		var committedFields, publicFields map[string]json.RawMessage
		if err := json.Unmarshal(doc.Metadata["beads.source_fields"], &committedFields); err != nil {
			t.Fatal("committed native source fields missing", err)
		}
		if err := json.Unmarshal(got.Bead.Metadata["beads.source_fields"], &publicFields); err != nil || !reflect.DeepEqual(committedFields, publicFields) {
			t.Fatal("public native source fields differ from the committed source provenance", err)
		}
		if len(native) != 1 || native[0][want.ID] == nil {
			t.Fatal("exact original native source fields required for imported current readback")
		}
		original, err := json.Marshal(native[0][want.ID])
		if err != nil || !b03SameJSON(t, original, doc.Metadata["beads.source_fields"]) {
			t.Fatal("literal native fields, including timestamp absence/null/value, changed in source metadata", err)
		}
	}
}

func b03AdmittedHistory(t *testing.T, f *b03Public, source b03AdmittedSource, dump b03PublicDump) {
	t.Helper()
	record := f.row(t, model.Ref{ID: driver.OfflineHistoryEvidenceID})
	var evidence driver.HistoryImportEvidence
	if err := json.Unmarshal(record.Body, &evidence); err != nil || evidence.Mode != driver.RestoreHistoryModeFull {
		t.Fatalf("actual admitted native source history evidence required: %v", err)
	}
	var public ontologyops.HistoryResult
	f.call(t, "ontology.history", map[string]any{"project": source.Project, "limit": 1000}, false, "", &public)
	if len(public.Revisions) == 0 || len(public.Revisions) == 1000 {
		t.Fatal("populated complete public source history page required")
	}
	for _, change := range public.Revisions {
		for _, c := range change.Changes {
			if c.Ref.ID != converterroundtrip.ImportedOntologySourceID {
				t.Fatal("public ontology history exposed an ordinary imported row")
			}
		}
	}
	current := b03AdmittedCatalog(t, f, source.Project, source.Catalog)
	var unchanged ontologyops.DiffResult
	f.call(t, "ontology.diff", map[string]any{"project": source.Project, "fromRevision": current.Revision, "toRevision": current.Revision}, false, "", &unchanged)
	if len(unchanged.Patch) != 0 {
		t.Fatal("current/current installed source diff must be empty")
	}
	var previous map[string]driver.Record
	var previousAt model.Revision
	changedStableRef := false
	for i := range source.History {
		milestone := &source.History[i]
		if milestone.CommitID == "" || len(milestone.Catalog) < 7 {
			t.Fatal("actual native milestone must name its source commit and full catalog")
		}
		var at model.Revision
		for _, mapping := range evidence.Mapping {
			if mapping.Project != source.Project || mapping.SourceStoreID != source.StoreID {
				continue
			}
			for _, commit := range mapping.CommitIDs {
				if commit == milestone.CommitID {
					if !at.IsZero() {
						t.Fatal("native milestone maps more than once")
					}
					at = mapping.Revision
				}
			}
		}
		if at.IsZero() {
			t.Fatalf("actual source commit %s has no admitted project history mapping", milestone.CommitID)
		}
		meta, err := f.store.Meta(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Native history is raw source history. The frozen importer forbids
		// grafting current business/ontology documents into those commits.
		actual := b03PublicNativeMilestone(t, f, source, *milestone, at, dump)
		for table, row := range actual {
			old, present := previous[table]
			if !present || old.Ref.ID != row.Ref.ID {
				continue
			}
			var diff struct {
				Patch        []json.RawMessage
				ChangedPaths []string
			}
			f.call(t, "history.diff", map[string]any{"ref": row.Ref, "fromRevision": previousAt, "toRevision": at, "_meta": b03ProjectMeta(source.Project)}, false, "", &diff)
			changedStableRef = b03VerifyMetaDiff(t, old.Body, row.Body, diff.Patch, diff.ChangedPaths) || changedStableRef
		}
		previous, previousAt = actual, at
		before := meta
		for _, row := range milestone.Rows {
			f.call(t, "bead.get", map[string]any{"id": model.Ref{ID: row.ID}, "asOf": map[string]any{"incarnation": meta.Incarnation, "revision": at}, "_meta": b03ProjectMeta(source.Project)}, false, "not_found", nil)
		}
		f.call(t, "ontology.diff", map[string]any{"project": source.Project, "fromRevision": at, "toRevision": current.Revision}, false, "not_found", nil)
		after, err := f.store.Meta(f.ctx)
		if err != nil || before != after {
			t.Fatal("refused historical portable projection changed admitted graph metadata", err)
		}
	}
	if !changedStableRef {
		t.Fatal("native history proof never reached an actually changed stable table-meta Ref")
	}
}

// Refusal conservation is deliberately scoped to graph metadata, the installed
// source, the named affected row, the complete journal suffix and this request's
// receipt. It is not advertised as an arbitrary SQL-table census.
func b03AdmittedRefuse(t *testing.T, f *b03Public, project, method string, params map[string]any) {
	t.Helper()
	before, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	refs := []model.Ref{f.source}
	if id, ok := params["id"].(model.Ref); ok {
		refs = append(refs, id)
	}
	r := f.reader(t)
	rows, err := r.Get(f.ctx, refs)
	if closeErr := r.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	meta := b03ProjectMeta(project)
	key := b03TargetKey(f.sequence + 1)
	meta["idempotencyKey"] = key
	params["_meta"] = meta
	f.call(t, method, params, true, "invalid_params", nil)
	after, err := f.store.Meta(f.ctx)
	if err != nil || before != after {
		t.Fatal("refused admitted catalog request changed graph metadata", err)
	}
	r = f.reader(t)
	defer r.Close()
	afterRows, err := r.Get(f.ctx, refs)
	if err != nil || !reflect.DeepEqual(rows, afterRows) {
		t.Fatal("refused admitted catalog request changed source or affected row", err)
	}
	entries, err := r.Changesets(f.ctx, &driver.Cursor{Incarnation: before.Incarnation, Revision: before.Head, Method: "changesets"}, 1000)
	if err != nil || len(entries) != 0 {
		t.Fatal("refused admitted catalog request appended graph history", err)
	}
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := r.IdempotencyByKey(f.ctx, principal, "b03-catalog-targets", method, key)
	if err != nil || receipt != nil {
		t.Fatal("refused admitted catalog request published a success receipt", err)
	}
}

func b03AdmittedLifecycle(t *testing.T, f *b03Public, source b03AdmittedSource) []b03TargetPoint {
	t.Helper()
	project := source.Project
	var points []b03TargetPoint
	for _, initial := range []string{"open", b03Active, b03WIP, b03Frozen} {
		commit := b03TargetWrite(t, f, project, "bead.create", map[string]any{"type": "task", "targetStatus": initial, "fields": map[string]any{"title": "admitted B03 " + initial}})
		if len(commit.Results) != 1 {
			t.Fatal("admitted catalog create must return one actual identity")
		}
		id := commit.Results[0].Ref
		active := source.Eligibility[initial]
		created := b03TargetObserve(t, f, project, id, "admitted-create-"+initial, initial, active)
		if created.Started != nil || created.Closed != nil {
			t.Fatal("ordinary exact creation fabricated a start or close instant")
		}
		points = append(points, created)
		var started *time.Time
		if active {
			// Ordinary sequential eligibility only. There is no contender,
			// authorization overlap or claim-writer implementation in this test.
			b03TargetWrite(t, f, project, "bead.claim", map[string]any{"id": id})
			claimed := b03TargetObserve(t, f, project, id, "admitted-claimed", "in_progress", false)
			if claimed.Started == nil || claimed.Started.IsZero() || claimed.Started.Before(created.Created) || claimed.Closed != nil || !claimed.Created.Equal(created.Created) {
				t.Fatal("actual accepted claim did not record its real start instant")
			}
			started = claimed.Started
			points = append(points, claimed)
		} else {
			b03AdmittedRefuse(t, f, project, "bead.claim", map[string]any{"id": id})
		}
		b03AdmittedRefuse(t, f, project, "bead.close", map[string]any{"id": id, "targetStatus": "not_in_native_catalog"})
		closeParams := map[string]any{"id": id, "targetStatus": b03Done, "reason": "B03 exact configured done"}
		closedCommit := b03TargetWrite(t, f, project, "bead.close", closeParams)
		closed := b03TargetObserve(t, f, project, id, "admitted-first-close", b03Done, false)
		b03AdmittedComments(t, f, project, id, []string{"B03 exact configured done"})
		var replayed model.Commit
		f.call(t, "bead.close", closeParams, true, "", &replayed)
		if !reflect.DeepEqual(closedCommit, replayed) {
			t.Fatal("admitted exact close replay lost its original committed result")
		}
		b03TargetWrite(t, f, project, "bead.close", map[string]any{"id": id, "targetStatus": b03Done, "reason": "B03 repeat same configured done"})
		repeated := b03TargetObserve(t, f, project, id, "admitted-repeat-close", b03Done, false)
		b03AdmittedComments(t, f, project, id, []string{"B03 exact configured done", "B03 repeat same configured done"})
		b03AdmittedRefuse(t, f, project, "bead.claim", map[string]any{"id": id})
		b03AdmittedRefuse(t, f, project, "bead.reopen", map[string]any{"id": id, "targetStatus": "not_in_native_catalog"})
		b03AdmittedRefuse(t, f, project, "bead.reopen", map[string]any{"id": id, "targetStatus": b03Frozen})
		b03TargetWrite(t, f, project, "bead.reopen", map[string]any{"id": id, "targetStatus": b03Active, "reason": "B03 exact active reopen"})
		reopened := b03TargetObserve(t, f, project, id, "admitted-reopen", b03Active, true)
		b03AdmittedComments(t, f, project, id, []string{"B03 exact configured done", "B03 repeat same configured done", "B03 exact active reopen"})
		if closed.Closed == nil || closed.Closed.IsZero() || closed.Closed.Before(created.Created) || !reflect.DeepEqual(closed.Closed, repeated.Closed) || reopened.Closed != nil {
			t.Fatal("admitted first/repeated close and configured-done reopen timestamps differ")
		}
		for _, point := range []b03TargetPoint{closed, repeated, reopened} {
			if !point.Created.Equal(created.Created) || !reflect.DeepEqual(point.Started, started) {
				t.Fatal("admitted lifecycle rewrote creation or last actual start instant")
			}
		}
		points = append(points, closed, repeated, reopened)
	}
	b03AdmittedRefuse(t, f, project, "bead.create", map[string]any{"type": "task", "targetStatus": "not_in_native_catalog", "fields": map[string]any{"title": "must not create"}})
	return points
}

func b03AdmittedComments(t *testing.T, f *b03Public, project string, id model.Ref, bodies []string) {
	t.Helper()
	var got struct {
		Comments []struct {
			Body      string
			Time      time.Time
			Principal model.PrincipalRef
		}
	}
	f.call(t, "bead.get", map[string]any{"id": id, "_meta": b03ProjectMeta(project)}, false, "", &got)
	doc, err := pipeline.DecodeDocument(f.row(t, id).Body)
	if err != nil || len(doc.Comments) != len(bodies) || len(got.Comments) != len(bodies) {
		t.Fatal("public/committed lifecycle reason comment census differs", err)
	}
	for i, body := range bodies {
		stored, public := doc.Comments[i], got.Comments[i]
		if stored.Body != body || public.Body != body || !stored.Time.Equal(public.Time) || stored.Time.IsZero() || !reflect.DeepEqual(stored.Principal, model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}) || !reflect.DeepEqual(stored.Principal, public.Principal) || i > 0 && !stored.Time.After(doc.Comments[i-1].Time) {
			t.Fatal("public lifecycle first/repeated-close/reopen reason differs from committed value")
		}
	}
}

// The capture-set importer owns admission and source publication. Composition
// then opens that actual imported store and recovers the installed source and
// workflow through its existing StoreFactory seam. There is no second importer
// and no change to the composition host or its lifetime semantics.
func b03AdmittedRuntime(t *testing.T, ctx context.Context, engine string, source b03AdmittedSource) (*composition.Runtime, composition.Config, driver.RestoreImportProfile, converterroundtrip.Report) {
	t.Helper()
	dsn := ""
	var store driver.Store
	var err error
	switch engine {
	case "memory":
		store, err = memory.NewWithOptions(memory.Options{StoreID: driver.StoreID("b03-" + source.Project), Incarnation: "b03-admitted-" + source.Project})
	case "sqlite":
		dsn = filepath.Join(t.TempDir(), "b03-admitted.db")
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: dsn})
	case "postgres":
		dsn = startPostgres(t)
		store, err = (postgres.Driver{}).Open(ctx, driver.Config{DSN: dsn})
	default:
		t.Fatal("unknown actual B03 engine", engine)
	}
	if err != nil {
		t.Fatal("open empty actual capture-set target", err)
	}
	runtimeOwns := false
	defer func() {
		if !runtimeOwns {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	op, lease, release, err := converterroundtrip.HoldRestoreLease(ctx, store, "b03-public-native-admission")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if release != nil {
			if err := release(); err != nil {
				t.Error(err)
			}
		}
	}()
	plan := b03AdmittedPlan(t, op, engine, source)
	meta, err := store.Meta(op)
	if err != nil {
		t.Fatal(err)
	}
	plan.Target.StoreID, plan.Target.Incarnation = string(meta.StoreID), meta.Incarnation
	profile := driver.RestoreImportProfile{
		Admission: b03AdmittedAdmission(), SourceStoreID: driver.StoreID(source.StoreID),
		Method: "offline.restore.import", IdempotencyKey: "b03-public-native-catalog-000001", ArchiveDigest: source.SHA256,
		HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: time.Now().UTC().Format(time.RFC3339Nano),
	}
	profile, err = converterroundtrip.BindNativeCaptureSetImport(op, source.archive, profile, plan)
	if err != nil {
		t.Fatal("bind actual authenticated native capture-set", err)
	}
	report, err := converterroundtrip.RestoreNativeCaptureSetImportWithLease(op, source.archive, store, lease, profile)
	if err != nil || report.Receipt == nil || report.SourcePlanes == nil || report.Archive == nil || report.Archive.SHA256() != source.SHA256 || !bytes.Equal(report.Archive.TarBytes(), source.archive.TarBytes()) {
		t.Fatalf("actual authenticated native capture-set admission failed: %v", err)
	}
	if err := converterroundtrip.VerifyNativeCaptureSetReadback(op, store, source.archive, report); err != nil {
		t.Fatal("exact authenticated native current/history readback", err)
	}
	if err := release(); err != nil {
		t.Fatal("release sole admission lease before runtime opens", err)
	}
	release = nil
	cfg := composition.Config{Driver: engine, DSN: dsn, AuthMode: "anonymous", Project: source.Project, NativeImportOntology: true,
		RestorePresence: restore.PresenceVerifierFunc(func(_ context.Context, method, digest string, presence restore.Presence) error {
			if (method != "admin.restore.begin" && method != "admin.dump") || len(digest) != 64 || presence["mode"] != "presence_check" || presence["challenge"] != "b03-public-native-catalog" || presence["proof"] != "b03-public-native-catalog" {
				return fmt.Errorf("B03 public host presence binding differs")
			}
			return nil
		}),
		StoreFactory: func(_ context.Context, selected string, config driver.Config) (driver.Store, error) {
			if selected != engine || config.DSN != dsn || runtimeOwns {
				return nil, fmt.Errorf("B03 imported store must be adopted once by its actual engine")
			}
			runtimeOwns = true
			return store, nil
		},
	}
	runtime, err := composition.Open(ctx, cfg)
	if err != nil {
		t.Fatal("open existing host over actual imported store", err)
	}
	cfg.StoreFactory = nil // SQL reconstruction must open the physical DSN.
	return runtime, cfg, profile, report
}

func b03AdmittedAdmission() driver.RestoreImportAdmission {
	return driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b03-public-restore-host"}, Authority: "restore-admin", AdmissionDigest: b03Digest([]byte("B03 public disposable host admission"))}
}

func TestB03AdmittedNativeCatalogLifecycleAndSourceHistory(t *testing.T) {
	inputs := b03AdmittedArchive(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, source := range inputs.Sources {
			t.Run(engine+"/"+source.Project, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				defer cancel()
				runtime, cfg, profile, report := b03AdmittedRuntime(t, ctx, engine, source)
				defer func() {
					if runtime != nil {
						if err := runtime.Close(context.Background()); err != nil {
							t.Error(err)
						}
					}
				}()
				wantLifetime := driver.CheckpointLifetimeRestart
				if engine == "memory" {
					wantLifetime = driver.CheckpointLifetimeProcess
					persistent, ok := runtime.Store.(driver.PersistentCheckpointStore)
					if !ok || persistent.Persistent() {
						t.Fatal("admitted memory host must explicitly report Persistent false")
					}
				}
				capability, err := driver.RequireWorkflowCheckpointStore(runtime.Store, runtime.Lease)
				if err != nil || capability.WorkflowCheckpointLifetime() != wantLifetime {
					t.Fatal("admitted host checkpoint lifetime differs from actual driver contract", err)
				}
				passes := 1
				if engine != "memory" {
					passes = 2
				}
				for pass := 0; pass < passes; pass++ {
					f := &b03Public{ctx: ctx, store: runtime.Store, lease: runtime.Lease, source: model.Ref{ID: converterroundtrip.ImportedOntologySourceID}, transport: inprocess.New(runtime.RPC)}
					dump := b03AdmittedPublicDump(t, f)
					b03RecordPublicDump(t, fmt.Sprintf("b03-admitted-dump-%s-%s-pass%d", engine, source.Project, pass), dump)
					b03AdmittedCatalog(t, f, source.Project, source.Catalog)
					b03AdmittedRows(t, f, source.Project, source.Current, nil, source.original)
					b03AdmittedHistory(t, f, source, dump)
					var ready struct {
						Items      []struct{ ID, Project string }
						NextCursor string
					}
					f.call(t, "bead.ready", map[string]any{"limit": 1000, "_meta": b03ProjectMeta(source.Project)}, false, "", &ready)
					got, want := []string{}, append([]string{}, source.ReadyIDs...)
					for _, row := range ready.Items {
						if row.Project != source.Project {
							t.Fatal("admitted public ready crossed projects")
						}
						if source.CategoryIDs[row.ID] {
							got = append(got, row.ID)
						}
					}
					sort.Strings(got)
					sort.Strings(want)
					if ready.NextCursor != "" || !reflect.DeepEqual(got, want) {
						t.Fatalf("admitted public ready differs from actual native category-carrier observation: got%v want%v", got, want)
					}
					replayed, err := converterroundtrip.RestoreNativeCaptureSetImportWithLease(ctx, source.archive, runtime.Store, runtime.Lease, profile)
					if err != nil || !reflect.DeepEqual(report.Receipt, replayed.Receipt) {
						t.Fatal("admitted capture-set receipt changed on exact replay", err)
					}
					b03Write(t, fmt.Sprintf("b03-admitted-%s-%s-pass%d", engine, source.Project, pass), map[string]any{"scope": "PUBLIC_NATIVE_TEST_HOST_ADMISSION", "capture_set_sha256": source.SHA256, "source": dumpformat.SourceIdentity{StoreID: source.StoreID, Project: source.Project}, "source_head": source.Head, "verified_native_commits": source.VerifiedCommits, "input_bundle_sha256": inputs.BundleSHA256, "exchanges": f.exchanges, "receipt": report.Receipt, "native_history": report.NativeHistories})
					if pass == 0 && engine != "memory" {
						if err := runtime.Close(ctx); err != nil {
							t.Fatal(err)
						}
						runtime = nil
						runtime, err = composition.Open(ctx, cfg)
						if err != nil {
							t.Fatal("actual admitted SQL reconstruction", err)
						}
					}
				}
				f := &b03Public{ctx: ctx, store: runtime.Store, lease: runtime.Lease, source: model.Ref{ID: converterroundtrip.ImportedOntologySourceID}, transport: inprocess.New(runtime.RPC)}
				sourceBefore := f.row(t, f.source)
				points := b03AdmittedLifecycle(t, f, source)
				if source.Catalog["project_local"] == "active" {
					id := b03TargetWrite(t, f, source.Project, "bead.create", map[string]any{"type": "task", "targetStatus": "project_local", "fields": map[string]any{"title": "same literal active in this project"}}).Results[0].Ref
					points = append(points, b03TargetObserve(t, f, source.Project, id, "admitted-project-local", "project_local", true))
				} else {
					b03AdmittedRefuse(t, f, source.Project, "bead.create", map[string]any{"type": "task", "targetStatus": "project_local", "fields": map[string]any{"title": "same literal terminal in this project"}})
				}
				if !reflect.DeepEqual(sourceBefore, f.row(t, f.source)) {
					t.Fatal("ordinary admitted lifecycle changed its installed source")
				}
				b03Write(t, "b03-admitted-lifecycle-"+engine+"-"+source.Project, map[string]any{"scope": "PUBLIC_NATIVE_TEST_HOST_ADMISSION", "points": points, "exchanges": f.exchanges, "lifetime": wantLifetime, "claim_scope": "ORDINARY_SEQUENTIAL_CATEGORY_ELIGIBILITY_ONLY", "adopted_differences": []string{"native historical commits retain raw source versions; current business/ontology images are not grafted", "native started_at/closed_at remain exact source metadata under the existing field-mapping contract", "a different nonempty repeat-close reason adds one Plug comment while native retains its original reason without that comment"}})
				if engine != "memory" {
					if err := runtime.Close(ctx); err != nil {
						t.Fatal(err)
					}
					runtime = nil
					runtime, err = composition.Open(ctx, cfg)
					if err != nil {
						t.Fatal("admitted lifecycle SQL reopen", err)
					}
					f.store, f.lease, f.transport = runtime.Store, runtime.Lease, inprocess.New(runtime.RPC)
					if !reflect.DeepEqual(sourceBefore, f.row(t, f.source)) {
						t.Fatal("SQL reopen changed installed native source")
					}
					dump := b03AdmittedPublicDump(t, f)
					b03RecordPublicDump(t, "b03-admitted-dump-"+engine+"-"+source.Project+"-after-lifecycle-reopen", dump)
					b03AdmittedCatalog(t, f, source.Project, source.Catalog)
					b03AdmittedHistory(t, f, source, dump)
					for _, point := range points {
						b03AdmittedRows(t, f, point.Project, []b03AdmittedRow{{ID: point.ID, Status: point.Status, Created: point.Created, Started: point.Started, Closed: point.Closed}}, &point.Revision)
					}
				}
			})
		}
	}
}
