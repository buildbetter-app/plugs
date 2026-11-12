package cardb20

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/server/viewrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

const b23RichFixtureRoot = "testdata/native-rich-review-project"

type b23RichFileBinding struct {
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type b23RichFixtureManifest struct {
	Schema         string                        `json:"schema"`
	Archive        b23RichFileBinding            `json:"archive"`
	ArchiveMembers map[string]b23RichFileBinding `json:"archive_members"`
	OracleFiles    map[string]b23RichFileBinding `json:"oracle_files"`
	CaptureSet     struct {
		Directory   string                        `json:"directory"`
		Format      string                        `json:"format"`
		Contract    string                        `json:"contract"`
		TotalBytes  int                           `json:"total_bytes"`
		Files       map[string]b23RichFileBinding `json:"files"`
		HeadMembers map[string]b23RichFileBinding `json:"head_members"`
	} `json:"capture_set"`
	Source struct {
		ProducerHead   string `json:"producer_head"`
		ProducerTree   string `json:"producer_tree"`
		ProducerParent string `json:"producer_parent"`
		Anchor         string `json:"anchor"`
		Project        string `json:"project"`
		StoreID        string `json:"store_id"`
		SourceHead     string `json:"source_head"`
		CommitCount    int    `json:"commit_count"`
		NativeHistory  string `json:"native_history"`
		PortableStatus string `json:"portable_history_status"`
	} `json:"source"`
}

type b23RichSourceCommits struct {
	Clean   bool   `json:"Clean"`
	Head    string `json:"Head"`
	Commits []struct {
		ID      string   `json:"id"`
		Parents []string `json:"parents"`
	} `json:"Commits"`
}

type b23RichState struct {
	Kind            string `json:"kind"`
	Sequence        uint64 `json:"sequence"`
	CommitID        string `json:"commit_id"`
	PhysicalRootSHA string `json:"physical_root_sha256"`
	TableCount      int    `json:"table_count"`
}

type b23RichTable struct {
	SourceIdentity struct {
		StoreID string `json:"store_id"`
		Project string `json:"project"`
	} `json:"source_identity"`
	Table   string           `json:"table"`
	History string           `json:"history"`
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
}

func b23RichReadFile(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(b23RichFixtureRoot, name))
	if err != nil {
		t.Fatalf("read committed rich fixture %s: %v", name, err)
	}
	return data
}

func b23RichHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Check raw TAR commitments without admitting or remapping the legacy input.
func b23RichTarBindings(t *testing.T, raw []byte, expected map[string]b23RichFileBinding) {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(raw))
	seen := map[string]bool{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		want, bound := expected[header.Name]
		if !bound {
			continue
		}
		member, err := io.ReadAll(reader)
		if err != nil || seen[header.Name] || len(member) != want.Bytes || b23RichHash(member) != want.SHA256 {
			t.Fatalf("rich raw archive member %s changed: %v", header.Name, err)
		}
		seen[header.Name] = true
	}
	if len(seen) != len(expected) {
		t.Fatalf("rich raw archive bindings missing: got=%d want=%d", len(seen), len(expected))
	}
}

func b23RichFixture(t *testing.T) (b23RichFixtureManifest, *dumpformat.Archive) {
	t.Helper()
	var manifest b23RichFixtureManifest
	if err := json.Unmarshal(b23RichReadFile(t, "fixture-manifest.json"), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Schema != "b23-native-public-fixture/1" || manifest.Source.Project != "review-project" || manifest.Source.CommitCount != 122 || manifest.Source.PortableStatus != "available-unverified" {
		t.Fatalf("rich fixture provenance changed: %+v", manifest)
	}
	data := b23RichReadFile(t, "native-rich-review-project.tar")
	if len(data) != manifest.Archive.Bytes || len(data) > 20*1024*1024 || b23RichHash(data) != manifest.Archive.SHA256 {
		t.Fatalf("rich fixture archive changed: bytes=%d sha=%s", len(data), b23RichHash(data))
	}
	for name, expected := range manifest.OracleFiles {
		got, err := os.ReadFile(filepath.Join(b23RichFixtureRoot, name))
		if err != nil {
			t.Fatalf("read committed rich oracle %s: %v", name, err)
		}
		if len(got) != expected.Bytes || b23RichHash(got) != expected.SHA256 {
			t.Fatalf("rich oracle %s changed", name)
		}
	}
	b23RichTarBindings(t, data, manifest.ArchiveMembers)
	// The original legacy TAR remains a refusal control, before any target is
	// opened. Native history is admitted only through the authenticated set.
	if _, err := dumpformat.Read(bytes.NewReader(data)); err == nil || err.Error() != "typed body/source mapping mismatch" {
		t.Fatalf("legacy native archive refusal changed: %v", err)
	}
	t.Logf("B23_LEGACY_NATIVE_REFUSAL sha256=%s reason=typed body/source mapping mismatch target_opened=false", manifest.Archive.SHA256)
	if manifest.CaptureSet.Directory != "capture-set" || manifest.CaptureSet.Format != "beads-native-capture-set/2" || manifest.CaptureSet.Contract != "beads-native-delta/2" || len(manifest.CaptureSet.Files) != 3 || manifest.CaptureSet.TotalBytes != 2678929 {
		t.Fatal("rich authenticated capture-set binding changed")
	}
	total := 0
	for _, name := range []string{"capture-set.json", "head.tar", "parts/00000000000000000001.part"} {
		expected, ok := manifest.CaptureSet.Files[name]
		member := b23RichReadFile(t, filepath.Join(manifest.CaptureSet.Directory, name))
		if !ok || len(member) != expected.Bytes || b23RichHash(member) != expected.SHA256 {
			t.Fatalf("rich capture-set file %s changed", name)
		}
		total += len(member)
	}
	if total != manifest.CaptureSet.TotalBytes || total > 20*1024*1024 {
		t.Fatalf("rich capture-set byte census=%d", total)
	}
	var root dumpformat.NativeCaptureSet
	if err := json.Unmarshal(b23RichReadFile(t, "capture-set/capture-set.json"), &root); err != nil {
		t.Fatal(err)
	}
	if root.Format != manifest.CaptureSet.Format || root.Contract != manifest.CaptureSet.Contract || root.StoreID != manifest.Source.StoreID || !reflect.DeepEqual(root.Projects, []string{manifest.Source.Project}) || root.Head != manifest.Source.SourceHead || root.Totals.Commits != uint64(manifest.Source.CommitCount) || root.PartCount != 1 {
		t.Fatal("rich capture-set source identity differs from independent oracle binding")
	}
	b23RichTarBindings(t, b23RichReadFile(t, "capture-set/head.tar"), manifest.CaptureSet.HeadMembers)
	archive, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), filepath.Join(b23RichFixtureRoot, manifest.CaptureSet.Directory), dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		t.Fatalf("read authenticated rich native capture set: %v", err)
	}
	if !archive.HasNativeDelta() || archive.Manifest().StoreID != manifest.Source.StoreID {
		t.Fatal("rich native capture set lost its authenticated source binding")
	}
	t.Cleanup(archive.ReleaseNativeDecoded)
	return manifest, archive
}

func b23RichOracle(t *testing.T) (b23RichSourceCommits, []b23RichState, b23RichTable, b23RichTable) {
	t.Helper()
	var commits b23RichSourceCommits
	if err := json.Unmarshal(b23RichReadFile(t, "oracle/source-commits.json"), &commits); err != nil {
		t.Fatal(err)
	}
	states := []b23RichState{}
	for _, line := range bytes.Split(bytes.TrimSpace(b23RichReadFile(t, "oracle/states.ndjson")), []byte{'\n'}) {
		var state b23RichState
		if err := json.Unmarshal(line, &state); err != nil {
			t.Fatal(err)
		}
		states = append(states, state)
	}
	var issues, labels b23RichTable
	if err := json.Unmarshal(b23RichReadFile(t, "oracle/issues-current.table.json"), &issues); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b23RichReadFile(t, "oracle/labels-current.table.json"), &labels); err != nil {
		t.Fatal(err)
	}
	return commits, states, issues, labels
}

func b23RichSourceValue(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	default:
		return fmt.Sprint(value)
	}
}

func b23RichSourceFlag(v any) bool {
	switch value := v.(type) {
	case bool:
		return value
	case json.Number:
		return value.String() == "1"
	case float64:
		return value == 1
	default:
		return false
	}
}

func b23RichCommitment(raw []byte, presence string) driver.SourcePlaneBytesV1 {
	return driver.SourcePlaneBytesV1{Presence: presence, Size: uint64(len(raw)), SHA256: b23RichHash(raw)}
}

func b23RichFields(t *testing.T, record driver.SourcePlaneRecordV1) []driver.SourcePlaneFieldV1 {
	t.Helper()
	var wire struct {
		Source struct {
			Fields map[string]json.RawMessage `json:"fields"`
		} `json:"source"`
	}
	if err := json.Unmarshal(record.Raw, &wire); err != nil {
		t.Fatal(err)
	}
	fields := make([]driver.SourcePlaneFieldV1, 0, len(wire.Source.Fields))
	for name, raw := range wire.Source.Fields {
		presence := "null"
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 {
			t.Fatal("empty source field commitment")
		}
		switch trimmed[0] {
		case '{':
			presence = "object"
		case '[':
			presence = "array"
		case '"':
			presence = "text"
			var text string
			if err := json.Unmarshal(trimmed, &text); err != nil {
				t.Fatal(err)
			}
			raw = []byte(text)
		case 't', 'f':
			presence = "boolean"
		case 'n':
		default:
			presence = "number"
		}
		fields = append(fields, driver.SourcePlaneFieldV1{Name: name, Value: b23RichCommitment(raw, presence)})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	return fields
}

func b23RichPlan(t *testing.T, archive *dumpformat.Archive, store driver.Store, lease driver.ActiveLease) driver.SourcePlanePlanV1 {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
	if err != nil {
		t.Fatal(err)
	}
	principal, _ := json.Marshal(model.PrincipalRef{Kind: "anonymous", ID: "anonymous"})
	plan := driver.SourcePlanePlanV1{
		Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1,
		SourceStoreID: archive.Manifest().StoreID, ArchiveSHA256: archive.SHA256(),
		Target:     driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, RequireEmpty: true, Lifetime: capability.WorkflowCheckpointLifetime(), RuntimeContract: driver.SourcePlaneRuntimeContractV1},
		Principals: []driver.SourcePlanePrincipalV1{{Key: "owner", Source: "rich native public fixture", Authority: "trusted-explicit/1", PrincipalJSON: principal}},
		Providers:  []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: workflowplugin.SourceStateSchema, Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: capability.WorkflowCheckpointLifetime(), OriginAdapter: "beads-workflow-origins/1"}},
	}
	raw, err := archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	type tuple struct {
		typ, storage         string
		ephemeral, noHistory bool
	}
	dispositionByTuple := map[tuple]string{}
	dispositionTupleByKey := map[string]tuple{}
	statusByTuple := map[tuple]map[string]bool{}
	fieldsByTuple := map[tuple][]driver.SourcePlaneFieldV1{}
	for _, record := range raw {
		var row dumpformat.Record
		if err := json.Unmarshal(record.Raw, &row); err != nil {
			t.Fatal(err)
		}
		origin := b23RichCommitment(record.Raw, "object")
		fields := b23RichFields(t, record)
		if row.Kind == "bead" {
			typ := b23RichSourceValue(row.Source.Fields["issue_type"])
			storage := ""
			if value, ok := row.Source.Fields["storage_class"]; ok && value != nil {
				storage = b23RichSourceValue(value)
			}
			key := tuple{typ: typ, storage: storage, ephemeral: b23RichSourceFlag(row.Source.Fields["ephemeral"]), noHistory: b23RichSourceFlag(row.Source.Fields["no_history"])}
			if _, ok := dispositionByTuple[key]; !ok {
				keyName := fmt.Sprintf("d-%02d", len(dispositionByTuple))
				dispositionByTuple[key] = keyName
				dispositionTupleByKey[keyName] = key
				fieldsByTuple[key] = fields
				statusByTuple[key] = map[string]bool{}
			}
			statusByTuple[key][b23RichSourceValue(row.Source.Fields["status"])] = true
			plan.Entities = append(plan.Entities, driver.SourcePlaneEntityV1{Source: driver.SourceEntityKey{Project: row.Project, Plane: row.Source.Table, ID: row.ID}, TargetKind: "bead", TargetID: row.ID, DispositionKey: dispositionByTuple[key], PrincipalKey: "owner", Route: driver.SourcePlaneRouteV1{Project: row.Project, SourceID: row.ID, DurableTargetID: row.ID, Preference: "active-first"}, Origin: origin, Fields: fields})
			continue
		}
		project := row.Project
		if row.Source.Identity != nil {
			project = row.Source.Identity.Project
		}
		plan.Origins = append(plan.Origins, driver.SourcePlaneOriginV1{Project: project, Table: row.Source.Table, ID: row.ID, Action: "inert-origin", Raw: origin, Fields: fields, Parsed: driver.SourcePlaneBytesV1{Presence: "absent"}})
	}
	keys := make([]tuple, 0, len(dispositionByTuple))
	for key := range dispositionByTuple {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := keys[i], keys[j]
		return fmt.Sprintf("%s\x00%s\x00%t\x00%t", left.typ, left.storage, left.ephemeral, left.noHistory) < fmt.Sprintf("%s\x00%s\x00%t\x00%t", right.typ, right.storage, right.ephemeral, right.noHistory)
	})
	for i, key := range keys {
		canonical := fmt.Sprintf("d-%02d", i)
		plan.Dispositions = append(plan.Dispositions, driver.SourcePlaneDispositionV1{Key: canonical, SourceType: key.typ, StorageClass: key.storage, Ephemeral: key.ephemeral, NoHistory: key.noHistory, SinkKind: "bead", Schema: "bead/1", Lifetime: "core", RefusalPolicy: "exact-only", Fields: b23RichDispositionFields(fieldsByTuple[key]), Statuses: b23RichStatuses(statusByTuple[key])})
		if old := dispositionByTuple[key]; old != canonical {
			dispositionByTuple[key] = canonical
		}
	}
	for i := range plan.Entities {
		if key, ok := dispositionTupleByKey[plan.Entities[i].DispositionKey]; ok {
			plan.Entities[i].DispositionKey = dispositionByTuple[key]
		}
	}
	sidecars, err := archive.SourcePlaneSidecarOrigins()
	if err != nil {
		t.Fatal(err)
	}
	plan.Origins = append(plan.Origins, sidecars...)
	sort.Slice(plan.Entities, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(plan.Entities[i].Source, plan.Entities[j].Source) < 0
	})
	sort.Slice(plan.Origins, func(i, j int) bool {
		left, right := plan.Origins[i], plan.Origins[j]
		return strings.Join([]string{left.Project, left.Table, left.ID, left.Path, left.Layer}, "\x00") < strings.Join([]string{right.Project, right.Table, right.ID, right.Path, right.Layer}, "\x00")
	})
	return plan
}

func b23RichDispositionFields(fields []driver.SourcePlaneFieldV1) []driver.SourcePlaneFieldRuleV1 {
	rules := make([]driver.SourcePlaneFieldRuleV1, 0, len(fields))
	for _, field := range fields {
		rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: "origin-only"}
		switch field.Name {
		case "id", "title", "status", "metadata", "created_at", "priority", "description":
			rule.Target, rule.Disposition = field.Name, "exact-map"
		case "issue_type":
			rule.Target, rule.Disposition = "type", "exact-map"
		}
		rules = append(rules, rule)
	}
	return rules
}

func b23RichStatuses(statuses map[string]bool) []driver.SourcePlaneStringPairV1 {
	keys := make([]string, 0, len(statuses))
	for status := range statuses {
		keys = append(keys, status)
	}
	sort.Strings(keys)
	pairs := make([]driver.SourcePlaneStringPairV1, 0, len(keys))
	for _, status := range keys {
		pairs = append(pairs, driver.SourcePlaneStringPairV1{From: status, To: status})
	}
	return pairs
}

func b23RichServer(t *testing.T, f *fixture, denied string) *rpc.Server {
	t.Helper()
	document := ontology.DefaultDocument()
	compiled, problems := ontology.Compile(document, nil, nil)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	ont := service.NewOntology(func(project string) ontology.Effective { return compiled.Effective(project) })
	writer := pipeline.New(f.store, f.lease, pipeline.WithOntology(ont), pipeline.WithClock(f.clock.Now))
	core := service.New(f.store, f.lease, writer, service.WithOntology(ont), service.WithClock(f.clock.Now))
	registry, err := rpc.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if err = rpc.BindService(registry, core); err != nil {
		t.Fatal(err)
	}
	views, err := viewops.New(f.store, f.lease, viewops.Config{OntologyRevision: model.Natural64(1), StaleDefaultOntology: ont, Now: f.clock.Now, AuthorizeRead: func(_ context.Context, _ model.PrincipalRef, resource model.ResourceRef, _ string) error {
		if denied != "" && resource.Ref.ID == denied {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("b23-rich-native-public-cursor-key-0123456789")
	if err = viewrpc.BindWithConfig(registry, views, viewrpc.Config{CursorKey: key}); err != nil {
		t.Fatal(err)
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := rpc.New(registry, rpc.Config{DefaultProject: "review-project", Incarnation: meta.Incarnation, CursorKey: key, OntologyRevision: model.Natural64(1)})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func b23RichCall(t *testing.T, server *rpc.Server, method, label string, params map[string]any) (json.RawMessage, *model.ErrorData) {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{"project": "review-project", "agent": "b23-rich-native", "idempotencyKey": b23RichHash([]byte("b23-rich-native/" + label))}
	request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": label, "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	response := server.Dispatch(context.Background(), request)
	var envelope struct {
		Result json.RawMessage
		Error  *struct{ Data model.ErrorData }
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatalf("%s response: %v", label, err)
	}
	if envelope.Error != nil {
		return envelope.Result, &envelope.Error.Data
	}
	return envelope.Result, nil
}

type b23RichPublicRow struct {
	ID     string         `json:"id"`
	Type   string         `json:"type"`
	Status string         `json:"status"`
	Labels []string       `json:"labels"`
	Fields map[string]any `json:"fields"`
}

func b23RichCurrentRows(t *testing.T, server *rpc.Server, label string, definition map[string]any, expected *viewops.AsOf) []b23RichPublicRow {
	t.Helper()
	raw, refusal := b23RichCall(t, server, "view.run", label, map[string]any{"definition": definition})
	if refusal != nil {
		t.Fatalf("current native view refused: %+v", refusal)
	}
	var result struct {
		Items      []b23RichPublicRow `json:"items"`
		AsOf       viewops.AsOf       `json:"asOf"`
		NextCursor string             `json:"nextCursor"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if expected != nil && result.AsOf != *expected {
		t.Fatalf("current query snapshot=%+v want=%+v: %s", result.AsOf, *expected, raw)
	}
	if result.NextCursor != "" {
		t.Fatalf("native current query unexpectedly paged: %s", raw)
	}
	return result.Items
}

func b23RichCurrentRowsAt(t *testing.T, server *rpc.Server, label string, definition map[string]any, at viewops.AsOf) []b23RichPublicRow {
	t.Helper()
	params := map[string]any{"definition": definition, "asOf": at}
	raw, refusal := b23RichCall(t, server, "view.run", label, params)
	if refusal != nil {
		t.Fatalf("native asOf view refused: %+v", refusal)
	}
	var result struct {
		Items      []b23RichPublicRow `json:"items"`
		AsOf       viewops.AsOf       `json:"asOf"`
		NextCursor string             `json:"nextCursor"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.AsOf != at {
		t.Fatalf("native asOf snapshot=%+v want=%+v: %s", result.AsOf, at, raw)
	}
	if result.NextCursor != "" {
		t.Fatalf("native asOf query unexpectedly paged: %s", raw)
	}
	return result.Items
}

func b23RichOracleRows(t *testing.T, table b23RichTable) map[string]map[string]any {
	t.Helper()
	if table.Table != "issues" || table.History != "retained-native-commit" || len(table.Rows) != 12 {
		t.Fatalf("independent native issues oracle changed: table=%q history=%q rows=%d", table.Table, table.History, len(table.Rows))
	}
	rows := map[string]map[string]any{}
	for _, row := range table.Rows {
		id, ok := row["id"].(string)
		if !ok || id == "" || rows[id] != nil {
			t.Fatalf("independent native issue identity changed: %#v", row["id"])
		}
		rows[id] = row
	}
	return rows
}

func b23RichOracleLabels(t *testing.T, table b23RichTable) map[string][]string {
	t.Helper()
	if table.Table != "labels" || table.History != "retained-native-commit" || len(table.Rows) != 20 {
		t.Fatalf("independent native labels oracle changed: table=%q history=%q rows=%d", table.Table, table.History, len(table.Rows))
	}
	labels := map[string][]string{}
	for _, row := range table.Rows {
		id, idOK := row["issue_id"].(string)
		label, labelOK := row["label"].(string)
		if !idOK || !labelOK {
			t.Fatalf("independent native label row is malformed: %#v", row)
		}
		labels[id] = append(labels[id], label)
	}
	for id := range labels {
		sort.Strings(labels[id])
	}
	return labels
}

func b23RichNativeCurrentDefinition(query string, resources, types []string) map[string]any {
	filter := map[string]any{"query": query}
	if resources != nil {
		filter["resources"] = resources
	}
	if types != nil {
		filter["types"] = types
	}
	return map[string]any{"filter": filter, "fields": []string{"id", "type", "status", "labels", "fields.title"}, "sort": []string{"id"}, "budgetTokens": 1000}
}

func b23RichNativeCurrentExpectations(t *testing.T, server *rpc.Server, issues map[string]map[string]any, labelRows map[string][]string, at viewops.AsOf) []b23RichPublicRow {
	t.Helper()
	all := b23RichCurrentRows(t, server, "rich-current-all", b23RichNativeCurrentDefinition("", nil, nil), &at)
	rows := all
	if len(rows) != len(issues) {
		t.Fatalf("imported public row count=%d want=%d", len(rows), len(issues))
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if seen[row.ID] || issues[row.ID] == nil {
			t.Fatalf("imported public row identity repeated/substituted: %q", row.ID)
		}
		seen[row.ID] = true
		want := issues[row.ID]
		if row.Type != b23RichSourceValue(want["issue_type"]) || row.Fields["title"] != want["title"] || row.Status != b23RichSourceValue(want["status"]) || !equalStringSet(row.Labels, labelRows[row.ID]) {
			t.Fatalf("imported public row differs from independent oracle: id=%s got=%+v want title=%v status=%v labels=%v", row.ID, row, want["title"], want["status"], labelRows[row.ID])
		}
	}
	if len(seen) != len(issues) {
		t.Fatal("imported public row set lost members")
	}
	// Intersections, OR, exclusion, status, type and resource-plane filters are
	// all evaluated by the public view seam over the imported native head.
	alpha := []string{"review-project-a1b"}
	rows = b23RichCurrentRows(t, server, "rich-filter-all", b23RichNativeCurrentDefinition("title=Rich AND label=alpha AND status=awaiting_review AND type=rich_kind", alpha, []string{"rich_kind"}), &at)
	if len(rows) != 1 || rows[0].ID != "review-project-a1b" {
		t.Fatalf("native all/intersection predicate=%+v", rows)
	}
	rows = b23RichCurrentRows(t, server, "rich-filter-any-exclude", b23RichNativeCurrentDefinition("title=Rich AND (label=alpha OR label=CaseTag) AND NOT label=renamecase", nil, []string{"rich_kind"}), &at)
	if len(rows) != 1 || rows[0].ID != "review-project-a1b" {
		t.Fatalf("native any/exclude predicate=%+v", rows)
	}
	rows = b23RichCurrentRows(t, server, "rich-filter-unknown", b23RichNativeCurrentDefinition("label=does-not-exist", nil, nil), &at)
	if len(rows) != 0 {
		t.Fatalf("unknown native label matched rows: %+v", rows)
	}
	return all
}

func equalStringSet(left, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return strings.Join(left, "\x00") == strings.Join(right, "\x00")
}

func b23RichNativePages(t *testing.T, server *rpc.Server, label string, definition map[string]any, at viewops.AsOf, want []string) {
	t.Helper()
	cursor := ""
	seen := map[string]bool{}
	cursors := map[string]bool{}
	got := []string{}
	for page := 0; page <= len(want); page++ {
		params := map[string]any{"definition": definition, "asOf": at}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, refusal := b23RichCall(t, server, "view.run", fmt.Sprintf("%s-%d", label, page), params)
		if refusal != nil {
			t.Fatalf("native page %d refused: %+v", page, refusal)
		}
		var result struct {
			Items      []struct{ ID string } `json:"items"`
			AsOf       viewops.AsOf          `json:"asOf"`
			NextCursor string                `json:"nextCursor"`
		}
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		if result.AsOf != at {
			t.Fatalf("native page snapshot=%+v want=%+v: %s", result.AsOf, at, raw)
		}
		for _, item := range result.Items {
			if seen[item.ID] {
				t.Fatalf("native page repeated id %s", item.ID)
			}
			seen[item.ID] = true
			got = append(got, item.ID)
		}
		if result.NextCursor == "" {
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("native page order=%v want=%v", got, want)
			}
			return
		}
		if len(result.Items) == 0 || result.NextCursor == cursor || cursors[result.NextCursor] {
			t.Fatalf("native page cursor did not advance: %s", raw)
		}
		cursors[result.NextCursor] = true
		cursor = result.NextCursor
	}
	t.Fatal("native pages did not terminate")
}

func b23RichNativeCount(t *testing.T, server *rpc.Server, label string, definition map[string]any, at viewops.AsOf, want int) {
	t.Helper()
	raw, refusal := b23RichCall(t, server, "view.run", label, map[string]any{"definition": definition, "asOf": at, "countOnly": true})
	if refusal != nil {
		t.Fatalf("native count refused: %+v", refusal)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	var count int
	var gotAt viewops.AsOf
	if err := json.Unmarshal(body["count"], &count); err != nil || count != want {
		t.Fatalf("native count=%s want=%d err=%v", raw, want, err)
	}
	if err := json.Unmarshal(body["asOf"], &gotAt); err != nil || gotAt != at {
		t.Fatalf("native count asOf=%s want=%+v err=%v", raw, at, err)
	}
	if string(body["items"]) != "[]" {
		t.Fatalf("native count returned items: %s", raw)
	}
	for _, field := range []string{"groups", "elisions", "nextCursor"} {
		if _, ok := body[field]; ok {
			t.Fatalf("native count emitted %s: %s", field, raw)
		}
	}
}

func b23RichReadyCount(t *testing.T, server *rpc.Server, ids []string, at viewops.AsOf) {
	t.Helper()
	filter := map[string]any{"resources": ids}
	for _, limit := range []int{1, 1000} {
		countRaw, refusal := b23RichCall(t, server, "bead.ready", fmt.Sprintf("rich-ready-count-%d", limit), map[string]any{"filter": filter, "limit": limit, "countOnly": true})
		if refusal != nil {
			t.Fatalf("native ready count refused: %+v", refusal)
		}
		var countBody struct {
			Items []json.RawMessage `json:"items"`
			AsOf  viewops.AsOf      `json:"asOf"`
			Count int               `json:"count"`
		}
		if err := json.Unmarshal(countRaw, &countBody); err != nil {
			t.Fatal(err)
		}
		if countBody.AsOf != at || len(countBody.Items) != 0 {
			t.Fatalf("native ready count shape=%s want asOf=%+v", countRaw, at)
		}
		ordinaryRaw, refusal := b23RichCall(t, server, "bead.ready", fmt.Sprintf("rich-ready-items-%d", limit), map[string]any{"filter": filter, "limit": 1000})
		if refusal != nil {
			t.Fatalf("native ready page refused: %+v", refusal)
		}
		var ordinary struct {
			Items []struct{ ID string } `json:"items"`
			AsOf  viewops.AsOf          `json:"asOf"`
		}
		if err := json.Unmarshal(ordinaryRaw, &ordinary); err != nil {
			t.Fatal(err)
		}
		if ordinary.AsOf != at || countBody.Count != len(ordinary.Items) {
			t.Fatalf("native ready count=%d items=%d snapshot=%+v/%+v", countBody.Count, len(ordinary.Items), countBody.AsOf, ordinary.AsOf)
		}
		seen := map[string]bool{}
		for _, item := range ordinary.Items {
			if seen[item.ID] {
				t.Fatalf("native ready repeated id %s", item.ID)
			}
			seen[item.ID] = true
		}
	}
}

func b23RichHistoryEvidence(t *testing.T, store driver.Store, meta driver.Meta) driver.HistoryImportEvidence {
	t.Helper()
	reader, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reader.Get(context.Background(), []model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID, model.Natural64(0))})
	closeErr := reader.Close()
	if err != nil || closeErr != nil || len(rows) != 1 || rows[0].Kind != "beads-archive" {
		t.Fatalf("native history evidence readback rows=%d err=%v close=%v", len(rows), err, closeErr)
	}
	var evidence driver.HistoryImportEvidence
	if err := json.Unmarshal(rows[0].Body, &evidence); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func b23RichAssertEvidence(t *testing.T, evidence driver.HistoryImportEvidence, scope dumpformat.NativeHistory, states []b23RichState) {
	t.Helper()
	if evidence.Source != driver.NativeTableHistorySource || evidence.Mode != driver.RestoreHistoryModeFull || !evidence.Complete || evidence.First != 1 || evidence.Head != uint64(len(scope.Commits)) || !evidence.Floor.IsZero() || len(evidence.Gaps) != 0 || len(evidence.Mapping) != len(scope.Commits) || len(states) != len(evidence.Mapping) {
		t.Fatalf("native history evidence boundary=%+v", evidence)
	}
	for i, mapping := range evidence.Mapping {
		state := states[i]
		if mapping.SourceStoreID != scope.StoreID || mapping.Project != scope.Projects[0] || mapping.SourceSequence != state.Sequence || mapping.Sequence != uint64(i+1) || mapping.Revision != model.Natural64(uint64(i+1)) || len(mapping.EventIDs) != 0 || len(mapping.CommitIDs) != 1 || mapping.CommitIDs[0] != state.CommitID {
			t.Fatalf("native history mapping %d differs: %+v source=%+v", i+1, mapping, state)
		}
	}
}

func b23RichAssertSourceOracle(t *testing.T, archive *dumpformat.Archive, manifest b23RichFixtureManifest, commits b23RichSourceCommits, states []b23RichState) (dumpformat.NativeHistory, []b23RichState) {
	t.Helper()
	scopes, err := archive.NativeSourceHistoriesChecked()
	if err != nil {
		t.Fatal(err)
	}
	if len(scopes) != 1 {
		t.Fatalf("rich native source scope count=%d want=1", len(scopes))
	}
	scope := scopes[0]
	if scope.StoreID != manifest.Source.StoreID || len(scope.Projects) != 1 || scope.Projects[0] != manifest.Source.Project || !scope.Complete || len(scope.Gaps) != 0 || len(scope.Commits) != manifest.Source.CommitCount || len(states) != manifest.Source.CommitCount+2 || len(commits.Commits) != manifest.Source.CommitCount {
		t.Fatalf("rich native source census store=%s projects=%v commits=%d states=%d oracle=%d", scope.StoreID, scope.Projects, len(scope.Commits), len(states), len(commits.Commits))
	}
	// The native graph is listed by ID; image order is recorded separately
	// by the producer's commit frames. Preserve both original qualifications.
	graphIndex := map[string]int{}
	for i, commit := range commits.Commits {
		if commit.ID == "" {
			t.Fatal("independent native graph contains an empty commit ID")
		}
		if _, duplicate := graphIndex[commit.ID]; duplicate {
			t.Fatal("independent native graph repeats a commit ID")
		}
		graphIndex[commit.ID] = i
	}
	seen := map[string]bool{}
	for _, commit := range scope.Commits {
		index, exists := graphIndex[commit.ID]
		if !exists || seen[commit.ID] || !reflect.DeepEqual(commit.Parents, commits.Commits[index].Parents) {
			t.Fatalf("native source graph identity/parents changed: %+v", commit)
		}
		seen[commit.ID] = true
	}
	commitStates := []b23RichState{}
	frameIDs := map[string]bool{}
	ancillary := map[string]b23RichState{}
	for _, state := range states {
		switch state.Kind {
		case "commit":
			if state.Sequence != uint64(len(commitStates)+1) || !seen[state.CommitID] || frameIDs[state.CommitID] {
				t.Fatal("independent native commit-frame sequence changed")
			}
			frameIDs[state.CommitID] = true
			commitStates = append(commitStates, state)
		case "head-checkpoint", "current":
			if _, duplicate := ancillary[state.Kind]; duplicate || state.Sequence != 0 || state.CommitID != manifest.Source.SourceHead || len(state.PhysicalRootSHA) != sha256.Size*2 {
				t.Fatal("independent native ancillary frame changed")
			}
			ancillary[state.Kind] = state
		default:
			t.Fatalf("unexpected independent native frame kind %q", state.Kind)
		}
	}
	if len(commitStates) != manifest.Source.CommitCount || len(ancillary) != 2 || !commits.Clean || commits.Head != manifest.Source.SourceHead {
		t.Fatal("independent native graph/frame completeness changed")
	}
	head, current := ancillary["head-checkpoint"], ancillary["current"]
	last := commitStates[len(commitStates)-1]
	if head.TableCount != 23 || current.TableCount != 36 || head.CommitID != last.CommitID || head.PhysicalRootSHA != last.PhysicalRootSHA || head.TableCount != last.TableCount {
		t.Fatal("independent HEAD/current frames or final committed root changed")
	}
	// V2 keeps HEAD detached and streams the historical states. Do not retain
	// callback tables or substitute HEAD for any of the 122 independent windows.
	if len(scope.Images) != 1 || scope.Images[0].CommitID != manifest.Source.SourceHead || scope.Images[0].Sequence != uint64(manifest.Source.CommitCount) {
		t.Fatal("native capture-set detached HEAD identity changed")
	}
	images := 0
	if err := archive.VisitNativeDeltaImages(context.Background(), func(image dumpformat.NativeHistoryImage, _ []string) error {
		if images >= len(commitStates) {
			return fmt.Errorf("native source stream has extra image %d", image.Sequence)
		}
		state := commitStates[images]
		if image.Sequence != uint64(images+1) || image.CommitID != state.CommitID || state.Sequence != image.Sequence || state.CommitID != image.CommitID || state.Kind != "commit" || state.TableCount != len(image.Tables) || len(state.PhysicalRootSHA) != sha256.Size*2 {
			return fmt.Errorf("native source image/state %d mismatch", images+1)
		}
		images++
		return nil
	}); err != nil || images != manifest.Source.CommitCount {
		t.Fatalf("native source stream incomplete: images=%d want=%d err=%v", images, manifest.Source.CommitCount, err)
	}
	issues, ok := scope.Current["issues"]
	if !ok || len(issues.Rows) != 12 {
		t.Fatalf("native current issues census=%d", len(issues.Rows))
	}
	labels, ok := scope.Current["labels"]
	if !ok || len(labels.Rows) != 20 {
		t.Fatalf("native current labels census=%d", len(labels.Rows))
	}
	return scope, commitStates
}

func b23RichNativeWindowReadback(t *testing.T, store driver.Store, meta driver.Meta, mapping driver.SourceImportRevision) {
	t.Helper()
	// The existing typed capture-set verifier checks every mapped native
	// row/meta snapshot. This separate selected-window check retains the
	// portable-row boundary without applying the legacy V1 row codec to V2.
	reader, err := store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &mapping.Revision})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	portable, err := reader.Get(context.Background(), []model.Ref{model.RefByID("review-project-a1b", model.Natural64(0))})
	if err != nil || len(portable) != 0 {
		t.Fatalf("native source window fabricated portable bead at revision %s: rows=%d err=%v", mapping.Revision, len(portable), err)
	}
	t.Logf("B23_NATIVE_WINDOW source_sequence=%d commit_ids=%v target_revision=%s portable_rows=0", mapping.SourceSequence, mapping.CommitIDs, mapping.Revision)
}

func b23RichNativeOldAsOf(t *testing.T, server *rpc.Server, store driver.Store, restored driver.Meta, firstMapping driver.SourceImportRevision, before driver.Meta) {
	t.Helper()
	at := viewops.AsOf{Revision: firstMapping.Revision, Incarnation: restored.Incarnation}
	raw, refusal := b23RichCall(t, server, "bead.get", "rich-old-get", map[string]any{"id": model.RefByID("review-project-a1b", model.Natural64(0)), "asOf": at})
	if refusal == nil || refusal.Code != model.ErrorCode("not_found") || len(raw) != 0 {
		t.Fatalf("native table history fabricated portable old bead: raw=%s refusal=%+v", raw, refusal)
	}
	raw, refusal = b23RichCall(t, server, "view.run", "rich-old-view", map[string]any{"asOf": at, "definition": b23RichNativeCurrentDefinition("", []string{"review-project-a1b"}, nil)})
	if refusal != nil {
		t.Fatalf("native old public view boundary returned unexpected refusal: %+v", refusal)
	}
	var result struct{ Items []b23RichPublicRow }
	if err := json.Unmarshal(raw, &result); err != nil || len(result.Items) != 0 {
		t.Fatalf("native table history fabricated old portable view: %s", raw)
	}
	after, err := store.Meta(context.Background())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("old-asOf read changed store metadata: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestB23NativeSourceHistoryPublicAcceptance(t *testing.T) {
	manifest, archive := b23RichFixture(t)
	commits, states, issueTable, labelTable := b23RichOracle(t)
	scope, commitStates := b23RichAssertSourceOracle(t, archive, manifest, commits, states)
	issues := b23RichOracleRows(t, issueTable)
	labelRows := b23RichOracleLabels(t, labelTable)
	fixedHistoryTime := time.Date(2026, 11, 11, 19, 0, 0, 0, time.UTC)

	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := open(t, engine)
			beforeRestore, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			plan := func() driver.SourcePlanePlanV1 {
				work, release := archive.NativeDeltaAccountingScope()
				defer release()
				return b23RichPlan(t, work, f.store, f.lease)
			}()
			profile := driver.RestoreImportProfile{
				Admission: driver.RestoreImportAdmission{
					Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "b23-native-importer"}, Authority: "restore-admin", AdmissionDigest: b23RichHash([]byte("b23 native rich authenticated admission")),
				},
				SourceStoreID: driver.StoreID(plan.SourceStoreID), Method: "offline.restore.import", IdempotencyKey: "b23-rich-native-history-20261111", ArchiveDigest: plan.ArchiveSHA256, HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: fixedHistoryTime.Format(time.RFC3339Nano),
			}
			bound, err := converterroundtrip.BindNativeCaptureSetImport(f.ctx, archive, profile, plan)
			if err != nil {
				t.Fatalf("bind actual rich native source import: %v", err)
			}
			report, err := converterroundtrip.RestoreNativeCaptureSetImportWithLease(f.ctx, archive, f.store, f.lease, bound)
			if err != nil {
				t.Fatalf("restore actual rich native source import: %v", err)
			}
			if report.SourcePlanes == nil || report.Receipt == nil || report.Restored.Head != model.Natural64(uint64(len(scope.Commits)+1)) || report.Restored.StoreID != driver.StoreID(plan.SourceStoreID) || report.Restored.Incarnation == beforeRestore.Incarnation {
				t.Fatalf("native import metadata/receipt=%+v", report)
			}
			evidence := b23RichHistoryEvidence(t, f.store, report.Restored)
			b23RichAssertEvidence(t, evidence, scope, commitStates)
			if err := converterroundtrip.VerifyNativeCaptureSetReadback(f.ctx, f.store, archive, report); err != nil {
				t.Fatalf("every mapped native typed source row/meta readback: %v", err)
			}
			if bound.HistoryDigest == "" || bound.DerivedDigest == "" || bound.SourcePlanes == nil {
				t.Fatalf("bound profile lacks exact source/history digests: %+v", bound)
			}

			// The native source-plane windows are checked against independently
			// retained source images. Portable business rows are checked separately
			// at the final restore head below.
			for _, index := range []int{0, len(evidence.Mapping) / 2, len(evidence.Mapping) - 1} {
				b23RichNativeWindowReadback(t, f.store, report.Restored, evidence.Mapping[index])
			}

			f.server = b23RichServer(t, f, "")
			at := viewops.AsOf{Revision: report.Restored.Head, Incarnation: report.Restored.Incarnation}
			rows := b23RichNativeCurrentExpectations(t, f.server, issues, labelRows, at)
			ids := make([]string, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			sort.Strings(ids)

			intersection := b23RichNativeCurrentDefinition("title=Rich AND label=alpha AND status=awaiting_review AND type=rich_kind", []string{"review-project-a1b"}, []string{"rich_kind"})
			intersection["budgetTokens"] = 1
			b23RichNativePages(t, f.server, "rich-intersection-pages", intersection, at, []string{"review-project-a1b"})
			for _, budget := range []int{1, 1000} {
				definition := b23RichNativeCurrentDefinition("title=Rich AND label=alpha AND status=awaiting_review AND type=rich_kind", []string{"review-project-a1b"}, []string{"rich_kind"})
				definition["budgetTokens"] = budget
				b23RichNativeCount(t, f.server, fmt.Sprintf("rich-intersection-count-%d", budget), definition, at, 1)
			}
			anyExclude := b23RichNativeCurrentDefinition("title=Rich AND (label=alpha OR label=CaseTag) AND NOT label=renamecase", nil, []string{"rich_kind"})
			anyExclude["budgetTokens"] = 1
			b23RichNativeCount(t, f.server, "rich-any-exclude-count", anyExclude, at, 1)
			unknown := b23RichNativeCurrentDefinition("label=does-not-exist", nil, nil)
			b23RichNativeCount(t, f.server, "rich-unknown-count", unknown, at, 0)
			blank := b23RichNativeCurrentDefinition("", nil, nil)
			b23RichNativeCount(t, f.server, "rich-blank-count", blank, at, len(ids))

			// Title ordering is an independent natural/tie oracle; the page
			// walk must conserve the complete keyset without repeats.
			natural := b23RichNativeCurrentDefinition("", nil, nil)
			natural["budgetTokens"] = 1
			b23RichNativePages(t, f.server, "rich-natural-pages", natural, at, []string{
				"review-project-afr", "review-project-702", "review-project-700", "review-project-703", "review-project-705", "review-project-704", "review-project-701", "review-project-a1b.1", "review-project-80a", "review-project-c2d", "review-project-a1b.2", "review-project-a1b",
			})

			// Current read authority applies while an old native source revision
			// is selected; native history itself must not fabricate a portable
			// bead/view image.
			deniedServer := b23RichServer(t, f, "review-project-a1b")
			deniedRows := b23RichCurrentRowsAt(t, deniedServer, "rich-current-denied", b23RichNativeCurrentDefinition("", nil, nil), at)
			if len(deniedRows) != len(rows)-1 {
				t.Fatalf("current native authority filtered rows=%d want=%d", len(deniedRows), len(rows)-1)
			}
			readMeta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			b23RichNativeOldAsOf(t, deniedServer, f.store, report.Restored, evidence.Mapping[0], readMeta)
			b23RichReadyCount(t, f.server, ids, at)

			if after, err := f.store.Meta(f.ctx); err != nil || !reflect.DeepEqual(after, readMeta) {
				t.Fatalf("native public reads mutated metadata: before=%+v after=%+v err=%v", readMeta, after, err)
			}

			f.reopen()
			reopened, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			f.server = b23RichServer(t, f, "")
			recoveredProfile, recovered, found, err := converterroundtrip.RecoverBoundSourceImport(f.ctx, f.store)
			if recovered.Archive != nil {
				defer recovered.Archive.ReleaseNativeDecoded()
			}
			if err != nil || !found || recovered.Receipt == nil || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.SourcePlanes, report.SourcePlanes) {
				t.Fatalf("native source receipt/recovery changed on %s reopen: found=%v err=%v", engine, found, err)
			}
			if recoveredProfile.ArchiveDigest != bound.ArchiveDigest || recoveredProfile.HistoryDigest != bound.HistoryDigest || recoveredProfile.HistoryTime != bound.HistoryTime {
				t.Fatalf("native source profile changed on reopen: got=%+v want=%+v", recoveredProfile, bound)
			}
			if err := converterroundtrip.VerifyNativeCaptureSetReadback(f.ctx, f.store, archive, recovered); err != nil {
				t.Fatalf("reopened every mapped native typed source row/meta readback: %v", err)
			}
			reopenedAt := viewops.AsOf{Revision: reopened.Head, Incarnation: reopened.Incarnation}
			b23RichNativeCurrentExpectations(t, f.server, issues, labelRows, reopenedAt)
			b23RichReadyCount(t, f.server, ids, reopenedAt)
			for _, index := range []int{0, len(evidence.Mapping) / 2, len(evidence.Mapping) - 1} {
				b23RichNativeWindowReadback(t, f.store, recovered.Restored, evidence.Mapping[index])
			}
			reopenedReadMeta, err := f.store.Meta(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			b23RichNativeOldAsOf(t, f.server, f.store, recovered.Restored, evidence.Mapping[0], reopenedReadMeta)
		})
	}
}
