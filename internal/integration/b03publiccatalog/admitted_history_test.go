package cardb20

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
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
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type b03DumpRecord struct {
	Ref      model.Ref       `json:"ref"`
	Kind     string          `json:"kind"`
	Revision model.Revision  `json:"revision"`
	Body     json.RawMessage `json:"body"`
}

type b03PublicDump struct {
	Data       []byte
	Records    []b03DumpRecord
	Versions   []driver.Version
	Changesets []model.Changeset
	Manifest   dumpformat.Manifest
}

func b03RecordPublicDump(t *testing.T, name string, dump b03PublicDump) {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) || filepath.Base(name) != name {
		t.Fatal("absolute output directory and one archive basename required")
	}
	if err := os.WriteFile(filepath.Join(root, name+".tar"), dump.Data, 0600); err != nil {
		t.Fatal(err)
	}
	b03Write(t, name, map[string]any{"archive_sha256": b03Digest(dump.Data), "archive_bytes": len(dump.Data), "manifest": dump.Manifest, "records": len(dump.Records), "versions": len(dump.Versions), "changesets": len(dump.Changesets), "native_census": "CURRENT_LATEST_VERSION_AND_CHANGESET_TUPLES_CLOSED"})
}

func b03DecodeLines[T any](t *testing.T, data []byte) []T {
	t.Helper()
	var rows []T
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	for {
		var row T
		if err := d.Decode(&row); err == io.EOF {
			return rows
		} else if err != nil {
			t.Fatal("invalid public dump row", err)
		}
		rows = append(rows, row)
	}
}

func b03SameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	return reflect.DeepEqual(b03JSONValue(t, a), b03JSONValue(t, b))
}

func b03DecodeJSON(t *testing.T, data []byte, value any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		t.Fatal("invalid lossless public JSON", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		t.Fatal("public JSON has trailing content", err)
	}
}

func b03JSONValue(t *testing.T, data []byte) any {
	t.Helper()
	var value any
	b03DecodeJSON(t, data, &value)
	return value
}

// The public API returns semantic JSON objects. Re-encode only this closed
// declared wire shape before calling the format-first canonical native codec.
// The original public response stays in f.exchanges and is compared losslessly
// with both this encoding and the actual mapped-snapshot stored body.
type b03NativeRowBody struct {
	Format       string                       `json:"format"`
	Source       dumpformat.SourceIdentity    `json:"source_identity"`
	Table        string                       `json:"table"`
	History      string                       `json:"history"`
	IdentityKind string                       `json:"identity_kind"`
	Key          string                       `json:"key"`
	Fields       dumpformat.Row               `json:"fields"`
	Cells        []dumpformat.NativeDeltaCell `json:"cells,omitempty"`
}

func b03CanonicalNativeBody(t *testing.T, wire []byte) []byte {
	t.Helper()
	object, ok := b03JSONValue(t, wire).(map[string]any)
	if !ok {
		t.Fatal("native public body must be an object")
	}
	var canonical any
	switch object["format"] {
	case dumpformat.NativeRowMetaFormat:
		var body dumpformat.NativeRowMeta
		b03DecodeJSON(t, wire, &body)
		canonical = body
	case dumpformat.NativeRowFormat:
		var body b03NativeRowBody
		b03DecodeJSON(t, wire, &body)
		canonical = body
	default:
		t.Fatal("public native body has an unknown declared format")
	}
	data, err := json.Marshal(canonical)
	if err != nil || !b03SameJSON(t, data, wire) {
		t.Fatal("native canonical encoding changed public semantic JSON", err)
	}
	return data
}

// The complete public archive supplies the native Ref census. Exact-ref
// history.get subsequently reads original source versions at their mapped
// revisions; neither a filtered business query nor guessed native IDs is used.
func b03AdmittedPublicDump(t *testing.T, f *b03Public) b03PublicDump {
	t.Helper()
	before, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	cursor, dumpID := "", ""
	for page := uint64(0); ; page++ {
		if page >= 4096 {
			t.Fatal("public native dump exceeded bounded page inventory")
		}
		params := map[string]any{"history": "full", "pageSize": 65536, "humanPresence": map[string]any{"mode": "presence_check", "challenge": "b03-public-native-catalog", "proof": "b03-public-native-catalog"}}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var result struct {
			DumpID       string `json:"dumpId"`
			Page         uint64 `json:"page"`
			Data, SHA256 string
			NextPage     *uint64 `json:"nextPage"`
			Done         bool
		}
		f.call(t, "admin.dump", params, false, "", &result)
		chunk, err := base64.StdEncoding.DecodeString(result.Data)
		if err != nil || result.Page != page || result.DumpID == "" || dumpID != "" && dumpID != result.DumpID || len(chunk) == 0 || len(chunk) > 65536 || b03Digest(chunk) != result.SHA256 {
			t.Fatal("public native dump page identity/order/checksum differs", err)
		}
		dumpID = result.DumpID
		data.Write(chunk)
		if result.Done {
			if result.NextPage != nil {
				t.Fatal("completed native dump retained a next page")
			}
			break
		}
		if result.NextPage == nil || *result.NextPage != page+1 {
			t.Fatal("public native dump lost its exact continuation")
		}
		cursor = fmt.Sprintf("%s:%d", dumpID, *result.NextPage)
	}
	result := b03PublicDump{Data: data.Bytes()}
	members := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(result.Data))
	for {
		header, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, duplicate := members[header.Name]; duplicate || header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > 64<<20 {
			t.Fatal("public native dump has a duplicate/nonregular/oversized member")
		}
		payload, err := io.ReadAll(r)
		if err != nil || int64(len(payload)) != header.Size {
			t.Fatal("public native dump member is incomplete", err)
		}
		members[header.Name] = payload
	}
	if err := json.Unmarshal(members["manifest.json"], &result.Manifest); err != nil {
		t.Fatal(err)
	}
	m := result.Manifest
	if len(members) != 6 || len(m.Files) != 5 || m.Scope != "plug-native-store" || m.Format != dumpformat.Format || m.History != "full" || m.NativeHead != before.Head.String() || m.NativeIncarnation != before.Incarnation || m.NativeStoreID != string(before.StoreID) {
		t.Fatal("public native dump source/snapshot/member inventory differs")
	}
	var digestInput bytes.Buffer
	for _, name := range []string{"allocator/snapshot.json", "changesets.ndjson", "config/sources.json", "records/native.ndjson", "versions.ndjson"} {
		payload, present := members[name]
		file, declared := m.Files[name]
		if !present || !declared || file.Size != int64(len(payload)) || file.SHA256 != b03Digest(payload) {
			t.Fatalf("public native dump member binding differs: %s", name)
		}
		digestInput.WriteString(name)
		digestInput.Write(payload)
	}
	if b03Digest(digestInput.Bytes()) != m.NativeDigest {
		t.Fatal("public native dump aggregate digest differs")
	}
	result.Records = b03DecodeLines[b03DumpRecord](t, members["records/native.ndjson"])
	result.Versions = b03DecodeLines[driver.Version](t, members["versions.ndjson"])
	result.Changesets = b03DecodeLines[model.Changeset](t, members["changesets.ndjson"])
	if m.RecordCount != len(result.Records) || m.VersionCount != len(result.Versions) || m.ChangesetCount != len(result.Changesets) {
		t.Fatal("public native dump record/version/changeset census differs")
	}
	b03CloseNativeCensus(t, result)
	after, err := f.store.Meta(f.ctx)
	if err != nil || before != after {
		t.Fatal("public native dump changed graph metadata", err)
	}
	return result
}

func b03RefKey(t *testing.T, ref model.Ref) string {
	t.Helper()
	key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// All exported tuple identities are unique. The complete native-history
// namespace additionally closes current records <-> latest versions and every
// retained version <-> its exact changeset membership. This does not advertise
// arbitrary non-native plugin state as a business-table conservation proof.
func b03CloseNativeCensus(t *testing.T, dump b03PublicDump) {
	t.Helper()
	current := map[string]b03DumpRecord{}
	versions := map[string]driver.Version{}
	latest := map[string]driver.Version{}
	changes := map[string]model.ChangeRecord{}
	head, err := model.Revision(dump.Manifest.NativeHead).Uint64()
	if err != nil {
		t.Fatal(err)
	}
	native := func(ref model.Ref) bool { return strings.HasPrefix(ref.ID, driver.NativeTableHistoryRecordPrefix) }
	for _, record := range dump.Records {
		key := b03RefKey(t, record.Ref)
		if _, present := current[key]; present {
			t.Fatal("public dump repeats a current record Ref")
		}
		current[key] = record
	}
	for _, version := range dump.Versions {
		key := b03RefKey(t, version.Ref)
		tuple := key + "@" + version.Revision.String()
		revision, err := version.Revision.Uint64()
		if _, present := versions[tuple]; err != nil || revision > head || present {
			t.Fatal("public dump repeats or invalidates a version Ref/revision", err)
		}
		versions[tuple] = version
		previous, present := latest[key]
		old, _ := previous.Revision.Uint64()
		if !present || old < revision {
			latest[key] = version
		}
	}
	seenChangesets := map[string]bool{}
	for _, change := range dump.Changesets {
		revision := change.Revision.String()
		number, err := change.Revision.Uint64()
		if err != nil || number > head || change.Incarnation != dump.Manifest.NativeIncarnation || seenChangesets[revision] {
			t.Fatal("public dump repeats or invalidates a changeset revision", err)
		}
		seenChangesets[revision] = true
		for _, item := range change.Changes {
			key := b03RefKey(t, item.Ref) + "@" + revision
			if _, present := changes[key]; present {
				t.Fatal("public dump repeats a Ref within one changeset")
			}
			changes[key] = item
			if native(item.Ref) {
				version, present := versions[key]
				if !present || version.Deleted != (item.Op == model.OpDelete) {
					t.Fatal("public native changeset has no matching version/tombstone")
				}
			}
		}
	}
	for tuple, version := range versions {
		if native(version.Ref) {
			if _, present := changes[tuple]; !present {
				t.Fatal("public native version has no exact changeset membership")
			}
		}
	}
	for key, version := range latest {
		if !native(version.Ref) {
			continue
		}
		record, present := current[key]
		if version.Deleted {
			if present {
				t.Fatal("public native tombstone retained a current record")
			}
		} else if !present || record.Kind != "beads-archive" || record.Revision.String() != version.Revision.String() || !b03SameJSON(t, record.Body, version.Body) {
			t.Fatal("public native current record differs from its latest live version")
		}
	}
	for key, record := range current {
		if native(record.Ref) {
			if _, present := latest[key]; !present {
				t.Fatal("public native current Ref is absent from the complete version census")
			}
		}
	}
}

func b03NativeRows(t *testing.T, rows []dumpformat.Row) []string {
	t.Helper()
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, string(encoded))
	}
	sort.Strings(values)
	return values
}

func b03SameNativeInstant(t *testing.T, actual []byte, expected any) bool {
	t.Helper()
	if expected == nil {
		return bytes.Equal(actual, []byte("null"))
	}
	want, ok := expected.(string)
	if !ok {
		t.Fatal("independent native instant is not a string or null")
	}
	var text string
	if err := json.Unmarshal(actual, &text); err != nil {
		return false
	}
	a, aerr := time.Parse(time.RFC3339Nano, text)
	b, berr := time.Parse(time.RFC3339Nano, want)
	return aerr == nil && berr == nil && !a.IsZero() && a.Equal(b)
}

// Table metadata has a closed flat set of columns; its source_identity object
// is constant for a stable Ref. Validate the exact patch by applying it to the
// observed old public body, and independently enumerate every changed member.
func b03VerifyMetaDiff(t *testing.T, before, after []byte, patch []json.RawMessage, paths []string) bool {
	t.Helper()
	old, ok := b03JSONValue(t, before).(map[string]any)
	if !ok {
		t.Fatal("old native table metadata is not an object")
	}
	want, ok := b03JSONValue(t, after).(map[string]any)
	if !ok || !reflect.DeepEqual(old["source_identity"], want["source_identity"]) {
		t.Fatal("stable table metadata source identity changed")
	}
	expected := []string{}
	for key, value := range old {
		if other, present := want[key]; !present || !reflect.DeepEqual(value, other) {
			expected = append(expected, "/"+key)
		}
	}
	for key := range want {
		if _, present := old[key]; !present {
			expected = append(expected, "/"+key)
		}
	}
	sort.Strings(expected)
	if !reflect.DeepEqual(paths, expected) || len(patch) != len(expected) {
		t.Fatal("public raw native diff paths/operation census differs from actual observed bodies")
	}
	for i, raw := range patch {
		var op struct {
			Op, Path string
			Value    json.RawMessage
		}
		b03DecodeJSON(t, raw, &op)
		if op.Path != expected[i] || !strings.HasPrefix(op.Path, "/") || strings.ContainsAny(op.Path[1:], "/~") {
			t.Fatal("public table-meta patch has an unexpected path")
		}
		key := op.Path[1:]
		previous, present := old[key]
		next, exists := want[key]
		if present && exists && reflect.DeepEqual(previous, next) {
			t.Fatal("public patch includes an unchanged metadata member")
		}
		switch op.Op {
		case "remove":
			if !present || exists || len(op.Value) != 0 {
				t.Fatal("invalid public native metadata remove")
			}
			delete(old, key)
		case "add", "replace":
			if !exists || op.Op == "add" && present || op.Op == "replace" && !present || len(op.Value) == 0 {
				t.Fatal("invalid public native metadata add/replace")
			}
			value := b03JSONValue(t, op.Value)
			if !reflect.DeepEqual(value, next) {
				t.Fatal("public native patch value differs from actual new metadata")
			}
			old[key] = value
		default:
			t.Fatal("unknown public native metadata patch operation")
		}
	}
	if !reflect.DeepEqual(old, want) {
		t.Fatal("applying public native patch does not produce the observed new metadata")
	}
	return len(expected) != 0
}

func b03PublicNativeMilestone(t *testing.T, f *b03Public, source b03AdmittedSource, milestone b03AdmittedMilestone, at model.Revision, dump b03PublicDump) map[string]driver.Record {
	t.Helper()
	expected, present := source.snapshots[milestone.CommitID]
	if !present {
		t.Fatal("independently named milestone has no actual original native image")
	}
	limit, err := at.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	selected := map[string]driver.Version{}
	for _, version := range dump.Versions {
		if !strings.HasPrefix(version.Ref.ID, driver.NativeTableHistoryRecordPrefix) {
			continue
		}
		if !version.Ref.Branch.IsZero() {
			t.Fatal("retained native history gained an unbound branch")
		}
		revision, err := version.Revision.Uint64()
		if err != nil || revision > limit {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		previous, present := selected[version.Ref.ID]
		previousRevision, err := previous.Revision.Uint64()
		if present && (err != nil || previousRevision == revision) {
			t.Fatal("native version census repeats or invalidates a Ref/revision")
		}
		if !present || previousRevision < revision {
			selected[version.Ref.ID] = version
		}
	}
	meta, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.store.Read(f.ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal("open actual mapped native source snapshot", err)
	}
	defer snapshot.Close()
	groups := map[string][]driver.Record{}
	metas := map[string]driver.Record{}
	identity := dumpformat.SourceIdentity{StoreID: source.StoreID, Project: source.Project}
	for _, version := range selected {
		if version.Deleted {
			continue
		}
		// The public dump above closes the complete native Ref/version census.
		// B03's semantic history comparison covers exactly its three business
		// families; unrelated and machine-local values remain opaque.
		var label struct{ Table string }
		if err := json.Unmarshal(version.Body, &label); err != nil || label.Table == "" {
			t.Fatal("native history version has no declared table", err)
		}
		if !b03NativeBusinessFamily(label.Table) {
			continue
		}
		var got struct {
			Ref      model.Ref
			Revision model.Revision
			Body     json.RawMessage
		}
		f.call(t, "history.get", map[string]any{"ref": version.Ref, "asOf": map[string]any{"incarnation": meta.Incarnation, "revision": at}, "_meta": b03ProjectMeta(source.Project)}, false, "", &got)
		if b03RefKey(t, got.Ref) != b03RefKey(t, version.Ref) || got.Revision != version.Revision || len(got.Body) == 0 || len(version.Body) == 0 || !b03SameJSON(t, version.Body, got.Body) {
			t.Fatal("public exact native version differs from its complete dump census")
		}
		observed, err := snapshot.Get(f.ctx, []model.Ref{got.Ref})
		if err != nil || len(observed) != 1 || b03RefKey(t, observed[0].Ref) != b03RefKey(t, got.Ref) || observed[0].Revision != got.Revision || observed[0].Kind != "beads-archive" || !b03SameJSON(t, observed[0].Body, got.Body) {
			t.Fatal("actual native snapshot kind/ref/revision/body differs from public history", err)
		}
		record := observed[0]
		record.Body = b03CanonicalNativeBody(t, got.Body)
		if !bytes.Equal(record.Body, observed[0].Body) {
			t.Fatal("public native body cannot reconstruct actual canonical stored bytes")
		}
		tableMeta, row, err := dumpformat.DecodeNativeRowRecord(record)
		if err != nil {
			t.Fatal("public raw native record is invalid", err)
		}
		actualSource, table, history := dumpformat.SourceIdentity{}, "", ""
		if tableMeta != nil {
			actualSource, table, history = tableMeta.Source, tableMeta.Table, tableMeta.History
		} else {
			actualSource, table, history = row.Source, row.Table, row.History
		}
		if actualSource != identity {
			continue
		}
		if history != "retained-native-commit" {
			t.Fatal("public mapped source revision selected a current native plane")
		}
		if tableMeta != nil {
			metas[table] = record
		}
		groups[table] = append(groups[table], record)
	}
	if len(groups) != len(expected.Tables) {
		t.Fatal("public B03 historical table membership differs from the authenticated source image")
	}
	actualTables := map[string]dumpformat.NativeTable{}
	for name, original := range expected.Tables {
		actual, _, err := dumpformat.ReconstructNativeRowTable(identity, name, "retained-native-commit", groups[name], nil)
		if err != nil || !reflect.DeepEqual(actual.Columns, original.Columns) || !reflect.DeepEqual(b03NativeRows(t, actual.Rows), b03NativeRows(t, original.Rows)) {
			t.Fatalf("public native table %s differs from actual source commit %s: %v", name, milestone.CommitID, err)
		}
		actualTables[name] = actual
	}
	actualCatalog := map[string]string{}
	for _, row := range actualTables["custom_statuses"].Rows {
		name, named := row["name"].(string)
		category, categorized := row["category"].(string)
		if !named || !categorized || actualCatalog[name] != "" {
			t.Fatal("public historical native category differs from independently recorded catalog")
		}
		actualCatalog[name] = category
	}
	if !reflect.DeepEqual(actualCatalog, milestone.SQLCatalog) || !reflect.DeepEqual(b03NativeRows(t, actualTables["config"].Rows), b03NativeRows(t, milestone.Config)) {
		t.Fatal("public historical SQL catalog/config membership differs from the complete independent native expectation")
	}
	if !reflect.DeepEqual(b03EffectiveStoredCatalog(t, actualCatalog, actualTables["config"].Rows), milestone.Catalog) {
		t.Fatal("historical effective stored catalog differs; current physical YAML is not historical SQL provenance")
	}
	for _, want := range milestone.Rows {
		found := false
		for _, row := range actualTables["issues"].Rows {
			if row["id"] != want.ID {
				continue
			}
			if found || row["status"] != want.Status {
				t.Fatal("public native source issue identity/status differs from its independent milestone")
			}
			found = true
			for name, instant := range map[string]any{"created_at": want.Created, "started_at": want.Started, "closed_at": want.Closed} {
				actual, err := json.Marshal(row[name])
				if err != nil {
					t.Fatal(err)
				}
				var expectedInstant any
				encoded, err := json.Marshal(instant)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(encoded, &expectedInstant); err != nil || !b03SameNativeInstant(t, actual, expectedInstant) {
					t.Fatalf("public native source %s differs from its independently observed value", name)
				}
			}
		}
		if !found {
			t.Fatal("independent native milestone issue is absent from public source history")
		}
	}
	b03Write(t, fmt.Sprintf("b03-native-history-%s-%s-%s-%s", meta.StoreID, source.Project, milestone.Name, milestone.CommitID), map[string]any{"source": identity, "milestone": milestone.Name, "commit": milestone.CommitID, "revision": at, "tables": actualTables, "stored_catalog": milestone.Catalog, "historical_filesystem_catalog": "NOT_VERSIONED_BY_NATIVE_SQL_HISTORY"})
	after, err := f.store.Meta(f.ctx)
	if err != nil || meta != after {
		t.Fatal("public native historical reads changed graph metadata", err)
	}
	return metas
}
