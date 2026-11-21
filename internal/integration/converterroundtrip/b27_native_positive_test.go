package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const b27PositiveSourceSHA256 = "eef26070c9c3868b8bcfb37b0bd45c7150d6d9cf9ae3cb5ccd8a0cc90db40e45"

type b27PositiveMember struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type b27PositiveCapsule struct {
	File    string                       `json:"file"`
	SHA256  string                       `json:"sha256"`
	Bytes   int64                        `json:"bytes"`
	Members map[string]b27PositiveMember `json:"members"`
}

type b27PositiveProject struct {
	b27PositiveCapsule
	Source struct {
		Project string `json:"project"`
		StoreID string `json:"store_id"`
		Head    string `json:"head"`
	} `json:"source"`
	CaptureSHA256 string            `json:"capture_sha256"`
	Milestones    map[string]string `json:"milestones"`
}

type b27PositiveInput struct {
	b27NativeInput
	Requirements string
	Binding      b27PositiveProject
}

// Transport extraction changes no member bytes. The complete frozen outer
// catalog, gzip EOF/CRC, and the producer's independent inner catalog all bind
// before any row is interpreted. This is not a portable archive constructor.
func b27PositiveUnpack(t *testing.T, capsule b27PositiveCapsule, prefix string) string {
	t.Helper()
	raw := b27NativeBytes(t, filepath.Join("testdata", capsule.File))
	if int64(len(raw)) != capsule.Bytes || hashBytes(raw) != capsule.SHA256 {
		t.Fatal("positive input transport binding differs", capsule.File)
	}
	root := t.TempDir()
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(z)
	seen := map[string]bool{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		name, ok := strings.CutPrefix(header.Name, prefix)
		if !ok || name == "" || filepath.IsAbs(name) || strings.Contains(name, "\\") || filepath.ToSlash(filepath.Clean(name)) != name || name == ".." || strings.HasPrefix(name, "../") || header.Typeflag != tar.TypeReg || seen[name] {
			t.Fatal("positive input member is unsafe or repeated", header.Name)
		}
		binding, exists := capsule.Members[name]
		if !exists || header.Size != binding.Bytes {
			t.Fatal("positive input member census differs", name)
		}
		data, err := io.ReadAll(io.LimitReader(reader, binding.Bytes+1))
		if err != nil || int64(len(data)) != binding.Bytes || hashBytes(data) != binding.SHA256 {
			t.Fatal("positive input member binding differs", name, err)
		}
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		seen[name] = true
	}
	// tar EOF can precede gzip EOF. Drain to verify the original CRC/footer.
	if _, err := io.Copy(io.Discard, z); err != nil {
		t.Fatal("positive transport gzip footer", err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(capsule.Members) {
		t.Fatal("positive input has missing members")
	}
	return root
}

func b27PositiveInputs(t *testing.T) []b27PositiveInput {
	t.Helper()
	requestBytes := b27NativeBytes(t, filepath.Join("testdata", "b27-native-positive-v2.REQUEST.json"))
	requestSHA := hashBytes(requestBytes)
	if requestSHA != "d00aadceddf0b3d9ec52bd58d43e1cc215e844a6788c52ccd1ba1fb05e996912" {
		t.Fatal("current positive consumer request binding differs")
	}
	var request struct {
		DeliverySHA256     string `json:"delivery_provenance_sha256"`
		RequirementsSHA256 string `json:"requirements_freeze_sha256"`
		PublicBundle       string `json:"public_bundle"`
	}
	b27NativeJSON(t, requestBytes, &request)
	if request.DeliverySHA256 != "63ad1ebc655d05fce5f1a2092c87881bb72d5de242e3626099762ef4807b021e" || request.RequirementsSHA256 != "856a30c84be5871f78872c68cd0093e9ac23ecddfe96c4318a4d74c797c5c635" {
		t.Fatal("current positive request input/requirements binding differs")
	}
	// Whole's canonical preset needs no environment additions. A focused
	// native opt-in additionally binds its caller's exact current request and
	// the existing admitted public executable descriptor once.
	if os.Getenv("PLUG_B27_NATIVE_POSITIVE_RUN") == "1" {
		if os.Getenv("PLUG_B27_NATIVE_POSITIVE_REQUEST_SHA256") != requestSHA || os.Getenv("PLUG_PUBLIC_BUNDLE") != request.PublicBundle {
			t.Fatal("focused positive native opt-in request/descriptor differs")
		}
	} else if os.Getenv("PLUG_B27_NATIVE_POSITIVE_REQUEST_SHA256") != "" {
		t.Fatal("positive request supplied without its native opt-in")
	}
	raw := b27NativeBytes(t, filepath.Join("testdata", "b27-native-positive-v2.SOURCE.json"))
	if hashBytes(raw) != b27PositiveSourceSHA256 {
		t.Fatal("positive input provenance binding differs")
	}
	var source struct {
		Format       string                        `json:"format"`
		ProducerHead string                        `json:"producer_head"`
		Requirements b27PositiveCapsule            `json:"requirements"`
		Projects     map[string]b27PositiveProject `json:"projects"`
	}
	b27NativeJSON(t, raw, &source)
	if source.Format != "b27-native-positive-consumer-inputs/2" || source.ProducerHead != "dc34dd9d9766555c98be36d26bb5273556f5d93d" || len(source.Projects) != 2 || len(source.Requirements.Members) != 48 {
		t.Fatal("positive input source tuple differs")
	}
	requirements := b27PositiveUnpack(t, source.Requirements, "")
	freeze := b27NativeBytes(t, filepath.Join(requirements, "FREEZE.json"))
	if hashBytes(freeze) != "856a30c84be5871f78872c68cd0093e9ac23ecddfe96c4318a4d74c797c5c635" {
		t.Fatal("independent v2 requirements freeze differs")
	}
	var frozen struct {
		Members []struct {
			b27PositiveMember
			Path string `json:"path"`
		} `json:"members"`
	}
	b27NativeJSON(t, freeze, &frozen)
	if len(frozen.Members) != 47 {
		t.Fatal("independent v2 requirements membership differs")
	}
	for _, member := range frozen.Members {
		if source.Requirements.Members[member.Path] != member.b27PositiveMember {
			t.Fatal("independent v2 member commitment differs", member.Path)
		}
	}
	var inputs []b27PositiveInput
	stores := map[string]bool{}
	for _, project := range []string{"b27-review", "b27-delivery"} {
		binding, exists := source.Projects[project]
		if !exists || binding.Source.Project != project || binding.Source.StoreID == "" || stores[binding.Source.StoreID] || len(binding.Members) != 636 {
			t.Fatal("positive source namespaces differ", project)
		}
		stores[binding.Source.StoreID] = true
		root := b27PositiveUnpack(t, binding.b27PositiveCapsule, "native-source/")
		var inner map[string]string
		b27NativeJSON(t, b27NativeBytes(t, filepath.Join(root, "INNER-HASHES.json")), &inner)
		if len(inner) != 635 {
			t.Fatal("positive inner member census differs")
		}
		for name, member := range binding.Members {
			if name != "INNER-HASHES.json" && inner[name] != member.SHA256 {
				t.Fatal("positive inner member digest differs", name)
			}
		}
		if !bytes.Equal(freeze, b27NativeBytes(t, filepath.Join(root, "consumer-requirements", "FREEZE.json"))) {
			t.Fatal("producer input does not bind the independent requirements")
		}
		input := b27PositiveInput{
			b27NativeInput: b27NativeInput{Project: project, StoreID: binding.Source.StoreID, Head: binding.Source.Head, CaptureSHA256: binding.CaptureSHA256, dir: root, producerHead: source.ProducerHead},
			Requirements:   requirements, Binding: binding,
		}
		b27NativeJSON(t, b27NativeBytes(t, filepath.Join(root, "raw-current.json")), &input.current)
		if len(input.current) != 33 {
			t.Fatal("positive original SQL family census differs")
		}
		capture := filepath.Join(root, "capture", "capture-set")
		captureRoot := b27NativeBytes(t, filepath.Join(capture, "capture-set.json"))
		var set dumpformat.NativeCaptureSet
		b27NativeJSON(t, captureRoot, &set)
		if hashBytes(captureRoot) != binding.CaptureSHA256 || set.StoreID != input.StoreID || set.Head != input.Head || !reflect.DeepEqual(set.Projects, []string{project}) || set.PartCount != 1 {
			t.Fatal("positive capture root/head/source binding differs")
		}
		// The SQL oracle above is deliberately only the 33-table query image.
		// Bind the complete physical current image independently from the
		// authenticated capture set so physical-only Dolt tables remain exact
		// readback obligations rather than being silently dropped from census.
		physicalArchive, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), capture, dumpformat.NativeCaptureSetOptions{})
		if err != nil {
			t.Fatal("authenticate positive physical current image", err)
		}
		histories := physicalArchive.NativeSourceHistories()
		physicalArchive.ReleaseNativeDecoded()
		if len(histories) != 1 || histories[0].StoreID != input.StoreID || histories[0].Head != input.Head || len(histories[0].Current) != 36 {
			t.Fatal("positive physical current source census differs")
		}
		input.physicalCurrent = histories[0].Current
		b27PositiveRequirements(t, input)
		b27PositiveSchemas(t, input)
		inputs = append(inputs, input)
	}
	return inputs
}

type b27PositiveConstraint struct {
	Kind       string `json:"kind"`
	Value      string `json:"value"`
	Base64     string `json:"utf8_base64"`
	Token      string `json:"decimal_token"`
	SQL        string `json:"sql_literal"`
	TypedValue map[string]struct {
		Kind  string `json:"kind"`
		Value any    `json:"value"`
		Token string `json:"decimal_token"`
	} `json:"typed_value"`
}

// Match only consumer-authored constants here. Dynamic IDs/times/JSON tokens
// stay bound to the complete original pre-capture table, never to the request
// spelling or imported output. The shared oracle additionally checks every
// physical cell, SQL type, primary key, schema and state-root commitment.
func b27PositiveCell(c b27PositiveConstraint, value any, present bool) bool {
	if !present {
		return false
	}
	switch c.Kind {
	case "text":
		text, ok := value.(string)
		return ok && text == c.Value && base64.StdEncoding.EncodeToString([]byte(text)) == c.Base64
	case "integer":
		n, ok := value.(json.Number)
		return ok && n.String() == c.Token
	case "sql-null":
		return value == nil
	case "datetime":
		text, ok := value.(string)
		want, err := time.Parse("2006-01-02 15:04:05", c.SQL)
		got, parseErr := time.Parse(time.RFC3339Nano, text)
		return ok && err == nil && parseErr == nil && got.Equal(want)
	case "source-bound-symbol":
		return value != nil
	case "source-bound-json", "source-bound-json-text", "json":
		raw, err := json.Marshal(value)
		if text, ok := value.(string); ok {
			raw = []byte(text)
		}
		if err != nil || !json.Valid(raw) {
			return false
		}
		if c.Kind != "json" {
			return true
		}
		want := map[string]any{}
		for key, cell := range c.TypedValue {
			switch cell.Kind {
			case "number":
				want[key] = json.Number(cell.Token)
			case "json-null":
				want[key] = nil
			case "string", "boolean":
				want[key] = cell.Value
			default:
				return false
			}
		}
		var got any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		return decoder.Decode(&got) == nil && reflect.DeepEqual(got, want)
	default:
		return false
	}
}

func b27PositiveMilestone(t *testing.T, input b27PositiveInput, name string) (string, map[string]dumpformat.NativeTable) {
	t.Helper()
	var milestone struct {
		CommitID string                            `json:"commit_id"`
		Tables   map[string]dumpformat.NativeTable `json:"tables"`
	}
	b27NativeJSON(t, b27NativeBytes(t, filepath.Join(input.dir, "milestones", name+".json")), &milestone)
	if milestone.CommitID == "" || milestone.CommitID != input.Binding.Milestones[name] {
		t.Fatal("positive original milestone binding differs", name)
	}
	return milestone.CommitID, milestone.Tables
}

func b27PositiveRequirements(t *testing.T, input b27PositiveInput) {
	t.Helper()
	for _, file := range []string{"cli-current-projections.ndjson", "synthetic-current.ndjson"} {
		raw := b27NativeBytes(t, filepath.Join(input.Requirements, "expectations", input.Project, file))
		count := 0
		for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
			var requirement struct {
				Project   string                           `json:"project"`
				Family    string                           `json:"family"`
				Milestone string                           `json:"milestone"`
				RowMode   string                           `json:"row_mode"`
				Cells     map[string]b27PositiveConstraint `json:"cells"`
			}
			b27NativeJSON(t, line, &requirement)
			if requirement.Project != input.Project || requirement.Family == "local_metadata" || len(requirement.Cells) == 0 {
				t.Fatal("invalid independent positive row requirement")
			}
			tables := input.current
			if requirement.Milestone != "final" {
				_, tables = b27PositiveMilestone(t, input, requirement.Milestone)
			}
			matches := 0
			for _, row := range tables[requirement.Family].Rows {
				ok := true
				for column, constraint := range requirement.Cells {
					value, present := row[column]
					ok = ok && b27PositiveCell(constraint, value, present)
				}
				if ok {
					if requirement.RowMode == "complete-column-map" && len(row) != len(requirement.Cells) {
						t.Fatal("independent synthetic complete-column census differs", requirement.Family)
					}
					matches++
				}
			}
			if matches != 1 {
				t.Fatal("original source does not satisfy the independent positive requirement", input.Project, requirement.Family, requirement.Milestone, matches)
			}
			count++
		}
		want := 14
		if file == "synthetic-current.ndjson" {
			want = 3
		}
		if count != want {
			t.Fatal("independent positive row requirement census differs", file)
		}
	}
	_, closed := b27PositiveMilestone(t, input, "historical-closed")
	_, reopened := b27PositiveMilestone(t, input, "historical-reopened")
	find := func(table dumpformat.NativeTable, status string) dumpformat.Row {
		for _, row := range table.Rows {
			if row["id"] == input.Project+"-dxz" && row["status"] == status {
				return row
			}
		}
		t.Fatal("genuine minted historical close/reopen control missing", status)
		return nil
	}
	before, after := find(closed["issues"], "closed"), find(reopened["issues"], "open")
	if before["close_reason"] != "B27 historical close 雪\nexact" || before["closed_at"] == nil || after["id"] != before["id"] || after["closed_at"] != nil {
		t.Fatal("genuine native history-close/reopen original differs")
	}
	if len(input.current["routes"].Rows) != 1 || len(input.current["interactions"].Rows) != 1 || len(input.current["issue_snapshots"].Rows) != 1 || len(input.current["compaction_snapshots"].Rows) != 0 {
		t.Fatal("synthetic family scope differs")
	}
}

func b27PositiveSchemas(t *testing.T, input b27PositiveInput) {
	t.Helper()
	var declared struct {
		Tables map[string]struct {
			Columns []struct {
				Name    string `json:"name"`
				SQLType string `json:"sql_type"`
			} `json:"columns"`
			PrimaryKey []string `json:"primary_key"`
		} `json:"tables"`
	}
	b27NativeJSON(t, b27NativeBytes(t, filepath.Join(input.Requirements, "EXPECTED-SEED-SCHEMAS.json")), &declared)
	var observed map[string][]struct {
		Name              string
		DatabaseTypeName  string
		PrimaryKeyOrdinal *uint64
	}
	b27NativeJSON(t, b27NativeBytes(t, filepath.Join(input.dir, "physical-current-schema.json")), &observed)
	if len(declared.Tables) != 3 || len(observed) != 36 {
		t.Fatal("positive original/declared physical schema census differs")
	}
	for family, table := range declared.Tables {
		columns := observed[family]
		if len(columns) != len(table.Columns) {
			t.Fatal("positive synthetic physical column census differs", family)
		}
		// Source ordinal order is authoritative; declaration order is not a
		// license to reorder the actual physical cells. Match names/types/PK,
		// leaving original ordinal bytes to the independent physical oracle.
		types := map[string]string{}
		pk := make([]string, len(table.PrimaryKey))
		for _, column := range columns {
			types[column.Name] = strings.ToUpper(column.DatabaseTypeName)
			if column.PrimaryKeyOrdinal != nil {
				if *column.PrimaryKeyOrdinal >= uint64(len(pk)) || pk[*column.PrimaryKeyOrdinal] != "" {
					t.Fatal("positive physical PK ordinal differs", family)
				}
				pk[*column.PrimaryKeyOrdinal] = column.Name
			}
		}
		for _, column := range table.Columns {
			if types[column.Name] != strings.ToUpper(column.SQLType) {
				t.Fatal("positive synthetic physical SQL type differs", family, column.Name)
			}
		}
		if !reflect.DeepEqual(pk, table.PrimaryKey) {
			t.Fatal("positive synthetic physical primary key differs", family)
		}
	}
	// The complete physical schema is a separate source commitment from the
	// 33-table SQL oracle. Check every physical-only table's column names too;
	// their rows are compared later against the authenticated native image.
	for family, table := range input.physicalCurrent {
		columns, exists := observed[family]
		if !exists || len(columns) != len(table.Columns) {
			t.Fatal("positive physical-only schema census differs", family)
		}
		wantNames := map[string]bool{}
		for _, name := range table.Columns {
			if wantNames[name] {
				t.Fatal("positive physical source repeats a column", family, name)
			}
			wantNames[name] = true
		}
		for _, column := range columns {
			if !wantNames[column.Name] {
				t.Fatal("positive physical-only schema column differs", family, column.Name)
			}
		}
	}
}

func TestB27NativePositiveAuthenticatedRequirements(t *testing.T) {
	for _, input := range b27PositiveInputs(t) {
		t.Run(input.Project, func(t *testing.T) {
			archive, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), filepath.Join(input.dir, "capture", "capture-set"), dumpformat.NativeCaptureSetOptions{})
			if err != nil {
				t.Fatal("authenticate supplied positive capture", err)
			}
			defer archive.ReleaseNativeDecoded()
			req := b27PositiveRequest(t, input)
			states := publicProofStates(t, req, publicProofLedger(t, req))
			if req.Commits != 100 || len(states.Commits) != 100 || states.Head.CommitID != input.Head || archive.SHA256() != input.CaptureSHA256 {
				t.Fatal("positive complete original physical history differs")
			}
			t.Log("authenticated_input_only consumer_runtime=NOT_RUN synthetic=rows_not_CLI portable_two_project_Full=NOT_SUPPLIED")
		})
	}
}

func TestB27NativePositivePortableInputRefusalBoundary(t *testing.T) {
	// The transport format is authenticated by the input loader first.
	// Its refusal at portable Read is a boundary result, never two-project
	// Full evidence. No bytes are rewritten to make the formats compatible.
	for _, input := range b27PositiveInputs(t) {
		t.Run(input.Project, func(t *testing.T) {
			raw := b27NativeBytes(t, filepath.Join("testdata", input.Binding.File))
			archive, err := dumpformat.Read(bytes.NewReader(raw))
			if archive != nil {
				archive.ReleaseNativeDecoded()
			}
			if err == nil || archive != nil {
				t.Fatal("source transport was admitted as a portable Plug archive")
			}
			t.Log("portable_pre_Merge_refusal=true two_project_Full=NOT_SUPPLIED")
		})
	}
}

func b27PositiveRequest(t *testing.T, input b27PositiveInput) publicProofRequest {
	t.Helper()
	req := b27NativeRequest(t, input.b27NativeInput, "full")
	req.RestoreTime = time.Date(2026, 11, 12, 23, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	if req.Commits != 100 {
		t.Fatal("positive original commit census differs")
	}
	return req
}

func b27PositiveDocuments(t *testing.T, store driver.Store, input b27PositiveInput, archive *dumpformat.Archive, appended string) {
	t.Helper()
	rows := b27NativeRows(t, store, nil)
	originalIDs := map[string]bool{}
	unresolved := 0
	for _, source := range input.current["issues"].Rows {
		id := source["id"].(string)
		originalIDs[id] = true
		record, exists := rows[id]
		if !exists || record.Kind != "bead" {
			t.Fatal("positive imported document missing", id)
		}
		doc, err := pipeline.DecodeDocument(record.Body)
		if err != nil {
			t.Fatal(err)
		}
		// The original explicit map_type maps the used custom kind to issue.
		if doc.ID != id || doc.Project != input.Project || doc.Type != "issue" {
			t.Fatal("positive document identity/type differs", id)
		}
		for _, name := range []string{"title", "description", "priority", "status"} {
			b27NativeEqual(t, id+"/"+name, doc.Fields[name], source[name])
		}
		var retained map[string]any
		b27NativeJSON(t, doc.Metadata["beads.source_fields"], &retained)
		admitted := map[string]any{}
		for name, value := range source {
			if name != "row_lock" && name != "is_blocked" {
				admitted[name] = value
			}
		}
		b27NativeEqual(t, id+"/complete-admitted-fields", retained, admitted)
		expectedComments := 0
		for _, comment := range input.current["comments"].Rows {
			if comment["issue_id"] == id {
				expectedComments++
			}
		}
		b27NativeSpecificFields(t, input.b27NativeInput, source, doc, true, expectedComments)
		labels := []string{}
		for _, row := range input.current["labels"].Rows {
			if row["issue_id"] == id {
				labels = append(labels, row["label"].(string))
			}
		}
		sort.Strings(labels)
		b27NativeEqual(t, id+"/labels", append([]string{}, doc.Labels...), labels)
		var parent string
		links := map[string]model.LinkTargetKind{}
		for _, edge := range input.current["dependencies"].Rows {
			if edge["issue_id"] != id {
				continue
			}
			arm, target := b27NativeDependencyTarget(t, edge)
			kind := edge["type"].(string)
			if kind == "parent-child" {
				if parent != "" || arm != "depends_on_issue_id" {
					t.Fatal("positive original parent is ambiguous", id)
				}
				parent = target
				continue
			}
			if kind == "related" {
				kind = "relates-to"
			}
			targetKind := model.LinkTargetKind("")
			if arm == "depends_on_external" {
				targetKind = model.LinkTargetExternal
				unresolved++
				if _, invented := rows[target]; invented {
					t.Fatal("unresolved target became a phantom local record")
				}
			}
			links[kind+"\x00"+target] = targetKind
		}
		if (parent == "" && doc.Parent != nil) || (parent != "" && (doc.Parent == nil || !doc.Parent.IsID() || doc.Parent.ID != parent || !doc.Parent.Branch.IsZero())) {
			t.Fatal("positive parent differs", id)
		}
		if len(doc.Links) != len(links) {
			t.Fatal("positive unresolved/local link census differs", id)
		}
		for _, link := range doc.Links {
			key := link.Kind + "\x00" + link.To.ID
			kind, exists := links[key]
			if !exists || !link.To.IsID() || !link.To.Branch.IsZero() || (kind == model.LinkTargetExternal && link.TargetKind != kind) || (kind != model.LinkTargetExternal && link.TargetKind == model.LinkTargetExternal) {
				t.Fatal("positive relation arm/value differs", id)
			}
			delete(links, key)
		}
		if id == input.Project+"-f5a" {
			want, err := time.Parse(time.RFC3339Nano, source["closed_at"].(string))
			if err != nil || doc.ClosedAt == nil || !doc.ClosedAt.Equal(want) || retained["close_reason"] != "B27 native current close 雪\nexact reason" {
				t.Fatal("genuine current native close value differs")
			}
		}
	}
	if unresolved != 1 || len(originalIDs) != 6 {
		t.Fatal("positive independent durable relation/issue census differs")
	}
	for id, row := range rows {
		if row.Kind == "bead" && !originalIDs[id] && id != appended {
			t.Fatal("unexpected positive imported document", id)
		}
	}
	// Check the independently observed original comment, including source
	// author normalization, exact Unicode/newlines and source/restore clocks.
	var comments []b27CommentTuple
	for _, row := range input.current["comments"].Rows {
		var tuple b27CommentTuple
		b27NativeJSON(t, encode(t, row), &tuple)
		tuple.Project, tuple.Table = input.Project, "comments"
		comments = append(comments, tuple)
	}
	ordered := b27NativeCommentOrder(t, archive, input.Project, comments)
	for id, tuples := range ordered {
		doc, err := pipeline.DecodeDocument(rows[id].Body)
		if err != nil || len(doc.Comments) != len(tuples) {
			t.Fatal("positive imported comment census differs", id)
		}
		for i, tuple := range tuples {
			sourceTime, err := time.Parse(time.RFC3339Nano, tuple.CreatedAt)
			author, authorErr := pipeline.NormalizeSourceAuthor(tuple.Author)
			comment := doc.Comments[i]
			if err != nil || authorErr != nil || comment.Body != tuple.Text || comment.Origin != "import" || comment.SourceAuthor != author || comment.SourceTime == nil || !comment.SourceTime.Equal(sourceTime) {
				t.Fatal("positive imported comment value differs", id)
			}
		}
	}
}

func b27PositiveCatalog(t *testing.T, store driver.Store, input b27PositiveInput, archive *dumpformat.Archive) {
	t.Helper()
	// These actual YAML bytes contain no custom type/status declaration.
	// Preserve that source absence; the distinct YAML-only positive remains
	// NOT_SUPPLIED under CONFIG-AND-METADATA.json, never a guessed union.
	yaml := b27NativeBytes(t, filepath.Join(input.dir, "filesystem", "config.yaml"))
	if !bytes.Contains(yaml, []byte("\nno-push: true\n")) || !bytes.Contains(yaml, []byte("\nrouting:\n    mode: explicit\n")) {
		t.Fatal("positive captured public YAML controls differ")
	}
	for _, line := range strings.Split(string(yaml), "\n") {
		if line == "types:" || line == "status:" || strings.HasPrefix(line, "  custom:") {
			t.Fatal("positive YAML custom-declaration absence changed")
		}
	}
	var builtin []dumpformat.ProjectStatus
	b27NativeJSON(t, b27NativeBytes(t, filepath.Join("testdata", "native-built-in-statuses.json")), &builtin)
	statuses := map[string]string{}
	for _, status := range builtin {
		statuses[status.Name] = status.Category
	}
	custom := map[string]string{"b27_ready": "active", "b27_review": "wip", "b27_done": "done", "b27_frozen": "frozen"}
	gotCustom := map[string]string{}
	for _, row := range input.current["custom_statuses"].Rows {
		gotCustom[row["name"].(string)] = row["category"].(string)
	}
	if !reflect.DeepEqual(gotCustom, custom) {
		t.Fatal("positive independent stored custom status declarations differ")
	}
	for name, category := range custom {
		statuses[name] = category
	}
	types := []string{}
	for _, row := range input.current["custom_types"].Rows {
		types = append(types, row["name"].(string))
	}
	sort.Strings(types)
	if !reflect.DeepEqual(types, []string{"b27_kind", "unused_b27_kind"}) {
		t.Fatal("positive independent stored custom type declarations differ")
	}
	rows := b27NativeRows(t, store, nil)
	var installed dumpformat.NativeStatusOntology
	b27NativeJSON(t, rows[ImportedOntologySourceID].Body, &installed)
	overlay, exists := installed.Overlays[input.Project]
	got := map[string]string{}
	for name, status := range overlay.Statuses {
		got[name] = string(status.Category)
	}
	if !exists || !reflect.DeepEqual(got, statuses) {
		t.Fatal("positive installed SQL/YAML status union differs")
	}
	directives := archive.Manifest().Ontology
	if len(directives) != 1 || directives[0].Kind != "map_type" || directives[0].From != "b27_kind" || directives[0].To != "issue" || directives[0].Target.Project != input.Project || directives[0].Target.Type != "b27_kind" || directives[0].Target.ID != "" {
		t.Fatal("positive original explicit type disposition differs")
	}
	t.Log("positive_catalog stored_names=exact YAML_custom_origins=absent distinct_YAML_positive=NOT_SUPPLIED unused_source_types=retained")
}

// This is the existing public dump manager, with a test-owned trusted
// presence verifier bound to the actual immutable export digest. It does not
// claim HTTP authentication, native CLI emission or a second importer.
func b27PositiveExport(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease) ([]byte, dumpformat.Manifest) {
	t.Helper()
	want, manifest, err := restore.Dump(ctx, store, "full")
	if err != nil {
		t.Fatal("positive full public dump", err)
	}
	digest := hashBytes(want)
	proof := restore.Presence{"mode": "presence_check", "challenge": "b27-positive-export", "proof": digest}
	manager := restore.NewManager(store, lease, restore.PresenceVerifierFunc(func(_ context.Context, method, got string, presence restore.Presence) error {
		if method != "admin.dump" || got != digest || !reflect.DeepEqual(presence, proof) {
			return errors.New("positive public dump authority binding differs")
		}
		return nil
	}))
	var out bytes.Buffer
	cursor, dumpID := "", ""
	pages := uint64(0)
	for {
		page, err := manager.DumpPage(ctx, "full", cursor, 32768, proof)
		if err != nil || page.Page != pages || page.SHA256 != hashBytes(page.Data) || (dumpID != "" && page.DumpID != dumpID) {
			t.Fatal("positive public dump page binding differs", err)
		}
		dumpID = page.DumpID
		_, _ = out.Write(page.Data)
		pages++
		if page.Done {
			if page.NextPage != nil {
				t.Fatal("terminal public dump page has a cursor")
			}
			break
		}
		if page.NextPage == nil || *page.NextPage != pages {
			t.Fatal("public dump page did not advance")
		}
		cursor = fmt.Sprintf("%s:%d", dumpID, *page.NextPage)
	}
	if !bytes.Equal(want, out.Bytes()) || pages < 2 {
		t.Fatal("positive public paged export differs from its pinned snapshot")
	}
	members := tarMembers(t, out.Bytes())
	var exported dumpformat.Manifest
	b27NativeJSON(t, members["manifest.json"], &exported)
	if !reflect.DeepEqual(manifest, exported) || exported.Scope != "plug-native-store" || exported.NativeHead == "" {
		t.Fatal("positive public native export manifest differs")
	}
	// All exported records, including opaque native commitments, are compared
	// as exact envelopes. Machine-local values are never decoded or printed.
	current := b27NativeRows(t, store, nil)
	seen := map[string]bool{}
	decoder := json.NewDecoder(bytes.NewReader(members["records/native.ndjson"]))
	for {
		var row struct {
			Ref      model.Ref       `json:"ref"`
			Kind     string          `json:"kind"`
			Revision model.Revision  `json:"revision"`
			Body     json.RawMessage `json:"body"`
		}
		if err := decoder.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal("positive public exported record", err)
		}
		original, exists := current[row.Ref.ID]
		if !exists {
			t.Fatalf("positive public export changed an existing record %s: ID expected in current rows, actual exported", row.Ref.ID)
		}
		if seen[row.Ref.ID] {
			t.Fatalf("positive public export changed an existing record %s: duplicate ID, actual exported twice", row.Ref.ID)
		}
		canonical, canonicalErr := json.Marshal(json.RawMessage(original.Body))
		ref := original.Ref
		if ref.IsID() && ref.Branch.IsZero() {
			ref.Branch = model.Natural64(0)
		}
		if ref.IsID() {
			ref = model.RefByID(ref.ID, ref.Branch)
		} else if ref.Key != "" {
			ref = model.RefByKey(ref.Key)
		}
		if row.Ref.IsID() {
			row.Ref = model.RefByID(row.Ref.ID, row.Ref.Branch)
		} else if row.Ref.Key != "" {
			row.Ref = model.RefByKey(row.Ref.Key)
		}
		if !reflect.DeepEqual(ref, row.Ref) {
			t.Fatalf("positive public export changed an existing record %s: Ref expected=%v actual=%v", row.Ref.ID, ref, row.Ref)
		}
		if original.Kind != row.Kind {
			t.Fatalf("positive public export changed an existing record %s: kind expected=%q actual=%q", row.Ref.ID, original.Kind, row.Kind)
		}
		if original.Revision != row.Revision {
			t.Fatalf("positive public export changed an existing record %s: revision expected=%v actual=%v", row.Ref.ID, original.Revision, row.Revision)
		}
		if canonicalErr != nil {
			t.Fatalf("positive public export changed an existing record %s: body expected=canonicalization-success actual=error=%v", row.Ref.ID, canonicalErr)
		}
		if !bytes.Equal(canonical, row.Body) {
			t.Fatalf("positive public export changed an existing record %s: body expected=%s actual=%s", row.Ref.ID, canonical, row.Body)
		}
		seen[row.Ref.ID] = true
	}
	if len(seen) != len(current) || exported.RecordCount != len(current) {
		t.Fatal("positive public export record census differs")
	}
	return out.Bytes(), exported
}

func b27PositivePublicNonemptyRefusal(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, actor model.PrincipalRef, manifest dumpformat.Manifest) {
	t.Helper()
	manifest.History = "head"
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := hashBytes(raw)
	proof := restore.Presence{"mode": "presence_check", "challenge": "b27-positive-refusal", "proof": digest}
	manager := restore.NewManager(store, lease, restore.PresenceVerifierFunc(func(_ context.Context, method, got string, presence restore.Presence) error {
		if method != "admin.restore.begin" || got != digest || !reflect.DeepEqual(presence, proof) {
			return errors.New("positive public restore authority binding differs")
		}
		return nil
	}))
	var request map[string]any
	b27NativeJSON(t, raw, &request)
	before := b27NativeConservationAt(t, store)
	_, err = manager.BeginAs(ctx, actor, request, "snapshot", proof)
	var typed *model.Error
	if !errors.As(err, &typed) || typed.Data.Code != "restore_requires_empty" {
		t.Fatal("positive public restore lost its typed nonempty refusal")
	}
	if !reflect.DeepEqual(before, b27NativeConservationAt(t, store)) {
		t.Fatal("positive public refusal changed native state")
	}
}

func b27PositiveHistoryControls(t *testing.T, store driver.Store, input b27PositiveInput, report Report) {
	t.Helper()
	rows := b27NativeRows(t, store, nil)
	var evidence driver.HistoryImportEvidence
	b27NativeJSON(t, rows[driver.OfflineHistoryEvidenceID].Body, &evidence)
	if evidence.Mode != driver.RestoreHistoryModeFull || !evidence.Complete || len(evidence.Mapping) != 100 || !evidence.Floor.IsZero() {
		t.Fatal("positive complete physical history mapping differs")
	}
	for _, name := range []string{"closed-current", "historical-closed", "historical-reopened", "native-appended", "final"} {
		commit, tables := b27PositiveMilestone(t, input, name)
		var at model.Revision
		matches := 0
		for _, mapping := range evidence.Mapping {
			if mapping.SourceStoreID == input.StoreID && mapping.Project == input.Project && reflect.DeepEqual(mapping.CommitIDs, []string{commit}) {
				at, matches = mapping.Revision, matches+1
			}
		}
		if matches != 1 {
			t.Fatal("positive named native milestone mapping differs", name)
		}
		reader, err := store.Read(context.Background(), driver.View{Incarnation: report.Restored.Incarnation, At: &at})
		if err != nil {
			t.Fatal(err)
		}
		groups, err := queryNativeRowGroups(context.Background(), reader, report.Restored.Incarnation, at, report.Archive)
		closeErr := reader.Close()
		if err != nil || closeErr != nil {
			t.Fatal("positive historical native readback", err, closeErr)
		}
		families := []string{"issues", "dependencies", "labels", "child_counters"}
		if name == "provenance" || name == "native-appended" || name == "final" {
			families = append(families, "provenance_events")
		}
		if name == "final" {
			families = append(families, "routes", "interactions", "issue_snapshots")
		}
		for _, family := range families {
			want, exists := tables[family]
			if !exists {
				t.Fatal("positive original milestone family absent", name, family)
			}
			source := dumpformat.SourceIdentity{StoreID: input.StoreID, Project: input.Project}
			currentHistory := ""
			for _, nativeFamily := range report.NativeFamilies {
				if nativeFamily.Source == source && nativeFamily.Family == family {
					currentHistory = nativeFamily.History
					break
				}
			}
			if currentHistory == "" {
				t.Fatal("positive imported family history disposition absent", family)
			}
			// A milestone pinned to an historical commit uses the retained
			// commit namespace; only the current image uses its family-specific
			// history disposition.
			history := currentHistory
			records, exists := groups[nativeRowGroup{Source: source, Table: family, History: history}]
			if !exists {
				history = "retained-native-commit"
				records, exists = groups[nativeRowGroup{Source: source, Table: family, History: history}]
			}
			got, _, err := dumpformat.ReconstructNativeRowTable(source, family, history, records, nil)
			if !exists || err != nil {
				t.Fatal("positive historical family missing", name, family, err)
			}
			b27NativeTableEqual(t, input.Project+"/"+name+"/"+family, got, want)
		}
	}
}

// Each project stays a separate authenticated source. The shared lifecycle
// owns import, both physical SQL reopens, recovery and full independent typed
// readback. Its callbacks add assertions and one canonical public write.
func TestB27NativePositiveStrictImportAppendExportAndReopen(t *testing.T) {
	for _, input := range b27PositiveInputs(t) {
		t.Run(input.Project, func(t *testing.T) {
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
					if output == "" {
						t.Fatal("canonical Bazel output required")
					}
					name := strings.ReplaceAll(t.Name(), "/", "_")
					req := b27PositiveRequest(t, input)
					raw := encode(t, req)
					path := filepath.Join(output, name+"-request.json")
					if err := os.WriteFile(path, raw, 0600); err != nil {
						t.Fatal(err)
					}
					self, err := os.Executable()
					if err != nil {
						t.Fatal(err)
					}
					t.Setenv("PLUG_PUBLIC_PROOF_DRIVER", engine)
					t.Setenv("PLUG_PUBLIC_PROOF_ELF_SHA256", publicProofHashFile(t, self))
					t.Setenv("PLUG_PUBLIC_PROOF_REQUEST", path)
					t.Setenv("PLUG_PUBLIC_PROOF_REQUEST_SHA256", hashBytes(raw))
					t.Setenv("PLUG_PUBLIC_PROOF_OUTPUT", filepath.Join(output, name))
					var appended string
					var final b27NativeConservation
					var finalExport string
					runPublicNativeExternalCaptureWithPlan(t, b27NativeIntent, func(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error {
						b27NativeCurrent(t, store, report.Archive, []b27NativeInput{input.b27NativeInput}, report)
						b27PositiveDocuments(t, store, input, report.Archive, "")
						b27PositiveCatalog(t, store, input, report.Archive)
						b27PositiveHistoryControls(t, store, input, report)
						before := b27NativeConservationAt(t, store)
						beforeExport, beforeManifest := b27PositiveExport(t, ctx, store, lease)
						b27NativeNonemptyRefusal(t, ctx, store, lease, profile, report.Archive, req.Capture)
						b27PositivePublicNonemptyRefusal(t, ctx, store, lease, profile.Admission.Actor, beforeManifest)
						afterRefusal, _ := b27PositiveExport(t, ctx, store, lease)
						if !bytes.Equal(beforeExport, afterRefusal) {
							t.Fatal("typed nonempty refusal changed public export")
						}
						writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)), pipeline.WithIDGenerator(func() (string, error) {
							return "", errors.New("restored project allocator was bypassed")
						}))
						change, err := writer.Apply(ctx, profile.Admission.Actor, model.ChangeSet{Ops: []model.Op{{
							Kind: model.OpCreate,
							Create: &model.CreateOp{Type: "issue", Project: input.Project, Fields: map[string]any{
								"title": "B27 post-import public append 雪", "description": "independent append preservation control",
							}},
						}}}, model.WriteMeta{Method: "tx.apply", Project: input.Project, IdempotencyKey: hashBytes([]byte(name + "/public-append"))})
						if err != nil || len(change.Results) != 1 {
							return fmt.Errorf("positive public append: %w", err)
						}
						appended = change.Results[0].Ref.ID
						suffix, ok := strings.CutPrefix(appended, input.Project+"-")
						if !ok || len(suffix) < 3 || len(suffix) > 8 || strings.Trim(suffix, "0123456789abcdefghijklmnopqrstuvwxyz") != "" || before.Rows[appended] != "" {
							t.Fatal("positive public append did not use its imported allocator profile")
						}
						final = b27NativeConservationAt(t, store)
						head, err := before.Meta.Head.Uint64()
						if err != nil || final.Meta.Head != model.Natural64(head+1) || final.Meta.StoreID != before.Meta.StoreID || final.Meta.Incarnation != before.Meta.Incarnation || final.Meta.HistoryFloor != before.Meta.HistoryFloor || len(final.Rows) != len(before.Rows)+1 {
							t.Fatal("public append changed identity/floor or unexpected records/revisions")
						}
						for id, digest := range before.Rows {
							if final.Rows[id] != digest {
								t.Fatal("public append deleted or rewrote an original record", id)
							}
						}
						newRow := b27NativeRows(t, store, nil)[appended]
						newDoc, err := pipeline.DecodeDocument(newRow.Body)
						if err != nil || newDoc.ID != appended || newDoc.Project != input.Project || newDoc.Type != "issue" || newDoc.Fields["title"] != "B27 post-import public append 雪" || newDoc.Fields["description"] != "independent append preservation control" {
							t.Fatal("independently declared public append fields differ")
						}
						b27PositiveDocuments(t, store, input, report.Archive, appended)
						b27NativeCurrent(t, store, report.Archive, []b27NativeInput{input.b27NativeInput}, report)
						exported, _ := b27PositiveExport(t, ctx, store, lease)
						finalExport = hashBytes(exported)
						b27NativeNonemptyRefusal(t, ctx, store, lease, profile, report.Archive, req.Capture)
						return nil
					}, func(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, _ driver.RestoreImportProfile, report Report) error {
						if appended == "" || !reflect.DeepEqual(final, b27NativeConservationAt(t, store)) {
							t.Fatal("physical reopen changed append/refusal conservation")
						}
						b27PositiveDocuments(t, store, input, report.Archive, appended)
						b27PositiveCatalog(t, store, input, report.Archive)
						b27NativeCurrent(t, store, report.Archive, []b27NativeInput{input.b27NativeInput}, report)
						b27PositiveHistoryControls(t, store, input, report)
						exported, _ := b27PositiveExport(t, ctx, store, lease)
						if hashBytes(exported) != finalExport {
							t.Fatal("physical SQL reopen changed exact public full export")
						}
						publicProofJSON(t, filepath.Join(output, name+"-positive-summary.json"), map[string]any{
							"project": input.Project, "store_id": input.StoreID, "capture_sha256": input.CaptureSHA256,
							"driver": engine, "request_sha256": hashBytes(raw), "source_commits": 100, "public_append": appended,
							"full_export_sha256": finalExport, "synthetic_qualification": "row preservation, not native CLI behavior",
							"physical_SQL_reopen": engine != "memory", "portable_two_project_Full": "NOT_SUPPLIED",
						})
						return nil
					})
				})
			}
		})
	}
}
