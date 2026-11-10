package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/plugdump"
	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/schema"
)

func boundaryIgnoredPrefix(t *testing.T, ctx context.Context, conn *sql.Conn) {
	t.Helper()
	for _, name := range ownVersionUnversioned {
		ownVersionExec(t, ctx, conn, "INSERT INTO dolt_ignore(pattern,ignored) VALUES (?,true) ON DUPLICATE KEY UPDATE ignored=true", name)
	}
	ownVersionExec(t, ctx, conn, "CREATE TABLE IF NOT EXISTS ignored_schema_migrations (version INT PRIMARY KEY, applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, content_hash CHAR(64))")
	for i, name := range ownVersionIgnoredMigrations[:26] {
		query, err := schema.IgnoredMigrationSQL(name)
		if err != nil {
			t.Fatal(err)
		}
		ownVersionExec(t, ctx, conn, query)
		ownVersionExec(t, ctx, conn, "INSERT INTO ignored_schema_migrations(version,content_hash) VALUES (?,?)", i+1, fmt.Sprintf("%x", sha256.Sum256([]byte(query))))
	}
}

func boundarySourceMember(t *testing.T, archive []byte) []byte {
	t.Helper()
	r := tar.NewReader(bytes.NewReader(archive))
	for {
		h, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == plugdump.SourceHistoryMember {
			data, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			return data
		}
	}
}

func TestPublicGenuineShallowHistoryBoundaryRetainsVerifiedCellsAndEvents(t *testing.T) {
	binary := diagnosticBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	sourceDir, cloneDir := diagnosticDir(t), diagnosticDir(t)
	db, closeDriver, err := openPublicMigrationFixture(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(); _ = closeDriver() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ownVersionExec(t, ctx, conn, "CREATE DATABASE `public_commit_history`")
	ownVersionExec(t, ctx, conn, "USE `public_commit_history`")
	if _, err := schema.MigrateUpTo(ctx, conn, 66); err != nil {
		t.Fatal(err)
	}
	boundaryIgnoredPrefix(t, ctx, conn)
	ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
	ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,metadata,timeout_ns,hook_bead,role_bead,payload) VALUES ('public-a1b','public shallow initial','','','','',JSON_OBJECT('n',9007199254740993),9007199254740993,NULL,'','null')")
	ownVersionExec(t, ctx, conn, "INSERT INTO compaction_snapshots(id,issue_id,compaction_level,snapshot_json) VALUES ('public-shallow-binary','public-a1b',1,?)", []byte{0, 255, 128, 42})
	commitHistoryCommit(t, ctx, conn)
	for i := 1; i <= 4; i++ {
		ownVersionExec(t, ctx, conn, "UPDATE issues SET title=? WHERE id='public-a1b'", fmt.Sprintf("public shallow title %d", i))
		commitHistoryCommit(t, ctx, conn)
	}
	fullLog, err := (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	if err != nil || !fullLog.Clean || len(fullLog.Commits) <= 3 {
		t.Fatal("full public source census unavailable")
	}
	fullWorking, err := physicalcapture.ReadWorkingRoot(ctx, conn, fullLog.Head)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDriver(); err != nil {
		t.Fatal(err)
	}

	// This is the pinned Dolt shallow clone procedure, not a rewritten commit
	// list, injected SQL failure or fabricated missing-parent fixture.
	db, closeDriver, err = openPublicMigrationFixture(cloneDir)
	if err != nil {
		t.Fatal(err)
	}
	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The pinned file-remote factory opens the URL path directly as the NBS
	// store; it does not append the working repository's .dolt/noms path.
	sourceStore := filepath.Join(sourceDir, "public_commit_history", ".dolt", "noms")
	if info, err := os.Stat(filepath.Join(sourceStore, "manifest")); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("public source chunk-store manifest missing: %v", err)
	}
	ownVersionExec(t, ctx, conn, "CALL DOLT_CLONE('--depth','3',?, 'public_commit_history')", "file://"+filepath.ToSlash(sourceStore))
	ownVersionExec(t, ctx, conn, "USE `public_commit_history`")
	observed, err := physicalcapture.ReadHistory(ctx, conn, fullLog.Head)
	if err != nil || !observed.ReadOnlyScope || len(observed.Commits) != 3 || len(observed.Boundaries) != 1 || observed.Boundaries[0].Reason != "ghost_parent" {
		t.Fatalf("genuine shallow frontier missing: err=%T commits=%d boundaries=%v", err, len(observed.Commits), observed.Boundaries)
	}
	frontier := observed.Boundaries[0].CommitID
	missing, missingErr := physicalcapture.ReadCommit(ctx, conn, frontier)
	if missingErr == nil || physicalcapture.UnavailableReason(missingErr) != "ghost_parent" || missing.CommitID != "" || missing.ReadOnlyScope || len(missing.Tables) != 0 {
		t.Fatal("shallow parent was not an actual unreadable ghost")
	}
	fullByID := map[string]plugdump.SourceCommit{}
	for _, c := range fullLog.Commits {
		fullByID[c.ID] = c
	}
	if _, present := fullByID[frontier]; !present {
		t.Fatal("ghost is not an original full-source commit")
	}
	originals := map[string]physicalcapture.Image{}
	for _, c := range observed.Commits {
		if original, present := fullByID[c.ID]; !present || !reflect.DeepEqual(original.Parents, c.Parents) {
			t.Fatal("shallow clone changed actual retained parent edges")
		}
		image, err := physicalcapture.ReadCommit(ctx, conn, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		originals[c.ID] = image
	}
	// Ignored current tables are recreated by their pinned public migrations;
	// they are not grafted onto any immutable shallow image.
	boundaryIgnoredPrefix(t, ctx, conn)
	ownVersionExec(t, ctx, conn, "INSERT INTO events(id,issue_id,event_type,actor,old_value,new_value,comment) VALUES ('public-shallow-event-1','public-a1b','updated','public',NULL,'','current-only first attachment'),('public-shallow-event-2','public-a1b','updated','public','','after','current-only second attachment')")
	if status := commitHistoryStatus(t, ctx, conn); len(status) != 0 {
		t.Fatalf("shallow source not clean after actual ignored bootstrap: %v", status)
	}
	beforeCurrent := commitHistoryCurrent(t, ctx, conn)
	beforeWorking, err := physicalcapture.ReadWorkingRoot(ctx, conn, fullLog.Head)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDriver(); err != nil {
		t.Fatal(err)
	}

	command, output := commitHistoryCommand(t, ctx, binary, cloneDir)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("genuine shallow capture: %T stdout=%s stderr=%s", err, stdout.Bytes(), stderr.Bytes())
	}
	archive, err := os.ReadFile(filepath.Join(output, "capture.tar"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, native := ownVersionArchive(t, archive)
	coverage := native.HistoryCoverage
	if manifest.MainVersion != 66 || manifest.IgnoredVersion != 26 || manifest.History != "available-unverified" || native.Head != fullLog.Head || native.Complete || len(native.Gaps) != 0 || coverage == nil || coverage.Format != plugdump.NativeHistoryCoverageFormat || coverage.Status != "verified-to-boundary" || coverage.VerifiedCommitCount != 3 || len(native.Commits) != 3 || len(native.Images) != 3 || len(coverage.Boundaries) != 1 {
		t.Fatal("shallow archive coverage/census differs")
	}
	boundary := coverage.Boundaries[0]
	if boundary.CommitID != frontier || boundary.Reason != "ghost_parent" || boundary.CommitOrdinalFromHead != 4 || boundary.CommitSchemaVersion != nil || len(boundary.SQLDoltCodes) != 0 {
		t.Fatal("boundary coordinate/type/schema/code inferred or changed")
	}
	children := []string{}
	for _, c := range native.Commits {
		for _, p := range c.Parents {
			if p == frontier {
				children = append(children, c.ID)
			}
		}
	}
	if !reflect.DeepEqual(children, boundary.ChildCommitIDs) || len(children) != 1 {
		t.Fatal("frontier child edges differ")
	}
	if !ownVersionEqual(t, native.Current, beforeCurrent) {
		t.Fatal("shallow current table/cell conservation failed")
	}
	sourceMember := boundarySourceMember(t, archive)
	var portable plugdump.SourceHistory
	decoder := json.NewDecoder(bytes.NewReader(sourceMember))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&portable); err != nil {
		t.Fatal(err)
	}
	if coverage.EventsAttachment.Member != plugdump.SourceHistoryMember || coverage.EventsAttachment.SHA256 != fmt.Sprintf("%x", sha256.Sum256(sourceMember)) || coverage.EventsAttachment.Rows != 2 || len(portable.Events) != 2 || portable.Range.Status != "unverified" || portable.Range.Reason != "native-history-boundary" || portable.Range.Complete || len(portable.Range.Frames) != 0 || !ownVersionEqual(t, portable.Events, native.Current["events"].Rows) {
		t.Fatal("exact current events member binding differs")
	}
	if !ownVersionEqual(t, portable.Commits, native.Commits) {
		t.Fatal("portable/native retained commit census differs")
	}
	evidenceData, err := os.ReadFile(filepath.Join(output, "physical-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence commitPhysicalEvidence
	decoder = json.NewDecoder(bytes.NewReader(evidenceData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(evidence.HistoryCoverage, coverage) || evidence.ArchiveSHA256 != fmt.Sprintf("%x", sha256.Sum256(archive)) || len(evidence.Images) != 3 || evidence.SQLDefaults != "not-captured" {
		t.Fatal("native/typed sidecar coverage equality failed")
	}
	for i, image := range native.Images {
		original, present := originals[image.CommitID]
		if !present || image.Sequence != uint64(i+1) || image.SchemaProfile == nil || image.SchemaProfile.MainVersion == nil || *image.SchemaProfile.MainVersion != 66 || image.SchemaProfile.IgnoredVersion != nil {
			t.Fatal("retained shallow image identity/schema differs")
		}
		want := map[string]plugdump.NativeTable{}
		for _, table := range original.Tables {
			want[table.Name] = commitHistoryNativeTable(t, table)
		}
		if !ownVersionEqual(t, want, image.Tables) {
			t.Fatal("shallow native image lost a physical cell")
		}
		typed, err := physicalcapture.EvidenceTables(original.Tables)
		if err != nil || !reflect.DeepEqual(typed, evidence.Images[i].Tables) || evidence.Images[i].CommitID != image.CommitID || !evidence.Images[i].ReadOnlyScope || !reflect.DeepEqual(image.SchemaProfile, evidence.Images[i].SchemaProfile) {
			t.Fatal("shallow sidecar physical type/cell/profile differs")
		}
		if _, invented := image.Tables["events"]; invented {
			t.Fatal("current events assigned to immutable shallow commit")
		}
	}
	typedWorking, err := physicalcapture.EvidenceTables(beforeWorking.Tables)
	if err != nil || !reflect.DeepEqual(typedWorking, evidence.Working.Tables) {
		t.Fatal("shallow current physical type/cell evidence differs")
	}
	stream := json.NewDecoder(bytes.NewReader(stderr.Bytes()))
	var schemaReport, boundaryReport safeCaptureReport
	if err := stream.Decode(&schemaReport); err != nil || schemaReport.Stage != stageSchema || schemaReport.InnermostGoErrorType != "" {
		t.Fatal("original schema success record lost")
	}
	if err := stream.Decode(&boundaryReport); err != nil || boundaryReport.Stage != stageHistory || boundaryReport.HistoryBoundaryReason != "ghost_parent" || boundaryReport.InnermostGoErrorType != "plugdump.ErrSourceHistoryUnavailable" || boundaryReport.CommitOrdinalFromHead == nil || *boundaryReport.CommitOrdinalFromHead != 4 || boundaryReport.VerifiedCommitCount == nil || *boundaryReport.VerifiedCommitCount != 3 || boundaryReport.CommitSchemaVersion != nil || len(boundaryReport.SQLCodes) != 0 {
		t.Fatal("actual safe boundary diagnostic differs")
	}
	if err := stream.Decode(new(any)); err != io.EOF {
		t.Fatal("extra/unparseable boundary diagnostic record")
	}
	if bytes.Contains(stderr.Bytes(), []byte(frontier)) || bytes.Contains(stdout.Bytes(), []byte(frontier)) || bytes.Contains(stderr.Bytes(), []byte(fullLog.Head)) {
		t.Fatal("safe published boundary output leaked commit IDs")
	}
	var summary struct {
		Capture    string           `json:"capture"`
		Verified   uint64           `json:"verified_commit_count"`
		Boundaries []map[string]any `json:"history_boundaries"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil || summary.Capture != "published-validated" || summary.Verified != 3 || len(summary.Boundaries) != 1 {
		t.Fatal("published boundary summary differs")
	}

	db, closeDriver, err = physicalcapture.Open(cloneDir, "public_commit_history")
	if err != nil {
		t.Fatal(err)
	}
	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	afterWorking, err := physicalcapture.ReadWorkingRoot(ctx, conn, fullLog.Head)
	if err != nil || !reflect.DeepEqual(beforeWorking, afterWorking) || !ownVersionEqual(t, beforeCurrent, commitHistoryCurrent(t, ctx, conn)) {
		t.Fatal("shallow source changed after capture/reopen")
	}
	afterHistory, err := physicalcapture.ReadHistory(ctx, conn, fullLog.Head)
	if err != nil || !reflect.DeepEqual(observed, afterHistory) {
		t.Fatal("actual shallow frontier/metadata changed")
	}
	for id, original := range originals {
		after, err := physicalcapture.ReadCommit(ctx, conn, id)
		if err != nil || !reflect.DeepEqual(after, original) {
			t.Fatal("immutable shallow source changed")
		}
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeDriver(); err != nil {
		t.Fatal(err)
	}
	db, closeDriver, err = physicalcapture.Open(sourceDir, "public_commit_history")
	if err != nil {
		t.Fatal(err)
	}
	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	afterFull, err := (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	if err != nil || !reflect.DeepEqual(afterFull, fullLog) {
		t.Fatal("full clone donor graph changed")
	}
	afterFullWorking, err := physicalcapture.ReadWorkingRoot(ctx, conn, fullLog.Head)
	if err != nil || !reflect.DeepEqual(fullWorking, afterFullWorking) {
		t.Fatal("full clone donor root changed")
	}
	artifacts := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	facts, err := json.MarshalIndent(map[string]any{"fixture": "pinned-dolt-file-shallow-clone-depth-3", "full_source": fullLog, "observed_shallow": observed, "coverage": coverage, "verified_images": len(native.Images), "source_unchanged_after_reopen": true, "clone_unchanged_after_reopen": true, "before_working": beforeWorking}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"public-shallow-boundary-v66.tar": archive, "public-shallow-boundary-v66-physical-evidence.json": evidenceData, "public-shallow-boundary-v66-facts.json": facts, "public-shallow-boundary-v66-stderr.jsonl": stderr.Bytes(), "public-shallow-boundary-v66-stdout.json": stdout.Bytes()} {
		if err := os.WriteFile(filepath.Join(artifacts, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("PUBLIC_GENUINE_SHALLOW_BOUNDARY verified_commits=3 boundaries=1 ordinal=4 events=2 all_physical_cells=true inputs_reopened_unchanged=true")
}

func TestSafeBoundaryTaxonomyKeepsOnlyObservedCoordinatesAndCodes(t *testing.T) {
	v, n := uint64(39), uint64(3)
	for _, reason := range []string{"missing_root_chunk", "missing_table_chunk", "unreadable_schema", "other"} {
		t.Run(reason, func(t *testing.T) {
			report := safeReport(&captureFailure{stage: stageHistory, cause: plugdump.ErrSourceHistoryUnavailable{Reason: "native-history-boundary", BoundaryReason: reason, SchemaVersion: &v, OrdinalFromHead: 4, VerifiedCommitCount: &n, SQLCodes: []uint16{1105}}})
			if report.HistoryBoundaryReason != reason || report.VerifiedCommitCount == nil || *report.VerifiedCommitCount != 3 || report.CommitSchemaVersion == nil || *report.CommitSchemaVersion != 39 || report.CommitOrdinalFromHead == nil || *report.CommitOrdinalFromHead != 4 || !reflect.DeepEqual(report.SQLCodes, []uint16{1105}) {
				t.Fatal("typed safe context changed")
			}
		})
	}
	report := safeReport(plugdump.ErrSourceHistoryUnavailable{Reason: "native-history-boundary", BoundaryReason: "ghost_parent", OrdinalFromHead: 4, VerifiedCommitCount: &n})
	if report.CommitSchemaVersion != nil || len(report.SQLCodes) != 0 {
		t.Fatal("unreadable frontier inherited HEAD/schema/SQL number")
	}
}
