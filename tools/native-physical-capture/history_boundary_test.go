package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
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
	if _, err := schema.MigrateUpTo(ctx, conn, 39); err != nil {
		t.Fatal(err)
	}
	ownVersionExec(t, ctx, conn, "INSERT INTO config (`key`,`value`) VALUES ('issue_prefix','public') ON DUPLICATE KEY UPDATE `value`='public'")
	for _, id := range []string{"public-a1b", "public-c2d"} {
		ownVersionExec(t, ctx, conn, "INSERT INTO issues (id,title,description,design,acceptance_criteria,notes,metadata,timeout_ns,hook_bead,role_bead,payload) VALUES (?,'public shallow initial','','','','',JSON_OBJECT('n',9007199254740993),9007199254740993,NULL,'','null')", id)
	}
	ownVersionExec(t, ctx, conn, "INSERT INTO compaction_snapshots(id,issue_id,compaction_level,snapshot_json) VALUES ('public-shallow-binary','public-a1b',1,?)", []byte{0, 255, 128, 42})
	ownVersionExec(t, ctx, conn, "INSERT INTO dependencies(issue_id,depends_on_id,type,created_by) VALUES ('public-a1b','public-c2d','blocks','public')")
	ownVersionExec(t, ctx, conn, "INSERT INTO events(id,issue_id,event_type,actor,old_value,new_value,comment) VALUES ('public-shallow-before-62','public-a1b','updated','public',NULL,'','retained before ignored-plane flip')")
	checkpoints := map[int]string{39: commitHistoryCommit(t, ctx, conn)}
	for _, version := range []int{40, 41, 61, 62, 66} {
		if _, err := schema.MigrateUpTo(ctx, conn, version); err != nil {
			t.Fatalf("pinned shallow migration checkpoint %d: %v", version, err)
		}
		ownVersionExec(t, ctx, conn, "UPDATE issues SET title=? WHERE id='public-a1b'", fmt.Sprintf("public shallow main %d", version))
		checkpoints[version] = commitHistoryCommit(t, ctx, conn)
	}
	// Recreate ignored current tables only after the real versioned migrations.
	boundaryIgnoredPrefix(t, ctx, conn)
	commitHistoryCommit(t, ctx, conn)
	// A file remote uses a non-journaling chunk store. Publish through the
	// pinned remote/push procedures instead of opening the donor's journal.
	remoteStore := diagnosticDir(t)
	remoteURL := "file://" + filepath.ToSlash(remoteStore)
	var sourceBranch string
	if err := conn.QueryRowContext(ctx, "SELECT active_branch()").Scan(&sourceBranch); err != nil || sourceBranch == "" {
		t.Fatalf("public source branch unavailable: %v", err)
	}
	ownVersionExec(t, ctx, conn, "CALL DOLT_REMOTE('add','public-boundary',?)", remoteURL)
	ownVersionExec(t, ctx, conn, "CALL DOLT_PUSH('public-boundary',?)", sourceBranch)
	fullLog, err := (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	if err != nil || !fullLog.Clean || len(fullLog.Commits) <= 3 {
		t.Fatal("full public source census unavailable")
	}
	// The public fixture is linear. Omit only its actual genesis so the
	// retained shallow range includes every populated migration checkpoint.
	roots := 0
	for _, commit := range fullLog.Commits {
		if len(commit.Parents) > 1 {
			t.Fatal("public shallow donor unexpectedly branches")
		}
		if len(commit.Parents) == 0 {
			roots++
		}
	}
	if roots != 1 {
		t.Fatal("public shallow donor genesis differs")
	}
	verifiedCount := len(fullLog.Commits) - 1
	boundaryOrdinal := uint64(verifiedCount + 1)
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
	// The pinned file-remote factory opens the published chunk store directly.
	if info, err := os.Stat(filepath.Join(remoteStore, "manifest")); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("public remote chunk-store manifest missing: %v", err)
	}
	ownVersionExec(t, ctx, conn, "CALL DOLT_CLONE('--depth',?,'--branch',?,?, 'public_commit_history')", fmt.Sprint(verifiedCount), sourceBranch, remoteURL)
	ownVersionExec(t, ctx, conn, "USE `public_commit_history`")
	observed, err := physicalcapture.ReadHistory(ctx, conn, fullLog.Head)
	if err != nil || !observed.ReadOnlyScope || len(observed.Commits) != verifiedCount || len(observed.Boundaries) != 1 || observed.Boundaries[0].Reason != "ghost_parent" {
		t.Fatalf("genuine shallow frontier missing: err=%T commits=%d boundaries=%v", err, len(observed.Commits), observed.Boundaries)
	}
	frontier := observed.Boundaries[0].CommitID
	missing, missingErr := physicalcapture.ReadCommit(ctx, conn, frontier)
	if missingErr == nil || physicalcapture.UnavailableReason(missingErr) != "ghost_parent" || missing.CommitID != "" || missing.ReadOnlyScope || len(missing.Tables) != 0 {
		t.Fatal("shallow parent was not an actual unreadable ghost")
	}
	// SQL-only census still refuses this genuinely shallow source. Inventory
	// must explicitly use the authenticated physical reader, as Build does.
	_, sqlCensusErr := (plugdump.SQLSource{DB: conn}).ReadSourceCommits(ctx)
	var sqlUnavailable plugdump.ErrSourceHistoryUnavailable
	if !errors.As(sqlCensusErr, &sqlUnavailable) || sqlUnavailable.Reason != "ancestry-unavailable" && sqlUnavailable.Reason != "commit-census-unavailable" {
		t.Fatalf("SQL-only shallow refusal changed: %T", sqlCensusErr)
	}
	fullByID := map[string]plugdump.SourceCommit{}
	for _, c := range fullLog.Commits {
		fullByID[c.ID] = c
	}
	if original, present := fullByID[frontier]; !present || len(original.Parents) != 0 {
		t.Fatal("ghost is not the original full-source genesis")
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
	for _, id := range checkpoints {
		if _, present := originals[id]; !present {
			t.Fatal("shallow range lost a populated migration checkpoint")
		}
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
	if manifest.MainVersion != 66 || manifest.IgnoredVersion != 26 || manifest.History != "available-unverified" || native.Head != fullLog.Head || native.Complete || len(native.Gaps) != 0 || coverage == nil || coverage.Format != plugdump.NativeHistoryCoverageFormat || coverage.Status != "verified-to-boundary" || coverage.VerifiedCommitCount != uint64(verifiedCount) || len(native.Commits) != verifiedCount || len(native.Images) != verifiedCount || len(coverage.Boundaries) != 1 {
		t.Fatal("shallow archive coverage/census differs")
	}
	boundary := coverage.Boundaries[0]
	if boundary.CommitID != frontier || boundary.Reason != "ghost_parent" || boundary.CommitOrdinalFromHead != boundaryOrdinal || boundary.CommitSchemaVersion != nil || len(boundary.SQLDoltCodes) != 0 {
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
	if !reflect.DeepEqual(evidence.HistoryCoverage, coverage) || evidence.ArchiveSHA256 != fmt.Sprintf("%x", sha256.Sum256(archive)) || len(evidence.Images) != verifiedCount || evidence.SQLDefaults != "not-captured" {
		t.Fatal("native/typed sidecar coverage equality failed")
	}
	for i, image := range native.Images {
		original, present := originals[image.CommitID]
		if !present {
			t.Fatal("retained shallow image has no original physical root")
		}
		version := commitHistoryCursor(t, original)
		if image.Sequence != uint64(i+1) || image.SchemaProfile == nil || image.SchemaProfile.MainVersion == nil || version == nil || *image.SchemaProfile.MainVersion != *version || image.SchemaProfile.IgnoredVersion != nil {
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
		_, hasEvents := image.Tables["events"]
		if *version >= 62 && hasEvents {
			t.Fatal("current events assigned to post-flip immutable shallow commit")
		}
		if image.CommitID == checkpoints[61] && (!hasEvents || len(image.Tables["events"].Rows) != 1 || image.Tables["events"].Rows[0]["id"] != "public-shallow-before-62") {
			t.Fatal("pre-0062 historical event lost")
		}
		if image.CommitID == checkpoints[40] || image.CommitID == checkpoints[41] {
			hasSplit := false
			for _, column := range image.Tables["dependencies"].Columns {
				hasSplit = hasSplit || column == "depends_on_issue_id"
			}
			if hasSplit != (image.CommitID == checkpoints[41]) || len(image.Tables["dependencies"].Rows) != 1 {
				t.Fatal("populated 0040/0041 dependency reshape lost")
			}
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
	if err := stream.Decode(&boundaryReport); err != nil || boundaryReport.Stage != stageHistory || boundaryReport.HistoryBoundaryReason != "ghost_parent" || boundaryReport.InnermostGoErrorType != "plugdump.ErrSourceHistoryUnavailable" || boundaryReport.CommitOrdinalFromHead == nil || *boundaryReport.CommitOrdinalFromHead != boundaryOrdinal || boundaryReport.VerifiedCommitCount == nil || *boundaryReport.VerifiedCommitCount != uint64(verifiedCount) || boundaryReport.CommitSchemaVersion != nil || len(boundaryReport.SQLCodes) != 0 {
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
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil || summary.Capture != "published-validated" || summary.Verified != uint64(verifiedCount) || len(summary.Boundaries) != 1 {
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
	facts, err := json.MarshalIndent(map[string]any{"fixture": "pinned-dolt-file-push-shallow-clone", "clone_depth": verifiedCount, "migration_checkpoints": checkpoints, "full_source": fullLog, "observed_shallow": observed, "coverage": coverage, "verified_images": len(native.Images), "source_unchanged_after_reopen": true, "clone_unchanged_after_reopen": true, "before_working": beforeWorking}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"public-shallow-boundary-v66.tar": archive, "public-shallow-boundary-v66-physical-evidence.json": evidenceData, "public-shallow-boundary-v66-facts.json": facts, "public-shallow-boundary-v66-stderr.jsonl": stderr.Bytes(), "public-shallow-boundary-v66-stdout.json": stdout.Bytes()} {
		if err := os.WriteFile(filepath.Join(artifacts, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("PUBLIC_GENUINE_SHALLOW_BOUNDARY verified_commits=%d boundaries=1 ordinal=%d events=2 populated_0040_0041_0062=true all_physical_cells=true inputs_reopened_unchanged=true", verifiedCount, boundaryOrdinal)
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
