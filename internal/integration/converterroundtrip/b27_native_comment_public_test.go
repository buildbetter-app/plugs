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
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const b27NativeCommentFixtureSHA256 = "752989c67b763078ee496b05b7cfc472407f354b591b4064852984bbf98d9421"

type b27CommentInput struct {
	Project       string
	StoreID       string
	CaptureSHA256 string
	ProducerHead  string
	Capture       string
	Oracle        string
	Request       publicProofRequest
	Comments      []b27CommentTuple
	Commits       int
}

type b27CommentTuple struct {
	Project   string `json:"project,omitempty"`
	Table     string `json:"table,omitempty"`
	ID        string `json:"id"`
	IssueID   string `json:"issue_id"`
	Author    string `json:"author"`
	Text      string `json:"text"`
	CreatedAt string `json:"created_at"`
}

type b27CommentBundle struct {
	Format       string `json:"format"`
	ProducerHead string `json:"producer_head"`
	ProducerTree string `json:"producer_tree"`
	Fixture      string `json:"fixture"`
	SHA256       string `json:"sha256"`
	Bytes        int64  `json:"bytes"`
	Files        map[string]struct {
		Bytes  int64  `json:"bytes"`
		SHA256 string `json:"sha256"`
	} `json:"files"`
}

type b27CommentSource struct {
	SourceIdentity dumpformat.SourceIdentity `json:"source_identity"`
	CaptureSHA256  string                    `json:"capture_sha256"`
	CommentTuples  int                       `json:"comment_tuples"`
	Commits        int                       `json:"commits"`
}

type b27CommentSnapshot struct {
	Source dumpformat.SourceIdentity `json:"source_identity"`
	Tables []struct {
		Name string `json:"name"`
		Rows []struct {
			NormalizedJSON string `json:"normalized_json"`
		} `json:"rows"`
	} `json:"tables"`
}

// b27NativeCommentInputs authenticates the committed outer fixture and then
// lets the native capture-set reader authenticate each source root, head and
// part stream. The independent comment tuples come from the detached physical
// checkpoint, never from the archive records being imported.
func b27NativeCommentInputs(t *testing.T) []b27CommentInput {
	t.Helper()
	fixture := b27NativeBytes(t, filepath.Join("testdata", "b27-native-comments-r11.tar.gz"))
	if hashBytes(fixture) != b27NativeCommentFixtureSHA256 {
		t.Fatal("native comment fixture bytes changed")
	}
	var bundle b27CommentBundle
	b27NativeJSON(t, b27NativeBytes(t, filepath.Join("testdata", "b27-native-comments-r11.SOURCE.json")), &bundle)
	if bundle.Format != "b27-public-native-comment-inputs/1" || bundle.Fixture != "b27-native-comments-r11.tar.gz" || bundle.SHA256 != b27NativeCommentFixtureSHA256 || bundle.Bytes != int64(len(fixture)) || len(bundle.ProducerHead) != 40 || len(bundle.ProducerTree) != 40 || len(bundle.Files) == 0 {
		t.Fatal("native comment fixture provenance changed")
	}

	root := t.TempDir()
	gz, err := gzip.NewReader(bytes.NewReader(fixture))
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(gz)
	seen := map[string]bool{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.ToSlash(header.Name)
		if filepath.IsAbs(name) || strings.Contains(name, "../") || name == ".." {
			t.Fatal("native comment fixture path escapes root", name)
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Typeflag != tar.TypeReg {
			t.Fatal("native comment fixture contains a non-regular member", name)
		}
		if seen[name] {
			t.Fatal("native comment fixture repeats a member", name)
		}
		seen[name] = true
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(bundle.Files) {
		t.Fatal("native comment fixture member census differs")
	}
	for name := range seen {
		declared, ok := bundle.Files[name]
		if !ok {
			t.Fatal("native comment fixture has undeclared member", name)
		}
		path := filepath.Join(root, filepath.FromSlash(name))
		raw, err := os.ReadFile(path)
		if err != nil || int64(len(raw)) != declared.Bytes || hashBytes(raw) != declared.SHA256 {
			t.Fatal("native comment fixture member digest differs", name, err)
		}
	}
	for name := range bundle.Files {
		if !seen[name] {
			t.Fatal("native comment fixture is missing a declared member", name)
		}
	}

	projects := []string{"commentcody", "commentsignals"}
	inputs := make([]b27CommentInput, 0, len(projects))
	seenStores := map[string]bool{}
	for _, project := range projects {
		projectRoot := filepath.Join(root, project)
		var source b27CommentSource
		b27NativeJSON(t, b27NativeBytes(t, filepath.Join(projectRoot, "SOURCE.json")), &source)
		if source.SourceIdentity.Project != project || source.SourceIdentity.StoreID == "" || seenStores[source.SourceIdentity.StoreID] || source.CommentTuples != 1018 || source.Commits != 85 {
			t.Fatal("independent native comment source identity/census changed", project)
		}
		seenStores[source.SourceIdentity.StoreID] = true
		var request publicProofRequest
		b27NativeJSON(t, b27NativeBytes(t, filepath.Join(projectRoot, "request.full.json")), &request)
		if request.Format != "public-native-capture-proof/1" || request.Source != source.SourceIdentity || request.CaptureSHA256 != source.CaptureSHA256 || request.ProducerHead != bundle.ProducerHead || request.RestoreMode != "full" || request.Commits != source.Commits || request.JournalTable != dumpformat.SourceEventJournalTable {
			t.Fatal("native comment public request binding changed", project)
		}
		capture := filepath.Join(projectRoot, "capture", "capture-set")
		rootBytes := b27NativeBytes(t, filepath.Join(capture, "capture-set.json"))
		if hashBytes(rootBytes) != source.CaptureSHA256 {
			t.Fatal("native comment capture root digest differs", project)
		}
		var binding dumpformat.NativeCaptureSet
		b27NativeJSON(t, rootBytes, &binding)
		if binding.StoreID != source.SourceIdentity.StoreID || len(binding.Projects) != 1 || binding.Projects[0] != project || binding.PartCount != 1 || binding.Head == "" {
			t.Fatal("native comment capture binding differs", project)
		}
		archive, err := dumpformat.ReadNativeCaptureSetDirectory(context.Background(), capture, dumpformat.NativeCaptureSetOptions{})
		if err != nil {
			t.Fatal("authenticate native comment capture set", project, err)
		}
		if archive.SHA256() != source.CaptureSHA256 || archive.Manifest().StoreID != source.SourceIdentity.StoreID {
			archive.ReleaseNativeDecoded()
			t.Fatal("authenticated native comment identity differs", project)
		}
		oracle := filepath.Join(projectRoot, "oracle")
		checkpoint := filepath.Join(oracle, "raw-head-checkpoint-000-"+binding.Head+".json")
		var snapshot b27CommentSnapshot
		b27NativeJSON(t, b27NativeBytes(t, checkpoint), &snapshot)
		if snapshot.Source != source.SourceIdentity {
			t.Fatal("independent comment checkpoint identity differs", project)
		}
		var checkpointIDs = map[string]bool{}
		for _, table := range snapshot.Tables {
			if table.Name != "comments" {
				continue
			}
			for _, row := range table.Rows {
				raw, err := base64.StdEncoding.DecodeString(row.NormalizedJSON)
				if err != nil {
					t.Fatal("independent comment row encoding", project, err)
				}
				var tuple b27CommentTuple
				b27NativeJSON(t, raw, &tuple)
				if tuple.ID == "" {
					t.Fatal("independent comment checkpoint row is incomplete", project)
				}
				if checkpointIDs[tuple.ID] {
					t.Fatal("independent comment checkpoint repeats an ID", project, tuple.ID)
				}
				checkpointIDs[tuple.ID] = true
			}
		}
		if len(checkpointIDs) != 509 {
			t.Fatal("independent native comment checkpoint census differs", project, len(checkpointIDs))
		}
		tuplesRaw, err := os.ReadFile(filepath.Join(root, "independent-reviewer-tuples", project+"-comments.ndjson"))
		if err != nil {
			t.Fatal(err)
		}
		var comments []b27CommentTuple
		expectedIDs := map[string]bool{}
		for _, line := range bytes.Split(bytes.TrimSpace(tuplesRaw), []byte{'\n'}) {
			var tuple b27CommentTuple
			b27NativeJSON(t, line, &tuple)
			if tuple.Project != project || tuple.Table != "comments" || tuple.ID == "" || tuple.IssueID == "" || tuple.CreatedAt == "" {
				t.Fatal("independent comment tuple is incomplete", project)
			}
			if _, err := time.Parse(time.RFC3339Nano, tuple.CreatedAt); err != nil {
				t.Fatal("independent comment timestamp", project, err)
			}
			if expectedIDs[tuple.ID] || !checkpointIDs[tuple.ID] {
				t.Fatal("independent comment expected ID census differs", project, tuple.ID)
			}
			expectedIDs[tuple.ID] = true
			comments = append(comments, tuple)
		}
		if len(comments) != 509 || len(expectedIDs) != len(checkpointIDs) {
			t.Fatal("independent native comment census differs", project, len(comments))
		}
		request.Capture, request.Oracle = capture, oracle
		inputs = append(inputs, b27CommentInput{Project: project, StoreID: source.SourceIdentity.StoreID, CaptureSHA256: source.CaptureSHA256, ProducerHead: bundle.ProducerHead, Capture: capture, Oracle: oracle, Request: request, Comments: comments, Commits: source.Commits})
		archive.ReleaseNativeDecoded()
	}
	return inputs
}

func b27NativeCommentRequest(t *testing.T, input b27CommentInput, mode string) publicProofRequest {
	t.Helper()
	request := input.Request
	if request.RestoreMode != mode {
		t.Fatal("native comment mode changed", input.Project, request.RestoreMode, mode)
	}
	return request
}

// b27NativeCommentOrder binds the independently authored tuple values to the
// authenticated capture's source-record order. The detached ledger is
// intentionally allowed to use a different order (for example, its lexical
// IDs put the two live comments after the imported IDs), while the restored
// document preserves the authenticated archive order for each issue.
func b27NativeCommentOrder(t *testing.T, archive *dumpformat.Archive, project string, expected []b27CommentTuple) map[string][]b27CommentTuple {
	t.Helper()
	if archive == nil {
		t.Fatal("authenticated native comment archive is required")
	}
	byID := make(map[string]b27CommentTuple, len(expected))
	for _, tuple := range expected {
		if tuple.Project != project || tuple.Table != "comments" || tuple.ID == "" || byID[tuple.ID].ID != "" {
			t.Fatal("independent native comment IDs are incomplete or repeated", project, tuple.ID)
		}
		byID[tuple.ID] = tuple
	}
	records := archive.Records("comment")
	if len(records) != len(expected) {
		t.Fatal("authenticated native comment census differs", project, len(records), len(expected))
	}
	ordered := make(map[string][]b27CommentTuple)
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		if record.Project != project || record.ID == "" || seen[record.ID] {
			t.Fatal("authenticated native comment IDs are incomplete or repeated", project, record.ID)
		}
		tuple, ok := byID[record.ID]
		if !ok {
			t.Fatal("authenticated native comment ID missing from independent ledger", project, record.ID)
		}
		seen[record.ID] = true
		ordered[tuple.IssueID] = append(ordered[tuple.IssueID], tuple)
	}
	if len(seen) != len(byID) {
		t.Fatal("independent native comment ID census differs from authenticated archive", project)
	}
	return ordered
}

func b27NativeCommentPublicReadback(t *testing.T, ctx context.Context, store driver.Store, project string, archive *dumpformat.Archive, expected []b27CommentTuple, actor model.PrincipalRef, restoreTime string) {
	t.Helper()
	wantByIssue := b27NativeCommentOrder(t, archive, project, expected)
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	at := meta.Head
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	documents := map[string]pipeline.Document{}
	seenCursors := map[string]bool{}
	pages := 0
	var cursor *driver.Cursor
	for {
		page, err := reader.Query(ctx, driver.Query{AsOf: &at, Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if page.AsOf.String() != at.String() {
			t.Fatal("public comment readback changed its pinned snapshot")
		}
		for _, record := range page.Items {
			if record.Kind != "bead" {
				continue
			}
			doc, err := pipeline.DecodeDocument(record.Body)
			if err != nil {
				t.Fatal("decode public comment document", err)
			}
			if doc.Project != project || documents[doc.ID].ID != "" {
				t.Fatal("public comment readback contains a repeated or foreign bead", doc.ID, doc.Project)
			}
			documents[doc.ID] = doc
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || page.NextCursor.Incarnation != meta.Incarnation || page.NextCursor.Revision.String() != at.String() || page.NextCursor.Method != "query" {
			t.Fatal("public comment readback cursor binding differs")
		}
		encoded, err := json.Marshal(page.NextCursor)
		if err != nil || seenCursors[string(encoded)] {
			t.Fatal("public comment readback cursor repeated")
		}
		seenCursors[string(encoded)] = true
		cursor = page.NextCursor
	}
	if pages < 2 {
		t.Fatal("public comment readback did not exercise pagination")
	}

	if len(documents) != len(wantByIssue) {
		t.Fatal("public comment bead census differs", len(documents), len(wantByIssue))
	}
	wantRestore, err := time.Parse(time.RFC3339Nano, restoreTime)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for issueID, tuples := range wantByIssue {
		doc, ok := documents[issueID]
		if !ok {
			t.Fatal("public comment issue missing", issueID)
		}
		if len(doc.Comments) != len(tuples) {
			t.Fatal("public comment count differs", issueID, len(doc.Comments), len(tuples))
		}
		for i, tuple := range tuples {
			comment := doc.Comments[i]
			sourceTime, err := time.Parse(time.RFC3339Nano, tuple.CreatedAt)
			if err != nil {
				t.Fatal(err)
			}
			author, err := pipeline.NormalizeSourceAuthor(tuple.Author)
			if err != nil {
				t.Fatal(err)
			}
			if comment.Body != tuple.Text || comment.Origin != "import" || comment.SourceAuthor != author || comment.SourceTime == nil || !comment.SourceTime.Equal(sourceTime) || !comment.Time.Equal(wantRestore) || comment.Time.Equal(sourceTime) || !reflect.DeepEqual(comment.Principal, actor) {
				t.Fatalf("public comment tuple %s/%d differs: expected_body_bytes=%d expected_body_sha256=%s actual_body_bytes=%d actual_body_sha256=%s",
					issueID, i, len(tuple.Text), hashBytes([]byte(tuple.Text)), len(comment.Body), hashBytes([]byte(comment.Body)))
			}
		}
		count += len(doc.Comments)
	}
	if count != len(expected) {
		t.Fatal("public comment total differs", count, len(expected))
	}
	t.Logf("b27_public_native_comments project=%s comments=%d pages=%d terminal=true", project, count, pages)
}

func TestB27RichNativeCommentPublicImportReadback(t *testing.T) {
	inputs := b27NativeCommentInputs(t)
	for _, input := range inputs {
		t.Run(input.Project, func(t *testing.T) {
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					request := b27NativeCommentRequest(t, input, "full")
					raw := encode(t, request)
					outputRoot := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
					if outputRoot == "" {
						t.Fatal("canonical Bazel output directory required")
					}
					name := strings.NewReplacer("/", "_", ":", "_", " ", "_").Replace(t.Name())
					requestPath := filepath.Join(outputRoot, name+"-request.json")
					if err := os.WriteFile(requestPath, raw, 0600); err != nil {
						t.Fatal(err)
					}
					self, err := os.Executable()
					if err != nil {
						t.Fatal(err)
					}
					t.Setenv("PLUG_PUBLIC_PROOF_DRIVER", engine)
					t.Setenv("PLUG_PUBLIC_PROOF_ELF_SHA256", publicProofHashFile(t, self))
					t.Setenv("PLUG_PUBLIC_PROOF_REQUEST", requestPath)
					t.Setenv("PLUG_PUBLIC_PROOF_REQUEST_SHA256", hashBytes(raw))
					t.Setenv("PLUG_PUBLIC_PROOF_OUTPUT", filepath.Join(outputRoot, name))

					var beforeMeta driver.Meta
					var beforeRows map[string]driver.Record
					runPublicNativeExternalCaptureWithPlan(t, nil, func(t *testing.T, ctx context.Context, store driver.Store, lease driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error {
						if report.Archive == nil {
							return errors.New("native comment report lost its authenticated archive")
						}
						beforeMeta, err = store.Meta(ctx)
						if err != nil {
							return err
						}
						beforeRows = b27NativeRows(t, store, nil)
						b27NativeCommentPublicReadback(t, ctx, store, input.Project, report.Archive, input.Comments, profile.Admission.Actor, profile.HistoryTime)
						source := b27NativeRefusalArchive(t, ctx, request.Capture, report.Archive.SHA256())
						defer source.ReleaseNativeDecoded()
						_, refusalErr := RestoreNativeCaptureSetImportWithLease(ctx, source, store, lease, profile)
						if !errors.Is(refusalErr, driver.ErrRestoreTargetNotEmpty) {
							return fmt.Errorf("nonempty native comment restore did not refuse with restore_requires_empty: %w", refusalErr)
						}
						afterMeta, err := store.Meta(ctx)
						if err != nil {
							return err
						}
						afterRows := b27NativeRows(t, store, nil)
						if !reflect.DeepEqual(beforeMeta, afterMeta) || !reflect.DeepEqual(beforeRows, afterRows) {
							return errors.New("typed nonempty refusal changed durable native state")
						}
						if err := VerifyNativeCaptureSetReadback(ctx, store, report.Archive, report); err != nil {
							return fmt.Errorf("native readback after refusal: %w", err)
						}
						b27NativeCommentPublicReadback(t, ctx, store, input.Project, report.Archive, input.Comments, profile.Admission.Actor, profile.HistoryTime)
						return nil
					}, func(t *testing.T, ctx context.Context, store driver.Store, _ driver.ActiveLease, profile driver.RestoreImportProfile, report Report) error {
						if beforeRows == nil {
							return errors.New("nonempty refusal baseline was not recorded")
						}
						afterMeta, err := store.Meta(ctx)
						if err != nil {
							return err
						}
						if !reflect.DeepEqual(beforeMeta, afterMeta) || !reflect.DeepEqual(beforeRows, b27NativeRows(t, store, nil)) {
							return errors.New("SQL reopen after refusal changed durable native state")
						}
						if report.Archive == nil {
							return errors.New("reopened native comment report lost its archive")
						}
						if err := VerifyNativeCaptureSetReadback(ctx, store, report.Archive, report); err != nil {
							return fmt.Errorf("reopened native readback after refusal: %w", err)
						}
						b27NativeCommentPublicReadback(t, ctx, store, input.Project, report.Archive, input.Comments, profile.Admission.Actor, profile.HistoryTime)
						return nil
					})
				})
			}
		})
	}
}
