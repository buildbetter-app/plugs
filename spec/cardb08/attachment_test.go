package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
)

func b08AttachParams(id model.Ref, attachment any) map[string]any {
	return map[string]any{"changeSet": map[string]any{"ops": []map[string]any{{"kind": "attach", "target": id, "attachment": attachment}}}}
}

func b08AttachmentRefEqual(left, right model.Ref) bool {
	return left.IsID() && right.IsID() && left.ID == right.ID && left.Branch.IsZero() && right.Branch.IsZero()
}

func (f *b08Fixture) attachmentRead(label string, id model.Ref, at *model.Revision, want []model.Attachment) model.Revision {
	f.t.Helper()
	before := f.state()
	params := map[string]any{"id": id}
	if at != nil {
		params["asOf"] = map[string]any{"revision": *at}
	}
	out := f.call(label, "bead.get", params, "")
	var result struct {
		Bead struct {
			ID       string
			Revision model.Revision
		}
		Attachments []model.Attachment
	}
	if err := json.Unmarshal(out.Result, &result); err != nil || result.Bead.ID != id.ID || !bytes.Equal(b08JSON(f.t, result.Attachments), b08JSON(f.t, want)) {
		f.t.Fatalf("attachment readback: %s err=%v want=%+v", out.Result, err, want)
	}
	if !bytes.Equal(b08JSON(f.t, before), b08JSON(f.t, f.state())) {
		f.t.Fatal("attachment read changed complete state")
	}
	return result.Bead.Revision
}

func (f *b08Fixture) attachmentWrite(label string, id model.Ref, attachment model.Attachment, want []model.Attachment) (b08Reply, model.Revision) {
	f.t.Helper()
	before := f.state()
	out := f.call(label, "tx.apply", b08AttachParams(id, attachment), "")
	var commit model.Commit
	if err := json.Unmarshal(out.Result, &commit); err != nil {
		f.t.Fatal(err)
	}
	after := f.state()
	oldHead, err := before.Meta.Head.Uint64()
	if err != nil || commit.Revision.String() != fmt.Sprint(oldHead+1) || after.Meta.Head != commit.Revision || out.Meta.Replayed || len(commit.Results) != 1 || !b08AttachmentRefEqual(commit.Results[0].Ref, id) || commit.Results[0].Revision != commit.Revision || commit.Results[0].SetBy["attachments"] != "request" || len(after.History) != len(before.History)+1 || len(after.Versions[id.ID]) != len(before.Versions[id.ID])+1 {
		f.t.Fatalf("attachment did not commit exactly one ordinary revision: %s err=%v", out.Result, err)
	}
	var returned pipeline.Document
	if err := json.Unmarshal(commit.Results[0].Record, &returned); err != nil || returned.ID != id.ID || !reflect.DeepEqual(returned.Attachments, want) || !reflect.DeepEqual(f.document(id).Attachments, want) {
		f.t.Fatalf("attachment commit/storage differs from independent tuple: %s err=%v", out.Result, err)
	}
	if len(after.Records) != len(before.Records) || !reflect.DeepEqual(before.Checkpoint, after.Checkpoint) {
		f.t.Fatal("ordinary attachment changed record membership or workflow checkpoint")
	}
	for _, row := range before.Records {
		if b08AttachmentRefEqual(row.Ref, id) {
			continue
		}
		found := false
		for _, current := range after.Records {
			if b08AttachmentRefEqual(current.Ref, row.Ref) {
				found = reflect.DeepEqual(current, row) && reflect.DeepEqual(before.Versions[row.Ref.ID], after.Versions[row.Ref.ID])
			}
		}
		if !found {
			f.t.Fatal("attachment rewrote unrelated row/versions", row.Ref)
		}
	}
	found := false
	for _, entry := range after.History {
		if entry.Revision != commit.Revision {
			continue
		}
		found = true
		if len(entry.Changes) != 1 || !b08AttachmentRefEqual(entry.Changes[0].Ref, id) || entry.Changes[0].Op != model.OpAttach || !reflect.DeepEqual(entry.Changes[0].ChangedPaths, []string{"/attachments"}) || len(entry.Events) != 1 || entry.Events[0].Type != model.EventBeadAttached || !b08AttachmentRefEqual(entry.Events[0].Resource.Ref, id) || entry.Events[0].Provenance.Method != "tx.apply" || entry.Events[0].Provenance.SetBy["attachments"] != "request" {
			f.t.Fatalf("attachment history/event mismatch: %+v", entry)
		}
	}
	if !found {
		f.t.Fatal("attachment commit missing from immutable history")
	}
	b08Save(f.t, f.root, label+"-state.json", map[string]any{"before": before, "after": after, "expected_attachments": want, "commit": commit})
	return out, commit.Revision
}

func (f *b08Fixture) attachmentReplay(label string, id model.Ref, attachment model.Attachment, original b08Reply) {
	f.t.Helper()
	before := f.state()
	replay := f.call(label, "tx.apply", b08AttachParams(id, attachment), "")
	if !replay.Meta.Replayed || !bytes.Equal(replay.Result, original.Result) || !bytes.Equal(b08JSON(f.t, before), b08JSON(f.t, f.state())) {
		f.t.Fatal("attachment replay duplicated an attachment, version, history, receipt or checkpoint")
	}
	b08Save(f.t, f.root, label+"-replay-conservation.json", map[string]any{"before": before, "after": f.state(), "original": original, "replay": replay})
}

func TestB08AttachmentsPublicCommitHistoryReplayAndEvidenceClose(t *testing.T) {
	root := b08Root(t, "b08-attachments")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB08Fixture(t, engine, filepath.Join(root, engine), false)
			id := f.create("attachment-evidence", "b08-evidence", nil, nil)
			f.create("attachment-unrelated", "b08-item", nil, nil)
			created := f.attachmentRead("get-before-evidence", id, nil, []model.Attachment{})
			f.conserved("attachment-close-without-evidence", "bead.close", map[string]any{"id": id}, "ontology_guard_failed", "evidence_required")
			link := model.Attachment{Kind: "link", URI: "https://example.invalid/proof?token=literal%2Fvalue", MediaType: "text/plain", Summary: "reviewed public proof"}
			commit := model.Attachment{Kind: "commit", URI: "git://example.invalid/repo/commit/0123456789abcdef", MediaType: "application/vnd.git.commit", Summary: ""}
			first, firstAt := f.attachmentWrite("attach-link", id, link, []model.Attachment{link})
			f.attachmentReplay("attach-link", id, link, first)
			second, secondAt := f.attachmentWrite("attach-commit", id, commit, []model.Attachment{link, commit})
			f.attachmentReplay("attach-commit", id, commit, second)
			f.attachmentRead("get-before-first-attachment", id, &created, []model.Attachment{})
			f.attachmentRead("get-first-attachment-history", id, &firstAt, []model.Attachment{link})
			if got := f.attachmentRead("get-current-attachments", id, nil, []model.Attachment{link, commit}); got != secondAt {
				t.Fatal("bead revision did not advance for attachment", got, secondAt)
			}
			changed := commit
			changed.URI += "different"
			f.conserved("attach-commit", "tx.apply", b08AttachParams(id, changed), "idempotency_key_reused", "")
			before := f.state()
			f.call("attachment-evidence-close-positive", "bead.close", map[string]any{"id": id}, "")
			after := f.state()
			if len(after.History) != len(before.History)+1 || len(after.Versions[id.ID]) != len(before.Versions[id.ID])+1 || f.document(id).Fields["status"] != "b08-finished" || !reflect.DeepEqual(f.document(id).Attachments, []model.Attachment{link, commit}) {
				t.Fatal("evidence_required close failed to publish exactly once with its attachments")
			}
			f.reopen("attachments-terminal")
			f.attachmentRead("reopened-attachments", id, nil, []model.Attachment{link, commit})
			f.attachmentRead("reopened-first-attachment-history", id, &firstAt, []model.Attachment{link})
			f.attachmentRead("reopened-pre-attachment-history", id, &created, []model.Attachment{})
			f.attachmentReplay("attach-link", id, link, first)
			f.attachmentReplay("attach-commit", id, commit, second)
		})
	}
}

func TestB08AttachmentsInvalidAndMissingStoreRefuseWithoutWrites(t *testing.T) {
	root := b08Root(t, "b08-attachment-refusals")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newB08Fixture(t, engine, filepath.Join(root, engine), false)
			id := f.create("attachment-invalid", "b08-item", nil, nil)
			for i, attachment := range []model.Attachment{
				{Kind: "link", MediaType: "text/plain"},
				{Kind: "commit", URI: "  ", MediaType: "text/plain"},
				{Kind: "link", URI: "https://example.invalid/proof", ContentRef: "unverified", MediaType: "text/plain"},
				{Kind: "link", URI: "https://example.invalid/proof", MediaType: " "},
				{Kind: "blob", URI: "https://example.invalid/proof", ContentRef: "unverified", MediaType: "text/plain"},
				{Kind: "log", MediaType: "text/plain"},
			} {
				// A valid earlier mutation must roll back too, not merely the
				// malformed attachment's target or its receipt.
				params := map[string]any{"changeSet": map[string]any{"ops": []map[string]any{
					{"kind": "patch", "target": id, "fields": map[string]any{"title": "must not publish"}},
					{"kind": "attach", "target": id, "attachment": attachment},
				}}}
				f.conserved(fmt.Sprintf("invalid-attachment-%d", i), "tx.apply", params, "invalid_params", "")
			}
			for _, kind := range []string{"blob", "log"} {
				attachment := model.Attachment{Kind: kind, ContentRef: "sha256:unverified-content", MediaType: "text/plain", Summary: "store is absent"}
				params := b08AttachParams(id, attachment)
				f.conserved("missing-store-"+kind, "tx.apply", params, "unsupported", "")
				out := f.call("missing-store-detail-"+kind, "tx.apply", params, "unsupported")
				// The transport supplies the catalog message; the typed details
				// identify the missing attachment store and requested kind.
				if out.Error.Message != "operation is unsupported" || out.Error.Data.Details["reason"] != "attachment_content_store_unavailable" || out.Error.Data.Details["kind"] != kind {
					t.Fatal("content-backed attachment refusal did not identify missing store", out.Error)
				}
			}
			valid := model.Attachment{Kind: "link", URI: "https://example.invalid/valid", MediaType: "text/plain"}
			f.conserved("attachment-missing-target", "tx.apply", b08AttachParams(model.RefByID("missing-attachment-target", model.Natural64(0)), valid), "not_found", "")
			f.conserved("attachment-protected-target", "tx.apply", b08AttachParams(b08CatalogRef, valid), "protected_record", "")
			f.reopen("attachment-refusals-terminal")
		})
	}
}

type b08AttachmentAuthorization struct {
	Stage port.AuthorizationStage
	Ref   model.Ref
	Op    model.OpKind
}

// This installed callback exercises the real Writer authorization port, not
// an authorization result in a request. It intentionally makes no claim about
// a particular installed policy's finite set of permitted methods.
type b08AttachmentAuthorizer struct {
	deny      model.Ref
	denyStage port.AuthorizationStage
	calls     []b08AttachmentAuthorization
}

func (a *b08AttachmentAuthorizer) Authorize(ctx context.Context, stage port.AuthorizationStage, proposal port.ProposedChange, reader port.SnapshotReader) error {
	if !reflect.DeepEqual(proposal.Principal, b08Actor) {
		return fmt.Errorf("attachment authorization principal changed")
	}
	for _, change := range proposal.Changes {
		a.calls = append(a.calls, b08AttachmentAuthorization{stage, change.Resource.Ref, change.Op.Kind})
		if change.Op.Kind != model.OpCreate {
			rows, err := reader.Get(ctx, []model.Ref{change.Resource.Ref})
			if err != nil || len(rows) != 1 || !b08AttachmentRefEqual(rows[0].Ref, change.Resource.Ref) {
				return fmt.Errorf("attachment authorization lost actual target: %v", err)
			}
		}
		if change.Resource.Ref == a.deny && stage == a.denyStage {
			return &model.Error{Message: "write to this bead is denied", Data: model.ErrorData{Code: "forbidden"}}
		}
	}
	return nil
}

func (a *b08AttachmentAuthorizer) EventContext(_ context.Context, proposal port.ProposedChange, _ port.SnapshotReader) (port.EventContext, error) {
	return port.EventContext{Cause: model.EventCause{Root: proposal.Meta.IdempotencyKey, Depth: model.Natural64(0), Chain: []string{}}, Chain: []string{}}, nil
}

func TestB08AttachmentsUseBeadWriteAuthorizationAndReauthorizeReplay(t *testing.T) {
	root := b08Root(t, "b08-attachment-authorization")
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			authority := &b08AttachmentAuthorizer{}
			f := newB08Fixture(t, engine, filepath.Join(root, engine), false, pipeline.WithAuthorizer(authority), pipeline.WithEventContext(authority))
			id := f.create("attachment-authorized", "b08-item", nil, nil)
			other := f.create("attachment-denied", "b08-item", nil, nil)
			attachment := model.Attachment{Kind: "link", URI: "https://example.invalid/authorized", MediaType: "text/plain", Summary: "current bead authority"}
			for _, stage := range []port.AuthorizationStage{port.AuthorizeBefore, port.AuthorizeAfter} {
				authority.deny, authority.denyStage = id, stage
				f.conserved("attachment-denied-"+string(stage), "tx.apply", b08AttachParams(id, attachment), "forbidden", "")
				f.conserved("ordinary-write-denied-"+string(stage), "bead.update", map[string]any{"id": id, "fields": map[string]any{"title": "also denied"}}, "forbidden", "")
			}
			authority.deny, authority.denyStage = other, port.AuthorizeBefore
			authority.calls = nil
			original, _ := f.attachmentWrite("authorized-attachment", id, attachment, []model.Attachment{attachment})
			want := []b08AttachmentAuthorization{{port.AuthorizeBefore, id, model.OpAttach}, {port.AuthorizeAfter, id, model.OpAttach}}
			if !reflect.DeepEqual(authority.calls, want) {
				t.Fatalf("attachment bypassed ordinary before/after authorization: %+v", authority.calls)
			}
			authority.calls = nil
			f.attachmentReplay("authorized-attachment", id, attachment, original)
			if !reflect.DeepEqual(authority.calls, []b08AttachmentAuthorization{{port.AuthorizeReplay, id, model.OpAttach}}) {
				t.Fatal("attachment receipt bypassed current target authorization", authority.calls)
			}
			authority.deny, authority.denyStage = id, port.AuthorizeReplay
			f.conserved("authorized-attachment", "tx.apply", b08AttachParams(id, attachment), "forbidden", "")
			authority.deny, authority.denyStage = other, port.AuthorizeBefore
			f.conserved("other-bead-attachment-denied", "tx.apply", b08AttachParams(other, attachment), "forbidden", "")
			f.reopen("attachment-authorization-terminal")
			f.attachmentReplay("authorized-attachment", id, attachment, original)
			b08Save(t, f.root, "actual-authorization-stages.json", authority.calls)
		})
	}
}
