package authzrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

// This test-only memory adapter exercises the entire guarded terminal request
// through the real Writer and its maintained Ready rows. The committed reader
// is acquired only after the real memory Head. It is not a SQL certificate or
// evidence that either SQL public capability may bypass its refusal.
func (s *readyRequestStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	if hook := s.beforeWrite; hook != nil {
		s.beforeWrite = nil
		hook()
	}
	tx, err := s.Store.Write(ctx, lease)
	if err != nil || !s.freshEnabled {
		return tx, err
	}
	return &readyTerminalTx{WriteTx: tx, store: s}, nil
}

type readyTerminalTx struct {
	driver.WriteTx
	store     *readyRequestStore
	committed driver.Reader
	probed    bool
}

func (t *readyTerminalTx) Head(ctx context.Context) (model.Revision, error) {
	head, err := t.WriteTx.Head(ctx)
	if err != nil {
		return head, err
	}
	reader, err := t.store.Store.Read(ctx, t.WriteTx.Snapshot())
	if err == nil {
		t.committed = &readyRequestReader{Reader: reader, store: t.store}
	}
	return head, err
}
func (t *readyTerminalTx) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	t.store.queries++
	return t.WriteTx.Query(ctx, q)
}
func (t *readyTerminalTx) Close() error {
	if t.committed != nil {
		_ = t.committed.Close()
	}
	return t.WriteTx.Close()
}
func (t *readyTerminalTx) PutGraphRecord(ctx context.Context, r driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	return t.WriteTx.(driver.GraphSourceWriter).PutGraphRecord(ctx, r, parent, links)
}
func (t *readyTerminalTx) FreshReadyAuthority(ctx context.Context) (driver.ReadyAuthoritySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.committed == nil {
		return nil, errors.New("terminal authority requested before Head")
	}
	return t.committed, nil
}
func (t *readyTerminalTx) FreshReadyOrderExists(ctx context.Context, p driver.ReadyOrderPlan) (bool, error) {
	if t.committed == nil {
		return false, errors.New("terminal Ready requested before Head")
	}
	t.store.freshCalls++
	t.probed = true
	if t.store.freshError != nil {
		return false, t.store.freshError
	}
	found, err := readyindex.Exists(ctx, t.committed, p)
	if found {
		t.store.foundFresh++
	}
	if err == nil && t.store.afterFresh != nil {
		hook := t.store.afterFresh
		t.store.afterFresh = nil
		hook()
	}
	return found, err
}
func (t *readyTerminalTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	terminal := h.Method == "bead.claimNext" && len(h.Changes) == 0
	if terminal && !t.probed {
		return model.Changeset{}, errors.New("terminal Commit bypassed fresh Ready probe")
	}
	out, err := t.WriteTx.Commit(ctx, h)
	if terminal && err == nil {
		t.store.terminalCommits++
	}
	return out, err
}

func TestCompiledClaimTerminalFreshProbeAndDurableReceipt(t *testing.T) {
	for _, name := range []string{"empty", "probe_error", "authority_change", "candidate_insertion"} {
		t.Run(name, func(t *testing.T) {
			f := newReadyRequestFixture(t, 0)
			f.store.freshEnabled = true
			ctx := context.Background()
			before, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			meta := f.meta()
			probeErr := errors.New("terminal fresh observation failed")
			var inserted model.Ref
			switch name {
			case "probe_error":
				f.store.freshError = probeErr
			case "authority_change":
				epoch := model.Natural64(1)
				f.app.Checker.caller = func(ctx context.Context, by model.PrincipalRef) (authz.Caller, error) {
					return DefaultCallerSource(epoch)(ctx, by)
				}
				f.store.afterFresh = func() { epoch = model.Natural64(2) }
			case "candidate_insertion":
				f.store.beforeWrite = func() {
					out, err := f.source.Create(ctx, f.by, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "inserted after both live walks"}}, f.meta())
					if err != nil {
						t.Fatal(err)
					}
					inserted = out.Results[0].Ref
				}
			}
			out, callErr := f.app.Service.ClaimNextWithOutcome(ctx, f.by, service.ClaimNextRequest{Project: "local"}, meta)
			after, err := f.store.Meta(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := f.store.Store.Read(ctx, driver.View{Incarnation: after.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			principal, err := driver.PrincipalKey(f.by)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := reader.IdempotencyByKey(ctx, principal, "", "bead.claimNext", meta.IdempotencyKey)
			if err != nil {
				t.Fatal(err)
			}
			if f.store.freshCalls != 1 || f.store.queries != 0 {
				t.Fatalf("fresh route probes%d raw_queries%d", f.store.freshCalls, f.store.queries)
			}
			if name == "probe_error" || name == "authority_change" {
				if callErr == nil || out != nil || receipt != nil || after.Head != before.Head || f.store.terminalCommits != 0 {
					t.Fatalf("refused terminal published: out%+v err%v receipt%+v before%s after%s commits%d", out, callErr, receipt, before.Head, after.Head, f.store.terminalCommits)
				}
				if name == "probe_error" && !errors.Is(callErr, probeErr) {
					t.Fatalf("probe error changed: %v", callErr)
				}
				if name == "authority_change" {
					var typed *model.Error
					if !errors.As(callErr, &typed) || typed.Data.Code != "authz_unavailable" {
						t.Fatalf("final authority error: %v", callErr)
					}
				}
				return
			}
			if callErr != nil || out == nil || receipt == nil || receipt.ResponsePending || receipt.Commit.Revision != out.Commit.Revision {
				t.Fatalf("durable outcome=%+v err=%v receipt=%+v", out, callErr, receipt)
			}
			start, _ := before.Head.Uint64()
			if name == "candidate_insertion" {
				if out.NullResult || len(out.Commit.Results) != 1 || out.Commit.Results[0].Ref.ID != inserted.ID || f.store.foundFresh != 1 || f.store.terminalCommits != 0 || len(receipt.Changeset.Changes) != 1 || after.Head != model.Natural64(start+2) {
					t.Fatalf("fresh insertion did not restart and claim: out%+v inserted%s found%d terminal_commits%d receipt%+v head%s", out, inserted.ID, f.store.foundFresh, f.store.terminalCommits, receipt, after.Head)
				}
				return
			}
			if !out.NullResult || len(out.Commit.Results) != 0 || f.store.terminalCommits != 1 || len(receipt.Changeset.Changes) != 0 || after.Head != model.Natural64(start+1) {
				t.Fatalf("null receipt: out%+v receipt%+v head%s commits%d", out, receipt, after.Head, f.store.terminalCommits)
			}
			if _, err := f.source.Create(ctx, f.by, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": "ready after durable null"}}, f.meta()); err != nil {
				t.Fatal(err)
			}
			replay, err := f.app.Service.ClaimNextWithOutcome(ctx, f.by, service.ClaimNextRequest{Project: "local"}, meta)
			if err != nil || replay == nil || !replay.Replayed || !replay.NullResult || replay.Commit.Revision != out.Commit.Revision || f.store.freshCalls != 1 || f.store.terminalCommits != 1 {
				t.Fatalf("durable null replay changed: out%+v err%v probes%d commits%d", replay, err, f.store.freshCalls, f.store.terminalCommits)
			}
		})
	}
}
