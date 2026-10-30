package pipeline

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNoopReceiptAndMixedRowRevisions(t *testing.T) {
	h := setup(t)
	var posts atomic.Int32
	w := h.writer(WithPostSubscribers(PostBinding{ID: "post", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
		posts.Add(1)
		return nil
	})}))
	first := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
	second := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(2)).Results[0].Ref
	cs := model.ChangeSet{Ops: []model.Op{patch(first, map[string]any{"title": "initial"})}}
	noop := apply(t, w, cs, meta(3))
	if noop.Revision.String() != "2" || noop.Results[0].Revision.String() != "1" || posts.Load() != 2 {
		t.Fatalf("no-op=%+v posts=%d", noop, posts.Load())
	}
	r := h.read(t)
	principal, _ := driver.PrincipalKey(by)
	receipt, err := r.IdempotencyByKey(context.Background(), principal, "", "tx.apply", meta(3).IdempotencyKey)
	if err != nil || receipt == nil || !reflect.DeepEqual(receipt.Commit, noop) || receipt.Changeset.Kind != "" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	changeset, err := r.ChangesetByKey(context.Background(), principal, "", "tx.apply", meta(3).IdempotencyKey)
	if err != nil || changeset != nil {
		t.Fatalf("no-op fabricated changeset=%+v err=%v", changeset, err)
	}
	// A mixed batch writes only its changed row; the other result retains its
	// actual row revision, including in the durable ordinary-commit response.
	mixedCS := model.ChangeSet{Ops: []model.Op{patch(first, map[string]any{"title": "initial"}), patch(second, map[string]any{"title": "changed"})}}
	mixed := apply(t, w, mixedCS, meta(4))
	if mixed.Revision.String() != "3" || mixed.Results[0].Revision.String() != "1" || mixed.Results[1].Revision.String() != "3" {
		t.Fatalf("mixed=%+v", mixed)
	}
	for _, replay := range []struct {
		cs   model.ChangeSet
		meta model.WriteMeta
		want model.Commit
	}{{cs, meta(3), noop}, {mixedCS, meta(4), mixed}} {
		got := apply(t, h.writer(), replay.cs, replay.meta)
		if !reflect.DeepEqual(got, replay.want) {
			t.Fatalf("durable replay=%+v want=%+v", got, replay.want)
		}
	}
	if posts.Load() != 3 {
		t.Fatalf("posts ran for no-op/replay: %d", posts.Load())
	}
	log, err := h.read(t).Changesets(context.Background(), nil, 10)
	if err != nil || len(log) != 3 || len(log[2].Changes) != 1 || log[2].Changes[0].Ref.ID != second.ID {
		t.Fatalf("log=%+v err=%v", log, err)
	}
	versions, err := h.read(t).Versions(context.Background(), first, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 {
		t.Fatalf("unchanged versions=%+v err=%v", versions, err)
	}
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(first, map[string]any{"title": "different"})}}, meta(3))
	code(t, err, "idempotency_key_reused")
}

func TestNoopStillRequiresAdmission(t *testing.T) {
	h := setup(t)
	w := h.writer()
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	ref := seed.Results[0].Ref
	cs := model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "initial"})}}
	tests := []struct {
		name string
		w    *Writer
		cs   model.ChangeSet
		code string
	}{
		{"authorization", h.writer(WithAuthorizer(authorizerFunc(func(context.Context, port.AuthorizationStage, port.ProposedChange, port.SnapshotReader) error {
			return failure("permission_denied", "test denied")
		}))), cs, "permission_denied"},
		{"ontology", h.writer(WithOntology(testOntology{reject: true})), cs, "ontology_guard_failed"},
		{"validator", h.writer(WithValidators(ValidatorBinding{ID: "deny", Validator: validatorFunc(func(context.Context, port.ProposedChange, port.SnapshotReader) (port.Verdict, error) {
			return port.Verdict{Allow: false}, nil
		})})), cs, "ontology_guard_failed"},
		{"expect", w, model.ChangeSet{Ops: cs.Ops, Expect: []model.Precondition{{Ref: ref, Revision: func() *model.Revision { v := model.Natural64(0); return &v }()}}}, "precondition_failed"},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := meta(i + 10)
			_, err := tt.w.Apply(context.Background(), by, tt.cs, m)
			code(t, err, tt.code)
			principal, _ := driver.PrincipalKey(by)
			receipt, err := h.read(t).IdempotencyByKey(context.Background(), principal, "", "tx.apply", m.IdempotencyKey)
			if err != nil || receipt != nil {
				t.Fatalf("refusal persisted receipt=%+v err=%v", receipt, err)
			}
		})
	}
	m := meta(20)
	m.DryRun = true
	dry := apply(t, w, cs, m)
	if dry.WouldCommit || dry.Revision.String() != "1" {
		t.Fatalf("no-op dry run=%+v", dry)
	}
	log, err := h.read(t).Changesets(context.Background(), nil, 10)
	if err != nil || len(log) != 1 {
		t.Fatalf("no-op admission mutated log=%+v err=%v", log, err)
	}
}

func TestClaimNextEmptyProbeRunsAfterHeadAndBlocksStaleNull(t *testing.T) {
	h := setup(t)
	var posts atomic.Int32
	w := h.writer(WithPostSubscribers(PostBinding{ID: "not-for-empty-receipts", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
		posts.Add(1)
		return nil
	})}))
	meta := meta(30)
	meta.Method = "bead.claimNext"
	var observed atomic.Int32
	probe := func(_ context.Context, reader driver.Reader) (bool, error) {
		observed.Add(1)
		if reader.Snapshot().At.String() != "0" {
			t.Fatalf("terminal probe did not observe the acquired head: %s", reader.Snapshot().At)
		}
		return true, nil
	}
	_, err := w.ApplyClaimNextEmpty(context.Background(), by, model.ChangeSet{Ops: []model.Op{}}, meta, probe)
	if !errors.Is(err, port.ErrClaimNextReady) {
		t.Fatalf("ready terminal probe error=%v", err)
	}
	if observed.Load() != 1 {
		t.Fatalf("terminal probe calls=%d", observed.Load())
	}
	if head := h.read(t).Snapshot().At.String(); head != "0" {
		t.Fatalf("stale null advanced head to %s", head)
	}
	// The same key was never committed, so a clean terminal observation can
	// publish exactly one durable H+1 zero-change receipt.
	result, err := w.ApplyClaimNextEmpty(context.Background(), by, model.ChangeSet{Ops: []model.Op{}}, meta, func(context.Context, driver.Reader) (bool, error) { return false, nil })
	if err != nil || result.Commit.Revision.String() != "1" {
		t.Fatalf("terminal empty result=%+v err=%v", result, err)
	}
	principal, _ := driver.PrincipalKey(by)
	receipt, err := h.read(t).IdempotencyByKey(context.Background(), principal, "", "bead.claimNext", meta.IdempotencyKey)
	if err != nil || receipt == nil || receipt.Commit.Revision.String() != "1" || len(receipt.Changeset.Changes) != 0 || receipt.ResponsePending || posts.Load() != 0 {
		t.Fatalf("durable terminal receipt=%+v err=%v", receipt, err)
	}
	replayed, err := h.writer().ApplyClaimNextEmpty(context.Background(), by, model.ChangeSet{Ops: []model.Op{}}, meta, func(context.Context, driver.Reader) (bool, error) {
		t.Fatal("durable null replay probed today's ready set")
		return true, nil
	})
	if err != nil || !replayed.Replayed || !reflect.DeepEqual(replayed.Commit, result.Commit) || h.read(t).Snapshot().At.String() != "1" {
		t.Fatalf("terminal receipt replay=%+v err=%v", replayed, err)
	}
}

func TestClaimNextTerminalProbeErrorsDoNotCommit(t *testing.T) {
	for _, name := range []model.ErrorCode{"busy", "write_conflict", "lease_not_held", "lease_expired"} {
		t.Run(string(name), func(t *testing.T) {
			h := setup(t)
			m := meta(31)
			m.Method = "bead.claimNext"
			refusal := failure(name, "terminal storage query failed")
			_, err := h.writer().ApplyClaimNextEmpty(context.Background(), by, model.ChangeSet{Ops: []model.Op{}}, m, func(context.Context, driver.Reader) (bool, error) {
				return false, refusal
			})
			if !errors.Is(err, refusal) {
				t.Fatalf("terminal error was lost: %v", err)
			}
			reader := h.read(t)
			principal, _ := driver.PrincipalKey(by)
			receipt, err := reader.IdempotencyByKey(context.Background(), principal, "", m.Method, m.IdempotencyKey)
			if err != nil || receipt != nil || reader.Snapshot().At.String() != "0" {
				t.Fatalf("terminal probe error committed a null: receipt=%+v err=%v", receipt, err)
			}
		})
	}
}

type sameClaimOntology struct{ seen atomic.Int32 }

func (o *sameClaimOntology) Normalize(ctx context.Context, p port.ProposedChange, r port.SnapshotReader) ([]port.Patch, error) {
	if p.SameClaim {
		o.seen.Add(1)
		return nil, nil
	}
	return (testOntology{}).Normalize(ctx, p, r)
}

func (o *sameClaimOntology) Validate(context.Context, port.ProposedChange, port.SnapshotReader) error {
	return nil
}

func TestSameHolderClaimPreservesDurableLease(t *testing.T) {
	h := setup(t)
	o := &sameClaimOntology{}
	w := h.writer(WithOntology(o))
	seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	ref := seed.Results[0].Ref
	cs := model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{})}}
	m := meta(2)
	m.Method = "bead.claim"
	claim := apply(t, w, cs, m)
	doc, err := DecodeDocument(claim.Results[0].Record)
	if err != nil || doc.Lease == nil || doc.Lease.TtlMs.String() != "300000" || doc.Lease.Epoch.String() != model.Natural64(h.lease.Epoch()).String() {
		t.Fatalf("lease=%+v err=%v", doc.Lease, err)
	}
	h.now = h.now.Add(time.Minute)
	apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(3))
	m.IdempotencyKey = meta(4).IdempotencyKey
	m.Return = "minimal"
	same := apply(t, w, cs, m)
	if o.seen.Load() != 1 || same.Revision.String() != "3" || same.Results[0].Revision.String() != "2" || same.LeaseExpiresAt == nil || !same.LeaseExpiresAt.Equal(doc.Lease.ExpiresAt) || same.LeaseTtlMs == nil || same.LeaseTtlMs.String() != doc.Lease.TtlMs.String() {
		t.Fatalf("same claim=%+v marker=%d", same, o.seen.Load())
	}
	records, err := h.read(t).Get(context.Background(), []model.Ref{ref})
	if err != nil || len(records) != 1 {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	persisted, err := DecodeDocument(records[0].Body)
	if err != nil || !reflect.DeepEqual(persisted.Lease, doc.Lease) {
		t.Fatalf("claim renewed lease=%+v original=%+v err=%v", persisted.Lease, doc.Lease, err)
	}
	if replay := apply(t, h.writer(), cs, m); !reflect.DeepEqual(replay, same) {
		t.Fatalf("claim replay=%+v want=%+v", replay, same)
	}
	other := model.PrincipalRef{Kind: "user", ID: "other"}
	m.IdempotencyKey = meta(5).IdempotencyKey
	_, err = w.Apply(context.Background(), other, cs, m)
	code(t, err, "busy")
	_, err = w.Apply(context.Background(), other, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "initial"})}}, meta(6))
	code(t, err, "lease_not_held")
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "initial"})}}, m)
	code(t, err, "invalid_params")
	h.now = doc.Lease.ExpiresAt
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "initial"})}}, meta(7))
	code(t, err, "lease_expired")
	// A receipt replays its original lease even after that lease has expired.
	m.IdempotencyKey = meta(4).IdempotencyKey
	if replay := apply(t, w, cs, m); !reflect.DeepEqual(replay, same) {
		t.Fatalf("expired claim receipt=%+v want=%+v", replay, same)
	}
	versions, err := h.read(t).Versions(context.Background(), ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 2 {
		t.Fatalf("claim no-op minted version=%+v err=%v", versions, err)
	}
}

func TestAnonymousClaimRequiresSameAgentLabel(t *testing.T) {
	h := setup(t)
	w := h.writer()
	ref := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
	cs := model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{})}}
	anonymous := model.PrincipalRef{Kind: "anonymous", ID: "local"}
	m := meta(2)
	m.Method, m.AgentLabel = "bead.claim", "worker-a"
	if _, err := w.Apply(context.Background(), anonymous, cs, m); err != nil {
		t.Fatal(err)
	}
	m.IdempotencyKey = meta(3).IdempotencyKey
	m.AgentLabel = "worker-b"
	_, err := w.Apply(context.Background(), anonymous, cs, m)
	code(t, err, "busy")
	m.AgentLabel = "worker-a"
	result, err := w.Apply(context.Background(), anonymous, cs, m)
	if err != nil || result.Revision.String() != "2" {
		t.Fatalf("same anonymous agent=%+v err=%v", result, err)
	}
}
