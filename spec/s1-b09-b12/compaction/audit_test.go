package compaction_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var anonymous = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}

func writeMeta(method, key string) model.WriteMeta {
	sum := sha256.Sum256([]byte(key))
	return model.WriteMeta{Method: method, Project: "b12", AgentLabel: "b12-public-recorder", IdempotencyKey: hex.EncodeToString(sum[:])}
}

// These wrappers forward to the actual engine, including real transactions
// and no-op receipts. They observe the exact header at the driver boundary.
type observedHeader struct {
	header driver.ChangesetHeader
	noop   bool
}
type observedStore struct {
	driver.Store
	mu                          sync.Mutex
	headers                     []observedHeader
	failPut, failCommit         bool
	putFailures, commitFailures int
}

func (s *observedStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &observedTx{WriteTx: tx, store: s}, nil
}
func (s *observedStore) record(h driver.ChangesetHeader, noop bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headers = append(s.headers, observedHeader{h, noop})
}

type observedTx struct {
	driver.WriteTx
	store *observedStore
}

func (t *observedTx) Put(ctx context.Context, r driver.Record) error {
	return t.afterPut(t.WriteTx.Put(ctx, r))
}

// Embedding WriteTx alone hides this optional capability and makes certified
// bead publication fail before the intended fault. Keep fault injection after
// the actual typed source image and its metadata have been staged together.
func (t *observedTx) PutGraphRecord(ctx context.Context, r driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	writer, ok := t.WriteTx.(driver.GraphSourceWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	return t.afterPut(writer.PutGraphRecord(ctx, r, parent, links))
}

func (t *observedTx) afterPut(err error) error {
	if err != nil {
		return err
	}
	if t.store.failPut {
		t.store.putFailures++
		return fmt.Errorf("injected failure after real staged Put")
	}
	return nil
}
func (t *observedTx) Commit(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	t.store.record(h, false)
	if t.store.failCommit {
		t.store.commitFailures++
		return model.Changeset{}, fmt.Errorf("injected failure before real Commit")
	}
	return t.WriteTx.Commit(ctx, h)
}
func (t *observedTx) CommitNoop(ctx context.Context, h driver.ChangesetHeader) (model.Commit, error) {
	t.store.record(h, true)
	v, ok := t.WriteTx.(driver.NoopCommitter)
	if !ok {
		return model.Commit{}, driver.ErrUnsupported
	}
	return v.CommitNoop(ctx, h)
}

// The method is audit metadata only. Public D32 enforcement is exercised by
// the RPC reversal tests; these calls isolate the two existing header sites.
func TestB12WriterAuditKindChangedNoopAndReplay(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newFixture(t, engine)
			ref := f.create("audit-original", "original text")
			observed := &observedStore{Store: f.store}
			f.store = observed
			f.bind()
			target := model.RefByID(ref["id"].(string), model.Natural64(0))
			for _, method := range []string{"tx.apply", "history.revert", "admin.compact"} {
				want := model.ChangesetWrite
				if method == "history.revert" {
					want = model.ChangesetRevert
				}
				cs := model.ChangeSet{Ops: []model.Op{{Kind: model.OpPatch, Patch: &model.PatchOp{Target: target, Fields: map[string]any{"description": method}}}}}
				changed, err := f.writer.ApplyWithOutcome(context.Background(), anonymous, cs, writeMeta(method, method+"-changed"))
				if err != nil {
					t.Fatal(err)
				}
				h := observed.headers[len(observed.headers)-1]
				if h.noop || h.header.Kind != want || h.header.Method != method {
					t.Fatalf("changed method=%s header=%+v; kind want %s", method, h, want)
				}
				before := f.meta()
				noopMeta := writeMeta(method, method+"-noop")
				noop, err := f.writer.ApplyWithOutcome(context.Background(), anonymous, cs, noopMeta)
				if err != nil || noop.Replayed {
					t.Fatalf("no-op=%+v err=%v", noop, err)
				}
				h = observed.headers[len(observed.headers)-1]
				if !h.noop || h.header.Kind != want {
					t.Fatalf("noop method=%s header=%+v; kind want %s", method, h, want)
				}
				f.assertSameHead(before)
				count := len(observed.headers)
				for _, pair := range []struct {
					key  string
					want model.Commit
				}{{method + "-changed", changed.Commit}, {method + "-noop", noop.Commit}} {
					replay, err := f.writer.ApplyWithOutcome(context.Background(), anonymous, cs, writeMeta(method, pair.key))
					if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Commit, pair.want) {
						t.Fatalf("replay=%+v err=%v", replay, err)
					}
				}
				if len(observed.headers) != count {
					t.Fatal("replay called a commit boundary")
				}
				r, err := f.store.Read(context.Background(), driver.View{Incarnation: before.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := r.IdempotencyByKey(context.Background(), "anonymous:anonymous", noopMeta.AgentLabel, method, noopMeta.IdempotencyKey)
				_ = r.Close()
				if err != nil || receipt == nil || receipt.Changeset.Kind != "" {
					t.Fatalf("noop fabricated changeset: %+v %v", receipt, err)
				}
				f.assertSameHead(before)
			}
			if got := len(f.versions(ref)); got != 4 {
				t.Fatalf("changed/noop/replay versions=%d want4", got)
			}
		})
	}
}
