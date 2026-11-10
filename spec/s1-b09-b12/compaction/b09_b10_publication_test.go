package compaction_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Both faults decorate real transactions. The determinate fault occurs only
// after Put and predicate staging; the uncertain fault occurs only after the
// underlying Commit actually succeeds. Neither manufactures stored success.
type b09B10PublicationStore struct {
	driver.Store
	refuse, loseAck bool
	commits         int
	beforeCommit    func(driver.WriteTx, driver.ChangesetHeader)
}

func (s *b09B10PublicationStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &b09B10PublicationTx{WriteTx: tx, store: s}, nil
}

type b09B10PublicationTx struct {
	driver.WriteTx
	store       *b09B10PublicationStore
	puts, bumps int
}

func (tx *b09B10PublicationTx) Put(ctx context.Context, record driver.Record) error {
	if err := tx.WriteTx.Put(ctx, record); err != nil {
		return err
	}
	tx.puts++
	return nil
}

// PutGraphRecord forwards typed source publication. Embedding WriteTx alone
// hides driver.GraphSourceWriter, and a certified store then refuses the raw
// Put before the commit hook runs.
func (tx *b09B10PublicationTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	writer, ok := tx.WriteTx.(driver.GraphSourceWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	if err := writer.PutGraphRecord(ctx, record, parent, links); err != nil {
		return err
	}
	tx.puts++
	return nil
}

// Retain the real driver's typed projection capability through the fault
// wrapper. Otherwise an ordinary fallback drops the source-bound annotations
// before the injected commit boundary instead of exercising their publication.
func (tx *b09B10PublicationTx) PutSelectionDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	writer, ok := tx.WriteTx.(driver.SelectionIndexWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	return writer.PutSelectionDerived(ctx, table, del, add)
}

func (tx *b09B10PublicationTx) BumpSentinels(ctx context.Context, keys []driver.Sentinel) error {
	if err := tx.WriteTx.BumpSentinels(ctx, keys); err != nil {
		return err
	}
	tx.bumps += len(keys)
	return nil
}

func (tx *b09B10PublicationTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	if header.Method != "admin.compact" || tx.puts != 1 || tx.bumps == 0 {
		return model.Changeset{}, fmt.Errorf("compaction hook missed real record/predicate staging: method=%s puts=%d bumps=%d", header.Method, tx.puts, tx.bumps)
	}
	tx.store.commits++
	tx.store.beforeCommit(tx.WriteTx, header)
	if tx.store.refuse {
		return model.Changeset{}, fmt.Errorf("injected refusal after real compaction staging")
	}
	entry, err := tx.WriteTx.Commit(ctx, header)
	if err == nil && tx.store.loseAck {
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: header.IdempotencyKey}
	}
	return entry, err
}

// B09/B10 conservation is asserted across a new adopted compaction boundary:
// staged compacted text, metadata and predicates remain invisible; refusal
// preserves every data row; publication and receipt become visible together;
// lost acknowledgment and later replay never compact the next eligible child.
// Full cards and production qualification require separate evidence.
func TestB09B10CompactionPublicationVisibilityRefusalAndReplay(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			const key = "b09-b10-late-compaction"
			calls := 0
			var inputs []service.CompactionInput
			config := service.CompactionConfig{Summary: service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
				calls++
				inputs = append(inputs, in)
				return "summary " + in.Ref.ID, nil
			})}
			f := newFixture(t, engine, service.WithCompaction(config))
			b09B10SeedHash(t, f)
			parent := f.create("b09-b10-parent", "uncompacted open parent")
			text := strings.Repeat("Permanent child source λ. ", 80)
			children := make([]map[string]any, 2)
			closed := make([]map[string]any, 2)
			for i := range children {
				created := f.call("bead.create", fmt.Sprintf("b09-b10-child-%d", i), map[string]any{"type": "issue", "parent": parent, "fields": map[string]any{"title": "same-time child", "description": text}}, "")
				children[i] = created["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
				if children[i]["id"] != fmt.Sprintf("%s.%d", parent["id"], i+1) {
					t.Fatalf("fixture did not use the real dotted allocator: %v", children[i])
				}
				closed[i] = f.call("bead.close", fmt.Sprintf("b09-b10-close-%d", i), map[string]any{"id": children[i]}, "")
			}
			f.clock = f.clock.Add(30 * 24 * time.Hour)
			meta := f.meta()
			pinned, err := f.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer pinned.Close()
			predicates := b09B10PredicateKeys(t, decodedRef(t, children[0]), decodedRef(t, children[1]))
			before := b09B10ReadImage(t, pinned, meta.HistoryFloor, key, predicates)
			for _, state := range before.Predicates {
				if !state.Present || state.Revision.IsZero() {
					t.Fatalf("baseline lacks an existing committed predicate: %+v", state)
				}
			}
			b09B10Equal(t, "fresh and pinned predicate baseline", before, b09B10FreshImage(t, f, key, predicates))
			if before.Receipt != nil || len(before.Records) != 3 || len(before.Allocator.Inventory.Projects) != 1 {
				t.Fatalf("unproven/nonempty baseline: %+v", before)
			}
			allocator := before.Allocator.Inventory.Projects[0]
			if allocator.Mode != driver.AllocatorHash || allocator.MaxHashLength != 10 || allocator.Children[parent["id"].(string)] != 2 {
				t.Fatalf("fixture omitted live hash policy/direct-child state: %+v", allocator)
			}
			sqlBefore := b09B10SQLImage(t, f)
			fault := &b09B10PublicationStore{Store: f.store, refuse: true}
			fault.beforeCommit = func(tx driver.WriteTx, header driver.ChangesetHeader) {
				staged, err := tx.Get(ctx, []model.Ref{decodedRef(t, children[0])})
				if err != nil || len(staged) != 1 {
					t.Fatalf("real staged record missing: %+v %v", staged, err)
				}
				var body struct {
					Fields   map[string]any             `json:"fields"`
					Metadata map[string]json.RawMessage `json:"metadata"`
				}
				if err := json.Unmarshal(staged[0].Body, &body); err != nil || body.Fields["description"] != "summary "+children[0]["id"].(string) || len(body.Metadata["plug.compaction"]) == 0 {
					t.Fatalf("hook preceded complete text/metadata staging: %s %v", staged[0].Body, err)
				}
				if header.RequestHash == "" || header.IdempotencyKey != writeMeta("admin.compact", key).IdempotencyKey {
					t.Fatal("hook missed the actual compaction receipt header")
				}
				b09B10Equal(t, "fresh reader during staged compaction", before, b09B10FreshImage(t, f, key, predicates))
				b09B10Equal(t, "pinned reader during staged compaction", before, b09B10ReadImage(t, pinned, meta.HistoryFloor, key, predicates))
				b09B10Equal(t, "independent SQL during staged compaction", sqlBefore, b09B10SQLImage(t, f))
			}
			f.store = fault
			f.bind()
			f.call("admin.compact", key, map[string]any{}, "internal_error")
			if calls != 1 || fault.commits != 1 {
				t.Fatalf("late refusal did not reach provider/commit hook once: calls=%d commits=%d", calls, fault.commits)
			}
			f.assertSameHead(meta)
			f.assertNoReceipt("admin.compact", key)
			b09B10Equal(t, "determinate refusal", before, b09B10FreshImage(t, f, key, predicates))
			b09B10Equal(t, "pinned reader after determinate refusal", before, b09B10ReadImage(t, pinned, meta.HistoryFloor, key, predicates))
			b09B10Equal(t, "SQL refusal", sqlBefore, b09B10SQLImage(t, f))

			// The identical failed key remains retryable. Only a real publication
			// is followed by an uncertain acknowledgment; Writer must resolve it.
			fault.refuse, fault.loseAck = false, true
			commit := f.call("admin.compact", key, map[string]any{}, "")
			assertReplayed(t, f)
			assertCommitTarget(t, commit, children[0])
			if calls != 2 || fault.commits != 2 || !reflect.DeepEqual(inputs[0], inputs[1]) {
				t.Fatalf("refusal/lost-ack selected or summarized extra rows: calls=%d commits=%d inputs=%+v", calls, fault.commits, inputs)
			}
			after := b09B10FreshImage(t, f, key, predicates)
			if after.Receipt == nil || after.Receipt.ResponsePending || after.Receipt.Commit.Revision.String() != fmt.Sprint(commit["revision"]) || len(after.Log) != len(before.Log)+1 || len(after.Records) != len(before.Records) {
				t.Fatalf("publication lacks exactly one durable final receipt/log: %+v", after.Receipt)
			}
			var durableResponse map[string]any
			raw, err := json.Marshal(after.Receipt.Commit)
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if err := decoder.Decode(&durableResponse); err != nil {
				t.Fatal(err)
			}
			b09B10Equal(t, "public response and durable receipt", commit, durableResponse)
			b09B10Equal(t, "receipt and committed log", after.Log[len(after.Log)-1], after.Receipt.Changeset)
			b09B10Equal(t, "retained log prefix", before.Log, after.Log[:len(before.Log)])
			if after.View.At.String() != revisionAfter(t, meta.Head).String() {
				t.Fatal("publication consumed more than one revision")
			}
			selected, err := driver.NewBeadSentinel(children[0]["id"].(string))
			if err != nil {
				t.Fatal(err)
			}
			wantPredicates := append([]driver.LockState(nil), before.Predicates...)
			changedPredicates := 0
			for i, state := range wantPredicates {
				if state.Key.Sentinel == driver.SentinelGraphBlocking || state.Key.Sentinel == selected {
					wantPredicates[i].Revision = *after.View.At
					wantPredicates[i].Present = true
					changedPredicates++
				}
			}
			if changedPredicates != 2 {
				t.Fatal("publication oracle omitted the selected child or graph predicate")
			}
			b09B10Equal(t, "selected/graph publication and unselected predicate conservation", wantPredicates, after.Predicates)
			b09B10Equal(t, "live allocator inventory after compaction", before.Allocator.Inventory, after.Allocator.Inventory)
			// A successful Writer mutation refreshes the selected source annotations
			// and its ready-rank projection. Every unrelated row stays byte-identical.
			wantDerived := b09B10CompactedDerived(t, before, after, children[0]["id"].(string))
			var sourceCreation time.Time
			for _, record := range before.Records {
				if record.Ref.ID == children[0]["id"] {
					var source struct {
						CreatedAt time.Time `json:"createdAt"`
					}
					if err := json.Unmarshal(record.Body, &source); err != nil {
						t.Fatal(err)
					}
					sourceCreation = source.CreatedAt
				}
			}
			if sourceCreation.IsZero() || !sourceCreation.Before(f.clock.Add(-48*time.Hour)) {
				t.Fatal("compaction rank fixture did not establish a real backlog creation instant")
			}
			rankRows := 0
			for i, row := range wantDerived[driver.DerivedSortKey] {
				if row.Key != "ready/"+children[0]["id"].(string) {
					continue
				}
				rankRows++
				// Literal backlog bucket/rank, actual creation and compaction
				// clock, and the one newly published revision form the oracle.
				expected := struct {
					Bucket    int            `json:"bucket"`
					Rank      int            `json:"rank"`
					CreatedAt time.Time      `json:"createdAt"`
					RankedAt  time.Time      `json:"rankedAt"`
					Revision  model.Revision `json:"revision"`
				}{1, 999, sourceCreation, f.clock, *after.View.At}
				value, err := json.Marshal(expected)
				if err != nil {
					t.Fatal(err)
				}
				wantDerived[driver.DerivedSortKey][i].Value = value
			}
			if rankRows != 1 {
				t.Fatal("compaction oracle omitted the selected ready-rank row")
			}
			b09B10Equal(t, "selected rank publication and all other derived conservation", wantDerived, after.Derived)
			for _, record := range before.Records {
				wantVersions := len(before.Versions[record.Ref.ID])
				if record.Ref.ID == children[0]["id"] {
					wantVersions++
				} else {
					for _, current := range after.Records {
						if current.Ref.ID == record.Ref.ID {
							b09B10Equal(t, "unselected record", record, current)
						}
					}
				}
				if len(after.Versions[record.Ref.ID]) != wantVersions {
					t.Fatalf("unexpected version publication for %s", record.Ref.ID)
				}
			}
			b09B10Equal(t, "old reader after publication", before, b09B10ReadImage(t, pinned, meta.HistoryFloor, key, predicates))
			assertTextAndTier(t, f, children[0], "summary "+children[0]["id"].(string), 1)
			assertHistoricalText(t, f, children[0], closed[0]["revision"], text)
			assertHistoricalText(t, f, children[1], closed[1]["revision"], text)
			preview := f.call("admin.compact", "b09-b10-next-preview", map[string]any{"dryRun": true}, "")
			assertCommitTarget(t, preview, children[1])
			f.assertNoReceipt("admin.compact", "b09-b10-next-preview")
			if err := pinned.Close(); err != nil {
				t.Fatal(err)
			}
			sqlAfter := b09B10SQLImage(t, f)

			// Remove the provider before reopen/replay. Selection now points at
			// child .2; the old key must reconstruct child .1 from durable history.
			f.options = []service.Option{service.WithCompaction(service.CompactionConfig{})}
			if engine != "memory" {
				f.close()
				f.open()
			} else {
				f.store = fault.Store
				f.bind()
			}
			b09B10Equal(t, "reopened publication", after, b09B10FreshImage(t, f, key, predicates))
			b09B10Equal(t, "reopened SQL publication", sqlAfter, b09B10SQLImage(t, f))
			replay := f.call("admin.compact", key, map[string]any{}, "")
			assertReplayed(t, f)
			b09B10Equal(t, "replayed public response", commit, replay)
			f.call("admin.compact", key, map[string]any{"beforeRevision": revisionAfter(t, *after.View.At)}, "idempotency_key_reused")
			if calls != 2 || fault.commits != 2 {
				t.Fatal("replay or changed-key refusal regenerated or republished compaction")
			}
			b09B10Equal(t, "replay and changed-request conservation", after, b09B10FreshImage(t, f, key, predicates))
			b09B10Equal(t, "SQL replay and changed-request conservation", sqlAfter, b09B10SQLImage(t, f))
			t.Logf("engine=%s; late refusal=1; real publication=1; provider calls=2; lost acknowledgment resolved by receipt; SQL reopen=%t", engine, engine != "memory")
		})
	}
}
