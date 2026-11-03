package compaction_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// B11 endpoint identity and B12's permanent versions must survive each other.
// The allocator is seeded through the existing driver restore seam; this is a
// public continuation regression, not native-producer/adoption evidence. SQL
// cases close the store and acquire a new lease; memory only rebinds services.
func TestB11B12ReopenCompactionIdentityRefusalAndAllocationRetry(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			original := strings.Repeat("Permanent original λ. ", 100)
			intermediate := strings.Repeat("Tier one retained. ", 20)
			authority := &presenceAuthority{}
			var tiers []int
			config := service.CompactionConfig{
				Summary: service.CompactionSummaryFunc(func(_ context.Context, in service.CompactionInput) (string, error) {
					tiers = append(tiers, in.Tier)
					switch in.Tier {
					case 1:
						if in.Text["description"] != original {
							return "", fmt.Errorf("tier one did not read the original")
						}
						return intermediate, nil
					case 2:
						if in.Text["description"] != intermediate {
							return "", fmt.Errorf("tier two did not continue the retained tier one")
						}
						return "Tier two.", nil
					default:
						return "", fmt.Errorf("unexpected tier %d", in.Tier)
					}
				}),
				VerifyPresence: authority.verify,
			}
			f := newFixture(t, engine, service.WithCompaction(config))
			b11B12SeedAllocator(t, f)
			source := f.create("b11-b12-source", original)
			targetCommit := f.call("bead.create", "b11-b12-target", map[string]any{"type": "issue", "fields": map[string]any{"title": "Endpoint without compactable text"}}, "")
			target := targetCommit["results"].([]any)[0].(map[string]any)["ref"].(map[string]any)
			if source["id"] != "b12-41" || target["id"] != "b12-42" {
				t.Fatalf("restored allocator was bypassed: source=%v target=%v", source, target)
			}
			f.call("bead.close", "b11-b12-close-target", map[string]any{"id": target}, "")
			edge := map[string]any{"from": source, "to": target, "kind": "blocks"}
			f.call("bead.link", "b11-b12-link", edge, "")
			closed := f.call("bead.close", "b11-b12-close-source", map[string]any{"id": source}, "")
			closedAt := f.clock
			f.clock = closedAt.Add(30 * 24 * time.Hour)
			beforeCompact := b11B12Image(t, f)
			first := f.call("admin.compact", "b11-b12-tier-one", map[string]any{}, "")
			assertCommitTarget(t, first, source)
			assertTextAndTier(t, f, source, intermediate, 1)
			compacted := b11B12Image(t, f)
			b11B12AllocatorUnchanged(t, beforeCompact, compacted)
			b11B12Append(t, beforeCompact, compacted, "b11-b12-tier-one")
			b11B12AssertEdge(t, compacted, "blocks")
			b11B12Reopen(t, f, compacted)

			// The allocating create is deliberately before the wrong-kind link.
			// A refused atomic request must not reserve b12-43, bump allocator
			// facts, publish a version, or save a receipt for this request key.
			batch := map[string]any{"changeSet": map[string]any{"ops": []any{
				map[string]any{"kind": "create", "type": "issue", "fields": map[string]any{"title": "Retry must consume exactly one ID"}},
				map[string]any{"kind": "link", "from": source, "to": target, "linkKind": "relates-to"},
			}}}
			f.call("tx.apply", "b11-b12-allocation-retry", batch, "ontology_guard_failed")
			b11B12SameImage(t, compacted, b11B12Image(t, f))
			f.assertNoReceipt("tx.apply", "b11-b12-allocation-retry")
			b11B12Reopen(t, f, compacted)

			// Remove the conflicting edge explicitly, then submit the exact
			// failed request/key again. It must execute, never replay failure.
			f.call("bead.unlink", "b11-b12-unlink", edge, "")
			beforeRetry := b11B12Image(t, f)
			retry := f.call("tx.apply", "b11-b12-allocation-retry", batch, "")
			rows := retry["results"].([]any)
			if len(rows) != 2 || rows[0].(map[string]any)["ref"].(map[string]any)["id"] != "b12-43" {
				t.Fatalf("refusal consumed or retry duplicated an ID: %v", retry)
			}
			afterRetry := b11B12Image(t, f)
			wantAllocator := beforeRetry.allocators
			wantAllocator.Projects = append([]driver.OfflineAllocator(nil), wantAllocator.Projects...)
			wantAllocator.Projects[0] = wantAllocator.Projects[0].Clone()
			wantAllocator.Projects[0].LastID++
			if !reflect.DeepEqual(wantAllocator, afterRetry.allocators) || len(afterRetry.records) != len(beforeRetry.records)+1 || len(afterRetry.changesets) != len(beforeRetry.changesets)+1 || len(afterRetry.versions["b12-43"]) != 1 || len(afterRetry.versions["b12-44"]) != 0 {
				t.Fatalf("retry did not publish exactly one allocation/changeset: before=%+v after=%+v", beforeRetry, afterRetry)
			}
			b11B12Append(t, beforeRetry, afterRetry, "b11-b12-allocation-retry")
			if afterRetry.facts[0].Revision.String() != fmt.Sprint(retry["revision"]) || reflect.DeepEqual(beforeRetry.facts, afterRetry.facts) {
				t.Fatal("allocation did not publish its allocator fact with the retry")
			}
			b11B12AssertEdge(t, afterRetry, "relates-to")
			if !reflect.DeepEqual(beforeRetry.versions["b12-42"], afterRetry.versions["b12-42"]) || len(afterRetry.versions["b12-41"]) != len(beforeRetry.versions["b12-41"])+1 {
				t.Fatal("retry changed target history or lost the source link version")
			}
			b11B12Reopen(t, f, afterRetry)
			b11B12Replay(t, f, "tx.apply", "b11-b12-allocation-retry", batch, retry, afterRetry)

			// Eligibility and prior level now come from the reopened durable
			// image, including the unrelated link change since tier one.
			f.clock = closedAt.Add(90 * 24 * time.Hour)
			second := f.call("admin.compact", "b11-b12-tier-two", map[string]any{}, "")
			assertCommitTarget(t, second, source)
			assertTextAndTier(t, f, source, "Tier two.", 2)
			secondImage := b11B12Image(t, f)
			b11B12AllocatorUnchanged(t, afterRetry, secondImage)
			b11B12Append(t, afterRetry, secondImage, "b11-b12-tier-two")
			b11B12AssertEdge(t, secondImage, "relates-to")
			b11B12Reopen(t, f, secondImage)
			for _, selected := range []struct {
				key, text string
				base      any
			}{{"b11-b12-revert-two", intermediate, second["revision"]}, {"b11-b12-revert-one", original, first["revision"]}} {
				params := map[string]any{"ref": source, "baseRevision": selected.base, "expect": []any{}}
				params["humanPresence"] = authority.issue(f, params, selected.key)
				before := b11B12Image(t, f)
				reverted := f.call("history.revert", selected.key, params, "")
				b11B12AssertKind(t, f, reverted["revision"], model.ChangesetRevert)
				after := b11B12Image(t, f)
				b11B12AllocatorUnchanged(t, before, after)
				b11B12Append(t, before, after, selected.key)
				b11B12AssertEdge(t, after, "relates-to")
				assertHistoricalText(t, f, source, reverted["revision"], selected.text)
				if len(after.versions["b12-41"]) != len(before.versions["b12-41"])+1 || len(after.changesets) != len(before.changesets)+1 {
					t.Fatal("reversal did not append exactly one permanent version/changeset")
				}
				b11B12Reopen(t, f, after)
				b11B12Replay(t, f, "history.revert", selected.key, params, reverted, after)
			}
			assertHistoricalText(t, f, source, closed["revision"], original)
			assertHistoricalText(t, f, source, first["revision"], intermediate)
			assertHistoricalText(t, f, source, second["revision"], "Tier two.")
			final := b11B12Image(t, f)
			for _, record := range final.records {
				if record.Ref.ID == "b12-41" {
					doc, err := pipeline.DecodeDocument(record.Body)
					if err != nil || len(doc.Metadata["plug.compaction"]) != 0 {
						t.Fatalf("tier one reversal retained active compaction metadata: %s (%v)", record.Body, err)
					}
				}
			}
			// Receipt replay still returns the original responses after both
			// reversals and a provider-free reopen, without allocating again.
			f.options = []service.Option{service.WithCompaction(service.CompactionConfig{VerifyPresence: authority.verify})}
			b11B12Reopen(t, f, final)
			b11B12Replay(t, f, "admin.compact", "b11-b12-tier-one", map[string]any{}, first, final)
			b11B12Replay(t, f, "admin.compact", "b11-b12-tier-two", map[string]any{}, second, final)
			b11B12Replay(t, f, "tx.apply", "b11-b12-allocation-retry", batch, retry, final)
			if !reflect.DeepEqual(tiers, []int{1, 2}) {
				t.Fatalf("replay or reversal called provider: tiers=%v", tiers)
			}
		})
	}
}

// Capture independent driver observations from one pinned reader. The absent
// next IDs are included in version reads to catch history-only leaked writes;
// receipts include the refused/retried request and every later replay boundary.
// This never derives expected state from the RPC's projected success fields.
type b11B12StoreImage struct {
	meta       driver.Meta
	records    []driver.Record
	versions   map[string][]driver.Version
	changesets []model.Changeset
	restore    *driver.IdempotencyRecord
	allocators driver.AllocatorInventory
	facts      []driver.LockState
	receipts   map[string]*driver.IdempotencyRecord
}

func b11B12Image(t *testing.T, f *fixture) b11B12StoreImage {
	t.Helper()
	ctx := context.Background()
	image := b11B12StoreImage{meta: f.meta(), versions: map[string][]driver.Version{}, receipts: map[string]*driver.IdempotencyRecord{}}
	r, err := f.store.Read(ctx, driver.View{Incarnation: image.meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || page.NextCursor != nil || page.AsOf != image.meta.Head {
		t.Fatalf("record census incomplete or unpinned: %+v %v", page, err)
	}
	image.records = page.Items
	for _, id := range []string{"b12-41", "b12-42", "b12-43", "b12-44"} {
		page, err := r.Versions(ctx, model.Ref{ID: id}, driver.PageReq{Limit: 1000})
		if err != nil || page.NextCursor != nil || page.AsOf != image.meta.Head {
			t.Fatalf("version census %s incomplete or unpinned: %+v %v", id, page, err)
		}
		image.versions[id] = page.Items
	}
	// The trusted fixture has a head-only restore floor. Changesets is
	// exclusive, so retain its boundary changeset/receipt separately from
	// the complete post-restore log rather than silently losing revision 1.
	sourceID, _ := b11B12SeedInput()
	hash := b11B12SeedHash(t)
	if image.meta.StoreID != sourceID || image.meta.HistoryFloor.String() != "1" {
		t.Fatalf("restore identity/floor changed: %+v", image.meta)
	}
	image.restore, err = r.IdempotencyByKey(ctx, "system:plug-converter", "", "offline.restore", "b11-b12-seed-"+hash)
	if err != nil || image.restore == nil || image.restore.ResponsePending || image.restore.RequestHash != hash {
		t.Fatalf("missing final restore receipt: %+v %v", image.restore, err)
	}
	boundary := image.restore.Changeset
	if boundary.Kind != model.ChangesetRestore || boundary.Incarnation != image.meta.Incarnation || boundary.Revision != image.meta.HistoryFloor || boundary.IdempotencyKey != "b11-b12-seed-"+hash || image.restore.Commit.Revision != boundary.Revision {
		t.Fatalf("restore receipt lost its boundary changeset: %+v", image.restore)
	}
	image.changesets, err = r.Changesets(ctx, &driver.Cursor{Incarnation: image.meta.Incarnation, Revision: image.meta.HistoryFloor, Index: model.Natural64(0), Method: "changesets", OntologyRevision: model.Natural64(0)}, 1000)
	if err != nil || len(image.changesets) >= 1000 {
		t.Fatalf("changeset census incomplete: %v", err)
	}
	allocatorReader, ok := r.(driver.AllocatorSnapshotReader)
	if !ok {
		t.Fatal("real driver lacks complete allocator snapshot")
	}
	snapshot, err := allocatorReader.AllocatorSnapshot(ctx)
	if err != nil || snapshot.StoreID != image.meta.StoreID || snapshot.Incarnation != image.meta.Incarnation || snapshot.At != image.meta.Head {
		t.Fatalf("allocator snapshot identity/head: %+v %v", snapshot, err)
	}
	image.allocators = snapshot.Inventory
	key, err := driver.AllocatorLock("b12")
	if err != nil {
		t.Fatal(err)
	}
	image.facts, err = r.Observe(ctx, []driver.LockKey{key})
	if err != nil || len(image.facts) != 1 {
		t.Fatalf("allocator fact observation: %+v %v", image.facts, err)
	}
	for _, request := range []struct{ method, key string }{
		{"bead.create", "b11-b12-source"}, {"bead.create", "b11-b12-target"},
		{"bead.close", "b11-b12-close-target"}, {"bead.link", "b11-b12-link"},
		{"bead.close", "b11-b12-close-source"}, {"bead.unlink", "b11-b12-unlink"},
		{"tx.apply", "b11-b12-allocation-retry"},
		{"admin.compact", "b11-b12-tier-one"}, {"admin.compact", "b11-b12-tier-two"},
		{"history.revert", "b11-b12-revert-one"}, {"history.revert", "b11-b12-revert-two"},
	} {
		meta := writeMeta(request.method, request.key)
		receipt, err := r.IdempotencyByKey(ctx, "anonymous:anonymous", meta.AgentLabel, meta.Method, meta.IdempotencyKey)
		if err != nil || receipt != nil && receipt.ResponsePending {
			t.Fatalf("unfinished receipt %s: %+v %v", request.key, receipt, err)
		}
		image.receipts[request.key] = receipt
	}
	return image
}

func b11B12SameImage(t *testing.T, want, got b11B12StoreImage) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("records/history/allocator/facts/receipts changed: before=%+v after=%+v", want, got)
	}
}

// Successful operations must append history and exactly their own final
// receipt. Earlier audit versions and all other receipts are immutable.
func b11B12Append(t *testing.T, before, after b11B12StoreImage, key string) {
	t.Helper()
	if !reflect.DeepEqual(before.restore, after.restore) {
		t.Fatal("write changed the restore boundary changeset/receipt")
	}
	if len(after.changesets) != len(before.changesets)+1 || !reflect.DeepEqual(before.changesets, after.changesets[:len(before.changesets)]) {
		t.Fatal("write replaced permanent changesets or published more than one")
	}
	for id, prior := range before.versions {
		next := after.versions[id]
		if len(next) < len(prior) || !reflect.DeepEqual(prior, next[:len(prior)]) {
			t.Fatalf("write replaced permanent versions for %s", id)
		}
	}
	for name, receipt := range before.receipts {
		if name != key && !reflect.DeepEqual(receipt, after.receipts[name]) {
			t.Fatalf("write changed another request's receipt: %s", name)
		}
	}
	receipt := after.receipts[key]
	if before.receipts[key] != nil || receipt == nil || receipt.RequestHash == "" || receipt.ResponsePending || receipt.Commit.Revision != after.meta.Head || receipt.Changeset.Revision != after.meta.Head {
		t.Fatalf("write failed to publish its one final receipt: %s %+v", key, receipt)
	}
}

func b11B12AllocatorUnchanged(t *testing.T, before, after b11B12StoreImage) {
	t.Helper()
	if !reflect.DeepEqual(before.allocators, after.allocators) || !reflect.DeepEqual(before.facts, after.facts) {
		t.Fatal("compaction or reversal changed allocator identity, counters, or fact revision")
	}
}

func b11B12Reopen(t *testing.T, f *fixture, want b11B12StoreImage) {
	t.Helper()
	if f.engine == "memory" {
		f.bind()
	} else {
		f.close()
		f.open()
	}
	b11B12SameImage(t, want, b11B12Image(t, f))
}

func b11B12Replay(t *testing.T, f *fixture, method, key string, params, want map[string]any, image b11B12StoreImage) {
	t.Helper()
	receipt := image.receipts[key]
	if receipt == nil || receipt.RequestHash == "" || receipt.Commit.Revision.String() != fmt.Sprint(want["revision"]) {
		t.Fatalf("missing exact committed receipt for %s: %+v", key, receipt)
	}
	if got := f.call(method, key, params, ""); !reflect.DeepEqual(want, got) {
		t.Fatalf("%s replay response changed: want=%v got=%v", method, want, got)
	}
	assertReplayed(t, f)
	b11B12SameImage(t, image, b11B12Image(t, f))
}

func b11B12AssertEdge(t *testing.T, image b11B12StoreImage, kind string) {
	t.Helper()
	for _, record := range image.records {
		if record.Ref.ID != "b12-41" {
			continue
		}
		doc, err := pipeline.DecodeDocument(record.Body)
		if err != nil || len(doc.Links) != 1 || doc.Links[0].To.ID != "b12-42" || doc.Links[0].Kind != kind {
			t.Fatalf("endpoint identity duplicated, retyped, or lost: %s (%v)", record.Body, err)
		}
		return
	}
	t.Fatal("source endpoint disappeared")
}

// Use the same floor-aware history census as conservation checks; the shared
// assertKind helper assumes an ordinary fixture whose history begins at zero.
func b11B12AssertKind(t *testing.T, f *fixture, revision any, kind model.ChangesetKind) {
	t.Helper()
	image := b11B12Image(t, f)
	for _, entry := range append([]model.Changeset{image.restore.Changeset}, image.changesets...) {
		if entry.Revision.String() == fmt.Sprint(revision) {
			if entry.Kind != kind {
				t.Fatalf("revision %v kind=%s want %s", revision, entry.Kind, kind)
			}
			return
		}
	}
	t.Fatalf("missing audit revision %v", revision)
}

func b11B12SeedInput() (driver.StoreID, driver.OfflineAllocator) {
	return "b11-b12-allocator-fixture", driver.OfflineAllocator{Prefix: "b12", Mode: driver.AllocatorSequential, LastID: 40, Children: map[string]uint64{"b12-40": 7}}
}

func b11B12SeedHash(t *testing.T) string {
	t.Helper()
	sourceID, seed := b11B12SeedInput()
	payload, err := json.Marshal(struct {
		Source driver.StoreID
		Value  driver.OfflineAllocator
	}{sourceID, seed})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func b11B12SeedAllocator(t *testing.T, f *fixture) {
	t.Helper()
	ctx := context.Background()
	key, err := driver.AllocatorLock("b12")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.Write(ctx, f.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err = tx.Lock(ctx, driver.LockSet{{Key: key, Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	restore, ok := tx.(driver.OfflineRestoreTx)
	if !ok {
		t.Fatal("real driver lacks trusted offline restore setup")
	}
	sourceID, seed := b11B12SeedInput()
	meta, err := restore.PrepareOfflineRestore(ctx, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	allocator, ok := tx.(driver.AllocatorContinuation)
	if !ok {
		t.Fatal("real driver lacks allocator continuation")
	}
	if err = allocator.RestoreAllocators(ctx, []driver.OfflineAllocator{seed}); err != nil {
		t.Fatal(err)
	}
	hash := b11B12SeedHash(t)
	committed, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetRestore, Principal: model.PrincipalRef{Kind: "system", ID: "plug-converter"}, Method: "offline.restore", IdempotencyKey: "b11-b12-seed-" + hash, RequestHash: hash, Response: model.Commit{Results: []model.OpResult{}, Warnings: []model.Warning{}}, LockKeys: []driver.LockKey{key}})
	if err != nil {
		t.Fatal(err)
	}
	// Assert the fixture, including unrelated child counters, independently
	// before invoking any public operation that could mask an invalid seed.
	image := b11B12Image(t, f)
	if !reflect.DeepEqual(image.meta, meta) || !reflect.DeepEqual(image.restore.Changeset, committed) || len(image.changesets) != 0 {
		t.Fatalf("restore metadata/changeset/receipt was not published together: meta=%+v commit=%+v image=%+v", meta, committed, image)
	}
	if len(image.allocators.Projects) != 1 || !reflect.DeepEqual(image.allocators.Projects[0], seed) {
		raw, _ := json.Marshal(image.allocators)
		t.Fatalf("allocator seed did not persist: %s", raw)
	}
}
