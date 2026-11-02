package s1b01b04

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func b04Reader(f *fixture) driver.Reader {
	f.t.Helper()
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: f.metaSnapshot().Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

func b04Versions(f *fixture, id string) []driver.Version {
	f.t.Helper()
	r := b04Reader(f)
	defer r.Close()
	page, err := r.Versions(f.ctx, ref(id), driver.PageReq{Limit: 1000})
	if err != nil || page.NextCursor != nil {
		f.t.Fatalf("versions %s: %v next=%v", id, err, page.NextCursor)
	}
	return page.Items
}

func b04Changes(f *fixture) []model.Changeset {
	f.t.Helper()
	r := b04Reader(f)
	defer r.Close()
	changes, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil {
		f.t.Fatal(err)
	}
	return changes
}

func b04Receipt(f *fixture, label, method, agent string) *driver.IdempotencyRecord {
	f.t.Helper()
	r := b04Reader(f)
	defer r.Close()
	principal, err := driver.PrincipalKey(model.PrincipalRef{Kind: "anonymous"})
	if err != nil {
		f.t.Fatal(err)
	}
	entry, err := r.IdempotencyByKey(f.ctx, principal, agent, method, f.key(label, method, agent))
	if err != nil {
		f.t.Fatal(err)
	}
	return entry
}

// Read the documented ClaimResult, including its full OpResult.record. A
// lease, revision, or successful envelope alone cannot satisfy hydration.
func b04Winner(f *fixture, raw []byte, id, status, agent string) model.Commit {
	f.t.Helper()
	value := result(f.t, raw)
	encoded, err := json.Marshal(value)
	if err != nil {
		f.t.Fatal(err)
	}
	var commit model.Commit
	if err := json.Unmarshal(encoded, &commit); err != nil {
		f.t.Fatal(err)
	}
	if len(commit.Results) != 1 || commit.Results[0].Ref != ref(id) || commit.Results[0].Revision != commit.Revision {
		f.t.Fatalf("claim must name exactly its winning row: %s", raw)
	}
	row := commit.Results[0]
	if len(row.Record) == 0 {
		f.t.Fatalf("claim omitted hydrated winning record: %s", raw)
	}
	doc, err := pipeline.DecodeDocument(row.Record)
	if err != nil {
		f.t.Fatal(err)
	}
	if doc.ID != id || doc.Project != project || doc.Fields["status"] != status || !reflect.DeepEqual(doc, f.document(id)) {
		f.t.Fatalf("returned row differs from committed postimage: %+v", doc)
	}
	if doc.Lease == nil || doc.Lease.Principal.Kind != "anonymous" || doc.Lease.Agent != agent || doc.Lease.Epoch != model.Natural64(f.lease.Epoch()) {
		f.t.Fatalf("winning ownership/epoch missing: %+v", doc.Lease)
	}
	deadline := f.clock.Now().Add(5 * time.Minute)
	if commit.LeaseExpiresAt == nil || !commit.LeaseExpiresAt.Equal(deadline) || commit.LeaseTtlMs == nil || commit.LeaseTtlMs.String() != "300000" || !doc.Lease.ExpiresAt.Equal(deadline) || doc.Lease.TtlMs.String() != "300000" {
		f.t.Fatalf("claim did not return its exact persisted five-minute lease: %+v", commit)
	}
	versions := b04Versions(f, id)
	if len(versions) != 2 {
		f.t.Fatalf("winning claim must add exactly one version after create: %d", len(versions))
	}
	var won *driver.Version
	for i := range versions {
		if versions[i].Revision == commit.Revision {
			won = &versions[i]
		}
	}
	if won == nil || !won.CreatedAt.Equal(f.clock.Now()) || won.Principal.Kind != "anonymous" {
		f.t.Fatalf("claim history lost the controlled start instant or actor: %+v", won)
	}
	return commit
}

func b04Null(t *testing.T, raw []byte) {
	t.Helper()
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope["error"]) != 0 || !bytes.Equal(envelope["result"], []byte("null")) {
		t.Fatalf("empty committed ready front must return explicit JSON null: %s", raw)
	}
}

func b04Unchanged(f *fixture, before string, versions []driver.Version, id, label, method, agent string, raw []byte, code string) {
	f.t.Helper()
	if got := errorCode(f.t, raw); got != code {
		f.t.Fatalf("refusal=%q want=%q frame=%s", got, code, raw)
	}
	if f.imageDigest() != before || !reflect.DeepEqual(b04Versions(f, id), versions) {
		f.t.Fatal("refusal changed current rows, head, changesets or versions")
	}
	if b04Receipt(f, label, method, agent) != nil {
		f.t.Fatal("refusal persisted a successful receipt")
	}
}

func TestS1B04PublicClaimContractAcrossStores(t *testing.T) {
	h := newPostgresHarness(t)
	for _, backend := range []string{"memory", "sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fresh := func(t *testing.T, ids []string) *fixture {
				dsn := ""
				if backend == "postgres" {
					dsn = h.dsn(t)
				}
				return newFixture(t, backend, dsn, defaultOntology(), ids)
			}
			t.Run("reference-hydration-history-and-noop", func(t *testing.T) {
				f := fresh(t, []string{"b04-next", "b04-direct", "b04-empty"})
				f.apply("seed", "tx.apply",
					createOp("next", project, map[string]any{"title": "claim next", "priority": 0}, ""),
					createOp("direct", project, map[string]any{"title": "direct claim", "priority": 1}, ""),
					createOp("empty", project, map[string]any{"title": "empty after claims", "priority": 2}, ""))
				beforeHead := f.metaSnapshot().Head
				claimed := b04Winner(f, f.call("next", "bead.claimNext", owner, nil), "b04-next", "in_progress", owner)
				before, _ := beforeHead.Int64()
				if claimed.Revision.String() != model.Natural64(uint64(before+1)).String() {
					t.Fatalf("claim did not commit exactly H+1: %s", claimed.Revision)
				}
				reference := decodeObject(t, []byte(decodeObject(t, runOneBytes)["rawJSON"].(string)))["projection"].(map[string]any)["B04"].(map[string]any)
				old := reference["claimNext"].([]any)
				if len(old) != 1 {
					t.Fatalf("recorded B04 winning projection is not singular: %v", old)
				}
				refRow := old[0].(map[string]any)
				doc := f.document("b04-next")
				for key, actual := range map[string]any{"id": doc.ID, "title": doc.Fields["title"], "priority": doc.Fields["priority"], "status": doc.Fields["status"]} {
					if !reflect.DeepEqual(actual, refRow[key]) {
						t.Fatalf("B04 reference %s=%v actual=%v", key, refRow[key], actual)
					}
				}
				entry := b04Receipt(f, "next", "bead.claimNext", owner)
				if entry == nil || entry.ResponsePending {
					t.Fatalf("returned winner differs from durable receipt: %+v", entry)
				}
				durable, _ := json.Marshal(entry.Commit)
				returned, _ := json.Marshal(claimed)
				if !reflect.DeepEqual(decodeObject(t, durable), decodeObject(t, returned)) {
					t.Fatal("returned winning result differs from durable receipt")
				}
				b04Winner(f, f.call("direct", "bead.claim", owner, map[string]any{"id": ref("b04-direct")}), "b04-direct", "in_progress", owner)
				beforeImage, versions := f.imageDigest(), b04Versions(f, "b04-direct")
				same := result(t, f.call("same", "bead.claim", owner, map[string]any{"id": ref("b04-direct")}))
				if f.imageDigest() != beforeImage || !reflect.DeepEqual(b04Versions(f, "b04-direct"), versions) || same["leaseTtlMs"] != json.Number("300000") {
					t.Fatal("same holder claim was not a quiet no-op")
				}
				if entry := b04Receipt(f, "same", "bead.claim", owner); entry == nil || entry.ResponsePending {
					t.Fatal("same holder response has no durable no-op receipt")
				}
				b04Unchanged(f, beforeImage, versions, "b04-direct", "foreign", "bead.claim", "foreign", f.call("foreign", "bead.claim", "foreign", map[string]any{"id": ref("b04-direct")}), "busy")
				if got := f.readyIDs("remaining", owner); !reflect.DeepEqual(got, referenceIDs(t, "B04", "remainingReadyIds")) {
					t.Fatalf("ready remainder differs from recorded reference: %v", got)
				}
				if backend == "sqlite" {
					f.reopenSQLite()
					beforeImage = f.imageDigest()
					if got := result(t, f.call("same-after-reopen", "bead.claim", owner, map[string]any{"id": ref("b04-direct")})); !reflect.DeepEqual(same, got) || f.imageDigest() != beforeImage {
						t.Fatal("same-owner durable result changed after physical reopen")
					}
				}
			})
			t.Run("empty-receipt-replay-and-late-arrival", func(t *testing.T) {
				f := fresh(t, []string{"b04-later"})
				head, _ := f.metaSnapshot().Head.Int64()
				raw := f.call("empty", "bead.claimNext", owner, nil)
				b04Null(t, raw)
				entry := b04Receipt(f, "empty", "bead.claimNext", owner)
				wantHead := model.Natural64(uint64(head + 1))
				if entry == nil || entry.ResponsePending || entry.Commit.Revision != wantHead || len(entry.Commit.Results) != 0 || entry.Commit.LeaseExpiresAt != nil || f.metaSnapshot().Head != wantHead {
					t.Fatalf("null did not persist exactly one empty H+1 receipt: %+v", entry)
				}
				changes := b04Changes(f)
				if len(changes) != 1 || changes[0].Revision != wantHead || len(changes[0].Changes) != 0 || changes[0].IdempotencyKey != f.key("empty", "bead.claimNext", owner) {
					t.Fatalf("null did not publish exactly one empty changeset: %+v", changes)
				}
				image := f.imageDigest()
				b04Null(t, f.call("empty", "bead.claimNext", owner, nil))
				if f.imageDigest() != image {
					t.Fatal("empty retry minted a second revision")
				}
				f.apply("late-create", "tx.apply", createOp("late", project, map[string]any{"title": "arrived after terminal null"}, ""))
				if backend == "sqlite" {
					f.reopenSQLite()
				}
				image = f.imageDigest()
				b04Null(t, f.call("empty", "bead.claimNext", owner, nil))
				if f.imageDigest() != image || f.document("b04-later").Lease != nil || !reflect.DeepEqual(f.readyIDs("late-ready", owner), []string{"b04-later"}) {
					t.Fatal("durable null replay claimed the later arrival")
				}
				if after := b04Receipt(f, "empty", "bead.claimNext", owner); !reflect.DeepEqual(entry, after) {
					t.Fatal("terminal receipt changed during replay")
				}
				versions := b04Versions(f, "b04-later")
				changedSelector := f.call("empty", "bead.claimNext", owner, map[string]any{"filter": map[string]any{"resources": []string{"b04-later"}}})
				if errorCode(t, changedSelector) != "idempotency_key_reused" || f.imageDigest() != image || !reflect.DeepEqual(versions, b04Versions(f, "b04-later")) || !reflect.DeepEqual(entry, b04Receipt(f, "empty", "bead.claimNext", owner)) {
					t.Fatal("changed selector did not refuse with the original null receipt and graph intact")
				}
				b04Winner(f, f.call("fresh-next", "bead.claimNext", owner, nil), "b04-later", "in_progress", owner)
			})
			t.Run("filters-and-ineligible-front-beyond-1000", func(t *testing.T) {
				const front = 1001
				ids := make([]string, 0, front+3)
				ops := make([]model.Op, 0, front+3)
				for i := range front {
					id := fmt.Sprintf("b04-front-%04d", i)
					ids = append(ids, id)
					inProject := project
					if i%2 == 0 {
						inProject = "other-project"
					}
					ops = append(ops, createOp(id, inProject, map[string]any{"title": id, "priority": 0, "template": i%2 != 0}, ""))
				}
				ids = append(ids, "b04-local-outside-filter", "b04-target-a", "b04-target-b")
				ops = append(ops,
					createOp("outside", project, map[string]any{"title": "ready but outside selector", "priority": 0}, ""),
					createOp("target-a", project, map[string]any{"title": "selected lower-priority work", "priority": 2}, ""),
					createOp("target-b", project, map[string]any{"title": "selected remaining work", "priority": 3}, ""))
				f := fresh(t, ids)
				// This is a new traversal workload, not a replacement for any
				// existing 1000-op atomic-write/deadline gate. All 1004 rows remain
				// in the same real store when the public selector runs.
				for start := 0; start < len(ops); start += 50 {
					f.apply(fmt.Sprintf("seed-%d", start), "tx.apply", ops[start:min(start+50, len(ops))]...)
				}
				selector := map[string]any{"filter": map[string]any{"types": []string{"issue"}, "resources": []string{"b04-target-b", "b04-target-a"}}}
				b04Winner(f, f.call("filtered", "bead.claimNext", owner, selector), "b04-target-a", "in_progress", owner)
				if got := f.readyIDs("unfiltered", owner); !reflect.DeepEqual(got, []string{"b04-local-outside-filter", "b04-target-b"}) {
					t.Fatalf("front/project/selector excluded qualifying rows or claimed unrelated work: %v", got)
				}
				// Canonical set order/duplicates must recover the same winner;
				// changing the selector must refuse without choosing target-b.
				image := f.imageDigest()
				reordered := map[string]any{"filter": map[string]any{"resources": []string{"b04-target-a", "b04-target-b", "b04-target-a"}, "types": []string{"issue", "issue"}}}
				b04Winner(f, f.call("filtered", "bead.claimNext", owner, reordered), "b04-target-a", "in_progress", owner)
				if f.imageDigest() != image {
					t.Fatal("equivalent selector replay changed the store")
				}
				b04Winner(f, f.call("filtered-second", "bead.claimNext", owner, selector), "b04-target-b", "in_progress", owner)
				b04Null(t, f.call("filtered-empty", "bead.claimNext", owner, selector))
				if f.document("b04-local-outside-filter").Lease != nil {
					t.Fatal("empty selected front claimed work outside its filter")
				}
				t.Logf("B04 real selector traversed %d stored rows with %d ineligible prefix rows", len(ids), front)
			})
			t.Run("custom-categories-and-validation-refuse-without-writes", func(t *testing.T) {
				dsn := ""
				if backend == "postgres" {
					dsn = h.dsn(t)
				}
				ids := []string{"b04-queued", "b04-parked", "b04-finished", "b04-abandoned", "b04-broken", "b04-blocker", "b04-blocked"}
				f := newFixture(t, backend, dsn, compileOntology(t, customDocument(), nil), ids)
				ops := make([]model.Op, len(ids))
				for i, id := range ids {
					ops[i] = createOp(id, project, map[string]any{"title": id}, "")
				}
				f.apply("custom-seed", "tx.apply", ops...)
				for _, transition := range []struct{ id, method string }{{"b04-parked", "bead.defer"}, {"b04-finished", "bead.close"}, {"b04-abandoned", "bead.cancel"}, {"b04-broken", "bead.fail"}} {
					result(t, f.call(transition.id, transition.method, owner, map[string]any{"id": ref(transition.id)}))
				}
				result(t, f.call("block", "bead.link", owner, map[string]any{"from": ref("b04-blocked"), "to": ref("b04-blocker"), "kind": "blocks"}))
				for _, id := range []string{"b04-parked", "b04-finished", "b04-abandoned", "b04-broken", "b04-blocked"} {
					t.Run(id, func(t *testing.T) {
						before, versions := f.imageDigest(), b04Versions(f, id)
						code := "invalid_params"
						if id == "b04-blocked" {
							code = "ontology_guard_failed"
						}
						b04Unchanged(f, before, versions, id, id+"-claim", "bead.claim", owner,
							f.call(id+"-claim", "bead.claim", owner, map[string]any{"id": ref(id)}), code)
					})
				}
				b04Winner(f, f.call("custom-claim", "bead.claim", owner, map[string]any{"id": ref("b04-queued")}), "b04-queued", "working", owner)
				for _, params := range []map[string]any{{"limit": 1}, {"offset": 1}, {"brief": true}, {"viewId": "not-installed"}} {
					label := fmt.Sprintf("invalid-%v", params)
					before, versions := f.imageDigest(), b04Versions(f, "b04-blocker")
					b04Unchanged(f, before, versions, "b04-blocker", label, "bead.claimNext", owner, f.call(label, "bead.claimNext", owner, params), "invalid_params")
				}
			})
			t.Run("concurrent-next-winners-are-distinct-and-versioned-once", func(t *testing.T) {
				f := fresh(t, []string{"b04-race-a", "b04-race-b"})
				f.apply("race-seed", "tx.apply", createOp("a", project, map[string]any{"title": "race a"}, ""), createOp("b", project, map[string]any{"title": "race b"}, ""))
				start := make(chan struct{})
				type response struct {
					agent string
					raw   []byte
				}
				responses := make(chan response, 2)
				var wg sync.WaitGroup
				for _, agent := range []string{"race-a", "race-b"} {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						responses <- response{agent, f.call(agent, "bead.claimNext", agent, nil)}
					}()
				}
				close(start)
				wg.Wait()
				close(responses)
				won := map[string]bool{}
				for reply := range responses {
					value := result(t, reply.raw)
					rows, ok := value["results"].([]any)
					if !ok || len(rows) != 1 {
						t.Fatalf("concurrent claim did not return one winner: %s", reply.raw)
					}
					id := rows[0].(map[string]any)["ref"].(map[string]any)["id"].(string)
					if won[id] || id != "b04-race-a" && id != "b04-race-b" {
						t.Fatalf("duplicate or unrelated concurrent winner: %s", id)
					}
					won[id] = true
					b04Winner(f, reply.raw, id, "in_progress", reply.agent)
				}
				if len(won) != 2 || len(b04Changes(f)) != 3 || len(f.readyIDs("race-drained", owner)) != 0 {
					t.Fatal("two wins did not drain exactly two rows with one changeset each")
				}
				b04Null(t, f.call("race-empty", "bead.claimNext", owner, nil))
			})
		})
	}
}
