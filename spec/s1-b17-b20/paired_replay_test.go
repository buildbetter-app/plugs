package s1b17b20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB17PairedStoreCASAndRecordedHistoryDeltas(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(os.Getenv("TEST_SRCDIR"), "_main", "spec", "recorded", "goldens.json"))
	if err != nil {
		t.Fatal(err)
	}
	if fileSHA256(data) != "26a086334357d55ea847daa301f45c9435a71bba75fb6c324456aebf459fbf32" {
		t.Fatal("retained B17 reference bytes changed")
	}
	var golden struct {
		Recorders map[string]struct {
			RawJSON string `json:"rawJSON"`
		} `json:"recorders"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	var reference struct {
		Observations struct {
			Durable struct {
				Swap int `json:"swapDelta"`
				NoOp int `json:"noOpDelta"`
			} `json:"durableHistory"`
		} `json:"observations"`
	}
	if err := json.Unmarshal([]byte(golden.Recorders["B17"].RawJSON), &reference); err != nil {
		t.Fatal(err)
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			ref := b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Create()}}, 1).Results[0].Ref
			p.advance(t)
			b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Set(ref, "cas", `{"b":2,"a":1}`), b17Set(ref, "sibling", `true`)}}, 2)
			before := b17StateOf(t, p.b17Harness, ref)
			cs := model.ChangeSet{Ops: []model.Op{b17Set(ref, "cas", `{"winner":true}`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.cas", b17Hash(`{"a":1,"b":2}`))}}
			input, err := json.Marshal(cs)
			if err != nil {
				t.Fatal(err)
			}
			p.advance(t)
			b17Commit(t, p.b17Harness, cs, 3)
			unchanged, err := json.Marshal(cs)
			if err != nil || !bytes.Equal(input, unchanged) {
				t.Fatal("CAS changed caller input")
			}
			swapped := b17StateOf(t, p.b17Harness, ref)
			if len(swapped.Versions)-len(before.Versions) != reference.Observations.Durable.Swap || len(swapped.Log)-len(before.Log) != reference.Observations.Durable.Swap {
				t.Fatal("CAS history delta differs from the independently recorded Beads observation")
			}
			_, err = p.writer().Apply(p.ctx, b17Actor, cs, b17Meta(4))
			var typed *model.Error
			if !errors.As(err, &typed) || typed.Data.Code != "precondition_failed" || !reflect.DeepEqual(b17StateOf(t, p.b17Harness, ref), swapped) {
				t.Fatalf("stale CAS must refuse atomically: %v", err)
			}
			noop := model.ChangeSet{Ops: []model.Op{b17Set(ref, "cas", `{"winner":true}`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.cas", b17Hash(`{"winner":true}`))}}
			commit := b17Commit(t, p.b17Harness, noop, 5)
			after := b17StateOf(t, p.b17Harness, ref)
			if commit.Revision.String() != swapped.Meta.Head.String() || len(after.Log)-len(swapped.Log) != reference.Observations.Durable.NoOp || !reflect.DeepEqual(after, swapped) {
				t.Fatal("equal CAS changed state/history")
			}
			p.advance(t)
			b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Set(ref, "cas", `null`)}, Expect: noop.Expect}, 6)
			removed := b17StateOf(t, p.b17Harness, ref)
			doc := b17Document(t, removed)
			if _, present := doc.Metadata["cas"]; present || string(doc.Metadata["sibling"]) != "true" {
				t.Fatal("adopted null-as-deletion lost a sibling or stored null")
			}
			b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Set(ref, "cas", `null`)}}, 7)
			if !reflect.DeepEqual(b17StateOf(t, p.b17Harness, ref), removed) {
				t.Fatal("absent-to-absent removal changed durable state")
			}
			_, err = p.writer().Apply(p.ctx, b17Actor, model.ChangeSet{Ops: []model.Op{b17Set(ref, "cas", `false`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.cas", b17Hash(`null`))}}, b17Meta(8))
			if !errors.As(err, &typed) || typed.Data.Code != "precondition_failed" || !reflect.DeepEqual(b17StateOf(t, p.b17Harness, ref), removed) {
				t.Fatalf("absent key matched stored-null digest or wrote state: %v", err)
			}
			p.reopen(t)
			if !reflect.DeepEqual(b17StateOf(t, p.b17Harness, ref), removed) {
				t.Fatal("reopen changed CAS state, raw bodies, versions or journal")
			}
			t.Logf("%s B17 recorded durable swap/no-op deltas and canonical object CAS match; null removal follows the accepted deviation; numeric-literal and ephemeral parity remain unqualified", engine)
		})
	}
}

func TestB17PairedStoreNumericLiteralGuardsAndReplay(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			ref := b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Create()}}, 1).Results[0].Ref
			for i, vector := range []struct{ stored, canonical, other string }{
				{`1.0`, `1.0`, `1`},
				{`{"b":[-0.00,1e0],"a":2.00}`, `{"a":2.00,"b":[-0.00,1e0]}`, `{"a":2,"b":[0,1]}`},
				{`1.25e-2`, `1.25e-2`, `0.0125`},
				{`10e999999999999999999999999999`, `10e999999999999999999999999999`, `1e1000000000000000000000000000`},
			} {
				n := 10 + i*4
				b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Set(ref, "number", vector.stored)}}, n)
				before := b17StateOf(t, p.b17Harness, ref)
				cs := model.ChangeSet{Ops: []model.Op{b17Set(ref, "number", `false`)}, Expect: []model.Precondition{model.PreconditionByHash(ref, "metadata.number", b17Hash(vector.other))}}
				_, err := p.writer().Apply(p.ctx, b17Actor, cs, b17Meta(n+1))
				var typed *model.Error
				if !errors.As(err, &typed) || typed.Data.Code != "precondition_failed" || typed.Data.Details["reason"] != "hash" || p.receipt(t, b17Meta(n+1)) != nil || !reflect.DeepEqual(before, b17StateOf(t, p.b17Harness, ref)) {
					t.Fatalf("numeric spelling mismatch accepted or published state/receipt for %s: %v", vector.stored, err)
				}
				cs.Expect[0] = model.PreconditionByHash(ref, "metadata.number", b17Hash(vector.canonical))
				commit := b17Commit(t, p.b17Harness, cs, n+2)
				after := b17StateOf(t, p.b17Harness, ref)
				if len(after.Log) != len(before.Log)+1 || len(after.Versions) != len(before.Versions)+1 || p.receipt(t, b17Meta(n+2)) == nil {
					t.Fatal("matching numeric literal failed to commit exactly once")
				}
				replayed := b17Commit(t, p.b17Harness, cs, n+2)
				if !reflect.DeepEqual(commit, replayed) || !reflect.DeepEqual(after, b17StateOf(t, p.b17Harness, ref)) {
					t.Fatal("numeric CAS idempotency replay rewrote state/history")
				}
			}
			before := b17StateOf(t, p.b17Harness, ref)
			p.reopen(t)
			if !reflect.DeepEqual(before, b17StateOf(t, p.b17Harness, ref)) {
				t.Fatal("reopen changed numeric CAS history")
			}
		})
	}
}

func pairedB18Reference(t *testing.T) stableObservation {
	t.Helper()
	var recording struct {
		SourceCommit string `json:"sourceCommit"`
		Runs         []struct {
			Path string `json:"path"`
			SHA  string `json:"artifactSha256"`
		} `json:"runs"`
	}
	if err := json.Unmarshal(readReference(t, "recording.json"), &recording); err != nil {
		t.Fatal(err)
	}
	if recording.SourceCommit != "9d6fff760b7a314b476539198504d77ce0959656" || len(recording.Runs) != 2 {
		t.Fatal("missing pinned B18 twins")
	}
	var observations []stableObservation
	for _, binding := range recording.Runs {
		name := filepath.Base(binding.Path)
		if fileSHA256(readReference(t, name)) != binding.SHA {
			t.Fatal("retained B18 raw-command recording digest changed")
		}
		observations = append(observations, referenceObservation(t, loadReferenceRun(t, name)))
	}
	if !reflect.DeepEqual(observations[0], observations[1]) {
		t.Fatal("independently derived B18 twin observations differ")
	}
	return observations[0]
}

func TestB18PairedStoreRPCReplayOfRetainedReference(t *testing.T) {
	reference := pairedB18Reference(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			r := p.rpc(t)
			created := success[model.Commit](t, "bead.create", r.call(t, "", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "original"}}, true))
			ref := created.Results[0].Ref
			initial := b17StateOf(t, p.b17Harness, ref)
			noop := success[model.Commit](t, "bead.update", r.call(t, "", "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "original"}}, true))
			noChange := b17StateOf(t, p.b17Harness, ref)
			for _, title := range []string{"second", "third"} {
				p.advance(t)
				success[model.Commit](t, "bead.update", r.call(t, "", "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": title}}, true))
			}
			final := b17StateOf(t, p.b17Harness, ref)
			p.checkSQLReceiptTimes(t, initial.Versions[0].CreatedAt)
			if [3]int{len(initial.Versions), len(noChange.Versions), len(final.Versions)} != reference.VersionCounts || noop.Revision.String() != created.Revision.String() || !reflect.DeepEqual(initial, noChange) || len(final.Log) != 3 {
				t.Fatal("Plug version/no-op deltas differ from independently derived Beads reference")
			}
			for i, version := range final.Versions {
				if version.Revision.String() != fmt.Sprint(i+1) || final.Log[i].Revision.String() != version.Revision.String() || !version.CreatedAt.Equal(final.Log[i].Time) || !version.CreatedAt.Equal(initial.Versions[0].CreatedAt.Add(time.Duration(i)*time.Second)) || !reflect.DeepEqual(version.Principal, b17Actor) {
					t.Fatalf("non-atomic revision/time/actor at version %d: %+v changeset=%+v", i, version, final.Log[i])
				}
			}
			check := func() {
				r = p.rpc(t)
				historical := success[map[string]any](t, "history.query", r.call(t, "", "history.query", map[string]any{"asOf": map[string]any{"revision": json.Number(created.Revision.String())}}, false))
				items := historical["items"].([]any)
				if len(items) != 1 || items[0].(map[string]any)["fields"].(map[string]any)["title"] != reference.HistoricalTitle {
					t.Fatalf("historical RPC substituted a later state: %+v", historical)
				}
				current := success[map[string]any](t, "bead.get", r.call(t, "", "bead.get", map[string]any{"id": ref}, false))
				if current["bead"].(map[string]any)["fields"].(map[string]any)["title"] != reference.CurrentTitle {
					t.Fatal("current RPC lost later state")
				}
				for _, revision := range []string{"0", "4"} {
					failure := r.call(t, "", "history.get", map[string]any{"ref": ref, "asOf": map[string]any{"revision": json.Number(revision)}}, false)
					if failure.Error == nil || failure.Error.Data.Code != "not_found" {
						t.Fatalf("unknown address %s substituted a nearby state: %+v", revision, failure)
					}
				}
			}
			check()
			p.reopen(t)
			if !reflect.DeepEqual(final, b17StateOf(t, p.b17Harness, ref)) {
				t.Fatal("persistent replay changed exact versions, patches, bodies, timestamps or changesets")
			}
			check()
			t.Logf("%s B18 independently projected pinned twins match create/no-op/two-change/as-of state; actual driver timestamps and persistent reopen verified; P6/P7/P30 address deviation retained", engine)
		})
	}
}

func TestB19PairedStoreDumpPinsActualConcurrentWrites(t *testing.T) {
	for _, history := range []string{"full", "head"} {
		t.Run(history, func(t *testing.T) {
			projections := map[string]b19SnapshotProjection{}
			for _, engine := range []string{"memory", "sqlite", "postgres"} {
				t.Run(engine, func(t *testing.T) {
					p := newPairedStore(t, engine)
					ref := b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Create()}}, 1).Results[0].Ref
					p.advance(t)
					b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Patch(ref, "declared snapshot")}}, 2)
					before := b17StateOf(t, p.b17Harness, ref)
					baseline, manifest, err := restore.Dump(p.ctx, p.store, history)
					if err != nil {
						t.Fatal(err)
					}
					scheduled := &b19WriteBeforeReadStore{Store: p.store, write: func() {
						p.advance(t)
						b17Commit(t, p.b17Harness, model.ChangeSet{Ops: []model.Op{b17Patch(ref, "later committed state")}}, 3)
					}}
					data, actual, err := restore.Dump(p.ctx, scheduled, history)
					if err != nil {
						t.Fatal(err)
					}
					if scheduled.write != nil || !bytes.Equal(data, baseline) || !reflect.DeepEqual(manifest, actual) || b17Document(t, b17StateOf(t, p.b17Harness, ref)).Fields["title"] != "later committed state" {
						t.Fatal("concurrent committed write contaminated captured archive bytes or failed to execute")
					}
					archive, err := dumpformat.Read(bytes.NewReader(data))
					if err != nil {
						t.Fatal(err)
					}
					if actual.NativeHead != "2" || actual.NativeIncarnation != before.Meta.Incarnation || actual.NativeStoreID != string(before.Meta.StoreID) || archive.SHA256() != b17Hash(string(data)) {
						t.Fatal("dump lost captured source identity, head or exact-byte binding")
					}
					rows := b19SnapshotMember[b19SnapshotRecord](t, archive, "records/native.ndjson")
					if len(rows) != 1 || !bytes.Equal(rows[0].Body, before.Body) || rows[0].Revision.String() != "2" {
						t.Fatal("dump substituted a later body")
					}
					wantVersions, wantChanges := before.Versions, before.Log
					if history == "head" {
						wantVersions, wantChanges = before.Versions[1:], nil
					}
					b19AssertSnapshotMember(t, archive, "versions.ndjson", wantVersions)
					b19AssertSnapshotMember(t, archive, "changesets.ndjson", wantChanges)
					projection := b19SnapshotProjection{Body: string(rows[0].Body), Revision: rows[0].Revision.String()}
					for _, version := range wantVersions {
						projection.Versions = append(projection.Versions, version.Revision.String())
					}
					for _, change := range wantChanges {
						projection.Changes = append(projection.Changes, change.Revision.String())
					}
					projections[engine] = projection
					if history == "head" {
						// The same captured public native bytes cross every engine pair.
						for _, target := range []string{"memory", "sqlite", "postgres"} {
							t.Run("restore-"+target, func(t *testing.T) {
								pairedRestoreSnapshot(t, newPairedStore(t, target), data, archive, ref, before)
							})
						}
					}
				})
			}
			for _, engine := range []string{"sqlite", "postgres"} {
				if !reflect.DeepEqual(projections["memory"], projections[engine]) {
					t.Fatalf("%s exported a different native snapshot/history projection", engine)
				}
			}
		})
	}
}

func pairedRestoreSnapshot(t *testing.T, p *pairedStore, data []byte, archive *dumpformat.Archive, ref model.Ref, source b17State) {
	t.Helper()
	before, err := p.store.Meta(p.ctx)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := json.Marshal(archive.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	presence := restore.Presence{"mode": "presence_check", "challenge": "paired-snapshot", "proof": "explicit-test-verifier"}
	manager := restore.NewManager(p.store, p.lease, restore.PresenceVerifierFunc(func(context.Context, string, string, restore.Presence) error { return nil }))
	status, err := manager.BeginAs(p.ctx, b17Actor, manifest, "snapshot", presence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ChunkAs(p.ctx, b17Actor, status.RestoreID, 0, data, b17Hash(string(data)), presence); err != nil {
		t.Fatal(err)
	}
	status, err = manager.Finish(p.ctx, status.RestoreID, b17Hash(string(manifestBytes)), b17Actor, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "paired-snapshot"}, presence)
	if err != nil || status.State != "complete" {
		t.Fatalf("native paired snapshot restore=%+v error=%v", status, err)
	}
	check := func() {
		t.Helper()
		actual, err := p.store.Meta(p.ctx)
		if err != nil || actual.StoreID != source.Meta.StoreID || actual.Incarnation != status.Incarnation || actual.Incarnation == before.Incarnation || actual.Incarnation == source.Meta.Incarnation || actual.Head.String() != "1" || actual.HistoryFloor.String() != "1" {
			t.Fatalf("restored native snapshot identity/floor mismatch: %+v %v", actual, err)
		}
		r, err := p.store.Read(p.ctx, driver.View{Incarnation: actual.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		rows, err := r.Get(p.ctx, []driver.Ref{ref})
		if err != nil || len(rows) != 1 || rows[0].Revision.String() != "1" || !bytes.Equal(rows[0].Body, source.Body) {
			t.Fatalf("restored native snapshot content mismatch: %+v %v", rows, err)
		}
		versions, err := r.Versions(p.ctx, ref, driver.PageReq{Limit: 100})
		if err != nil || versions.NextCursor != nil || len(versions.Items) != 1 || !versions.Items[0].Base || versions.Items[0].Revision.String() != "1" || !bytes.Equal(versions.Items[0].Body, source.Body) {
			t.Fatalf("restored native snapshot base mismatch: %+v %v", versions, err)
		}
		receipt := p.receipt(t, model.WriteMeta{Method: "admin.restore.finish", IdempotencyKey: "paired-snapshot"})
		if receipt == nil || receipt.ResponsePending || receipt.RequestHash != b17Hash(string(data)) || receipt.Commit.Revision.String() != "1" || receipt.Changeset.Kind != model.ChangesetRestore || receipt.Changeset.Revision.String() != "1" || receipt.Changeset.Incarnation != actual.Incarnation || !reflect.DeepEqual(receipt.Changeset.Principal, b17Actor) || len(receipt.Changeset.Changes) != 1 {
			t.Fatalf("missing atomic durable restore receipt: %+v", receipt)
		}
		// A snapshot establishes floor 1. A nil cursor asks for discarded
		// revision-zero history; inspect the restore commit by its receipt,
		// and require typed truncation rather than treating nil as a log scan.
		var truncated driver.ErrTruncated
		if log, err := r.Changesets(p.ctx, nil, 100); !errors.As(err, &truncated) || truncated.Floor.String() != "1" || len(log) != 0 {
			t.Fatalf("discarded journal history did not refuse without a partial page: %+v %v", log, err)
		}
		cursor := &driver.Cursor{Incarnation: actual.Incarnation, Revision: actual.HistoryFloor}
		if log, err := r.Changesets(p.ctx, cursor, 100); err != nil || len(log) != 0 {
			t.Fatalf("caught-up journal at the retained floor: %+v %v", log, err)
		}
		zero := model.Natural64(0)
		if _, err := p.store.Read(p.ctx, driver.View{Incarnation: actual.Incarnation, At: &zero}); !errors.As(err, &truncated) {
			t.Fatalf("snapshot invented discarded history: %v", err)
		}
		var old driver.ErrIncarnationChanged
		if _, err := p.store.Read(p.ctx, driver.View{Incarnation: source.Meta.Incarnation}); !errors.As(err, &old) {
			t.Fatalf("old incarnation survived snapshot restore: %v", err)
		}
	}
	check()
	p.reopen(t)
	check()
	t.Logf("native captured snapshot restored into %s with explicit test PresenceVerifier; new incarnation/floor/base and persistent reopen verified, no source-history replay claim", p.engine)
}

func TestB20PairedStoreRPCJournalMatchesRetainedProjection(t *testing.T) {
	var reference []map[string]any
	for i, name := range []string{"beads-run-a.json", "beads-run-b.json"} {
		data, err := retained.ReadFile("reference/b20/" + name)
		if err != nil {
			t.Fatal(err)
		}
		const shaA = "e2c06f3c984146e4f5b33f3b0b15127e938bef3243b7f955b3c349e0e933b9e4"
		const shaB = "c0e6d264e73da1bbd38762cedb5c8970422c961fb38b6a53c5597376c8966dd4"
		if fileSHA256(data) != []string{shaA, shaB}[i] {
			t.Fatal("retained B20 reference bytes changed")
		}
		var run referenceRecording
		if err := json.Unmarshal(data, &run); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			reference = run.Projection
		} else if !reflect.DeepEqual(reference, run.Projection) {
			t.Fatal("B20 reference twins differ")
		}
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			p := newPairedStore(t, engine)
			r := p.rpc(t)
			r.principal = model.PrincipalRef{Kind: "user", ID: "b20-recorder"}
			created := success[model.Commit](t, "bead.create", r.call(t, "", "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "journal anchor"}}, true))
			ref := created.Results[0].Ref
			p.advance(t)
			success[model.Commit](t, "bead.update", r.call(t, "", "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "journal edited"}}, true))
			before := b17StateOf(t, p.b17Harness, ref)
			success[model.Commit](t, "bead.update", r.call(t, "", "bead.update", map[string]any{"id": ref, "fields": map[string]any{"title": "journal edited"}}, true))
			refusal := r.call(t, "", "tx.apply", map[string]any{"changeSet": map[string]any{"ops": []any{
				map[string]any{"kind": "patch", "target": ref, "fields": map[string]any{"title": "must roll back"}},
				map[string]any{"kind": "link", "from": ref, "to": ref, "linkKind": "blocks"},
			}}}, true)
			if refusal.Error == nil || refusal.Error.Data.Code != "ontology_guard_failed" || !reflect.DeepEqual(before, b17StateOf(t, p.b17Harness, ref)) {
				t.Fatalf("no-op or atomic refusal changed state/journal: %+v", refusal)
			}
			p.advance(t)
			closed := success[model.Commit](t, "bead.close", r.call(t, "", "bead.close", map[string]any{"id": ref, "reason": "complete"}, true))
			if closed.Revision.String() != "3" {
				t.Fatal("rollback/no-op burned a revision")
			}
			final := b17StateOf(t, p.b17Harness, ref)
			check := func() {
				t.Helper()
				r = p.rpc(t)
				r.principal = model.PrincipalRef{Kind: "user", ID: "b20-recorder"}
				var actual []map[string]any
				after := ""
				for i := 0; i < 3; i++ {
					params := map[string]any{"filter": map[string]any{}, "limit": 1}
					if after != "" {
						params["after"] = after
					}
					page := success[eventPage](t, "event.list", r.call(t, "", "event.list", params, false))
					if len(page.Events) != 1 || page.Events[0].Durable == nil || page.Cursor == after {
						t.Fatalf("missing one-row ascending page at %d: %+v", i, page)
					}
					event := page.Events[0].Durable
					if event.Revision.String() != fmt.Sprint(i+1) || event.Index.String() != "0" || event.Principal.ID != "b20-recorder" || event.Resource != "bead/"+ref.ID || event.After["id"] != ref.ID {
						t.Fatalf("journal order/identity/actor drift at %d: %+v", i, event)
					}
					fields := event.After["fields"].(map[string]any)
					status := map[string]string{"todo": "open", "done": "closed"}[fmt.Sprint(fields["status"])]
					if status == "" {
						t.Fatalf("unmapped Plug status %v", fields["status"])
					}
					// Explicit one-fixture ID mapping and ontology category mapping;
					// sequence, event type, content and principal remain observed.
					row := map[string]any{"seq": float64(i + 1), "issueId": "b20-1", "actor": event.Principal.ID, "title": fields["title"], "status": status}
					row["op"] = map[model.DurableEventType]string{model.EventBeadCreated: "create", model.EventBeadUpdated: "update", model.EventBeadClosed: "close"}[event.Type]
					if event.Type == model.EventBeadClosed {
						comments := event.After["comments"].([]any)
						if len(comments) != 1 {
							t.Fatal("close reason was not committed atomically")
						}
						row["closeReason"] = comments[0].(map[string]any)["body"]
					}
					actual = append(actual, row)
					after = page.Cursor
				}
				caughtUp := success[eventPage](t, "event.list", r.call(t, "", "event.list", map[string]any{"filter": map[string]any{}, "after": after, "limit": 1}, false))
				if len(caughtUp.Events) != 0 || !reflect.DeepEqual(actual, reference) {
					t.Fatalf("independent Plug/Beads journal projections differ: actual=%+v reference=%+v caughtUp=%+v", actual, reference, caughtUp)
				}
			}
			check()
			p.reopen(t)
			if !reflect.DeepEqual(final, b17StateOf(t, p.b17Harness, ref)) {
				t.Fatal("reopen changed committed versions/events")
			}
			check()
			t.Logf("%s B20 retained pinned sequence/type/content/actor/reason projection matches across no-op, rollback, one-row pages and reopen; full vocabulary/pruning remains unqualified", engine)
		})
	}
}
