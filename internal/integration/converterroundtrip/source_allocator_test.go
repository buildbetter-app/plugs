package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Add active-only source identities to an explicitly synthetic archive. Each
// changed row has its own content address; real captures are never rewritten.
func jointAllocatorFixture(t *testing.T, aliases []string) []byte {
	t.Helper()
	base := jointSourceFixture(t)
	return rewriteData(t, func(members map[string][]byte) {
		reader := tar.NewReader(bytes.NewReader(base))
		for {
			header, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			members[header.Name], err = io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
		}
		var manifest dumpformat.Manifest
		decode(t, members["manifest.json"], &manifest)
		var rows []dumpformat.Record
		var active dumpformat.Record
		for _, line := range bytes.Split(bytes.TrimSpace(members["records/beads-archive.ndjson"]), []byte{'\n'}) {
			var row dumpformat.Record
			decode(t, line, &row)
			rows = append(rows, row)
			if row.Source.Table == dumpformat.EphemeralEntityTable {
				active = row
			}
		}
		if active.Source.Fields == nil {
			t.Fatal("fixture lacks an active row")
		}
		for _, id := range aliases {
			row := active
			row.Source.Fields = cloneRow(active.Source.Fields)
			row.Source.Fields["id"] = id
			row.ID = row.Source.Table + "/" + digestBytes(encode(t, row.Source.Fields))
			rows = append(rows, row)
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].Project != rows[j].Project {
				return rows[i].Project < rows[j].Project
			}
			return rows[i].ID < rows[j].ID
		})
		var data []byte
		for _, row := range rows {
			data = append(data, encode(t, row)...)
		}
		members["records/beads-archive.ndjson"] = data
		for i := range manifest.Kinds {
			if manifest.Kinds[i].Name == "beads-archive" {
				manifest.Kinds[i].Count = len(rows)
			}
		}
		for i := range manifest.Sources {
			if manifest.Sources[i].Family == dumpformat.EphemeralEntityTable {
				manifest.Sources[i].Rows += len(aliases)
			}
		}
		members["manifest.json"] = encode(t, manifest)
	})
}

func TestJointSourceAllocatorReservesActiveAndTerminalAliasesAcrossThreeStores(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 123456000, time.UTC)
	actor := model.PrincipalRef{Kind: "user", ID: "restore-admin"}
	creator, err := driver.PrincipalKey(actor)
	if err != nil {
		t.Fatal(err)
	}
	input := driver.HashInput{Title: "reserved source root", Creator: creator, Timestamp: now}
	candidate := func(nonce uint32) string {
		t.Helper()
		id, err := driver.HashCandidate("demo", input, 3, nonce)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	// The imported parents are closed or blocked. Reserve the first child
	// of the ordinary root this test will create without changing either guard.
	parentID := candidate(2)
	aliases := []string{candidate(0), candidate(1), parentID + ".1"}
	data := jointAllocatorFixture(t, aliases)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			p := jointProfileWithPlan(t, data, store, lease, func(plan *driver.SourcePlanePlanV1) {
				for i := range plan.Entities {
					e := &plan.Entities[i]
					if e.TargetKind == "workflow:item" && e.Source.ID != "demo-7" {
						e.TargetID = fmt.Sprintf("workflow:item/reserved-%d", i)
						e.Route.ActiveTargetID, e.Route.DurableTargetID = e.TargetID, ""
						plan.Runs[0].Members = append(plan.Runs[0].Members, e.Source)
					}
				}
			})
			report, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p)
			if err != nil {
				t.Fatal(err)
			}
			policy := &sourcePublicPolicy{}
			options := []pipeline.Option{pipeline.WithClock(func() time.Time { return now })}
			_, server := sourcePublicService(t, store, lease, p, report, policy, options...)
			ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			checkpoint := func() driver.Checkpoint {
				t.Helper()
				value, err := capability.LoadCheckpoint(ctx, report.SourcePlanes.Providers[0].Provider.Namespace)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			initial := savedLabelImport(t, store, report.Archive, p, "")
			for _, id := range aliases {
				for _, record := range initial.Records {
					if record.Ref.ID == id {
						t.Fatal("source alias unexpectedly has a core record", id)
					}
				}
				sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": ref(id)}, "")
			}
			create := model.Op{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "demo", Fields: map[string]any{"title": input.Title}}}
			rootOnly := model.ChangeSet{Ops: []model.Op{create}}
			before, sourceBefore := savedLabelImport(t, store, report.Archive, p, ""), checkpoint()
			policy.deny = true
			sourcePublicCall(t, server, actor, "tx.apply", 601, map[string]any{"changeSet": rootOnly}, "forbidden")
			policy.deny = false
			bad := model.ChangeSet{Ops: []model.Op{create, {Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("missing"), Fields: map[string]any{"title": "refused after allocation"}}}}}
			sourcePublicCall(t, server, actor, "tx.apply", 602, map[string]any{"changeSet": bad}, "not_found")
			sourcePublicCall(t, server, actor, "bead.create", 608, map[string]any{"type": "issue", "parent": ref("demo-8"), "fields": map[string]any{"title": "refused under closed parent"}}, "ontology_guard_failed")
			sourcePublicCall(t, server, actor, "bead.create", 609, map[string]any{"type": "issue", "parent": ref("demo-7.4"), "fields": map[string]any{"title": "refused under blocked parent"}}, "ontology_guard_failed")
			if !reflect.DeepEqual(before, savedLabelImport(t, store, report.Archive, p, "")) || !reflect.DeepEqual(sourceBefore, checkpoint()) {
				t.Fatal("refused allocation changed records, history, receipts, blobs or source checkpoint")
			}
			victim := sourcePublicCall(t, server, actor, "bead.create", 603, map[string]any{"type": "issue", "fields": map[string]any{"title": "ordinary temporary victim"}}, "")
			var victimCommit model.Commit
			decode(t, victim.Result, &victimCommit)
			if len(victimCommit.Results) != 1 || victimCommit.Results[0].Ref.ID == "" {
				t.Fatal("missing ordinary victim result")
			}
			// A successful joined delete replaces SourceOperations. Identity
			// reservations must survive that replacement and all staged writes.
			mixed := model.ChangeSet{Ops: []model.Op{create, {Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref(victimCommit.Results[0].Ref.ID)}}}}
			root := sourcePublicCall(t, server, actor, "tx.apply", 604, map[string]any{"changeSet": mixed}, "")
			var rootCommit model.Commit
			decode(t, root.Result, &rootCommit)
			if len(rootCommit.Results) != 2 || rootCommit.Results[0].Ref.ID != candidate(2) {
				t.Fatalf("active-only source identities did not advance the hash nonce: %+v", rootCommit)
			}
			parent := ref(parentID)
			child := sourcePublicCall(t, server, actor, "bead.create", 605, map[string]any{"type": "issue", "parent": parent, "fields": map[string]any{"title": "child after restored alias"}}, "")
			var childCommit model.Commit
			decode(t, child.Result, &childCommit)
			if len(childCommit.Results) != 1 || childCommit.Results[0].Ref.ID != parentID+".2" || !reflect.DeepEqual(sourceBefore, checkpoint()) {
				t.Fatal("active child alias was reused or ordinary allocation changed source state", childCommit)
			}
			// Completion removes the current active target, but its original
			// source address must continue to reserve its naked identity.
			sourcePublicCall(t, server, actor, "bead.close", 606, map[string]any{"id": ref(aliases[0]), "reason": "terminal identity remains reserved"}, "")
			sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": ref(aliases[0])}, "not_found")
			second := sourcePublicCall(t, server, actor, "tx.apply", 607, map[string]any{"changeSet": rootOnly}, "")
			var secondCommit model.Commit
			decode(t, second.Result, &secondCommit)
			if len(secondCommit.Results) != 1 || secondCommit.Results[0].Ref.ID != candidate(3) {
				t.Fatal("terminal alias became available to the allocator", secondCommit)
			}
			final, sourceFinal := savedLabelImport(t, store, report.Archive, p, ""), checkpoint()
			for _, original := range initial.Records {
				found := false
				for _, current := range final.Records {
					if current.Ref.ID == original.Ref.ID {
						found = reflect.DeepEqual(current, original)
					}
				}
				if !found {
					t.Fatal("allocation altered an imported record", original.Ref.ID)
				}
			}
			if !reflect.DeepEqual(initial.Blobs, final.Blobs) || !reflect.DeepEqual(initial.ImportReceipt, final.ImportReceipt) {
				t.Fatal("allocation altered raw source commitments or original import receipt")
			}
			store, lease = reopen()
			capability, err = driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			_, server = sourcePublicService(t, store, lease, p, report, policy, options...)
			for _, replay := range []struct {
				key    int
				cs     model.ChangeSet
				commit model.Commit
			}{{604, mixed, rootCommit}, {607, rootOnly, secondCommit}} {
				reply := sourcePublicCall(t, server, actor, "tx.apply", replay.key, map[string]any{"changeSet": replay.cs}, "")
				var commit model.Commit
				decode(t, reply.Result, &commit)
				if !reply.Meta.Replayed || !reflect.DeepEqual(commit, replay.commit) || !reflect.DeepEqual(final, savedLabelImport(t, store, report.Archive, p, "")) || !reflect.DeepEqual(sourceFinal, checkpoint()) {
					t.Fatal("reopen replay changed allocation, source state or durable result")
				}
			}
			sourcePublicCall(t, server, actor, "bead.get", 0, map[string]any{"id": ref(aliases[0])}, "not_found")
		})
	}
}
