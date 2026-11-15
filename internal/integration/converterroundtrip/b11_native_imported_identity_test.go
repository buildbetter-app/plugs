package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	"github.com/buildbetter-app/plugs/internal/server/rpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

const (
	b11NativeParentWorkflowItemID = "workflow:item/00000000000000000000000000000011"
	b11NativeChildWorkflowItemID  = "workflow:item/00000000000000000000000000000012"
)

// These are independent expectations from the delivered native SQL ledger,
// including current-only rows. They are never constructed from Plug output.
func b11NativeEdgeExpectations() map[positiveSourceTuple]string {
	return map[positiveSourceTuple]string{
		{"review-project", dumpformat.DurableDependencyTable, "39a0da1c-ae92-57fc-a508-a32795aaf3a5", "review-project-80a", "review-project-afr", "blocks"}:            "depends_on_issue_id",
		{"review-project", dumpformat.DurableDependencyTable, "8606de67-826a-5495-aefe-a9a8722fdb3d", "review-project-c2d", "review-project-a1b", "related"}:           "depends_on_issue_id",
		{"review-project", dumpformat.DurableDependencyTable, "c2d867f4-ab7a-5b4d-871a-c203cd92e052", "review-project-a1b.2", "review-project-a1b", "parent-child"}:    "depends_on_issue_id",
		{"review-project", dumpformat.DurableDependencyTable, "d9a96525-cd00-5dd1-88ce-7cc8f2dcff87", "review-project-a1b.1", "review-project-a1b", "parent-child"}:    "depends_on_issue_id",
		{"review-project", dumpformat.DurableDependencyTable, "f59089ac-4601-542f-94df-36b1fea3b41a", "review-project-a1b.1", "review-project-c2d", "discovered-from"}: "depends_on_issue_id",
		{"review-project", dumpformat.EphemeralDependencyTable, "0f39a7f3-56d7-5517-8c08-69eb0e55962f", "review-project-e4f.1", "review-project-a1b", "related"}:       "depends_on_issue_id",
		{"review-project", dumpformat.EphemeralDependencyTable, "4f09eb7b-a0cd-58e2-9f69-5c5759690f8a", "review-project-e4f.1", "review-project-e4f", "parent-child"}:  dumpformat.EphemeralDependencyTargetField,
	}
}

// UUIDv5 namespace and separator come from the pinned native contract. This
// oracle deliberately does not call the copied depid implementation.
func b11NativeEndpointUUID(from, to string) string {
	namespace := []byte{0xbd, 0xd7, 0x4e, 0xb9, 0xa2, 0x0a, 0x55, 0x4a, 0x98, 0x5b, 0x54, 0xb8, 0xf3, 0xe6, 0x4d, 0x8b}
	hash := sha1.New()
	hash.Write(namespace)
	hash.Write([]byte(from + "\x1f" + to))
	digest := hash.Sum(nil)
	digest[6] = digest[6]&0x0f | 0x50
	digest[8] = digest[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", digest[:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16])
}

func b11NativeEdgeArchive(t *testing.T) *dumpformat.Archive {
	t.Helper()
	path := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), "spec/cardb11/testdata/public-r5-edge-inputs.tar.gz")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("committed public native runfile", err)
	}
	if proof.Hash(raw) != "80d21b1573730cb0b44d9984e3bf1f39b3f57a5da6988ff2bb04c067cc100b66" {
		t.Fatal("public native capture package changed")
	}
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	files := map[string][]byte{}
	archiveReader := tar.NewReader(reader)
	for {
		header, err := archiveReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg || files[header.Name] != nil {
			t.Fatal("capture package has an unexpected or duplicate member")
		}
		files[header.Name], err = io.ReadAll(archiveReader)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(files) != 9 {
		t.Fatal("public native package member census changed")
	}
	for name, want := range map[string]string{
		"capture-set/capture-set.json":                "a63f4a39ab3579301be116a8ba13a0b4af76b728f6a440da484d662fb374cc06",
		"capture-set/head.tar":                        "f82ab7b8b97da0a3322c8fd47d3f40024fe7d64d3c9f99f6a2b07b79b7a8917b",
		"capture-set/parts/00000000000000000001.part": "775633d7234e5d676766e5a7abbe9095964c85ea003be1479a5efa6ef85760c6",
		"source/capture-set-provenance.json":          "955a9b4d085adc0724e6a2937c4939ef9aa2d04af7ecff41ef12c63b6330ac42",
		"source/typed-current.json":                   "c946aa47f3ff4a28801c3f2a5c01fbf4eaa30bb54583d9fe8edd45736a48f623",
		"source/physical-current-schema.json":         "b83f812aa0fa81b951f2aa125f689d3b1bfe2a3bef5bae1f59f2fc615be4e841",
		"source/payload-manifest.json":                "84e47cb5b52d8424a37cca328143d69bad14294adfdcf76108edf0096e8da516",
		"source/a9d11febd181f8c2b61a865ad014d2b054a79980eb4cd6c78ee7b50b7f94180b.table.json": "a9d11febd181f8c2b61a865ad014d2b054a79980eb4cd6c78ee7b50b7f94180b",
		"source/d2990693f7174eb563f11cf43870e57cf1454f702fa8267950a0870506e2e3f0.table.json": "d2990693f7174eb563f11cf43870e57cf1454f702fa8267950a0870506e2e3f0",
	} {
		if proof.Hash(files[name]) != want {
			t.Fatal("independently delivered inner input changed", name)
		}
	}
	archive, err := dumpformat.ReadNativeCaptureSet(context.Background(), bytes.NewReader(files["capture-set/capture-set.json"]), bytes.NewReader(files["capture-set/head.tar"]), func(_ context.Context, n uint64) (io.ReadCloser, error) {
		part, ok := files[fmt.Sprintf("capture-set/parts/%020d.part", n)]
		if !ok {
			return nil, os.ErrNotExist
		}
		return io.NopCloser(bytes.NewReader(part)), nil
	}, dumpformat.NativeCaptureSetOptions{})
	if err != nil {
		t.Fatal("authenticate complete existing public native capture", err)
	}
	if archive.SHA256() != "a63f4a39ab3579301be116a8ba13a0b4af76b728f6a440da484d662fb374cc06" || archive.Manifest().StoreID != "native/2b6da2db2dd34017be9ac3878b39ce1ae036add2d4c76f95a6e0921d104ec362" {
		t.Fatal("native source identity differs")
	}
	verification, verified := archive.NativeDeltaVerification()
	if !verified || verification.Totals.Commits != 122 || verification.Usage.LimitBytes != 2147483648 || verification.Usage.Accounting != dumpformat.NativeDeltaDecodedAccounting {
		t.Fatal("native source history or default decoded accounting differs")
	}
	want := b11NativeEdgeExpectations()
	actual := map[positiveSourceTuple]string{}
	for _, row := range archive.Records("edge") {
		key := positiveSourceTuple{row.Project, row.Source.Table, row.ID, stringValue(row.Body["from"]), stringValue(row.Body["to"]), stringValue(row.Body["type"])}
		if _, duplicate := actual[key]; duplicate || b11NativeEndpointUUID(key.From, key.To) != key.ID {
			t.Fatal("captured edge has duplicate or noncanonical endpoint identity", key)
		}
		actual[key] = stringValue(row.Body["target_kind"])
	}
	if !reflect.DeepEqual(want, actual) {
		t.Fatal("native source edges differ from independent typed SQL expectations", actual)
	}
	return archive
}

// This is explicit acceptance host intent over the unchanged capture, using
// the installed source-plan contract and receiver. No importer is implemented.
func b11NativeEdgePlan(t *testing.T, archive *dumpformat.Archive, store driver.Store, lease driver.ActiveLease, actor model.PrincipalRef) driver.SourcePlanePlanV1 {
	t.Helper()
	plan := nativeHistoryPlanFromArchive(t, archive, store, lease)
	plan.Principals[0].PrincipalJSON, _ = json.Marshal(actor)
	base := plan.Dispositions[0]
	base.StorageClass = ""
	base.Statuses = nil
	for _, status := range archive.Manifest().Projects[0].Statuses {
		base.Statuses = append(base.Statuses, driver.SourcePlaneStringPairV1{From: status.Name, To: status.Name})
	}
	sort.Slice(base.Statuses, func(i, j int) bool { return base.Statuses[i].From < base.Statuses[j].From })
	plan.Dispositions = nil
	for _, sourceType := range []string{"gate", "rich_kind", "task"} {
		disposition := base
		disposition.Key, disposition.SourceType = "durable-"+sourceType, sourceType
		plan.Dispositions = append(plan.Dispositions, disposition)
	}
	for i := range plan.Entities {
		for _, row := range archive.Records("bead") {
			if row.ID == plan.Entities[i].Source.ID {
				plan.Entities[i].DispositionKey = "durable-" + stringValue(row.Source.Fields["issue_type"])
			}
		}
	}
	activeIDs := map[string]string{"review-project-e4f": b11NativeParentWorkflowItemID, "review-project-e4f.1": b11NativeChildWorkflowItemID}
	active := driver.SourcePlaneDispositionV1{Key: "active-task", SourceType: "task", Ephemeral: true, NoHistory: false, SinkKind: "workflow:item", Schema: workflowplugin.SourceStateSchema, Lifetime: string(plan.Target.Lifetime), Statuses: []driver.SourcePlaneStringPairV1{{From: "open", To: "open"}}, RefusalPolicy: "exact-only"}
	plan.Runs = []driver.SourcePlaneRunV1{{Project: "review-project", Key: "b11-r5", TargetID: "workflow:run/b11-r5", Template: "public captured endpoints", Schema: workflowplugin.SourceStateSchema, Mode: "ephemeral", State: "active", PrincipalKey: "owner"}}
	records, err := archive.SourcePlaneRecords()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		var row dumpformat.Record
		decode(t, record.Raw, &row)
		origin, fields := jointCommitment(record.Raw, "object"), jointFields(t, record)
		if record.Table == dumpformat.EphemeralEntityTable {
			id := stringValue(row.Source.Fields["id"])
			target, ok := activeIDs[id]
			if !ok {
				t.Fatal("unexpected current-only native endpoint", id)
			}
			key := driver.SourceEntityKey{Project: "review-project", Plane: record.Table, ID: id}
			plan.Entities = append(plan.Entities, driver.SourcePlaneEntityV1{Source: key, TargetKind: "workflow:item", TargetID: target, DispositionKey: active.Key, PrincipalKey: "owner", RunKey: "b11-r5", Route: driver.SourcePlaneRouteV1{Project: key.Project, SourceID: id, ActiveTargetID: target, Preference: "active-first"}, Origin: origin, Fields: fields})
			plan.Runs[0].Members = append(plan.Runs[0].Members, key)
			if active.Fields == nil {
				for _, field := range fields {
					rule := driver.SourcePlaneFieldRuleV1{Source: field.Name, Disposition: "origin-only"}
					switch field.Name {
					case "id", "title", "status", "metadata", "created_at":
						rule.Target, rule.Disposition = field.Name, "exact-map"
					case "issue_type":
						rule.Target, rule.Disposition = "type", "exact-map"
					}
					active.Fields = append(active.Fields, rule)
				}
			}
		}
		if row.Kind == "edge" {
			fromPlane, toPlane := dumpformat.DurableEntityTable, dumpformat.DurableEntityTable
			if record.Table == dumpformat.EphemeralDependencyTable {
				fromPlane = dumpformat.EphemeralEntityTable
			}
			if row.Body["target_kind"] == dumpformat.EphemeralDependencyTargetField {
				toPlane = dumpformat.EphemeralEntityTable
			}
			from := driver.SourceEntityKey{Project: row.Project, Plane: fromPlane, ID: stringValue(row.Body["from"])}
			to := driver.SourceEntityKey{Project: row.Project, Plane: toPlane, ID: stringValue(row.Body["to"])}
			predicate, err := dumpformat.SourceEdgeMetadata(dumpformat.EdgeVersionLocalOpaque, record.ID, stringValue(row.Body["type"]), row.Source.Fields["metadata"])
			if err != nil {
				t.Fatal(err)
			}
			predicateJSON, err := json.Marshal(predicate)
			if err != nil {
				t.Fatal(err)
			}
			plan.Relations = append(plan.Relations, driver.SourcePlaneRelationV1{Source: driver.SourceEdgeKey{Project: row.Project, Table: record.Table, ID: record.ID}, From: from, To: &to, Kind: stringValue(row.Body["type"]), PredicateContract: "beads-typed-metadata/1", PredicateJSON: predicateJSON, Origin: origin, Fields: fields})
		}
	}
	plan.Dispositions = append(plan.Dispositions, active)
	sort.Slice(plan.Entities, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(plan.Entities[i].Source, plan.Entities[j].Source) < 0
	})
	sort.Slice(plan.Relations, func(i, j int) bool {
		return driver.CompareSourceEdgeKeys(plan.Relations[i].Source, plan.Relations[j].Source) < 0
	})
	sort.Slice(plan.Runs[0].Members, func(i, j int) bool {
		return driver.CompareSourceEntityKeys(plan.Runs[0].Members[i], plan.Runs[0].Members[j]) < 0
	})
	sort.Slice(plan.Dispositions, func(i, j int) bool { return plan.Dispositions[i].Key < plan.Dispositions[j].Key })
	if len(plan.Entities) != 14 || len(plan.Relations) != 7 || len(plan.Runs[0].Members) != 2 {
		t.Fatal("explicit public native source plan omitted an endpoint or edge")
	}
	return plan
}

func TestB11NativeCapturedPublicEdgesMetadataAndSQLReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			archive := b11NativeEdgeArchive(t)
			defer archive.ReleaseNativeDecoded()
			root := proof.Output(t)
			store, initialLease, reopen := openIdentityTarget(t, engine)
			if err := initialLease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, lease, release, err := HoldRestoreLease(context.Background(), store, "b11-native-public")
			if err != nil {
				t.Fatal(err)
			}
			release = sync.OnceValue(release)
			defer func() {
				if err := release(); err != nil {
					t.Error(err)
				}
			}()
			ctx = archive.WithNativeDeltaAccounting(ctx)
			profile := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "restore-admin"}, Authority: "restore-admin", AdmissionDigest: strings.Repeat("a", 64)}, SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: strings.Repeat("b", 32), ArchiveDigest: archive.SHA256(), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: nativeFixtureTime.Format("2006-01-02T15:04:05.999999999Z07:00")}
			plan := func() driver.SourcePlanePlanV1 {
				work, release := archive.NativeDeltaAccountingScope()
				defer release()
				return b11NativeEdgePlan(t, work, store, lease, profile.Admission.Actor)
			}()
			profile, err = BindNativeCaptureSetImport(ctx, archive, profile, plan)
			if err != nil {
				t.Fatal("bind unchanged native source capture", err)
			}
			report, err := RestoreNativeCaptureSetImportWithLease(ctx, archive, store, lease, profile)
			if err != nil || report.SourcePlanes == nil || report.Receipt == nil {
				t.Fatal("publish native captured endpoints and edges", err)
			}
			if err := VerifyNativeCaptureSetReadback(ctx, store, archive, report); err != nil {
				t.Fatal("complete native row/source readback", err)
			}
			capability, err := driver.RequireWorkflowCheckpointStore(store, lease)
			if err != nil {
				t.Fatal(err)
			}
			persistent := false
			marker, markerPresent := capability.(driver.PersistentCheckpointStore)
			if markerPresent {
				persistent = marker.Persistent()
			}
			wantLifetime := driver.CheckpointLifetimeRestart
			if engine == "memory" {
				wantLifetime = driver.CheckpointLifetimeProcess
			}
			if capability.WorkflowCheckpointLifetime() != wantLifetime || persistent != (engine != "memory") {
				t.Fatal("native receiving target misrepresented lifetime or persistence")
			}
			proof.Save(t, root, "native-binding.json", map[string]any{"profile": profile, "plan": plan, "originalReceipt": report.Receipt, "sourcePlanes": report.SourcePlanes, "archiveSHA256": archive.SHA256(), "lifetime": capability.WorkflowCheckpointLifetime(), "persistenceMarkerPresent": markerPresent, "persistent": persistent, "nativeHistoryQualification": "separate from public edge behavior"})
			domain, server := sourcePublicService(t, store, lease, profile, report, &sourcePublicPolicy{})
			if !domain.SourceRuntimeInstalled() {
				t.Fatal("native imported source runtime is absent")
			}
			scopes := map[string]proof.Scope{"restore": {profile.Method, profile.IdempotencyKey}}
			sequence, observations := 0, 0
			call := func(method, label string, params map[string]any, want string) sourcePublicReply {
				t.Helper()
				key := proof.Hash([]byte(label))[:32]
				scopes[label] = proof.Scope{method, key}
				params["_meta"] = map[string]any{"project": "review-project", "idempotencyKey": key}
				request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
				if err != nil {
					t.Fatal(err)
				}
				response := server.Dispatch(rpc.WithPrincipal(ctx, profile.Admission.Actor), request)
				sequence++
				proof.Write(t, root, fmt.Sprintf("rpc-%04d-request.json", sequence), request)
				proof.Write(t, root, fmt.Sprintf("rpc-%04d-response.json", sequence), response)
				var reply sourcePublicReply
				decode(t, response, &reply)
				if want != "" {
					if reply.Error == nil || reply.Error.Data.Code != want || len(reply.Result) != 0 {
						t.Fatalf("%s expected %s: %s", method, want, response)
					}
				} else if reply.Error != nil || len(reply.Result) == 0 {
					t.Fatalf("%s failed: %s", method, response)
				}
				return reply
			}
			capture := func() proof.State {
				t.Helper()
				state := proof.Capture(t, store, profile.Admission.Actor, profile.AgentLabel(), scopes)
				observations++
				proof.Save(t, root, fmt.Sprintf("state-%04d.json", observations), state)
				return state
			}
			bySource := map[string]driver.SourcePlaneEntityV1{}
			for _, entity := range plan.Entities {
				bySource[entity.Source.ID] = entity
			}
			ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
			wantMetadata := map[string]string{
				"8606de67-826a-5495-aefe-a9a8722fdb3d": `{"explicit_null":null,"number":42,"source":"public-stored-sql"}`,
				"0f39a7f3-56d7-5517-8c08-69eb0e55962f": `{}`,
			}
			check := func(label string, state proof.State) []byte {
				t.Helper()
				reversed, err := ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
				if err != nil {
					t.Fatal("native source edge reverse", err)
				}
				proof.Write(t, root, label+"-reverse.jsonl", reversed.Result.JSONL)
				expected := map[positiveSourceTuple]map[string]string{}
				for key := range b11NativeEdgeExpectations() {
					metadata := `{}`
					if token, present := wantMetadata[key.ID]; present {
						metadata = token
					}
					expected[key] = map[string]string{"beads.metadata": metadata, "beads.thread_id": `""`}
				}
				// The public reverse projection has a generic local target ID.
				// Physical target arms are asserted independently in the captured
				// source rows; no nonexistent public arm field is inferred here.
				actual := positiveReverseSourceEdges(t, reversed.Result.JSONL, bySource)
				if !reflect.DeepEqual(expected, actual) {
					t.Fatal("native endpoint UUID/type/lexical-token conservation differs", actual)
				}
				proof.Save(t, root, label+"-edges.json", positiveEdgeList(actual))
				for key, tokens := range expected {
					read := call("bead.get", label+"-read-"+key.From, map[string]any{"id": ref(key.From)}, "")
					var view struct {
						Bead struct {
							ID     string
							Parent string
							Links  []workflowplugin.RelationView
						}
					}
					decode(t, read.Result, &view)
					from, to := bySource[key.From].TargetID, bySource[key.To].TargetID
					if view.Bead.ID != from {
						t.Fatal("native source alias changed its public endpoint")
					}
					if key.Kind == "parent-child" {
						if view.Bead.Parent != to {
							t.Fatal("captured native parent endpoint differs")
						}
						continue
					}
					count := 0
					for _, link := range view.Bead.Links {
						if link.From == from && link.To == ref(to) {
							count++
							if link.Kind != positiveSourceLiveKind(key.Kind) || link.Metadata == nil || link.Metadata.HasPredicate() || !reflect.DeepEqual(link.Metadata.NativeExtensions, tokens) {
								t.Fatal("native public edge metadata/type readback differs")
							}
						}
					}
					if count != 1 {
						t.Fatal("captured native public edge duplicated or missing")
					}
				}
				proof.Same(t, state, capture())
				return reversed.Result.JSONL
			}
			before := capture()
			initialReverse := check("initial", before)
			mixedFrom, mixedTo := ref("review-project-e4f.1"), ref("review-project-a1b")
			mixedEqualParams := map[string]any{"from": mixedFrom, "to": mixedTo, "kind": "relates-to", "metadata": &model.LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": ` { } `}}}
			mixedEqual := call("bead.link", "native-mixed-semantic-equal", mixedEqualParams, "")
			var commit model.Commit
			decode(t, mixedEqual.Result, &commit)
			after := capture()
			b11EqualSourceRelationEffect(t, before, after, commit, "native-mixed-semantic-equal")
			if !bytes.Equal(initialReverse, check("mixed-semantic-equal", after)) {
				t.Fatal("equivalent captured mixed edge changed UUID/type/tokens")
			}
			call("bead.link", "native-mixed-retype-refused", map[string]any{"from": mixedFrom, "to": mixedTo, "kind": "blocks"}, "ontology_guard_failed")
			proof.Same(t, after, capture())
			before = capture()
			// Re-add the captured mixed relation through endpoints admitted by
			// the active task workflow. The native durable c2d endpoint carries
			// custom status qa_testing, which is intentionally outside that
			// workflow and must remain source evidence rather than a writable
			// ordinary endpoint.
			from, to := mixedFrom, mixedTo
			params := func(kind, token string) map[string]any {
				params := map[string]any{"from": from, "to": to, "kind": kind}
				if token != "" {
					params["metadata"] = &model.LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": token}}
				}
				return params
			}
			equalParams := params("relates-to", ` { } `)
			equal := call("bead.link", "native-semantic-equal", equalParams, "")
			decode(t, equal.Result, &commit)
			after = capture()
			receipt, found := after.Receipts["native-semantic-equal"]
			if !found || receipt.Pending || !reflect.DeepEqual(receipt.Commit, commit) || !reflect.DeepEqual(receipt.Changeset, model.Changeset{}) || commit.Revision != before.Meta.Head {
				t.Fatal("native imported semantic re-add did not publish an exact no-op receipt")
			}
			after.Receipts = maps.Clone(after.Receipts)
			delete(after.Receipts, "native-semantic-equal")
			proof.Same(t, before, after)
			after = capture()
			if !bytes.Equal(initialReverse, check("semantic-equal", after)) {
				t.Fatal("native source identity or lexical tokens changed on equivalent re-add")
			}
			before = capture()
			call("bead.link", "native-retype-refused", params("blocks", ""), "ontology_guard_failed")
			proof.Same(t, before, capture())
			call("tx.apply", "native-late-retype-refused", map[string]any{"changeSet": model.ChangeSet{Ops: []model.Op{
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref(b11NativeChildWorkflowItemID), Fields: map[string]any{"title": "discard captured active draft"}}},
				{Kind: model.OpPatch, Patch: &model.PatchOp{Target: ref("review-project-a1b.1"), Fields: map[string]any{"title": "discard captured durable draft"}}},
				{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: "blocks"}},
			}}}, "ontology_guard_failed")
			proof.Same(t, before, capture())
			// Mutate the captured current-only relation through its installed
			// public provider. The native durable witness above remains exact;
			// this host does not invent a workflow for its custom status catalog.
			changedToken := ` {"outer":{"n":1.0,"array":[true,null,{"text":"x"}]},"source":"public-captured-mixed"} `
			changedParams := map[string]any{"from": mixedFrom, "to": mixedTo, "kind": "relates-to", "metadata": &model.LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": changedToken}}}
			changed := call("bead.link", "native-metadata-change", changedParams, "")
			decode(t, changed.Result, &commit)
			after = capture()
			positiveJoinedEffect(t, before, after, commit, "native-metadata-change", "", "bead.link")
			wantMetadata["0f39a7f3-56d7-5517-8c08-69eb0e55962f"] = changedToken
			changedReverse := check("changed", after)
			before = capture()
			changedEqualParams := map[string]any{"from": mixedFrom, "to": mixedTo, "kind": "relates-to", "metadata": &model.LinkMetadata{NativeExtensions: map[string]string{"beads.metadata": `{"source":"public-captured-mixed","outer":{"array":[true,null,{"text":"\u0078"}],"n":1e0}}`}}}
			changedEqual := call("bead.link", "native-changed-semantic-equal", changedEqualParams, "")
			decode(t, changedEqual.Result, &commit)
			after = capture()
			b11EqualSourceRelationEffect(t, before, after, commit, "native-changed-semantic-equal")
			if !bytes.Equal(changedReverse, check("changed-semantic-equal", after)) {
				t.Fatal("equivalent changed metadata rewrote native source identity/tokens")
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			oldStore, oldEpoch := store, lease.Epoch()
			store, initialLease = reopen()
			if err := initialLease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, lease, release, err = HoldRestoreLease(context.Background(), store, "b11-native-reopened")
			if err != nil {
				t.Fatal(err)
			}
			release = sync.OnceValue(release)
			ctx = archive.WithNativeDeltaAccounting(ctx)
			if engine != "memory" && (store == oldStore || lease.Epoch() <= oldEpoch) {
				t.Fatal("native SQL reopen did not replace the physical store handle and lease")
			}
			proof.Save(t, root, "reopen.json", map[string]any{"engine": engine, "oldStore": fmt.Sprintf("%p", oldStore), "newStore": fmt.Sprintf("%p", store), "oldEpoch": oldEpoch, "newEpoch": lease.Epoch(), "sameProcessMemory": engine == "memory"})
			bound, recovered, found, err := RecoverBoundSourceImport(ctx, store)
			if recovered.Archive != nil {
				defer recovered.Archive.ReleaseNativeDecoded()
			}
			if err != nil || !found || !reflect.DeepEqual(recovered.Receipt, report.Receipt) || !reflect.DeepEqual(recovered.SourcePlanes, report.SourcePlanes) {
				t.Fatal("native source binding/original receipt recovery changed", err)
			}
			if err := VerifyNativeCaptureSetReadback(ctx, store, archive, recovered); err != nil {
				t.Fatal("reopened native source-row readback", err)
			}
			_, server = sourcePublicService(t, store, lease, bound, recovered, &sourcePublicPolicy{})
			if !bytes.Equal(changedReverse, check("reopened", after)) {
				t.Fatal("native SQL reopen changed live endpoint identities or metadata")
			}
			call("bead.link", "native-reopened-retype-refused", params("blocks", ""), "ontology_guard_failed")
			proof.Same(t, after, capture())
			for _, original := range []struct {
				label  string
				params map[string]any
				reply  sourcePublicReply
			}{{"native-mixed-semantic-equal", mixedEqualParams, mixedEqual}, {"native-semantic-equal", equalParams, equal}, {"native-metadata-change", changedParams, changed}, {"native-changed-semantic-equal", changedEqualParams, changedEqual}} {
				replay := call("bead.link", original.label, original.params, "")
				if !replay.Meta.Replayed || !bytes.Equal(replay.Result, original.reply.Result) {
					t.Fatal("reopened native edge write lost the original exact replay response")
				}
				proof.Same(t, after, capture())
			}
		})
	}
}
