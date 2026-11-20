package converterroundtrip

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
)

// These full-archive preservation controls explicitly request the pinned --all
// form. Default filtering and omission reports have separate joint controls.
func reverseAllRestored(ctx context.Context, store driver.Store) (LiveReverse, error) {
	return ReverseRestoredWithOptions(ctx, store, reverseconverter.Options{All: true})
}

func TestLiveReverseReadsPostRestoreEditsAndOpaqueValues(t *testing.T) {
	ctx := context.Background()
	opaque := map[string]any{
		"agent_state": "state\x00", "await_id": "await\x00", "await_type": "human",
		"timeout_ns": json.Number("9007199254740993"),
		"payload":    map[string]any{"number": json.Number("9007199254740993"), "null": nil},
		"waiters":    []any{"other", nil},
	}
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			data := admittedFixtureBytesWithFields(t, opaque)
			profile, archive := admittedImportProfile(t, data)
			opaqueID := ""
			for _, record := range archive.Records("bead") {
				if record.Source.Fields["agent_state"] == opaque["agent_state"] {
					opaqueID = record.ID
				}
			}
			if opaqueID == "" {
				t.Fatal("fixture is missing the opaque source bead")
			}
			store, lease, cleanup := openReverseTarget(t, kind)
			defer cleanup()
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatal(err)
			}
			before, err := reverseAllRestored(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			ontology := service.NewOntology(nil)
			writer := pipeline.New(store, lease, pipeline.WithOntology(ontology))
			apply := func(description, key string) {
				t.Helper()
				_, err := writer.Apply(ctx, profile.Admission.Actor, model.ChangeSet{Ops: []model.Op{{
					Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"description": description}},
				}}}, model.WriteMeta{Method: "tx.apply", Project: "demo", Agent: "live-reverse", IdempotencyKey: strings.Repeat(key, 32)})
				if err != nil {
					t.Fatal(err)
				}
			}
			apply("live edit", "f")
			state := reverseState(t, store)
			after, err := reverseAllRestored(ctx, store)
			if err != nil || after.Snapshot.At == nil || after.Snapshot.At.String() != state.Meta.Head.String() || after.Result.Digest == before.Result.Digest {
				t.Fatalf("live reverse revision/digest failed: %v", err)
			}
			assertReverseDescription(t, after.Result.JSONL, "live edit")
			decoder := json.NewDecoder(bytes.NewReader(after.Result.JSONL))
			foundOpaque := false
			for {
				var row struct {
					ID       string                     `json:"id"`
					Metadata map[string]json.RawMessage `json:"metadata"`
				}
				if err := decoder.Decode(&row); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if row.ID != opaqueID {
					continue
				}
				foundOpaque = true
				for field, want := range opaque {
					var got any
					if err := decodeReverseJSON(row.Metadata["beads."+field], &got); err != nil || !sameReverseJSON(got, want) {
						t.Fatalf("opaque field %s changed: %v", field, err)
					}
				}
			}
			if !foundOpaque {
				t.Fatal("reverse omitted the opaque source bead")
			}
			if !reflect.DeepEqual(state, reverseState(t, store)) {
				t.Fatal("reverse changed the store")
			}
			// The wrapper writes after opening the reader. Export must remain
			// entirely on its captured snapshot, including all source blobs.
			bound := reverseReadHook{Store: store, afterRead: func() { apply("concurrent edit", "a") }}
			captured, err := reverseAllRestored(ctx, bound)
			if err != nil || captured.Result.Digest != after.Result.Digest {
				t.Fatalf("reverse mixed snapshots: %v", err)
			}
			assertReverseDescription(t, captured.Result.JSONL, "live edit")
		})
	}
}

func assertReverseDescription(t *testing.T, data []byte, want string) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	for {
		var row struct {
			ID          string `json:"id"`
			Description string `json:"description"`
		}
		if err := d.Decode(&row); err != nil {
			t.Fatalf("missing live bead: %v", err)
		}
		if row.ID == "demo-7" {
			if row.Description != want {
				t.Fatal("reverse exported stale source state")
			}
			return
		}
	}
}

func TestLiveReverseRefusesUnrepresentableDeltasWithoutPartialOutput(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			data := admittedFixtureBytes(t)
			profile, _ := admittedImportProfile(t, data)
			store, lease, cleanup := openReverseTarget(t, kind)
			defer cleanup()
			if _, err := reverseAllRestored(ctx, store); err == nil {
				t.Fatal("empty store was treated as a restored source")
			}
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatal(err)
			}
			state := reverseState(t, store)
			for _, mode := range []string{"extra", "missing", "comment", "opaque", "provenance", "unknown-reserved", "claim"} {
				t.Run(mode, func(t *testing.T) {
					got, err := reverseAllRestored(ctx, reverseReadHook{Store: store, mode: mode})
					var refused ReverseRefusal
					if !errors.As(err, &refused) || len(got.Result.JSONL) != 0 || got.Result.Digest != "" {
						t.Fatalf("unrepresentable image produced partial output: %v", err)
					}
				})
			}
			if !reflect.DeepEqual(state, reverseState(t, store)) {
				t.Fatal("refused reverse changed the store")
			}
		})
	}
}

func TestLiveReverseFromCheckedSourceHistory(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		file string
		mode driver.RestoreHistoryMode
	}{
		{"full.tar", driver.RestoreHistoryModeFull},
		{"dolt-source-partial.tar", driver.RestoreHistoryModePartial},
		{"dolt-source-partial.tar", driver.RestoreHistoryModeSnapshot},
	} {
		for _, engine := range []string{"memory", "sqlite", "postgres"} {
			t.Run(test.file+"/"+string(test.mode)+"/"+engine, func(t *testing.T) {
				data, err := os.ReadFile("../offlinehistory/testdata/" + test.file)
				if err != nil {
					t.Fatal(err)
				}
				p, archive := admittedImportProfile(t, data)
				p.HistoryMode = test.mode
				if test.mode != driver.RestoreHistoryModeSnapshot {
					p, err = BindHistoryImport(ctx, bytes.NewReader(data), p, time.Now().UTC())
					if err != nil {
						t.Fatal(err)
					}
				}
				store, lease, cleanup := openReverseTarget(t, engine)
				defer cleanup()
				if _, err = RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, p); err != nil {
					t.Fatal(err)
				}
				before, err := reverseAllRestored(ctx, store)
				if err != nil {
					t.Fatal(err)
				}
				assertSourceOpaque(t, before.Result.JSONL, archive)
				if len(before.Result.Report.LostPostSwitchHistory) == 0 {
					t.Fatal("head-state reverse omitted its history-loss disclosure")
				}
				writer := pipeline.New(store, lease, pipeline.WithOntology(service.NewOntology(nil)))
				_, err = writer.Apply(ctx, p.Admission.Actor, model.ChangeSet{Ops: []model.Op{{
					Kind: model.OpPatch, Patch: &model.PatchOp{Target: model.RefByID("demo-7", model.Natural64(0)), Fields: map[string]any{"description": "after history restore"}},
				}}}, model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: strings.Repeat("e", 32)})
				if err != nil {
					t.Fatal(err)
				}
				state := reverseState(t, store)
				after, err := reverseAllRestored(ctx, store)
				if err != nil || after.Result.Digest == before.Result.Digest {
					t.Fatalf("live history reverse failed: %v", err)
				}
				assertReverseDescription(t, after.Result.JSONL, "after history restore")
				assertSourceOpaque(t, after.Result.JSONL, archive)
				if !reflect.DeepEqual(state, reverseState(t, store)) {
					t.Fatal("history reverse changed the store")
				}
				if test.mode != driver.RestoreHistoryModeSnapshot {
					for _, mode := range []string{"history-changed", "history-missing"} {
						got, err := reverseAllRestored(ctx, reverseReadHook{Store: store, mode: mode})
						var refused ReverseRefusal
						if !errors.As(err, &refused) || len(got.Result.JSONL) != 0 {
							t.Fatalf("changed history binding did not refuse: %v", err)
						}
					}
				}
			})
		}
	}
}

func assertSourceOpaque(t *testing.T, data []byte, archive *dumpformat.Archive) {
	t.Helper()
	got := map[string]map[string]json.RawMessage{}
	d := json.NewDecoder(bytes.NewReader(data))
	for {
		var row struct {
			ID       string                     `json:"id"`
			Metadata map[string]json.RawMessage `json:"metadata"`
		}
		if err := d.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if row.ID != "" {
			got[row.ID] = row.Metadata
		}
	}
	for _, record := range archive.Records("bead") {
		for _, field := range dumpformat.OpaqueIssueFields() {
			var value any
			if err := decodeReverseJSON(got[record.ID]["beads."+field], &value); err != nil || !sameReverseJSON(value, record.Source.Fields[field]) {
				t.Fatalf("reverse changed source opaque field %s: %v", field, err)
			}
		}
	}
}

func openReverseTarget(t *testing.T, kind string) (driver.Store, driver.ActiveLease, func()) {
	t.Helper()
	if kind != "postgres" {
		return openTarget(t, kind)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	docker := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "DOCKER_HOST=") && !strings.HasPrefix(entry, "DOCKER_CONTEXT=") && !strings.HasPrefix(entry, "DOCKER_TLS=") && !strings.HasPrefix(entry, "DOCKER_TLS_VERIFY=") && !strings.HasPrefix(entry, "DOCKER_CERT_PATH=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		return cmd.CombinedOutput()
	}
	name := "plug-swarm-i6-reverse-" + digestBytes([]byte(t.TempDir()))[:16]
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	if out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_reverse", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("real PostgreSQL start failed: %v %s", err, out)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if out, err := exec.CommandContext(cleanupCtx, "docker", "--host=unix:///var/run/docker.sock", "rm", "-f", name).CombinedOutput(); err != nil {
			t.Errorf("owned PostgreSQL cleanup failed: %v %s", err, out)
		}
	})
	address, err := docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	addr := strings.TrimSpace(string(address))
	if host, _, err := net.SplitHostPort(addr); err != nil || host != "127.0.0.1" {
		t.Fatal("test PostgreSQL must bind loopback")
	}
	dsn := "postgres://postgres@" + addr + "/plug_reverse?sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("real PostgreSQL did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	store, err := postgres.Open(ctx, driver.Config{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Lease(ctx, "converter-reverse-postgres", driver.LeaseTTL)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	return store, lease, func() { _ = lease.Release(context.Background()); _ = store.Close() }
}

func reverseState(t *testing.T, store driver.Store) struct {
	Meta    driver.Meta
	Records []driver.Record
} {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Query(ctx, driver.Query{Limit: 1000})
	if err != nil || rows.NextCursor != nil {
		t.Fatalf("test census incomplete: %v", err)
	}
	return struct {
		Meta    driver.Meta
		Records []driver.Record
	}{meta, rows.Items}
}

type reverseReadHook struct {
	driver.Store
	afterRead func()
	mode      string
}

func (s reverseReadHook) ReadCheckpointInventory(ctx context.Context, view driver.View) (driver.CheckpointInventory, error) {
	capability, ok := s.Store.(driver.CheckpointInventoryStore)
	if !ok {
		return nil, driver.ErrUnsupported
	}
	r, err := capability.ReadCheckpointInventory(ctx, view)
	if err != nil {
		return nil, err
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	return reverseInventoryQueryHook{CheckpointInventory: r, mode: s.mode}, nil
}

type reverseInventoryQueryHook struct {
	driver.CheckpointInventory
	mode string
}

func (r reverseInventoryQueryHook) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	return (reverseQueryHook{Reader: r.CheckpointInventory, mode: r.mode}).Query(ctx, q)
}

func (s reverseReadHook) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	return reverseQueryHook{Reader: r, mode: s.mode}, nil
}

type reverseQueryHook struct {
	driver.Reader
	mode string
}

func (r reverseQueryHook) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	page, err := r.Reader.Query(ctx, q)
	if err != nil {
		return page, err
	}
	for i, row := range page.Items {
		if row.Ref.ID == driver.OfflineHistoryEvidenceID {
			switch r.mode {
			case "history-changed":
				page.Items[i].Body = []byte(`{"changed":true}`)
			case "history-missing":
				page.Items = append(page.Items[:i], page.Items[i+1:]...)
			}
			if strings.HasPrefix(r.mode, "history-") {
				break
			}
		}
		if strings.HasPrefix(r.mode, "history-") {
			continue
		}
		if row.Ref.ID != "demo-7" {
			continue
		}
		if r.mode == "missing" {
			page.Items = append(page.Items[:i], page.Items[i+1:]...)
			break
		}
		doc, err := pipeline.DecodeDocument(row.Body)
		if err != nil {
			return page, err
		}
		switch r.mode {
		case "extra":
			page.Items = append(page.Items, driver.Record{Ref: model.RefByID("unmapped", model.Natural64(0)), Kind: "memory", Body: []byte(`{}`)})
		case "comment":
			doc.Comments = append(doc.Comments, pipeline.Comment{
				Body: "live comment", Principal: model.PrincipalRef{Kind: "user", ID: "live-author"},
				Time: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
			})
		case "opaque":
			delete(doc.Metadata, "beads.await_id")
		case "provenance":
			doc.Metadata["beads.source_fields"] = json.RawMessage(`{}`)
		case "unknown-reserved":
			doc.Metadata["beads.unknown"] = json.RawMessage(`true`)
		case "claim":
			doc.Lease = &pipeline.ClaimLease{
				Principal: model.PrincipalRef{Kind: "user", ID: "live-author"},
				ExpiresAt: time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC),
				TtlMs:     model.Natural64(1000), Epoch: model.Natural64(1),
			}
		}
		page.Items[i].Body, err = json.Marshal(doc)
		if err != nil {
			return page, err
		}
		break
	}
	return page, nil
}

func TestLiveReverseRetainsExactValidatedEnvelope(t *testing.T) {
	// Extra zero tar padding is valid but cannot be reproduced from members.
	data := append(admittedFixtureBytes(t), make([]byte, 512)...)
	profile, archive := admittedImportProfile(t, data)
	detached := archive.TarBytes()
	if !bytes.Equal(detached, data) {
		t.Fatal("validated envelope was not retained")
	}
	detached[0] ^= 1
	if !bytes.Equal(archive.TarBytes(), data) {
		t.Fatal("envelope accessor is not detached")
	}
	store, lease, cleanup := openTarget(t, "memory")
	defer cleanup()
	if _, err := RestoreImportWithLease(context.Background(), bytes.NewReader(data), store, lease, profile); err != nil {
		t.Fatal(err)
	}
	got, err := reverseAllRestored(context.Background(), store)
	if err != nil || len(got.Result.JSONL) == 0 {
		t.Fatalf("exact padded source reverse: %v", err)
	}
}

func TestProjectLiveBeadPreservesUnchangedFailureOutcome(t *testing.T) {
	ctx := context.Background()
	data := admittedFixtureBytes(t)
	profile, _ := admittedImportProfile(t, data)
	store, lease, cleanup := openTarget(t, "memory")
	defer cleanup()
	if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
		t.Fatal(err)
	}
	dump, err := reverseconverter.ReadBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	var source reverseconverter.Record
	for _, row := range dump.Records["bead"] {
		if row.Body["status_category"] == "completed" {
			source = row
			break
		}
	}
	if source.ID == "" {
		t.Fatal("unit fixture needs a completed bead")
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(source.ID, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatalf("completed fixture: %v", err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	// A unit-level failure variant of the validated fixture. This is not a
	// claim that its source was exported or reimported by a Beads executable.
	source.Body["result"] = "failure"
	source.Source.Fields["close_reason"] = "failed"
	doc.Metadata["beads.source.result"] = json.RawMessage(`"failure"`)
	doc.Metadata["beads.source_fields"], err = json.Marshal(source.Source.Fields)
	if err != nil {
		t.Fatal(err)
	}
	rows[0].Body, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	got, err := projectLiveBead(source, rows[0], dump)
	if err != nil || got.Body["result"] != "failure" {
		t.Fatalf("unchanged imported failure was rewritten: result=%v err=%v", got.Body["result"], err)
	}
}

func TestProjectLiveBeadAcceptsJoinedSourceIdentityProvenance(t *testing.T) {
	ctx := context.Background()
	data := admittedFixtureBytes(t)
	profile, _ := admittedImportProfile(t, data)
	store, lease, cleanup := openTarget(t, "memory")
	defer cleanup()
	if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
		t.Fatal(err)
	}
	dump, err := reverseconverter.ReadBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	source := dump.Records["bead"][0]
	identity := dumpformat.SourceIdentity{StoreID: "joined-source", Project: source.Project}
	source.Source.Identity = &identity
	key, ok := reverseSourceRecord(source).SourceKey()
	if !ok {
		t.Fatal("source identity did not produce a scoped source key")
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(source.ID, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatalf("restored fixture bead: %v", err)
	}
	doc, err := pipeline.DecodeDocument(rows[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	doc.Metadata["beads.source_identity"], err = json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	doc.Metadata["beads.source_key"], err = json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	rows[0].Body, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	got, err := projectLiveBead(source, rows[0], dump)
	if err != nil {
		t.Fatalf("joined provenance was refused: %v", err)
	}
	if got.ID != source.ID || got.Project != source.Project || !sameReverseJSON(got.Source.Identity, identity) {
		t.Fatalf("joined source identity changed: got=%+v want=%+v", got.Source.Identity, identity)
	}
}

func TestLiveReverseComparesVersionedDependencyPredicatesExactly(t *testing.T) {
	// Detached relationship controls exercise the live-source comparison, not
	// a native capture or an accepted successor producer.
	for _, test := range []struct {
		name string
		raw  any
		want *model.LinkMetadata
	}{
		{"legacy NULL", nil, nil},
		{"ID only", `{"_plug_edge_id":"edge-1"}`, nil},
		{"explicit default", `{}`, &model.LinkMetadata{Gate: "all-children"}},
		{"SQL predicate", ` {"gate":"any-children","also_blocks":true,"spawner_id":"not-the-target","opaque":9007199254740993} `, &model.LinkMetadata{Gate: "any-children", AlsoBlocks: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dump := reverseconverter.Dump{
				Manifest: reverseconverter.Manifest{Kinds: []reverseconverter.Kind{{Name: "edge", Version: dumpformat.EdgeVersionTypedDependencies}}},
				Records: map[string][]reverseconverter.Record{"edge": {{
					Kind: "edge", ID: "edge-1", Body: map[string]any{"from": "demo-2", "to": "demo-1", "target_kind": "depends_on_issue_id", "type": "waits-for", "metadata": test.raw},
				}}},
			}
			doc := pipeline.Document{Links: []pipeline.Link{{To: model.RefByID("demo-1", model.Natural64(0)), Kind: "waits-for", Metadata: test.want}}}
			if err := checkLiveRelationsAndComments("demo-2", doc, dump); err != nil {
				t.Fatalf("unchanged versioned source relationship refused: %v", err)
			}
			changed := model.LinkMetadata{Gate: "all-children"}
			if test.want != nil {
				changed = *test.want
				changed.AlsoBlocks = !changed.AlsoBlocks
			}
			doc.Links[0].Metadata = &changed
			if err := checkLiveRelationsAndComments("demo-2", doc, dump); err == nil {
				t.Fatal("changed predicate or absent-versus-explicit metadata was accepted")
			}
		})
	}
}

func TestLiveReverseRetainsV3LocalOpaqueRelationshipAndRefusesEdits(t *testing.T) {
	dump := reverseconverter.Dump{
		Manifest: reverseconverter.Manifest{Kinds: []reverseconverter.Kind{{Name: "edge", Version: dumpformat.EdgeVersionLocalOpaque}}},
		Records: map[string][]reverseconverter.Record{"edge": {{
			Kind: "edge", ID: "edge-opaque", Body: map[string]any{
				"from": "demo-2", "to": "demo-1", "target_kind": "depends_on_issue_id", "type": "local-opaque", "metadata": map[string]any{"large": json.Number("9007199254740993")}, "thread_id": "thread-opaque",
			},
		}}},
	}
	doc := pipeline.Document{Links: []pipeline.Link{{To: model.RefByID("demo-1", model.Natural64(0)), Kind: "local-opaque"}}}
	if err := checkLiveRelationsAndComments("demo-2", doc, dump); err != nil {
		t.Fatalf("unchanged v3 local opaque relationship refused: %v", err)
	}
	doc.Links[0].To = model.RefByID("demo-missing", model.Natural64(0))
	if err := checkLiveRelationsAndComments("demo-2", doc, dump); err == nil {
		t.Fatal("changed v3 local opaque endpoint was accepted")
	}
}

func TestProjectLiveBeadRetainsRawMetadataUntilAKeyChanges(t *testing.T) {
	ctx := context.Background()
	data := admittedFixtureBytes(t)
	profile, _ := admittedImportProfile(t, data)
	store, lease, cleanup := openTarget(t, "memory")
	defer cleanup()
	if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
		t.Fatal(err)
	}
	dump, err := reverseconverter.ReadBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	baseline := dump.Records["bead"][0]
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rows, err := r.Get(ctx, []model.Ref{model.RefByID(baseline.ID, model.Natural64(0))})
	if err != nil || len(rows) != 1 {
		t.Fatalf("restored fixture bead: %v", err)
	}
	const sqlText = " {\"large\":9007199254740993,\"literal\":1.0,\"null\":null} \n"
	for _, test := range []struct {
		name string
		raw  any
		edit bool
	}{
		{"SQL text", sqlText, false},
		{"SQL NULL", nil, false},
		{"empty SQL object", " {} \n", false},
		{"object", map[string]any{"large": json.Number("9007199254740993"), "literal": json.Number("1.0")}, false},
		{"changed SQL text", sqlText, true},
		{"changed SQL NULL", nil, true},
		{"changed object", map[string]any{"literal": json.Number("1.0")}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// A source-representation unit variant of the admitted fixture,
			// not an input or readback attributed to a native executable.
			source := baseline
			source.Body, source.Source.Fields = cloneRow(source.Body), cloneRow(source.Source.Fields)
			source.Body["metadata"], source.Source.Fields["metadata"] = test.raw, test.raw
			doc, err := pipeline.DecodeDocument(rows[0].Body)
			if err != nil {
				t.Fatal(err)
			}
			for key := range doc.Metadata {
				if !strings.HasPrefix(key, "beads.") && !strings.HasPrefix(key, "plug.restore.") {
					delete(doc.Metadata, key)
				}
			}
			metadata, err := dumpformat.SourceMetadataObject(test.raw)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range metadata {
				doc.Metadata[key], err = json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
			}
			if test.edit {
				// 1 and 1.0 remain distinct even when their numeric values agree.
				doc.Metadata["literal"] = json.RawMessage("1")
				if metadata == nil {
					metadata = map[string]any{}
				}
				metadata["literal"] = json.Number("1")
			}
			doc.Metadata["beads.source_fields"], err = json.Marshal(source.Source.Fields)
			if err != nil {
				t.Fatal(err)
			}
			row := rows[0]
			row.Body, err = json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			got, err := projectLiveBead(source, row, dump)
			if err != nil {
				t.Fatal(err)
			}
			want := test.raw
			if test.edit {
				want = metadata
			}
			if !reflect.DeepEqual(got.Body["metadata"], want) || !reflect.DeepEqual(got.Source.Fields["metadata"], want) {
				t.Fatalf("projected metadata representation changed: body=%#v source=%#v want=%#v", got.Body["metadata"], got.Source.Fields["metadata"], want)
			}
			if !reflect.DeepEqual(source.Body["metadata"], test.raw) || !reflect.DeepEqual(source.Source.Fields["metadata"], test.raw) {
				t.Fatal("live projection mutated the retained source")
			}
		})
	}
}
