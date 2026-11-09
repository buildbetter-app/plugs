package converterroundtrip

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestIDStyleAStoreWideHashRetriesRollbackAndReopen(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			data := admittedFixtureBytes(t)
			profile, _ := admittedImportProfile(t, data)
			store, lease, reopen := openIdentityTarget(t, engine)
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 11, 9, 12, 0, 0, 123456000, time.UTC)
			creator, err := driver.PrincipalKey(profile.Admission.Actor)
			if err != nil {
				t.Fatal(err)
			}
			input := driver.HashInput{Title: "global collision", Creator: creator, Timestamp: now}
			var occupied []string
			for nonce := uint32(0); nonce < 10; nonce++ {
				id, err := driver.HashCandidate("demo", input, 3, nonce)
				if err != nil {
					t.Fatal(err)
				}
				occupied = append(occupied, id)
			}
			tombstone, _ := driver.HashCandidate("demo", input, 4, 0)
			occupied = append(occupied, tombstone)
			putIdentityOccupants(t, store, lease, occupied, false)
			putIdentityOccupants(t, store, lease, []string{tombstone}, true)
			before := reverseState(t, store)
			want, _ := driver.HashCandidate("demo", input, 4, 1)
			parent := "demo-7"
			child := parent + ".5"
			// Consume both allocator forms, reserve each once, and abandon the
			// actual transaction after staging. Neither counter nor reservation
			// may leak into the next public create.
			tx, err := store.Write(ctx, lease)
			if err != nil {
				t.Fatal(err)
			}
			ids := []string{child, want}
			sort.Strings(ids)
			var locks driver.LockSet
			for _, id := range ids {
				ref := model.RefByID(id, model.Natural64(0))
				locks = append(locks, driver.LockRequest{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate})
			}
			allocatorKey, _ := driver.AllocatorLock("demo")
			locks = append(locks, driver.LockRequest{Key: allocatorKey, Mode: driver.LockUpdate})
			if _, err = tx.Lock(ctx, locks); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Head(ctx); err != nil {
				t.Fatal(err)
			}
			allocator := tx.(driver.HashAllocatorContinuation)
			if err = allocator.AllocateHashTopLevel(ctx, "demo", input, want); err != nil {
				t.Fatal(err)
			}
			if err = allocator.AllocateHashTopLevel(ctx, "demo", input, want); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("hash reservation did not refuse a duplicate: %v", err)
			}
			if err = allocator.AllocateChild(ctx, parent, child); err != nil {
				t.Fatal(err)
			}
			if err = allocator.AllocateChild(ctx, parent, child); !errors.Is(err, driver.ErrConflict) {
				t.Fatalf("child reservation did not refuse a duplicate: %v", err)
			}
			if err = tx.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, reverseState(t, store)) {
				t.Fatal("rollback leaked allocator state or records")
			}
			options := []pipeline.Option{pipeline.WithClock(func() time.Time { return now }), pipeline.WithOntology(service.NewOntology(nil))}
			writer := pipeline.New(store, lease, options...)
			changes := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "demo", Fields: map[string]any{"title": input.Title}}}}}
			meta := model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: hashBytes([]byte("global-root"))}
			commit, err := writer.Apply(ctx, profile.Admission.Actor, changes, meta)
			if err != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID != want {
				t.Fatalf("ten global collisions plus tombstone did not select length4 nonce1: %+v %v want=%s", commit, err, want)
			}
			saved := reverseState(t, store)
			store, lease = reopen()
			if !reflect.DeepEqual(saved, reverseState(t, store)) {
				t.Fatal("reopen changed collision records or allocator state")
			}
			writer = pipeline.New(store, lease, options...)
			replay, err := writer.ApplyWithOutcome(ctx, profile.Admission.Actor, changes, meta)
			if err != nil || !replay.Replayed || !reflect.DeepEqual(commit, replay.Commit) || !reflect.DeepEqual(saved, reverseState(t, store)) {
				t.Fatalf("reopen replay changed IDs/state: %+v %v", replay, err)
			}
			// Two real writers race the same restored parent, preserving the
			// stored .4 floor and each other's committed numeric suffix.
			var wg sync.WaitGroup
			errs := make(chan error, 2)
			children := make(chan string, 2)
			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ref := model.RefByID(parent, model.Natural64(0))
					w := pipeline.New(store, lease, options...)
					c, err := w.Apply(ctx, profile.Admission.Actor, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: "demo", Parent: &ref, Fields: map[string]any{"title": "concurrent child"}}}}}, model.WriteMeta{Method: "tx.apply", Project: "demo", IdempotencyKey: hashBytes([]byte(fmt.Sprintf("concurrent-child-%d", i)))})
					if err != nil {
						errs <- err
						return
					}
					children <- c.Results[0].Ref.ID
				}(i)
			}
			wg.Wait()
			close(errs)
			close(children)
			for err := range errs {
				t.Fatal(err)
			}
			var got []string
			for id := range children {
				got = append(got, id)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, []string{parent + ".5", parent + ".6"}) {
				t.Fatalf("concurrent children=%v", got)
			}
		})
	}
}

// Use the public driver write boundary to occupy IDs outside the bead plane.
// This fixture is deliberately a config kind with another project, so a bead-
// only or project-only scan cannot accidentally satisfy the collision oracle.
func putIdentityOccupants(t *testing.T, store driver.Store, lease driver.ActiveLease, ids []string, deleted bool) {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	ids = append([]string(nil), ids...)
	sort.Strings(ids)
	var locks driver.LockSet
	var keys []driver.LockKey
	for _, id := range ids {
		ref := model.RefByID(id, model.Natural64(0))
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &ref}
		keys = append(keys, key)
		locks = append(locks, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
	}
	keys = append(keys, driver.GraphBlockingSentinelLock)
	locks = append(locks, driver.LockRequest{Key: driver.GraphBlockingSentinelLock, Mode: driver.LockUpdate})
	if _, err = tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	var changes []model.ChangeRecord
	for _, id := range ids {
		ref := model.RefByID(id, model.Natural64(0))
		op := model.OpCreate
		from := model.Natural64(0)
		if deleted {
			rows, readErr := tx.Get(ctx, []model.Ref{ref})
			if readErr != nil || len(rows) != 1 {
				t.Fatalf("existing collision occupant: %+v %v", rows, readErr)
			}
			ref = rows[0].Ref
			from = rows[0].Revision
			err = tx.Delete(ctx, ref)
			op = model.OpDelete
		} else {
			body, _ := json.Marshal(map[string]any{"project": "other-project", "value": id})
			err = tx.Put(ctx, driver.Record{Ref: ref, Kind: "config", Body: body})
		}
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, model.ChangeRecord{Ref: ref, Op: op, FromRevision: from, ChangedPaths: []string{"/"}})
	}
	if err = tx.BumpSentinels(ctx, []driver.Sentinel{driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: restorePrincipal, LockKeys: keys, Changes: changes}); err != nil {
		t.Fatal(err)
	}
}

// This is a detached synthetic snapshot with two nonempty source projects.
// It checks allocator composition only; it does not replace native history
// merge evidence, which remains owned by the retained consumer lane.
func TestIDStyleATwoRestoredProjectsPreserveIDsAndIndependentCounters(t *testing.T) {
	admitted := admittedFixtureBytes(t)
	data := rewriteData(t, func(members map[string][]byte) {
		reader := tar.NewReader(bytes.NewReader(admitted))
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
		var manifest map[string]any
		decode(t, members["manifest.json"], &manifest)
		second := manifest["projects"].([]any)[1].(map[string]any)
		second["last_id"] = 7
		second["child_counters"] = map[string]any{"empty-7": 4}
		original := bytes.Split(bytes.TrimSuffix(members["records/bead.ndjson"], []byte{'\n'}), []byte{'\n'})
		for _, line := range original {
			var record map[string]any
			decode(t, line, &record)
			if record["id"] != "demo-7" && record["id"] != "demo-7.4" {
				continue
			}
			cloned := bytes.ReplaceAll(line, []byte("demo-"), []byte("empty-"))
			decode(t, cloned, &record)
			record["project"] = "empty"
			members["records/bead.ndjson"] = append(members["records/bead.ndjson"], encode(t, record)...)
		}
		for _, raw := range manifest["sources"].([]any) {
			row := raw.(map[string]any)
			switch row["family"] {
			case "issues", "issues.is_blocked", "issues.row_lock":
				row["rows"] = 5
			case "labels":
				row["rows"] = 4 // The cloned parent retains both original labels.
			}
		}
		for _, raw := range manifest["kind_registry"].([]any) {
			row := raw.(map[string]any)
			if row["name"] == "bead" {
				row["count"] = 5
			}
		}
		members["manifest.json"] = encode(t, manifest)
	})
	profile, source := admittedImportProfile(t, data)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
			store, lease, reopen := openIdentityTarget(t, engine)
			if _, err := RestoreImportWithLease(ctx, bytes.NewReader(data), store, lease, profile); err != nil {
				t.Fatal(err)
			}
			initial := reverseState(t, store)
			now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
			creator, err := driver.PrincipalKey(profile.Admission.Actor)
			if err != nil {
				t.Fatal(err)
			}
			input := driver.HashInput{Title: "Two restored projects", Creator: creator, Timestamp: now}
			options := []pipeline.Option{pipeline.WithClock(func() time.Time { return now }), pipeline.WithOntology(service.NewOntology(nil))}
			writer := pipeline.New(store, lease, options...)
			saved := map[string]model.Commit{}
			for _, project := range []string{"demo", "empty"} {
				changes := model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: project, Fields: map[string]any{"title": input.Title}}}}}
				meta := model.WriteMeta{Method: "tx.apply", Project: project, IdempotencyKey: hashBytes([]byte("two-project-root-" + project))}
				commit, err := writer.Apply(ctx, profile.Admission.Actor, changes, meta)
				want, _ := driver.HashCandidate(project, input, 3, 0)
				if err != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID != want {
					t.Fatalf("%s root=%+v want=%s err=%v", project, commit, want, err)
				}
				saved[project] = commit
				parent := model.RefByID(project+"-7", model.Natural64(0))
				child, err := writer.Apply(ctx, profile.Admission.Actor, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: project, Parent: &parent, Fields: map[string]any{"title": "Independent restored child"}}}}}, model.WriteMeta{Method: "tx.apply", Project: project, IdempotencyKey: hashBytes([]byte("two-project-child-" + project))})
				if err != nil || len(child.Results) != 1 || child.Results[0].Ref.ID != project+"-7.5" {
					t.Fatalf("%s counter=%+v err=%v", project, child, err)
				}
			}
			committed := reverseState(t, store)
			for _, original := range initial.Records {
				found := false
				for _, actual := range committed.Records {
					if reflect.DeepEqual(original, actual) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("new allocation changed restored record %s", original.Ref.ID)
				}
			}
			store, lease = reopen()
			if !reflect.DeepEqual(committed, reverseState(t, store)) {
				t.Fatal("two-project reopen changed committed state")
			}
			writer = pipeline.New(store, lease, options...)
			for _, project := range []string{"demo", "empty"} {
				outcome, err := writer.ApplyWithOutcome(ctx, profile.Admission.Actor, model.ChangeSet{Ops: []model.Op{{Kind: model.OpCreate, Create: &model.CreateOp{Type: "issue", Project: project, Fields: map[string]any{"title": input.Title}}}}}, model.WriteMeta{Method: "tx.apply", Project: project, IdempotencyKey: hashBytes([]byte("two-project-root-" + project))})
				if err != nil || !outcome.Replayed || !reflect.DeepEqual(saved[project], outcome.Commit) {
					t.Fatalf("%s reopened replay=%+v err=%v", project, outcome, err)
				}
			}
			if !reflect.DeepEqual(committed, reverseState(t, store)) {
				t.Fatal("replayed project allocation changed state")
			}
			assertImportedIdentityEvidence(t, store, source)
		})
	}
}
