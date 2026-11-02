package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type observationReader struct {
	driver.Reader
	states       map[string]driver.LockState
	fallback     map[string]driver.Record
	getCalls     [][]model.Ref
	observations int
	wantContext  context.Context
	getError     error
}

func (r *observationReader) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.wantContext != nil && ctx != r.wantContext {
		return nil, errors.New("snapshot changed observation context")
	}
	if err := driver.ValidateLockKeys(keys); err != nil {
		return nil, err
	}
	r.observations++
	states := make([]driver.LockState, 0, len(keys))
	for _, key := range keys {
		state := r.states[key.Ref.ID]
		state.Key = key
		states = append(states, state)
	}
	return states, nil
}

func (r *observationReader) Get(ctx context.Context, refs []model.Ref) ([]driver.Record, error) {
	if r.wantContext != nil && ctx != r.wantContext {
		return nil, errors.New("snapshot changed fallback context")
	}
	r.getCalls = append(r.getCalls, append([]model.Ref(nil), refs...))
	if r.getError != nil {
		return nil, r.getError
	}
	rows := []driver.Record{}
	for _, ref := range refs {
		if row, exists := r.fallback[ref.ID]; exists {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func TestSnapshotGetReusesOnlyFreshCompleteObservation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ref := model.RefByID("live", model.Natural64(0))
	row := driver.Record{Ref: ref, Kind: "bead", Revision: model.Natural64(7), Body: []byte(`{"value":1}`)}
	reader := &observationReader{wantContext: ctx, states: map[string]driver.LockState{
		"live":    {Revision: row.Revision, Present: true, Record: &row},
		"missing": {Revision: model.Natural64(0)},
		"deleted": {Revision: model.Natural64(9)},
	}}
	snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	refs := []model.Ref{model.RefByID("missing", model.Natural64(0)), ref, model.RefByID("deleted", model.Natural64(0)), ref}
	got, err := snapshot.Get(ctx, refs)
	if err != nil || len(got) != 2 || !reflect.DeepEqual(got[0], row) || !reflect.DeepEqual(got[1], row) || len(reader.getCalls) != 0 {
		t.Fatalf("fresh hit/miss/tombstone/duplicate result=%+v calls=%v err=%v", got, reader.getCalls, err)
	}
	for _, id := range []string{"missing", "deleted", "live"} {
		key, _ := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &model.Ref{ID: id}})
		if state, ok := snapshot.reads.states[key]; !ok || state.Revision.String() != reader.states[id].Revision.String() || state.Present != reader.states[id].Present {
			t.Fatalf("lost tracked presence/revision for %s: %+v", id, state)
		}
	}
	got[0].Body[0] = '!'
	if row.Body[0] != '{' || got[1].Body[0] != '{' {
		t.Fatal("returned bodies alias an observation or another result")
	}
	// A fresh observation supersedes the retained read set. No record cache is
	// allowed to hide a later staged view or skip the original context check.
	row.Revision, row.Body = model.Natural64(8), []byte(`{"value":2}`)
	reader.states["live"] = driver.LockState{Revision: row.Revision, Present: true, Record: &row}
	got, err = snapshot.Get(ctx, []model.Ref{ref})
	if err != nil || len(got) != 1 || got[0].Revision.String() != "8" || reader.observations != 2 {
		t.Fatalf("stale cached record=%+v observations=%d err=%v", got, reader.observations, err)
	}
	cancel()
	if _, err := snapshot.Get(ctx, []model.Ref{ref}); !errors.Is(err, context.Canceled) {
		t.Fatalf("fresh context cancellation was bypassed: %v", err)
	}
}

func TestSnapshotGetFallsBackForPartialLiveObservation(t *testing.T) {
	ctx := context.Background()
	ref := model.RefByID("partial", model.Natural64(0))
	row := driver.Record{Ref: ref, Kind: "bead", Revision: model.Natural64(3), Body: []byte(`{"value":3}`)}
	reader := &observationReader{wantContext: ctx, states: map[string]driver.LockState{"partial": {Revision: row.Revision, Present: true}}, fallback: map[string]driver.Record{"partial": row}}
	snapshot := &snapshotReader{reader: reader, reads: &readSet{states: map[string]driver.LockState{}}}
	got, err := snapshot.Get(ctx, []model.Ref{ref, ref})
	if err != nil || len(got) != 2 || !reflect.DeepEqual(got[0], row) || !reflect.DeepEqual(reader.getCalls, [][]model.Ref{{ref}}) {
		t.Fatalf("partial observation fallback=%+v calls=%v err=%v", got, reader.getCalls, err)
	}
	reader.getError = errors.New("fallback unavailable")
	if _, err := snapshot.Get(ctx, []model.Ref{ref}); !errors.Is(err, reader.getError) {
		t.Fatalf("fallback error was hidden: %v", err)
	}
	bounded := &snapshotReader{reader: reader, reads: snapshot.reads, bounded: true}
	if _, err := bounded.Get(ctx, make([]model.Ref, 33)); err == nil {
		t.Fatal("optimization bypassed subscriber point-read budget")
	}
}
