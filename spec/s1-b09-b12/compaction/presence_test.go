package compaction_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is an explicitly issued synthetic proof authority, never a client
// object accepted as human presence merely because it has the right shape.
type presenceAuthority struct {
	mu     sync.Mutex
	issued map[string]string
	calls  atomic.Int32
}

func (a *presenceAuthority) verify(_ context.Context, method, digest string, proof map[string]any) error {
	a.calls.Add(1)
	a.mu.Lock()
	defer a.mu.Unlock()
	token, ok := a.issued[digest]
	if method != "history.revert" || !ok || proof["mode"] != "presence_check" || proof["challenge"] != digest || proof["proof"] != token {
		return fmt.Errorf("unissued proof or changed method/request/incarnation")
	}
	return nil
}
func (a *presenceAuthority) issue(f *fixture, params map[string]any, key string) map[string]any {
	f.t.Helper()
	var req service.CompactionRevertRequest
	raw, err := json.Marshal(params)
	if err != nil {
		f.t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &req); err != nil {
		f.t.Fatal(err)
	}
	digest, err := service.CompactionRevertDigest(req, writeMeta("history.revert", key), f.meta().Incarnation)
	if err != nil {
		f.t.Fatal(err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.issued == nil {
		a.issued = map[string]string{}
	}
	token := "synthetic-issued-" + digest
	a.issued[digest] = token
	return map[string]any{"mode": "presence_check", "challenge": digest, "proof": token}
}

func (f *fixture) assertNoReceipt(method, key string) {
	f.t.Helper()
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: f.meta().Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	m := writeMeta(method, key)
	got, err := r.IdempotencyByKey(context.Background(), "anonymous:anonymous", m.AgentLabel, method, m.IdempotencyKey)
	if err != nil || got != nil {
		f.t.Fatalf("unexpected receipt for %s/%s: %+v %v", method, key, got, err)
	}
}
func (f *fixture) changesets() []model.Changeset {
	f.t.Helper()
	r, err := f.store.Read(context.Background(), driver.View{Incarnation: f.meta().Incarnation})
	if err != nil {
		f.t.Fatal(err)
	}
	defer r.Close()
	got, err := r.Changesets(context.Background(), nil, 1000)
	if err != nil {
		f.t.Fatal(err)
	}
	return got
}
func assertKind(t *testing.T, f *fixture, revision any, kind model.ChangesetKind) {
	t.Helper()
	for _, entry := range f.changesets() {
		if entry.Revision.String() == fmt.Sprint(revision) {
			if entry.Kind != kind {
				t.Fatalf("revision %v kind=%s want %s", revision, entry.Kind, kind)
			}
			return
		}
	}
	t.Fatalf("missing audit revision %v", revision)
}
