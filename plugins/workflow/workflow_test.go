package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

type testDurableState struct {
	value []byte
	rev   uint64
}

func (*testDurableState) Persistent() bool { return true }

func (s *testDurableState) Read(_ context.Context, key string, dst any) (bool, pluginapi.Revision, error) {
	if key != stateKey {
		return false, pluginapi.Natural64(0), nil
	}
	if len(s.value) == 0 {
		return false, pluginapi.Natural64(s.rev), nil
	}
	if err := json.Unmarshal(s.value, dst); err != nil {
		return false, pluginapi.Natural64(s.rev), err
	}
	return true, pluginapi.Natural64(s.rev), nil
}

func (s *testDurableState) Get(ctx context.Context, key string, dst any) (bool, error) {
	found, _, err := s.Read(ctx, key, dst)
	return found, err
}

func (s *testDurableState) Put(_ context.Context, key string, value any, expect pluginapi.Revision) (pluginapi.Revision, error) {
	if key != stateKey {
		return "", errors.New("unexpected key")
	}
	want, err := expect.Uint64()
	if err != nil {
		return "", err
	}
	if want != s.rev {
		return "", driver.ErrCheckpointConflict
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	s.value = encoded
	s.rev++
	return pluginapi.Natural64(s.rev), nil
}

func (s *testDurableState) Delete(_ context.Context, key string, expect pluginapi.Revision) error {
	if key != stateKey {
		return errors.New("unexpected key")
	}
	want, err := expect.Uint64()
	if err != nil {
		return err
	}
	if want != s.rev {
		return driver.ErrCheckpointConflict
	}
	s.value = nil
	s.rev++
	return nil
}

func (s *testDurableState) Revision(_ context.Context, key string) (pluginapi.Revision, error) {
	if key != stateKey {
		return "", errors.New("unexpected key")
	}
	return pluginapi.Natural64(s.rev), nil
}

func (s *testDurableState) List(_ context.Context, prefix string, limit int) ([]pluginapi.DurableEntry, error) {
	if prefix != "workflow/" || limit <= 0 {
		return nil, nil
	}
	return nil, nil
}

var _ pluginapi.DurableState = (*testDurableState)(nil)

func TestNativeRequiresTrustedCallerAdapter(t *testing.T) {
	_, err := Native(nil).New(nil)
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "unauthenticated" {
		t.Fatalf("native registration invented a fallback caller: %v", err)
	}
}

func TestEphemeralInvocationRequiresExplicitPersistentCapability(t *testing.T) {
	// Embedding only the public state interface deliberately hides the optional
	// marker. An unknown provider must not inherit a restart guarantee.
	state := struct{ pluginapi.DurableState }{&testDurableState{}}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Invoke(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, InvokeRequest{
		Template: "template", Project: "project", Mode: "ephemeral",
		Checklist: []ChecklistItem{{Label: "one", Title: "title"}},
	}, model.WriteMeta{Method: "x.workflow.invoke", Project: "project", IdempotencyKey: strings.Repeat("1", 64)})
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "invalid_params" {
		t.Fatalf("unmarked state accepted an ephemeral restart guarantee: %v", err)
	}
}

func TestTerminalClaimsPreserveEachOriginalItemIdentity(t *testing.T) {
	ctx := context.Background()
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	provider.WithIdentity(Alias, "default", "inc-1")
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := func(method, key string) model.WriteMeta {
		return model.WriteMeta{Method: method, Project: "project", Agent: "owner", AgentLabel: "owner", IdempotencyKey: strings.Repeat(key, 64)}
	}
	run, err := provider.Invoke(ctx, by, InvokeRequest{
		Template: "template", Project: "project", Mode: "ephemeral",
		Checklist: []ChecklistItem{{Label: "one", Title: "first private title"}, {Label: "two", Title: "second private title"}},
	}, meta("x.workflow.invoke", "1"))
	if err != nil {
		t.Fatal(err)
	}
	firstRef := model.RefByID(run.Items[0].ID, model.Natural64(0))
	claimed, err := provider.Claim(ctx, by, firstRef, meta("bead.claim", "2"))
	if err != nil {
		t.Fatal(err)
	}
	next, found, _, err := provider.ClaimNext(ctx, by, "project", nil, meta("bead.claimNext", "3"))
	if err != nil || !found || next.ID == claimed.ID {
		t.Fatalf("claimNext did not select the other item: item=%+v found=%v err=%v", next, found, err)
	}
	if _, err := provider.Close(ctx, by, firstRef, "done", meta("bead.close", "4")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Close(ctx, by, model.RefByID(next.ID, model.Natural64(0)), "done", meta("bead.close", "5")); err != nil {
		t.Fatal(err)
	}
	replayed, err := provider.Claim(ctx, by, firstRef, meta("bead.claim", "2"))
	if err != nil || replayed.ID != claimed.ID || replayed.Revision.String() != claimed.Revision.String() {
		t.Fatalf("terminal claim changed its committed identity: first=%+v replay=%+v err=%v", claimed, replayed, err)
	}
	replayedNext, found, replayedReceipt, err := provider.ReplayClaimNext(ctx, by, "project", nil, meta("bead.claimNext", "3"))
	if err != nil || !found || !replayedReceipt || replayedNext.ID != next.ID || replayedNext.Revision.String() != next.Revision.String() {
		t.Fatalf("terminal claimNext changed its committed identity: first=%+v replay=%+v found=%v replayed=%v err=%v", next, replayedNext, found, replayedReceipt, err)
	}
	for _, item := range []ItemView{replayed, replayedNext} {
		if item.Title != "" || item.Project != "" || item.Claim != nil || len(item.Comments) != 0 {
			t.Fatalf("terminal replay retained operational payload: %+v", item)
		}
	}
}

func TestReceiptsStayLiveUntilCompletionAndResetTerminalWindow(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 11, 2, 23, 0, 0, 0, time.UTC)
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	provider.WithClock(func() time.Time { return clock }).WithIdentity(Alias, "default", "inc-1")
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	invokeMeta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", Agent: "agent", AgentLabel: "agent", IdempotencyKey: "1111111111111111111111111111111111111111111111111111111111111111"}
	req := InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "private title", Type: "task"}}}
	first, err := provider.Invoke(ctx, by, req, invokeMeta)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 {
		t.Fatalf("first invocation items=%d", len(first.Items))
	}

	clock = clock.Add(24*time.Hour + time.Second)
	replayedLive, err := provider.Invoke(ctx, by, req, invokeMeta)
	if err != nil {
		t.Fatal(err)
	}
	if replayedLive.Run == nil || first.Run == nil || replayedLive.Run.ID != first.Run.ID || !reflect.DeepEqual(replayedLive.Items, first.Items) {
		t.Fatalf("expired live receipt did not replay the active run: first=%+v replay=%+v", first, replayedLive)
	}

	closeMeta := model.WriteMeta{Method: "bead.close", Project: "project", Agent: "agent", AgentLabel: "agent", IdempotencyKey: "2222222222222222222222222222222222222222222222222222222222222222"}
	itemRef := model.RefByID(first.Items[0].ID, model.Natural64(0))
	terminalCommit, err := provider.Close(ctx, by, itemRef, "done", closeMeta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Get(ctx, by, itemRef, model.WriteMeta{Method: "bead.get", Project: "project", Agent: "agent", AgentLabel: "agent"}); err == nil {
		t.Fatal("completed item remained readable")
	}

	// A lost final response is safe to replay from the completion receipt and
	// cannot recreate the deleted item.
	replayedClose, err := provider.Close(ctx, by, itemRef, "done", closeMeta)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayedClose, terminalCommit) {
		t.Fatalf("terminal close replay changed commit: first=%+v replay=%+v", terminalCommit, replayedClose)
	}
	replayedTerminal, err := provider.Invoke(ctx, by, req, invokeMeta)
	if err != nil {
		t.Fatal(err)
	}
	if replayedTerminal.Run == nil || replayedTerminal.Run.State != "completed" || len(replayedTerminal.Items) != 0 {
		t.Fatalf("terminal invocation replay was not identity-only: %+v", replayedTerminal)
	}
	if replayedTerminal.Run.Template != "" || replayedTerminal.Run.Project != "" {
		t.Fatalf("terminal replay retained private run fields: %+v", replayedTerminal.Run)
	}
	var saved persisted
	if err := json.Unmarshal(state.value, &saved); err != nil {
		t.Fatalf("decode saved workflow state: %v", err)
	}
	if len(saved.Runs) != 0 || len(saved.Items) != 0 {
		t.Fatalf("completion retained operational payloads: runs=%d items=%d", len(saved.Runs), len(saved.Items))
	}
	if len(saved.Audit) < 3 {
		t.Fatalf("completion audit entries=%d, want invocation and completion entries", len(saved.Audit))
	}
	auditBytes, err := json.Marshal(saved.Audit)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"private title", "comments", "claims", "private workflow"} {
		if strings.Contains(string(auditBytes), forbidden) {
			t.Fatalf("audit ledger retained private payload %q: %s", forbidden, auditBytes)
		}
	}
	for _, entry := range saved.Audit {
		if entry.Principal == "" || entry.PrincipalHash == "" || entry.Project != "project" || entry.Plugin != Alias || entry.Instance != "default" || entry.Incarnation != "inc-1" || entry.Method == "" || entry.Key == "" || entry.Digest == "" {
			t.Fatalf("audit entry omitted receipt scope: %+v", entry)
		}
	}

	clock = clock.Add(24*time.Hour - time.Second)
	if err := provider.GC(ctx); err != nil {
		t.Fatal(err)
	}
	stillReplayable, err := provider.Invoke(ctx, by, req, invokeMeta)
	if err != nil {
		t.Fatal(err)
	}
	if stillReplayable.Run == nil || stillReplayable.Run.State != "completed" || len(stillReplayable.Items) != 0 {
		t.Fatalf("terminal receipt expired before fresh window: %+v", stillReplayable)
	}

	clock = clock.Add(2 * time.Second)
	if err := provider.GC(ctx); err != nil {
		t.Fatal(err)
	}
	newRun, err := provider.Invoke(ctx, by, req, model.WriteMeta{Method: invokeMeta.Method, Project: invokeMeta.Project, Agent: invokeMeta.Agent, AgentLabel: invokeMeta.AgentLabel, IdempotencyKey: "3333333333333333333333333333333333333333333333333333333333333333"})
	if err != nil {
		t.Fatal(err)
	}
	if newRun.Run == nil || newRun.Run.ID == first.Run.ID || len(newRun.Items) != 1 {
		t.Fatalf("expired terminal receipt did not GC for a fresh invocation: %+v", newRun)
	}
}
