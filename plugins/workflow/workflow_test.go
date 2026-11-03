package workflow

import (
	"bytes"
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

func TestInvocationUsesTheSameEffectiveAgentAsStandardReads(t *testing.T) {
	for _, by := range []model.PrincipalRef{{Kind: "user", ID: "owner"}, {Kind: "anonymous", ID: "anonymous"}} {
		t.Run(by.Kind, func(t *testing.T) {
			provider, err := New(&testDurableState{})
			if err != nil {
				t.Fatal(err)
			}
			meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", IdempotencyKey: strings.Repeat("1", 64)}
			if by.Kind == "user" {
				meta.AgentLabel, meta.Agent = "first-client-label", "first-client-label"
			}
			run, err := provider.Invoke(context.Background(), by, InvokeRequest{
				Template: "template", Project: "project", Mode: "ephemeral",
				Checklist: []ChecklistItem{{Label: "one", Title: "title"}},
			}, meta)
			if err != nil {
				t.Fatal(err)
			}
			meta.Method = "bead.get"
			if by.Kind == "user" {
				meta.AgentLabel, meta.Agent = "second-client-label", "second-client-label"
			}
			if _, err := provider.Get(context.Background(), by, model.RefByID(run.Items[0].ID, model.Natural64(0)), meta); err != nil {
				t.Fatalf("invocation and standard read disagreed about effective caller: %v", err)
			}
		})
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

func TestRetainedReceiptFromAnotherIncarnationRefusesDuplicateRun(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "terminal"}[terminal], func(t *testing.T) {
			ctx := context.Background()
			clock := time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC)
			state := &testDurableState{}
			provider, err := New(state)
			if err != nil {
				t.Fatal(err)
			}
			provider.WithClock(func() time.Time { return clock }).WithIdentity(Alias, "default", "inc-1")
			by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", Agent: "owner", AgentLabel: "owner", IdempotencyKey: strings.Repeat("1", 64)}
			req := InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "private title"}}}
			first, err := provider.Invoke(ctx, by, req, meta)
			if err != nil {
				t.Fatal(err)
			}
			// Live receipts must remain fenced even beyond their initial window.
			clock = clock.Add(driver.IdempotencyWindow + time.Hour)
			if terminal {
				closeMeta := meta
				closeMeta.Method, closeMeta.IdempotencyKey = "bead.close", strings.Repeat("2", 64)
				if _, err := provider.Close(ctx, by, model.RefByID(first.Items[0].ID, model.Natural64(0)), "done", closeMeta); err != nil {
					t.Fatal(err)
				}
			}
			restored, err := New(state)
			if err != nil {
				t.Fatal(err)
			}
			restored.WithClock(func() time.Time { return clock }).WithIdentity(Alias, "default", "inc-2")
			before := append([]byte(nil), state.value...)
			_, err = restored.Invoke(ctx, by, req, meta)
			var failure *model.Error
			if !errors.As(err, &failure) || failure.Data.Code != "idempotency_key_reused" {
				t.Fatalf("changed incarnation treated retained receipt as a miss: %v", err)
			}
			if !bytes.Equal(state.value, before) {
				t.Fatal("refused prior-incarnation replay changed durable state")
			}
			// A process/provider restart within the original incarnation still
			// replays the original live or identity-only terminal result.
			restored.WithIdentity(Alias, "default", "inc-1")
			replayed, err := restored.Invoke(ctx, by, req, meta)
			if err != nil || replayed.Run == nil || replayed.Run.ID != first.Run.ID {
				t.Fatalf("same-incarnation restart lost replay: result=%+v err=%v", replayed, err)
			}
			if terminal && (replayed.Run.State != "completed" || len(replayed.Items) != 0) {
				t.Fatalf("terminal replay recreated operational state: %+v", replayed)
			}
		})
	}
}

func TestSelectedClaimNextKeepsSelectorReplayAndErasesTerminalPayload(t *testing.T) {
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
	selected := model.RefByID(run.Items[1].ID, model.Natural64(0))
	if _, err := provider.Comment(ctx, by, selected, "private comment", meta("bead.comment", "2")); err != nil {
		t.Fatal(err)
	}
	selector := meta("bead.claimNext", "3")
	selector.ClaimNextIdentity = `{"viewId":"","filter":{"resources":["selected"],"types":["task"]}}`
	claimed, found, replayed, err := provider.ClaimNextSelected(ctx, by, selected, "project", nil, selector)
	if err != nil || !found || replayed || claimed.ID != selected.ID || claimed.Claim == nil {
		t.Fatalf("selected claimNext did not commit its candidate: item=%+v found=%v replayed=%v err=%v", claimed, found, replayed, err)
	}
	retry, found, replayed, err := provider.ReplayClaimNext(ctx, by, "project", []string{"task"}, selector)
	if err != nil || !found || !replayed || !reflect.DeepEqual(retry, claimed) {
		t.Fatalf("selector retry did not replay selected claim: item=%+v found=%v replayed=%v err=%v", retry, found, replayed, err)
	}
	changed := selector
	changed.ClaimNextIdentity += "changed-selector"
	_, _, _, err = provider.ReplayClaimNext(ctx, by, "project", nil, changed)
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "idempotency_key_reused" {
		t.Fatalf("changed selector reused selected receipt: %v", err)
	}
	// Another target supplied after response loss cannot change the receipt's
	// committed selection, even if the original item is no longer ready.
	other := model.RefByID(run.Items[0].ID, model.Natural64(0))
	retry, found, replayed, err = provider.ClaimNextSelected(ctx, by, other, "project", nil, selector)
	if err != nil || !found || !replayed || retry.ID != selected.ID {
		t.Fatalf("selected retry changed target: item=%+v found=%v replayed=%v err=%v", retry, found, replayed, err)
	}
	if _, err := provider.Close(ctx, by, selected, "done", meta("bead.close", "4")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Close(ctx, by, other, "done", meta("bead.close", "5")); err != nil {
		t.Fatal(err)
	}
	retry, found, replayed, err = provider.ReplayClaimNext(ctx, by, "project", nil, selector)
	if err != nil || !found || !replayed || retry.ID != selected.ID || retry.Revision.String() != claimed.Revision.String() || retry.Title != "" || retry.Project != "" || retry.Claim != nil || len(retry.Comments) != 0 {
		t.Fatalf("terminal selected claim replay retained payload or changed identity: item=%+v found=%v replayed=%v err=%v", retry, found, replayed, err)
	}
	for _, forbidden := range []string{"first private title", "second private title", "private comment"} {
		if bytes.Contains(state.value, []byte(forbidden)) {
			t.Fatalf("completion retained operational payload %q", forbidden)
		}
	}
}

func TestCompletionErasesLegacyBareClaimNextReceipt(t *testing.T) {
	ctx := context.Background()
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", Agent: "owner", AgentLabel: "owner", IdempotencyKey: strings.Repeat("1", 64)}
	run, err := provider.Invoke(ctx, by, InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "legacy private title"}}}, meta)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID(run.Items[0].ID, model.Natural64(0))
	claimMeta := meta
	claimMeta.Method, claimMeta.IdempotencyKey = "bead.claim", strings.Repeat("2", 64)
	claimed, err := provider.Claim(ctx, by, ref, claimMeta)
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the prior persisted format, where public claimNext entered
	// Claim and stored a bare ItemView. A new provider must still erase it on
	// completion instead of keeping its body in a terminal receipt.
	value, revision, err := provider.load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for key, receipt := range value.Receipts {
		if receipt.Method == "bead.claim" {
			receipt.Method = "bead.claimNext"
			value.Receipts[key] = receipt
		}
	}
	if err := provider.save(ctx, value, revision); err != nil {
		t.Fatal(err)
	}
	meta.Method, meta.IdempotencyKey = "bead.close", strings.Repeat("3", 64)
	if _, err := provider.Close(ctx, by, ref, "done", meta); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(state.value, []byte("legacy private title")) {
		t.Fatal("completion retained legacy claimNext body")
	}
	value, _, err = provider.load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, receipt := range value.Receipts {
		if receipt.Method != "bead.claimNext" {
			continue
		}
		checked++
		var result claimNextResult
		if err := json.Unmarshal(receipt.Result, &result); err != nil {
			t.Fatal(err)
		}
		if !receipt.Terminal || !result.Found || result.Item.ID != ref.ID || result.Item.Revision.String() != claimed.Revision.String() || result.Item.Claim != nil {
			t.Fatalf("legacy terminal receipt lost identity or retained a claim: %+v", result)
		}
	}
	if checked != 1 {
		t.Fatalf("legacy claimNext receipt count=%d, want 1", checked)
	}
}

func TestSelectedClaimNextReplaysEmptyBeforeNewCandidate(t *testing.T) {
	ctx := context.Background()
	provider, err := New(&testDurableState{})
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := model.WriteMeta{Method: "bead.claimNext", Project: "project", Agent: "owner", AgentLabel: "owner", IdempotencyKey: strings.Repeat("1", 64), ClaimNextIdentity: "empty-selector"}
	if _, found, replayed, err := provider.ClaimNext(ctx, by, "project", nil, meta); err != nil || found || replayed {
		t.Fatalf("empty selection was not committed: found=%v replayed=%v err=%v", found, replayed, err)
	}
	invokeMeta := meta
	invokeMeta.Method, invokeMeta.IdempotencyKey = "x.workflow.invoke", strings.Repeat("2", 64)
	run, err := provider.Invoke(ctx, by, InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "new item"}}}, invokeMeta)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID(run.Items[0].ID, model.Natural64(0))
	if item, found, replayed, err := provider.ClaimNextSelected(ctx, by, ref, "project", nil, meta); err != nil || found || !replayed || item.ID != "" {
		t.Fatalf("selected call replaced durable empty with a later item: item=%+v found=%v replayed=%v err=%v", item, found, replayed, err)
	}
	item, err := provider.Get(ctx, by, ref, meta)
	if err != nil || item.Status != "open" || item.Claim != nil {
		t.Fatalf("empty replay claimed later candidate: item=%+v err=%v", item, err)
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

type conflictedGCState struct {
	*testDurableState
	putFailure error
}

func (s *conflictedGCState) Put(ctx context.Context, key string, value any, expect pluginapi.Revision) (pluginapi.Revision, error) {
	if s.putFailure != nil {
		err := s.putFailure
		s.putFailure = nil
		return "", err
	}
	return s.testDurableState.Put(ctx, key, value, expect)
}

func TestOrdinaryAccessCollectsTerminalReceiptsAndRetainsUnfinishedRun(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC)
	state := &conflictedGCState{testDurableState: &testDurableState{}}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	provider.WithClock(func() time.Time { return clock })
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", AgentLabel: "agent", IdempotencyKey: strings.Repeat("1", 64)}
	req := InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "title"}}}
	ended, err := provider.Invoke(ctx, by, req, meta)
	if err != nil {
		t.Fatal(err)
	}
	closeMeta := meta
	closeMeta.Method, closeMeta.IdempotencyKey = "bead.close", strings.Repeat("2", 64)
	if _, err := provider.Close(ctx, by, model.RefByID(ended.Items[0].ID, model.Natural64(0)), "done", closeMeta); err != nil {
		t.Fatal(err)
	}
	meta.IdempotencyKey = strings.Repeat("3", 64)
	live, err := provider.Invoke(ctx, by, req, meta)
	if err != nil {
		t.Fatal(err)
	}
	before := append([]byte(nil), state.value...)
	beforeRevision := state.rev
	clock = clock.Add(driver.IdempotencyWindow + time.Second)
	state.putFailure = driver.ErrCheckpointConflict
	_, err = provider.Ready(ctx, by, "project", nil, meta)
	var conflict *model.Error
	if !errors.As(err, &conflict) || conflict.Data.Code != "write_conflict" {
		t.Fatalf("cleanup conflict was not typed: %v", err)
	}
	if !bytes.Equal(before, state.value) || state.rev != beforeRevision {
		t.Fatal("failed cleanup published a partial map or audit")
	}
	state.putFailure = driver.ErrFenced{ExpectedEpoch: 1, ObservedEpoch: 2}
	_, err = provider.Ready(ctx, by, "project", nil, meta)
	if !errors.As(err, &conflict) || conflict.Data.Code != "lease_not_held" {
		t.Fatalf("cleanup lease refusal was not typed: %v", err)
	}
	if !bytes.Equal(before, state.value) || state.rev != beforeRevision {
		t.Fatal("fenced cleanup published a partial map or audit")
	}
	// This one ordinary mutation must use the revision returned by cleanup,
	// rather than attempting its second CAS with the pre-cleanup revision.
	commentMeta := meta
	commentMeta.Method, commentMeta.IdempotencyKey = "bead.comment", strings.Repeat("4", 64)
	if _, err := provider.Comment(ctx, by, model.RefByID(live.Items[0].ID, model.Natural64(0)), "after cleanup", commentMeta); err != nil {
		t.Fatalf("ordinary mutation did not use the cleanup revision: %v", err)
	}
	ready, err := provider.Ready(ctx, by, "project", nil, meta)
	if err != nil || len(ready) != 1 || ready[0].ID != live.Items[0].ID || len(ready[0].Comments) != 1 {
		t.Fatalf("ordinary ready lost unfinished run during cleanup: %+v err=%v", ready, err)
	}
	var saved persisted
	if err := json.Unmarshal(state.value, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Receipts) != 2 || len(saved.Runs) != 1 || len(saved.Items) != 1 || state.rev != beforeRevision+2 {
		t.Fatalf("cleanup did not publish exactly the terminal removal: receipts=%d runs=%d items=%d revision=%d", len(saved.Receipts), len(saved.Runs), len(saved.Items), state.rev)
	}
	gcAudits := 0
	for _, entry := range saved.Audit {
		if entry.Event == "workflow.receipt.gc" {
			gcAudits++
		}
	}
	if gcAudits != 2 {
		t.Fatalf("receipt deletion audit count=%d, want 2", gcAudits)
	}
	replay, err := provider.Invoke(ctx, by, req, meta)
	if err != nil || !reflect.DeepEqual(live, replay) || state.rev != beforeRevision+2 {
		t.Fatalf("old unfinished receipt failed to replay without another write: %+v err=%v", replay, err)
	}
}
