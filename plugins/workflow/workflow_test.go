package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestUserContentGuardRunsAtNativeCreationBoundary(t *testing.T) {
	ctx := context.Background()
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	refusal := errors.New("rollback window refusal")
	provider.WithUserContentGuard(func(kinds []string) error {
		got = append([]string(nil), kinds...)
		return refusal
	})
	_, err = provider.Invoke(ctx, model.PrincipalRef{Kind: "user", ID: "guard"}, InvokeRequest{
		Template: "guard", Project: "guard", Mode: "ephemeral",
		Checklist: []ChecklistItem{{Label: "one", Title: "one"}},
	}, model.WriteMeta{Method: "x.workflow.invoke", Project: "guard", IdempotencyKey: strings.Repeat("a", 64)})
	if !errors.Is(err, refusal) {
		t.Fatalf("native creation guard error=%v, want %v", err, refusal)
	}
	if !reflect.DeepEqual(got, []string{RunKind, ItemKind}) {
		t.Fatalf("native creation kinds=%v, want [%q %q]", got, RunKind, ItemKind)
	}
	if state.rev != 0 || len(state.value) != 0 {
		t.Fatalf("guarded native write published checkpoint revision=%d payload=%q", state.rev, state.value)
	}
}

func TestM03CreationProvenanceSurvivesReceiptReplay(t *testing.T) {
	ctx := context.Background()
	created := time.Date(2026, 11, 5, 1, 0, 0, 0, time.UTC)
	clock := created
	state := &testDurableState{}
	newProvider := func() *Provider {
		provider, err := New(state)
		if err != nil {
			t.Fatal(err)
		}
		return provider.WithClock(func() time.Time { return clock }).WithIdentity(Alias, "default", "m03-incarnation")
	}
	provider := newProvider()
	by := model.PrincipalRef{Kind: "user", ID: "m03-owner"}
	meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "m03", IdempotencyKey: strings.Repeat("1", 64)}
	request := InvokeRequest{Template: "m03", Project: "m03", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "one", Type: "task"}}}
	first, err := provider.Invoke(ctx, by, request, meta)
	if err != nil || len(first.Items) != 1 || !first.Items[0].CreatedAt.Equal(created) {
		t.Fatalf("initial item lost authoritative run creation: result=%+v err=%v", first, err)
	}
	wire, err := json.Marshal(first.Items[0])
	if err != nil || bytes.Contains(wire, []byte(`"createdAt"`)) {
		t.Fatalf("internal order metadata changed item wire projection: %s err=%v", wire, err)
	}
	before, revision := append([]byte(nil), state.value...), state.rev
	clock = clock.Add(time.Hour)
	provider = newProvider()
	replay, err := provider.Invoke(ctx, by, request, meta)
	if err != nil || !reflect.DeepEqual(first, replay) || !bytes.Equal(before, state.value) || revision != state.rev {
		t.Fatalf("reopened provider replay changed creation provenance or checkpoint: result=%+v err=%v", replay, err)
	}
	claimMeta := meta
	claimMeta.Method, claimMeta.IdempotencyKey = "bead.claimNext", strings.Repeat("2", 64)
	claimed, found, replayed, err := provider.ClaimNextSelected(ctx, by, model.RefByID(first.Items[0].ID, model.Natural64(0)), "m03", nil, claimMeta)
	if err != nil || !found || replayed || !claimed.CreatedAt.Equal(created) {
		t.Fatalf("selected claim lost run creation: item=%+v found=%v replayed=%v err=%v", claimed, found, replayed, err)
	}
	before, revision = append([]byte(nil), state.value...), state.rev
	provider = newProvider()
	retry, found, replayed, err := provider.ReplayClaimNext(ctx, by, "m03", nil, claimMeta)
	if err != nil || !found || !replayed || !reflect.DeepEqual(claimed, retry) || !bytes.Equal(before, state.value) || revision != state.rev {
		t.Fatalf("reopened selected replay changed creation provenance or checkpoint: item=%+v found=%v replayed=%v err=%v", retry, found, replayed, err)
	}
}

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
	if !errors.As(err, &failure) || failure.Data.Code != "plugin_durable_state_unavailable" {
		t.Fatalf("unmarked state accepted an ephemeral restart guarantee: %v", err)
	}
	_, err = provider.Invoke(context.Background(), model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}, InvokeRequest{
		Template: "template", Project: "project", Mode: "ephemeral",
	}, model.WriteMeta{Method: "x.workflow.invoke", Project: "project", IdempotencyKey: strings.Repeat("2", 64)})
	if !errors.As(err, &failure) || failure.Data.Code != "invalid_params" {
		t.Fatalf("malformed request was conflated with missing persistence: %v", err)
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

func TestReadySnapshotTracksWritesAndLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	clock := time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC)
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	provider.WithClock(func() time.Time { return clock })
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", AgentLabel: "agent", IdempotencyKey: strings.Repeat("1", 64)}
	run, err := provider.Invoke(ctx, by, InvokeRequest{
		Template: "template", Project: "project", Mode: "ephemeral",
		Checklist: []ChecklistItem{{Label: "one", Title: "one"}, {Label: "two", Title: "two"}, {Label: "three", Title: "three"}},
	}, meta)
	if err != nil {
		t.Fatal(err)
	}
	sortItems(run.Items)
	meta.Method, meta.IdempotencyKey = "bead.claim", strings.Repeat("2", 64)
	first, err := provider.Claim(ctx, by, model.RefByID(run.Items[0].ID, model.Natural64(0)), meta)
	if err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Minute)
	meta.IdempotencyKey = strings.Repeat("3", 64)
	second, err := provider.Claim(ctx, by, model.RefByID(run.Items[1].ID, model.Natural64(0)), meta)
	if err != nil {
		t.Fatal(err)
	}
	before, err := provider.ReadySnapshot(ctx, by, "project", nil, meta)
	if err != nil || len(before.Items) != 1 || before.Items[0].ID != run.Items[2].ID || before.Revision != pluginapi.Natural64(state.rev) || before.ValidUntil == nil || !before.ValidUntil.Equal(first.Claim.ExpiresAt) {
		t.Fatalf("ready snapshot did not bind current membership and first expiry: %+v err=%v", before, err)
	}
	compat, err := provider.Ready(ctx, by, "project", nil, meta)
	if err != nil || !reflect.DeepEqual(compat, before.Items) {
		t.Fatalf("Ready compatibility changed: %+v err=%v", compat, err)
	}
	clock = first.Claim.ExpiresAt
	afterFirst, err := provider.ReadySnapshot(ctx, by, "project", nil, meta)
	if err != nil || len(afterFirst.Items) != 2 || afterFirst.Revision != before.Revision || afterFirst.ValidUntil == nil || !afterFirst.ValidUntil.Equal(second.Claim.ExpiresAt) {
		t.Fatalf("time-only membership change lost its binding: %+v err=%v", afterFirst, err)
	}
	clock = second.Claim.ExpiresAt
	afterBoth, err := provider.ReadySnapshot(ctx, by, "project", nil, meta)
	if err != nil || len(afterBoth.Items) != 3 || afterBoth.Revision != before.Revision || afterBoth.ValidUntil != nil {
		t.Fatalf("last expiry did not clear the time boundary: %+v err=%v", afterBoth, err)
	}
	for i := range afterBoth.Items {
		if afterBoth.Items[i].ID != run.Items[i].ID {
			t.Fatalf("snapshot order differs from ready ID order: %+v", afterBoth.Items)
		}
	}
	meta.Method, meta.IdempotencyKey = "bead.comment", strings.Repeat("4", 64)
	if _, err := provider.Comment(ctx, by, model.RefByID(run.Items[2].ID, model.Natural64(0)), "mutation", meta); err != nil {
		t.Fatal(err)
	}
	afterWrite, err := provider.ReadySnapshot(ctx, by, "project", nil, meta)
	if err != nil || afterWrite.Revision == before.Revision || afterWrite.Revision != pluginapi.Natural64(state.rev) || len(afterWrite.Items) != 3 {
		t.Fatalf("write did not advance snapshot generation: %+v err=%v", afterWrite, err)
	}
}

func TestReadySnapshotScopesExpiryToMatchingCandidates(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC)
	clock := start
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	provider.WithClock(func() time.Time { return clock })
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	for _, row := range []struct {
		agent, project, kind, key string
		age                       time.Duration
		claim                     bool
	}{
		{"owner", "project", "task", "1", 0, true},
		{"owner", "project", "task", "2", 0, false},
		{"other", "project", "task", "3", 10 * time.Minute, true},
		{"owner", "other", "task", "4", 9 * time.Minute, true},
		{"owner", "project", "issue", "5", 8 * time.Minute, true},
	} {
		clock = start.Add(-row.age)
		meta := model.WriteMeta{Method: "x.workflow.invoke", Project: row.project, AgentLabel: row.agent, IdempotencyKey: strings.Repeat(row.key, 64)}
		run, err := provider.Invoke(ctx, by, InvokeRequest{Template: "template", Project: row.project, Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "title", Type: row.kind}}}, meta)
		if err != nil {
			t.Fatal(err)
		}
		if row.claim {
			meta.Method = "bead.claim"
			if _, err := provider.Claim(ctx, by, model.RefByID(run.Items[0].ID, model.Natural64(0)), meta); err != nil {
				t.Fatal(err)
			}
		}
	}
	clock = start.Add(time.Minute)
	meta := model.WriteMeta{Project: "project", AgentLabel: "owner"}
	snapshot, err := provider.ReadySnapshot(ctx, by, "project", []string{"task"}, meta)
	if err != nil || len(snapshot.Items) != 1 || snapshot.Items[0].Type != "task" || snapshot.ValidUntil == nil || !snapshot.ValidUntil.Equal(start.Add(ClaimTTL)) {
		t.Fatalf("another owner/project/type changed snapshot scope: %+v err=%v", snapshot, err)
	}
	empty, err := provider.ReadySnapshot(ctx, by, "missing", nil, meta)
	if err != nil || empty.Items == nil || len(empty.Items) != 0 || empty.ValidUntil != nil || empty.Revision != snapshot.Revision {
		t.Fatalf("empty scope acquired another scope's deadline: %+v err=%v", empty, err)
	}
}

type snapshotReadState struct {
	*testDurableState
	reads     int
	afterRead func()
}

func (s *snapshotReadState) Read(ctx context.Context, key string, dst any) (bool, pluginapi.Revision, error) {
	found, revision, err := s.testDurableState.Read(ctx, key, dst)
	s.reads++
	if err == nil && s.afterRead != nil {
		hook := s.afterRead
		s.afterRead = nil
		hook()
	}
	return found, revision, err
}

func (s *snapshotReadState) Revision(context.Context, string) (pluginapi.Revision, error) {
	return "", errors.New("snapshot attempted a separate revision read")
}

func TestReadySnapshotPairsItemsWithTheirAtomicReadRevision(t *testing.T) {
	ctx := context.Background()
	state := &snapshotReadState{testDurableState: &testDurableState{}}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", AgentLabel: "owner", IdempotencyKey: strings.Repeat("1", 64)}
	run, err := provider.Invoke(ctx, by, InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "one"}, {Label: "two", Title: "two"}}}, meta)
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(state.testDurableState)
	if err != nil {
		t.Fatal(err)
	}
	beforeRevision := pluginapi.Natural64(state.rev)
	state.reads = 0
	state.afterRead = func() {
		meta.Method, meta.IdempotencyKey = "bead.close", strings.Repeat("2", 64)
		if _, err := other.Close(ctx, by, model.RefByID(run.Items[0].ID, model.Natural64(0)), "done", meta); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := provider.ReadySnapshot(ctx, by, "project", nil, meta)
	if err != nil || state.reads != 1 || len(snapshot.Items) != 2 || snapshot.Revision != beforeRevision || snapshot.Revision == pluginapi.Natural64(state.rev) {
		t.Fatalf("snapshot mixed items with a later generation: %+v reads=%d err=%v", snapshot, state.reads, err)
	}
	next, err := provider.ReadySnapshot(ctx, by, "project", nil, meta)
	if err != nil || len(next.Items) != 1 || next.Revision != pluginapi.Natural64(state.rev) || next.Revision == snapshot.Revision {
		t.Fatalf("following snapshot did not observe the other provider's write: %+v err=%v", next, err)
	}
}

func TestClaimNextWithResourcesRetainsSelectedAndEmptyReplay(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected", true: "empty"}[empty], func(t *testing.T) {
			ctx := context.Background()
			clock := time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC)
			state := &testDurableState{}
			provider, err := New(state)
			if err != nil {
				t.Fatal(err)
			}
			provider.WithClock(func() time.Time { return clock })
			by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", AgentLabel: "owner", IdempotencyKey: strings.Repeat("1", 64)}
			run, err := provider.Invoke(ctx, by, InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "private one"}, {Label: "two", Title: "private two"}}}, meta)
			if err != nil {
				t.Fatal(err)
			}
			sortItems(run.Items)
			allowed := run.Items[1].ID
			if empty {
				meta.Method, meta.IdempotencyKey = "bead.claim", strings.Repeat("2", 64)
				if _, err := provider.Claim(ctx, by, model.RefByID(allowed, model.Natural64(0)), meta); err != nil {
					t.Fatal(err)
				}
			}
			meta.Method, meta.IdempotencyKey, meta.ClaimNextIdentity = "bead.claimNext", strings.Repeat("3", 64), "canonical-resource-selector"
			item, found, replayed, err := provider.ClaimNextWithResources(ctx, by, "project", nil, []string{allowed}, meta)
			if err != nil || found != !empty || replayed || found && item.ID != allowed {
				t.Fatalf("resource selector chose an excluded candidate: %+v found=%v replayed=%v err=%v", item, found, replayed, err)
			}
			ready, err := provider.Ready(ctx, by, "project", nil, meta)
			if err != nil || len(ready) != 1 || ready[0].ID != run.Items[0].ID {
				t.Fatalf("filtered claim mutated the excluded ready item: %+v err=%v", ready, err)
			}
			before := append([]byte(nil), state.value...)
			encodedItem, err := json.Marshal(item)
			if err != nil {
				t.Fatal(err)
			}
			clock = clock.Add(ClaimTTL + time.Second)
			for _, receiptOnly := range []bool{false, true} {
				var retry ItemView
				var retryFound, retryReplay bool
				if receiptOnly {
					retry, retryFound, retryReplay, err = provider.ReplayClaimNext(ctx, by, "project", nil, meta)
				} else {
					retry, retryFound, retryReplay, err = provider.ClaimNextWithResources(ctx, by, "project", nil, []string{allowed}, meta)
				}
				encodedRetry, encodeErr := json.Marshal(retry)
				if err != nil || encodeErr != nil || !retryReplay || retryFound != found || !bytes.Equal(encodedRetry, encodedItem) || !bytes.Equal(before, state.value) {
					t.Fatalf("retry changed selector result after lease expiry: %+v found=%v replayed=%v err=%v", retry, retryFound, retryReplay, err)
				}
			}
			for i, key := range []string{"4", "5"} {
				closeMeta := meta
				closeMeta.Method, closeMeta.IdempotencyKey = "bead.close", strings.Repeat(key, 64)
				if _, err := provider.Close(ctx, by, model.RefByID(run.Items[i].ID, model.Natural64(0)), "done", closeMeta); err != nil {
					t.Fatal(err)
				}
			}
			terminal, terminalFound, replayed, err := provider.ClaimNextWithResources(ctx, by, "project", nil, []string{allowed}, meta)
			if err != nil || !replayed || terminalFound != found || found && (terminal.ID != item.ID || terminal.Revision != item.Revision) || terminal.Title != "" || terminal.Claim != nil || len(terminal.Comments) != 0 {
				t.Fatalf("resource receipt lost terminal identity or retained payload: %+v found=%v err=%v", terminal, terminalFound, err)
			}
		})
	}
}

func TestClaimNextWithResourcesBindsDirectSelectorAndIntersectsTypes(t *testing.T) {
	ctx := context.Background()
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := model.WriteMeta{Method: "x.workflow.invoke", Project: "project", AgentLabel: "owner", IdempotencyKey: strings.Repeat("1", 64)}
	run, err := provider.Invoke(ctx, by, InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "one", Title: "one", Type: "issue"}, {Label: "two", Title: "two", Type: "task"}}}, meta)
	if err != nil {
		t.Fatal(err)
	}
	resources := []string{run.Items[0].ID, run.Items[1].ID}
	meta.Method, meta.IdempotencyKey = "bead.claimNext", strings.Repeat("2", 64)
	first, found, replayed, err := provider.ClaimNextWithResources(ctx, by, "project", []string{"task"}, resources, meta)
	if err != nil || !found || replayed || first.ID != run.Items[1].ID {
		t.Fatalf("resource/type intersection selected another item: %+v found=%v err=%v", first, found, err)
	}
	before := append([]byte(nil), state.value...)
	_, _, _, err = provider.ClaimNextWithResources(ctx, by, "project", []string{"task"}, resources[:1], meta)
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "idempotency_key_reused" || !bytes.Equal(before, state.value) {
		t.Fatalf("changed resource selector reused a direct provider receipt: %v", err)
	}
	retry, found, replayed, err := provider.ClaimNextWithResources(ctx, by, "project", []string{"task"}, resources, meta)
	if err != nil || !found || !replayed || !reflect.DeepEqual(retry, first) || !bytes.Equal(before, state.value) {
		t.Fatalf("equal direct selector failed to replay without mutation: %+v err=%v", retry, err)
	}
}

func TestB26AuxiliaryFieldsBlockersMixedRelationsAndAtomicRollback(t *testing.T) {
	ctx := context.Background()
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := func(method, key string) model.WriteMeta {
		return model.WriteMeta{Method: method, Project: "project", Agent: "owner", AgentLabel: "owner", IdempotencyKey: strings.Repeat(key, 64)}
	}
	run, err := provider.Invoke(ctx, by, InvokeRequest{
		Template: "auxiliary", Project: "project", Mode: "ephemeral",
		Checklist: []ChecklistItem{
			{Label: "dependent", Title: "dependent title", Type: "task", Labels: []string{"one", "two"}, Metadata: map[string]any{"owner": "team"}},
			{Label: "blocker", Title: "blocker title", Type: "task"},
		},
	}, meta("x.workflow.invoke", "1"))
	if err != nil {
		t.Fatal(err)
	}
	dependent, blocker := model.RefByID(run.Items[0].ID, model.Natural64(0)), model.RefByID(run.Items[1].ID, model.Natural64(0))
	if _, err := provider.Update(ctx, by, dependent, map[string]any{
		"parent": blocker,
		"links":  []RelationSpec{{To: blocker, Kind: "blocks"}},
	}, meta("bead.update", "2")); err != nil {
		t.Fatal(err)
	}
	got, err := provider.Get(ctx, by, dependent, meta("bead.get", "3"))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Blocked || len(got.Labels) != 2 || got.Metadata["owner"] != "team" || got.Parent == nil || got.Parent.ID != blocker.ID || len(got.Links) != 1 {
		t.Fatalf("auxiliary fields or blocker state were not exposed: %+v", got)
	}
	blockerView, err := provider.Get(ctx, by, blocker, meta("bead.get", "4"))
	if err != nil || blockerView.ChildCounter != 1 || len(blockerView.Links) != 1 {
		t.Fatalf("parent counter/inbound relation was not preserved: %+v err=%v", blockerView, err)
	}
	ready, err := provider.Ready(ctx, by, "project", nil, meta("bead.ready", "5"))
	if err != nil || len(ready) != 1 || ready[0].ID != blocker.ID {
		t.Fatalf("blocked workflow item entered ready: %+v err=%v", ready, err)
	}
	if _, err := provider.Close(ctx, by, blocker, "done", meta("bead.close", "6")); err != nil {
		t.Fatal(err)
	}
	ready, err = provider.Ready(ctx, by, "project", nil, meta("bead.ready", "7"))
	if err != nil || len(ready) != 1 || ready[0].ID != dependent.ID {
		t.Fatalf("closed workflow blocker did not release dependent: %+v err=%v", ready, err)
	}
	before := append([]byte(nil), state.value...)
	missing := model.RefByID(ItemPrefix+strings.Repeat("f", 32), model.Natural64(0))
	_, err = provider.Update(ctx, by, dependent, map[string]any{"links": []RelationSpec{{To: missing, Kind: "blocks"}}}, meta("bead.update", "8"))
	var failure *model.Error
	if !errors.As(err, &failure) || failure.Data.Code != "not_found" || !bytes.Equal(before, state.value) {
		t.Fatalf("failed relation update was not atomic: err=%v stateChanged=%v", err, !bytes.Equal(before, state.value))
	}
	// Both mixed-plane directions remain typed in the workflow checkpoint. A
	// durable endpoint has no plugin body, so it never creates a core version or
	// lease row. A dependency blocks its source, never its target.
	durable := model.RefByID("durable-source", model.Natural64(0))
	if _, err := provider.Link(ctx, by, LinkRequest{From: dependent, To: durable, Kind: "relates-to"}, meta("bead.link", "9")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Link(ctx, by, LinkRequest{From: durable, To: dependent, Kind: "blocks"}, meta("bead.link", "a")); err != nil {
		t.Fatal(err)
	}
	got, err = provider.Get(ctx, by, dependent, meta("bead.get", "b"))
	if err != nil || len(got.Links) != 3 {
		t.Fatalf("mixed-plane relation read lost an edge: %+v err=%v", got, err)
	}
	if ready, err := provider.Ready(ctx, by, "project", nil, meta("bead.ready", "c")); err != nil || len(ready) != 1 || ready[0].ID != dependent.ID {
		t.Fatalf("incoming dependency incorrectly blocked its target: %+v err=%v", ready, err)
	}
	blocked, err := provider.OrdinaryBlocked(ctx, by, []string{durable.ID}, meta("bead.ready", "c"))
	if err != nil || !blocked[durable.ID] {
		t.Fatalf("durable dependent lost its workflow blocker: %+v %v", blocked, err)
	}
	if _, err := provider.Unlink(ctx, by, LinkRequest{From: durable, To: dependent, Kind: "blocks"}, meta("bead.unlink", "d")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Unlink(ctx, by, LinkRequest{From: dependent, To: durable, Kind: "relates-to"}, meta("bead.unlink", "e")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Close(ctx, by, dependent, "done", meta("bead.close", "f")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Get(ctx, by, dependent, meta("bead.get", "10")); err == nil {
		t.Fatal("completed workflow item remained in the active checkpoint")
	}
}

func TestB26PromotionMovesRelationsAndPreservesChildCounter(t *testing.T) {
	ctx := context.Background()
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := func(method, key string) model.WriteMeta {
		return model.WriteMeta{Method: method, Project: "project", Agent: "owner", AgentLabel: "owner", IdempotencyKey: strings.Repeat(key, 64)}
	}
	parentRun, err := provider.Invoke(ctx, by, InvokeRequest{Template: "parent", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "parent", Title: "parent"}}}, meta("x.workflow.invoke", "1"))
	if err != nil {
		t.Fatal(err)
	}
	childRun, err := provider.Invoke(ctx, by, InvokeRequest{Template: "child", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "child", Title: "child"}}}, meta("x.workflow.invoke", "2"))
	if err != nil {
		t.Fatal(err)
	}
	grandchildRun, err := provider.Invoke(ctx, by, InvokeRequest{Template: "grandchild", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "grandchild", Title: "grandchild"}}}, meta("x.workflow.invoke", "3"))
	if err != nil {
		t.Fatal(err)
	}
	parent := model.RefByID(parentRun.Items[0].ID, model.Natural64(0))
	child := model.RefByID(childRun.Items[0].ID, model.Natural64(0))
	grandchild := model.RefByID(grandchildRun.Items[0].ID, model.Natural64(0))
	if _, err := provider.Update(ctx, by, child, map[string]any{"parent": parent, "links": []RelationSpec{{To: parent, Kind: "relates-to"}}}, meta("bead.update", "4")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Update(ctx, by, grandchild, map[string]any{"parent": child}, meta("bead.update", "5")); err != nil {
		t.Fatal(err)
	}
	durable := model.RefByID("durable-source", model.Natural64(0))
	if _, err := provider.Link(ctx, by, LinkRequest{From: durable, To: child, Kind: "relates-to"}, meta("bead.link", "6")); err != nil {
		t.Fatal(err)
	}
	parentBefore, err := provider.Get(ctx, by, parent, meta("bead.get", "7"))
	if err != nil || parentBefore.ChildCounter != 1 {
		t.Fatalf("parent counter before promotion=%+v err=%v", parentBefore, err)
	}
	target := model.RefByID("durable-promoted", model.Natural64(0))
	promoted, err := provider.Promote(ctx, by, PromoteRequest{Item: child, Target: target}, meta("x.workflow.run.promote", "8"))
	if err != nil {
		t.Fatal(err)
	}
	if promoted.Source.ID != child.ID || promoted.Target.ID != target.ID || promoted.ChildCount != 1 || len(promoted.Relations) != 2 {
		t.Fatalf("promotion lost auxiliary state: %+v", promoted)
	}
	parentAfter, err := provider.Get(ctx, by, parent, meta("bead.get", "9"))
	if err != nil || parentAfter.ChildCounter != 0 || len(parentAfter.Links) != 1 || parentAfter.Links[0].From != target.ID {
		t.Fatalf("promotion did not reconcile parent/index state: %+v err=%v", parentAfter, err)
	}
	grandchildView, err := provider.Get(ctx, by, grandchild, meta("bead.get", "a"))
	if err != nil || grandchildView.Parent == nil || grandchildView.Parent.ID != target.ID {
		t.Fatalf("promotion did not preserve child parent identity: %+v err=%v", grandchildView, err)
	}
	if _, err := provider.Get(ctx, by, child, meta("bead.get", "b")); err == nil {
		t.Fatal("promoted item remained in active workflow state")
	}
	before := append([]byte(nil), state.value...)
	_, err = provider.Promote(ctx, by, PromoteRequest{Item: grandchild, Target: parent}, meta("x.workflow.run.promote", "c"))
	if err == nil || !bytes.Equal(before, state.value) {
		t.Fatalf("active workflow collision changed state: err=%v stateChanged=%v", err, !bytes.Equal(before, state.value))
	}
}

func TestB26PromotionErasesRemovedPayloadsFromLiveAndTerminalReplay(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("items-%d", count), func(t *testing.T) {
			ctx := context.Background()
			state := &testDurableState{}
			provider, err := New(state)
			if err != nil {
				t.Fatal(err)
			}
			by := model.PrincipalRef{Kind: "user", ID: "owner"}
			meta := func(method, key string) model.WriteMeta {
				return model.WriteMeta{Method: method, Project: "project", IdempotencyKey: strings.Repeat(key, 64)}
			}
			request := InvokeRequest{Template: "template", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "first", Title: "removed-title-marker", Fields: map[string]any{"description": "removed-field-marker"}, Metadata: map[string]any{"note": "removed-metadata-marker"}}}}
			if count == 2 {
				request.Checklist = append(request.Checklist, ChecklistItem{Label: "keeper", Title: "keeper-title-marker"})
			}
			run, err := provider.Invoke(ctx, by, request, meta("x.workflow.invoke", "1"))
			if err != nil {
				t.Fatal(err)
			}
			item := model.RefByID(run.Items[0].ID, model.Natural64(0))
			if _, err := provider.Comment(ctx, by, item, "removed-comment-marker", meta("bead.comment", "2")); err != nil {
				t.Fatal(err)
			}
			if _, err := provider.Claim(ctx, by, item, meta("bead.claim", "3")); err != nil {
				t.Fatal(err)
			}
			if _, err := provider.Promote(ctx, by, PromoteRequest{Item: item, Target: model.RefByID("durable-target", model.Natural64(0))}, meta("x.workflow.run.promote", "4")); err != nil {
				t.Fatal(err)
			}
			for _, payload := range []string{"removed-title-marker", "removed-field-marker", "removed-metadata-marker", "removed-comment-marker"} {
				if bytes.Contains(state.value, []byte(payload)) {
					t.Fatalf("promotion retained removed payload %q in its raw state", payload)
				}
			}
			beforeReplay := append([]byte(nil), state.value...)
			replay, err := provider.Invoke(ctx, by, request, meta("x.workflow.invoke", "1"))
			if err != nil || replay.Run == nil || len(replay.Items) != count-1 {
				t.Fatalf("invocation replay after promotion=%+v err=%v", replay, err)
			}
			if count == 1 && replay.Run.State != "completed" || count == 2 && replay.Run.State != "active" {
				t.Fatalf("promotion lost the actual remaining run state: %+v", replay.Run)
			}
			claim, err := provider.Claim(ctx, by, item, meta("bead.claim", "3"))
			if err != nil || claim.ID != item.ID || claim.Title != "" || claim.Fields != nil || claim.Metadata != nil || claim.Comments != nil || claim.Claim != nil {
				t.Fatalf("claim replay retained the removed payload: %+v err=%v", claim, err)
			}
			if !bytes.Equal(beforeReplay, state.value) {
				t.Fatal("identity replay recreated or republished workflow state")
			}
		})
	}
}

func TestB26CompletedDemotionErasesPayloadsAndReplaysIdentity(t *testing.T) {
	ctx := context.Background()
	state := &testDurableState{}
	provider, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "user", ID: "owner"}
	meta := model.WriteMeta{Method: "x.workflow.run.demote", Project: "project", IdempotencyKey: strings.Repeat("1", 64)}
	request := DemoteRequest{Source: model.RefByID("durable-source", model.Natural64(0)), Project: "project", Label: "item", Title: "demoted-title-marker", Fields: map[string]any{"description": "demoted-field-marker"}, Metadata: map[string]any{"note": "demoted-metadata-marker"}, Comments: []CommentView{{Body: "demoted-comment-marker", Principal: by, Time: time.Now().UTC()}}}
	result, err := provider.Demote(ctx, by, request, meta)
	if err != nil {
		t.Fatal(err)
	}
	closeMeta := model.WriteMeta{Method: "bead.close", Project: "project", IdempotencyKey: strings.Repeat("2", 64)}
	if _, err := provider.Close(ctx, by, model.RefByID(result.Item.ID, model.Natural64(0)), "done", closeMeta); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{"demoted-title-marker", "demoted-field-marker", "demoted-metadata-marker", "demoted-comment-marker"} {
		if bytes.Contains(state.value, []byte(payload)) {
			t.Fatalf("completed demotion retained payload %q", payload)
		}
	}
	beforeReplay := append([]byte(nil), state.value...)
	replay, err := provider.Demote(ctx, by, request, meta)
	if err != nil || replay.Run.State != "completed" || replay.Item.ID != result.Item.ID || replay.Item.Title != "" || replay.Item.Fields != nil || replay.Item.Metadata != nil || replay.Item.Comments != nil || replay.Item.Claim != nil {
		t.Fatalf("completed demotion replay=%+v err=%v", replay, err)
	}
	if !bytes.Equal(beforeReplay, state.value) {
		t.Fatal("terminal demotion replay recreated or republished workflow state")
	}
}

func TestB26CompletionClearsCrossRunParentAndMissingParentIsAtomic(t *testing.T) {
	ctx := context.Background()
	state := &testDurableState{}
	p, err := New(state)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := func(method, key string) model.WriteMeta {
		return model.WriteMeta{Method: method, Project: "project", Agent: "owner", AgentLabel: "owner", IdempotencyKey: key}
	}
	invoke := func(key string, parent *model.Ref) (InvokeResult, error) {
		return p.Invoke(ctx, by, InvokeRequest{Template: key, Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "item", Title: key, Parent: parent}}}, meta("x.workflow.invoke", key))
	}
	parentRun, err := invoke("parent", nil)
	if err != nil {
		t.Fatal(err)
	}
	parent := model.RefByID(parentRun.Items[0].ID, model.Natural64(0))
	childRun, err := invoke("child", &parent)
	if err != nil {
		t.Fatal(err)
	}
	child := model.RefByID(childRun.Items[0].ID, model.Natural64(0))
	before := append([]byte(nil), state.value...)
	missing := model.RefByID(ItemPrefix+strings.Repeat("e", 32), model.Natural64(0))
	if _, err := invoke("missing-parent", &missing); err == nil || !bytes.Equal(before, state.value) {
		t.Fatalf("missing parent published: %v", err)
	}
	if _, err := p.Close(ctx, by, parent, "done", meta("bead.close", "finish")); err != nil {
		t.Fatal(err)
	}
	got, err := p.Get(ctx, by, child, meta("bead.get", "read"))
	if err != nil || got.Parent != nil {
		t.Fatalf("completion left dangling parent: %+v %v", got, err)
	}
}

func TestB26RelationsRetainDistinctMetadata(t *testing.T) {
	ctx := context.Background()
	p, err := New(&testDurableState{})
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
	meta := func(method, key string) model.WriteMeta {
		return model.WriteMeta{Method: method, Project: "project", Agent: "owner", AgentLabel: "owner", IdempotencyKey: key}
	}
	run, err := p.Invoke(ctx, by, InvokeRequest{Template: "metadata", Project: "project", Mode: "ephemeral", Checklist: []ChecklistItem{{Label: "from", Title: "from"}, {Label: "to", Title: "to"}}}, meta("x.workflow.invoke", "invoke"))
	if err != nil {
		t.Fatal(err)
	}
	from, to := model.RefByID(run.Items[0].ID, model.Natural64(0)), model.RefByID(run.Items[1].ID, model.Natural64(0))
	for _, gate := range []string{"all-children", "any-children"} {
		if _, err := p.Link(ctx, by, LinkRequest{From: from, To: to, Kind: "waits-for", Metadata: &model.LinkMetadata{Gate: gate, AlsoBlocks: true}}, meta("bead.link", gate)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := p.Get(ctx, by, from, meta("bead.get", "read"))
	if err != nil || len(got.Links) != 2 {
		t.Fatalf("metadata collapsed: %+v %v", got, err)
	}
	if _, err := p.Unlink(ctx, by, LinkRequest{From: from, To: to, Kind: "waits-for", Metadata: &model.LinkMetadata{Gate: "all-children", AlsoBlocks: true}}, meta("bead.unlink", "remove-one")); err != nil {
		t.Fatal(err)
	}
	got, err = p.Get(ctx, by, from, meta("bead.get", "read-again"))
	if err != nil || len(got.Links) != 1 || got.Links[0].Metadata.Gate != "any-children" {
		t.Fatalf("metadata unlink removed sibling: %+v %v", got, err)
	}
}
