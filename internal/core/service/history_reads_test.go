package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	coreevents "github.com/buildbetter-app/plugs/internal/core/events"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	serverevents "github.com/buildbetter-app/plugs/internal/server/events"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type eventProvider struct{ service serverevents.Service }

func (p eventProvider) List(ctx context.Context, req EventProviderRequest) (EventPage, error) {
	page, err := p.service.List(ctx, serverevents.Request{
		Principal: req.Principal,
		Filter:    coreevents.Filter{Types: req.Filter.Types, Resources: req.Filter.Resources},
		After:     req.After,
		Limit:     req.Limit,
	})
	return EventPage{Events: page.Events, Cursor: page.Cursor}, err
}

type memoryHistoryRead struct {
	name string
	run  func(context.Context, *Service, model.PrincipalRef) (any, error)
}

func memoryHistoryReads(ref model.Ref, first, second model.Revision) []memoryHistoryRead {
	return []memoryHistoryRead{
		{"get-base", func(ctx context.Context, svc *Service, by model.PrincipalRef) (any, error) {
			return svc.HistoryGet(ctx, by, HistoryVersionRequest{Ref: ref, AsOf: AsOf{Revision: first}})
		}},
		{"get-patch", func(ctx context.Context, svc *Service, by model.PrincipalRef) (any, error) {
			return svc.HistoryGet(ctx, by, HistoryVersionRequest{Ref: ref, AsOf: AsOf{Revision: second}})
		}},
		{"diff", func(ctx context.Context, svc *Service, by model.PrincipalRef) (any, error) {
			return svc.HistoryDiff(ctx, by, HistoryDiffRequest{Ref: ref, FromRevision: first, ToRevision: second})
		}},
		{"equal-diff", func(ctx context.Context, svc *Service, by model.PrincipalRef) (any, error) {
			return svc.HistoryDiff(ctx, by, HistoryDiffRequest{Ref: ref, FromRevision: second, ToRevision: second})
		}},
	}
}

func saveHistoryMemory(t *testing.T, f *fixture, content string) model.Commit {
	t.Helper()
	commit, err := f.svc.MemorySave(context.Background(), anonymous, MemorySaveRequest{
		Key: "history-memory", Content: content, Metadata: map[string]any{"policy": content},
	}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

// The Writer allocates a new ID after forget. Exercise storage's supported
// same-reference recreation with its existing conformance fixture as well.
func recreateHistoryMemory(t *testing.T, f *fixture, ref model.Ref, content string) model.Revision {
	t.Helper()
	entry, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{
		Ref: ref, Kind: "memory", Op: model.OpCreate,
		Body: mustJSON(t, pipeline.MemoryDocument{ID: ref.ID, Key: "history-memory", Content: content}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return entry.Revision
}

func assertHistoryRefusal(t *testing.T, value any, err error, code model.ErrorCode) {
	t.Helper()
	wantCode(t, err, code)
	if !reflect.ValueOf(value).IsZero() {
		t.Fatalf("refused history returned a partial result: %#v", value)
	}
	var typed *model.Error
	if !errors.As(err, &typed) || len(typed.Data.Details) != 0 {
		t.Fatalf("memory refusal acquired history details: %v", err)
	}
	if code == "not_found" && typed.Data.Hint != "memory was not found" {
		t.Fatalf("memory point-read denial changed: %v", err)
	}
	raw := mustJSON(t, err)
	for _, secret := range []string{"old-secret", "changed-secret", "current-secret"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("history refusal disclosed %q: %s", secret, raw)
		}
	}
}

func TestHistoryMemoryUsesCurrentPolicyAndReturnsHistoricalValues(t *testing.T) {
	f := setup(t)
	first := saveHistoryMemory(t, f, "old-secret")
	second := saveHistoryMemory(t, f, "changed-secret")
	current := saveHistoryMemory(t, f, "current-secret")
	ref := first.Results[0].Ref
	by := model.PrincipalRef{Kind: "agent", ID: "worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: "sponsor"}, Caveats: map[string]any{"project": "local"}}
	before, err := f.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, read := range memoryHistoryReads(ref, first.Revision, second.Revision) {
		t.Run(read.name, func(t *testing.T) {
			for _, allow := range []bool{false, true} {
				calls := 0
				svc := New(f.store, f.lease, f.writer, WithAuthorizationInstalled(func() bool { return true }),
					WithMemoryReadAuthorizer(MemoryReadAuthorizerFunc(func(_ context.Context, principal model.PrincipalRef, memory Memory) (bool, error) {
						calls++
						if !reflect.DeepEqual(principal, by) || memory.ID != ref.ID || memory.Content != "current-secret" || memory.Metadata["policy"] != "current-secret" || memory.Revision != current.Revision {
							t.Fatalf("authorization did not receive the current memory/principal: %+v %+v", memory, principal)
						}
						// Policy inputs remain caller-owned even across final revalidation.
						principal.Sponsor.ID = "changed"
						principal.Caveats["project"] = "changed"
						memory.Metadata["policy"] = "changed"
						return allow, nil
					})))
				value, err := read.run(context.Background(), svc, by)
				if !allow {
					assertHistoryRefusal(t, value, err, "not_found")
					continue
				}
				if err != nil || calls < 2 {
					t.Fatalf("history did not reauthorize current memory: value=%+v calls=%d err=%v", value, calls, err)
				}
				switch read.name {
				case "get-base":
					v := value.(HistoryVersion)
					if !v.Base || v.Body["content"] != "old-secret" || len(v.Patch) != 0 {
						t.Fatalf("wrong historical base: %+v", v)
					}
				case "get-patch":
					v := value.(HistoryVersion)
					if v.Base || v.Body["content"] != "changed-secret" || len(v.Patch) != 1 || !reflect.DeepEqual(v.Patch[0]["value"], v.Body) || v.Revision != second.Revision || v.Ref.ID != ref.ID || v.Deleted || !v.CreatedAt.Equal(f.now) || v.Principal.ID != anonymous.ID {
						t.Fatalf("historical Body/Patch lost: %+v", v)
					}
				case "diff":
					v := value.(HistoryDiff)
					if !reflect.DeepEqual(v.ChangedPaths, []string{"/content", "/metadata/policy"}) || len(v.Patch) != 2 || v.Patch[0].Value != "changed-secret" {
						t.Fatalf("historical diff lost: %+v", v)
					}
				case "equal-diff":
					v := value.(HistoryDiff)
					if v.Patch == nil || v.ChangedPaths == nil || len(v.Patch)+len(v.ChangedPaths) != 0 {
						t.Fatalf("equal diff changed: %+v", v)
					}
				}
			}
		})
	}
	after, err := f.store.Meta(context.Background())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("history reads changed storage metadata: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestHistoryMemoryAuthorizationCapabilityFailures(t *testing.T) {
	f := setup(t)
	first := saveHistoryMemory(t, f, "old-secret")
	second := saveHistoryMemory(t, f, "changed-secret")
	for _, read := range memoryHistoryReads(first.Results[0].Ref, first.Revision, second.Revision) {
		for _, scenario := range []string{"missing-policy", "policy-error", "policy-cancellation", "disabled-agent", "disabled-sponsored", "disabled-caveated", "disabled-unscoped", "disabled-during-policy"} {
			t.Run(read.name+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				installed, calls := true, 0
				by := anonymous
				code := model.ErrorCode("authz_unavailable")
				var policy MemoryReadAuthorizer = MemoryReadAuthorizerFunc(func(context.Context, model.PrincipalRef, Memory) (bool, error) {
					calls++
					switch scenario {
					case "policy-error":
						return false, errors.New("current-secret: policy failed")
					case "policy-cancellation":
						cancel()
						return false, ctx.Err()
					case "disabled-during-policy":
						installed = false
					}
					return true, nil
				})
				switch scenario {
				case "missing-policy":
					policy = nil
				case "policy-cancellation":
					code = "busy"
				case "disabled-agent":
					installed, code = false, "authz_not_installed"
					by = model.PrincipalRef{Kind: "agent", ID: "worker"}
				case "disabled-sponsored":
					installed, code = false, "authz_not_installed"
					by = model.PrincipalRef{Kind: "user", ID: "reader", Sponsor: &model.PrincipalRef{Kind: "user", ID: "sponsor"}}
				case "disabled-caveated":
					installed, code = false, "authz_not_installed"
					by = model.PrincipalRef{Kind: "user", ID: "reader", Caveats: map[string]any{"project": "local"}}
				case "disabled-unscoped":
					installed, code = false, ""
				}
				svc := New(f.store, f.lease, f.writer, WithMemoryReadAuthorizer(policy), WithAuthorizationInstalled(func() bool { return installed }))
				value, err := read.run(ctx, svc, by)
				if code != "" {
					assertHistoryRefusal(t, value, err, code)
				} else if err != nil {
					t.Fatalf("unscoped no-plugin read failed: %v", err)
				}
				if (code == "authz_not_installed" || scenario == "disabled-unscoped") && calls != 0 {
					t.Fatal("disabled installation called a stale policy")
				}
			})
		}
	}
}

func TestHistoryMemoryRequiresCurrentSameLifetime(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	first := saveHistoryMemory(t, f, "old-secret")
	second := saveHistoryMemory(t, f, "changed-secret")
	ref := first.Results[0].Ref
	deleted, err := f.svc.MemoryDelete(ctx, anonymous, MemoryDeleteRequest{ID: ref}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	svc := New(f.store, f.lease, f.writer, WithMemoryReadAuthorizer(MemoryReadAuthorizerFunc(func(_ context.Context, _ model.PrincipalRef, memory Memory) (bool, error) {
		calls++
		if memory.Content == "" {
			return true, nil
		}
		if memory.Content != "current-secret" {
			t.Fatalf("old content or tombstone supplied to policy: %+v", memory)
		}
		return true, nil
	})))
	for _, read := range memoryHistoryReads(ref, first.Revision, second.Revision) {
		value, err := read.run(ctx, svc, anonymous)
		assertHistoryRefusal(t, value, err, "not_found")
	}
	if calls == 0 {
		t.Fatal("deleted memory did not authorize its missing identity before refusal")
	}
	recreated := recreateHistoryMemory(t, f, ref, "replacement")
	current := saveHistoryMemory(t, f, "current-secret")
	for _, read := range memoryHistoryReads(ref, first.Revision, second.Revision) {
		value, err := read.run(ctx, svc, anonymous)
		assertHistoryRefusal(t, value, err, "not_found")
	}
	for _, revision := range []model.Revision{model.Natural64(0), deleted.Revision} {
		v, err := svc.HistoryGet(ctx, anonymous, HistoryVersionRequest{Ref: ref, AsOf: AsOf{Revision: revision}})
		wantCode(t, err, "not_found")
		if !reflect.ValueOf(v).IsZero() {
			t.Fatal("absent historical memory returned values")
		}
		for _, to := range []model.Revision{revision, recreated, current.Revision} {
			value, err := svc.HistoryDiff(ctx, anonymous, HistoryDiffRequest{Ref: ref, FromRevision: revision, ToRevision: to})
			assertHistoryRefusal(t, value, err, "not_found")
		}
	}
	cross, err := svc.HistoryDiff(ctx, anonymous, HistoryDiffRequest{Ref: ref, FromRevision: second.Revision, ToRevision: current.Revision})
	assertHistoryRefusal(t, cross, err, "not_found")
	for _, read := range memoryHistoryReads(ref, recreated, current.Revision) {
		if _, err := read.run(ctx, svc, anonymous); err != nil {
			t.Fatalf("current lifetime %s was refused: %v", read.name, err)
		}
	}
}

// Inconsistent driver evidence must never turn a historical memory into an
// ordinary record or prove a lifetime. Bodies and versions still come from the
// real store; each test removes or contradicts one piece of identity evidence.
type historyEvidenceStore struct {
	driver.Store
	readErr  func(driver.View) error
	get      func(bool, []driver.Record) []driver.Record
	versions func(bool, driver.Page[driver.Version]) driver.Page[driver.Version]
}

func (s *historyEvidenceStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	if s.readErr != nil {
		if err := s.readErr(view); err != nil {
			return nil, err
		}
	}
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return historyEvidenceReader{Reader: r, store: s, historical: view.At != nil}, nil
}

type historyEvidenceReader struct {
	driver.Reader
	store      *historyEvidenceStore
	historical bool
}

func (r historyEvidenceReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	rows, err := r.Reader.Get(ctx, refs)
	if err == nil && r.store.get != nil {
		rows = r.store.get(r.historical, rows)
	}
	return rows, err
}

func (r historyEvidenceReader) Versions(ctx context.Context, ref driver.Ref, req driver.PageReq) (driver.Page[driver.Version], error) {
	page, err := r.Reader.Versions(ctx, ref, req)
	if err == nil && r.store.versions != nil {
		page = r.store.versions(r.historical, page)
	}
	return page, err
}

func TestHistoryMemoryRefusesUncertainKindAndLifetime(t *testing.T) {
	f := setup(t)
	first := saveHistoryMemory(t, f, "old-secret")
	second := saveHistoryMemory(t, f, "changed-secret")
	saveHistoryMemory(t, f, "current-secret")
	for _, scenario := range []string{"unknown-current-kind", "unknown-historical-kind", "current-non-memory", "historical-non-memory", "absent-current", "missing-current-versions", "missing-requested-version", "inconsistent-version-ref"} {
		t.Run(scenario, func(t *testing.T) {
			store := &historyEvidenceStore{Store: f.store}
			store.get = func(historical bool, rows []driver.Record) []driver.Record {
				if len(rows) == 0 {
					return rows
				}
				switch {
				case scenario == "unknown-current-kind" && !historical, scenario == "unknown-historical-kind" && historical:
					rows[0].Kind = ""
				case scenario == "current-non-memory" && !historical, scenario == "historical-non-memory" && historical:
					rows[0].Kind = "bead"
				case scenario == "absent-current" && !historical:
					return nil
				}
				return rows
			}
			store.versions = func(historical bool, page driver.Page[driver.Version]) driver.Page[driver.Version] {
				if !historical {
					switch scenario {
					case "missing-current-versions":
						page.Items = nil
					case "missing-requested-version":
						page.Items = page.Items[len(page.Items)-1:]
					case "inconsistent-version-ref":
						page.Items[0].Ref = model.RefByID("pl-unrelated", model.Natural64(0))
					}
				}
				return page
			}
			svc := New(store, f.lease, f.writer)
			for _, read := range memoryHistoryReads(first.Results[0].Ref, first.Revision, second.Revision) {
				value, err := read.run(context.Background(), svc, anonymous)
				assertHistoryRefusal(t, value, err, "not_found")
			}
		})
	}
}

func TestHistoryMemoryPeriodicBaseDoesNotStartANewLifetime(t *testing.T) {
	f := setup(t)
	first := saveHistoryMemory(t, f, "old-secret")
	second := saveHistoryMemory(t, f, "changed-secret")
	for i := 2; i < driver.VersionBaseEvery+2; i++ {
		saveHistoryMemory(t, f, fmt.Sprintf("update-%d", i))
	}
	for _, read := range memoryHistoryReads(first.Results[0].Ref, first.Revision, second.Revision) {
		if _, err := read.run(context.Background(), f.svc, anonymous); err != nil {
			t.Fatalf("periodic base split the live lifetime for %s: %v", read.name, err)
		}
	}
}

func TestHistoryDeletedMemoryWithoutKindEvidenceIsHidden(t *testing.T) {
	f := setup(t)
	first := saveHistoryMemory(t, f, "old-secret")
	second := saveHistoryMemory(t, f, "changed-secret")
	if _, err := f.svc.MemoryDelete(context.Background(), anonymous, MemoryDeleteRequest{ID: first.Results[0].Ref}, meta(t)); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"no-live-version", "no-kind", "unavailable-kind-snapshot"} {
		t.Run(scenario, func(t *testing.T) {
			store := &historyEvidenceStore{Store: f.store}
			store.versions = func(_ bool, page driver.Page[driver.Version]) driver.Page[driver.Version] {
				if scenario == "no-live-version" {
					page.Items = page.Items[len(page.Items)-1:]
				}
				return page
			}
			store.get = func(historical bool, rows []driver.Record) []driver.Record {
				if scenario == "no-kind" && historical && len(rows) != 0 {
					rows[0].Kind = ""
				}
				return rows
			}
			store.readErr = func(view driver.View) error {
				if scenario == "unavailable-kind-snapshot" && view.At != nil {
					return driver.ErrTruncated{Floor: second.Revision}
				}
				return nil
			}
			svc := New(store, f.lease, f.writer, WithMemoryReadAuthorizer(MemoryReadAuthorizerFunc(func(context.Context, model.PrincipalRef, Memory) (bool, error) {
				// Missing history identity is authorized with an ID-only
				// placeholder before the non-disclosing refusal.
				return true, nil
			})))
			for _, read := range memoryHistoryReads(first.Results[0].Ref, first.Revision, second.Revision) {
				v, err := read.run(context.Background(), svc, anonymous)
				assertHistoryRefusal(t, v, err, "not_found")
			}
		})
	}
}

func TestHistoryMaterializationRejectsMissingOrMismatchedRecords(t *testing.T) {
	f := setup(t)
	first := saveHistoryMemory(t, f, "old-secret")
	second := saveHistoryMemory(t, f, "changed-secret")
	for _, scenario := range []string{"missing", "wrong-ref", "wrong-branch", "wrong-revision", "extra-row"} {
		t.Run(scenario, func(t *testing.T) {
			store := &historyEvidenceStore{Store: f.store, get: func(historical bool, rows []driver.Record) []driver.Record {
				if !historical || len(rows) == 0 {
					return rows
				}
				switch scenario {
				case "missing":
					return nil
				case "wrong-ref":
					rows[0].Ref = model.RefByID("pl-unrelated", model.Natural64(0))
				case "wrong-branch":
					rows[0].Ref.Branch = model.Natural64(1)
				case "wrong-revision":
					rows[0].Revision = first.Revision
				case "extra-row":
					return append(rows, rows[0])
				}
				return rows
			}}
			svc := New(store, f.lease, f.writer)
			v, err := svc.HistoryGet(context.Background(), anonymous, HistoryVersionRequest{Ref: first.Results[0].Ref, AsOf: AsOf{Revision: second.Revision}})
			assertHistoryRefusal(t, v, err, "internal_error")
		})
	}
}

// Trigger a competing change after a real historical read. Current/head reads
// remain fresh; neither the policy nor the history result is replaced by a stub.
type historyHookStore struct {
	driver.Store
	afterRead func()
}

func (s *historyHookStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil || view.At == nil {
		return r, err
	}
	return historyHookReader{Reader: r, store: s}, nil
}

type historyHookReader struct {
	driver.Reader
	store *historyHookStore
}

func (r historyHookReader) after() {
	if hook := r.store.afterRead; hook != nil {
		r.store.afterRead = nil
		hook()
	}
}

func (r historyHookReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	rows, err := r.Reader.Get(ctx, refs)
	if err == nil {
		r.after()
	}
	return rows, err
}

func (r historyHookReader) Versions(ctx context.Context, ref driver.Ref, req driver.PageReq) (driver.Page[driver.Version], error) {
	page, err := r.Reader.Versions(ctx, ref, req)
	if err == nil {
		r.after()
	}
	return page, err
}

func TestHistoryMemoryRevalidatesAfterHistoricalRead(t *testing.T) {
	for _, scenario := range []string{"disable", "install", "current-value-denied", "policy-now-denies", "delete", "recreate"} {
		for _, readName := range []string{"get-base", "get-patch", "diff", "equal-diff"} {
			t.Run(scenario+"/"+readName, func(t *testing.T) {
				f := setup(t, pipeline.WithIDGenerator(func() (string, error) { return "pl-history", nil }))
				first := saveHistoryMemory(t, f, "old-secret")
				second := saveHistoryMemory(t, f, "changed-secret")
				installed, allow, calls := scenario != "install", true, 0
				by := model.PrincipalRef{Kind: "agent", ID: "worker"}
				if !installed {
					by = anonymous
				}
				store := &historyHookStore{Store: f.store}
				store.afterRead = func() {
					switch scenario {
					case "disable", "install":
						installed = !installed
					case "current-value-denied":
						saveHistoryMemory(t, f, "current-secret")
					case "policy-now-denies":
						allow = false
					case "delete", "recreate":
						if _, err := f.svc.MemoryDelete(context.Background(), anonymous, MemoryDeleteRequest{ID: first.Results[0].Ref}, meta(t)); err != nil {
							t.Fatal(err)
						}
						if scenario == "recreate" {
							recreateHistoryMemory(t, f, first.Results[0].Ref, "replacement")
						}
					}
				}
				svc := New(store, f.lease, f.writer, WithAuthorizationInstalled(func() bool { return installed }),
					WithMemoryReadAuthorizer(MemoryReadAuthorizerFunc(func(_ context.Context, _ model.PrincipalRef, memory Memory) (bool, error) {
						calls++
						return allow && memory.Content != "current-secret", nil
					})))
				for _, read := range memoryHistoryReads(first.Results[0].Ref, first.Revision, second.Revision) {
					if read.name != readName {
						continue
					}
					value, err := read.run(context.Background(), svc, by)
					code := model.ErrorCode("not_found")
					if scenario == "disable" || scenario == "install" {
						code = "authz_unavailable"
					}
					assertHistoryRefusal(t, value, err, code)
				}
				if store.afterRead != nil || scenario == "disable" && calls != 1 || scenario == "install" && calls != 0 {
					t.Fatalf("transition not exercised or stale policy called: calls=%d hook=%v", calls, store.afterRead != nil)
				}
			})
		}
	}
}

func TestOrdinaryBeadHistoryDoesNotRequireMemoryAuthorization(t *testing.T) {
	f := setup(t)
	ref, first := create(t, f, "ordinary")
	second, err := f.svc.Update(context.Background(), anonymous, UpdateRequest{ID: ref, Fields: map[string]any{"title": "updated"}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Update(context.Background(), anonymous, UpdateRequest{ID: ref, Fields: map[string]any{"title": "later"}}, meta(t)); err != nil {
		t.Fatal(err)
	}
	for _, deleted := range []bool{false, true} {
		var removal model.Commit
		if deleted {
			removal, err = f.writer.Apply(context.Background(), anonymous, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref}}}}, meta(t))
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, installed := range []bool{false, true} {
			svc := New(f.store, f.lease, f.writer, WithAuthorizationInstalled(func() bool { return installed }))
			for _, read := range memoryHistoryReads(ref, first.Revision, second.Revision) {
				value, err := read.run(context.Background(), svc, model.PrincipalRef{Kind: "agent", ID: "worker"})
				if err != nil {
					t.Fatalf("ordinary bead %s requires memory capability: installed=%t deleted=%t err=%v", read.name, installed, deleted, err)
				}
				switch read.name {
				case "get-base", "get-patch":
					v := value.(HistoryVersion)
					title, revision := "ordinary", first.Revision
					if read.name == "get-patch" {
						title, revision = "updated", second.Revision
						if v.Base || len(v.Patch) != 1 || !reflect.DeepEqual(v.Patch[0]["value"], v.Body) {
							t.Fatalf("non-base Body/Patch are inconsistent: %+v", v)
						}
					}
					if v.Body["fields"].(map[string]any)["title"] != title || v.Revision != revision || v.Ref.ID != ref.ID || v.Ref.Branch.String() != ref.Branch.String() || v.Deleted || !v.CreatedAt.Equal(f.now) || v.Principal.Kind != anonymous.Kind || v.Principal.ID != anonymous.ID {
						t.Fatalf("historical body or metadata changed: %+v", v)
					}
				case "diff":
					v := value.(HistoryDiff)
					if !reflect.DeepEqual(v.ChangedPaths, []string{"/fields/title"}) || len(v.Patch) != 1 || v.Patch[0].Value != "updated" {
						t.Fatalf("historical diff changed: %+v", v)
					}
				case "equal-diff":
					v := value.(HistoryDiff)
					if v.Patch == nil || v.ChangedPaths == nil || len(v.Patch)+len(v.ChangedPaths) != 0 {
						t.Fatalf("equal diff changed: %+v", v)
					}
				}
			}
			if deleted {
				v, err := svc.HistoryGet(context.Background(), anonymous, HistoryVersionRequest{Ref: ref, AsOf: AsOf{Revision: removal.Revision}})
				if err != nil || !v.Deleted || v.Body != nil {
					t.Fatalf("ordinary bead tombstone lost: %+v %v", v, err)
				}
				vDiff, err := svc.HistoryDiff(context.Background(), anonymous, HistoryDiffRequest{Ref: ref, FromRevision: second.Revision, ToRevision: removal.Revision})
				if err != nil || len(vDiff.Patch) != 1 || vDiff.Patch[0].Op != "remove" || vDiff.Patch[0].Path != "" {
					t.Fatalf("ordinary bead deletion diff lost: %+v %v", vDiff, err)
				}
			}
		}
	}
}

func TestHistoryEventAndHealthReadsUsePinnedStorageWithoutMutation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	storeMeta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cursorCodec, err := serverevents.NewCursorCodec([]byte("history-read-test-cursor-key-000000"))
	if err != nil {
		t.Fatal(err)
	}
	f.svc.events = eventProvider{service: serverevents.Service{
		Reader:  &coreevents.Reader{Log: serverevents.Log{Store: f.store}, Materializer: serverevents.HistoryMaterializer{Store: f.store}, Authorization: coreevents.AllowAll{}},
		Cursors: cursorCodec,
	}}

	ref, first := create(t, f, "first")
	second, err := f.svc.Update(ctx, anonymous, UpdateRequest{ID: ref, Fields: map[string]any{"title": "second"}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.svc.EventList(ctx, anonymous, EventListRequest{Filter: &Filter{}, Limit: 1})
	if err != nil || len(page.Events) != 1 || page.Cursor == "" {
		t.Fatalf("first event page=%+v err=%v", page, err)
	}
	resumed, err := f.svc.EventList(ctx, anonymous, EventListRequest{Filter: &Filter{}, After: page.Cursor, Limit: 1})
	if err != nil || len(resumed.Events) != 1 || resumed.Events[0].Durable == nil || resumed.Events[0].Durable.Revision.String() != second.Revision.String() {
		t.Fatalf("resumed event page=%+v err=%v", resumed, err)
	}

	version, err := f.svc.HistoryGet(ctx, anonymous, HistoryVersionRequest{Ref: ref, AsOf: AsOf{Revision: first.Revision, Incarnation: storeMeta.Incarnation}})
	if err != nil || version.Body["fields"].(map[string]any)["title"] != "first" || !version.Base {
		t.Fatalf("history version=%+v err=%v", version, err)
	}
	diff, err := f.svc.HistoryDiff(ctx, anonymous, HistoryDiffRequest{Ref: ref, FromRevision: first.Revision, ToRevision: second.Revision})
	if err != nil || len(diff.Patch) != 1 || diff.Patch[0].Path != "/fields/title" || diff.Patch[0].Value != "second" {
		t.Fatalf("history diff=%+v err=%v", diff, err)
	}
	healthBefore, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	health, err := f.svc.Health(ctx, anonymous)
	if err != nil || health.Head.String() != healthBefore.Head.String() || health.Incarnation != healthBefore.Incarnation || health.Role != "active" {
		t.Fatalf("health=%+v err=%v meta=%+v", health, err, healthBefore)
	}
	if health.Head.String() != second.Revision.String() {
		t.Fatalf("health head=%s want %s", health.Head, second.Revision)
	}
	reader, err := f.store.Read(ctx, driver.View{Incarnation: storeMeta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()

	// The reviewed event capability and the memory capability coexist on one
	// service, including when memory history is denied by the current policy.
	f.svc.memoryReadAuthorizer = MemoryReadAuthorizerFunc(func(context.Context, model.PrincipalRef, Memory) (bool, error) { return false, nil })
	oldMemory := saveHistoryMemory(t, f, "old-secret")
	newMemory := saveHistoryMemory(t, f, "current-secret")
	for _, read := range memoryHistoryReads(oldMemory.Results[0].Ref, oldMemory.Revision, newMemory.Revision) {
		v, err := read.run(ctx, f.svc, anonymous)
		assertHistoryRefusal(t, v, err, "not_found")
	}
	page, err = f.svc.EventList(ctx, anonymous, EventListRequest{Filter: &Filter{Events: []string{"bead.created"}}, Limit: 10})
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("memory capability displaced event provider: %+v %v", page, err)
	}
	health, err = f.svc.Health(ctx, anonymous)
	if err != nil || health.Head != newMemory.Revision {
		t.Fatalf("memory capability displaced health reads: %+v %v", health, err)
	}
}

func TestHistoryReadsMapFutureRevisionToNotFound(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	ref, created := create(t, f, "future revision")
	storeMeta, err := f.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	head, err := storeMeta.Head.Uint64()
	if err != nil {
		t.Fatal(err)
	}
	future := model.Natural64(head + 1)

	assertFutureHistoryNotFound := func(name string, err error) {
		t.Helper()
		var typed *model.Error
		if !errors.As(err, &typed) || typed.Data.Code != "not_found" {
			t.Fatalf("%s error=%T %v, want typed not_found", name, err, err)
		}
		if typed.Data.Details["requestedRevision"] != future || typed.Data.Details["headRevision"] != storeMeta.Head {
			t.Fatalf("%s details=%#v, want requested=%s head=%s", name, typed.Data.Details, future, storeMeta.Head)
		}
	}

	_, err = f.svc.HistoryGet(ctx, anonymous, HistoryVersionRequest{
		Ref:  ref,
		AsOf: AsOf{Revision: future, Incarnation: storeMeta.Incarnation},
	})
	assertFutureHistoryNotFound("history.get", err)

	_, err = f.svc.HistoryDiff(ctx, anonymous, HistoryDiffRequest{
		Ref:          ref,
		FromRevision: created.Revision,
		ToRevision:   future,
	})
	assertFutureHistoryNotFound("history.diff", err)
}

// Keep the real reader and its historical snapshot, but expose incomplete or
// inconsistent driver evidence to verify typed refusal without a partial body.
type historyMaterializationStore struct {
	driver.Store
	scenario string
}

func (s historyMaterializationStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	return historyMaterializationReader{Reader: r, scenario: s.scenario, historical: view.At != nil}, nil
}

type historyMaterializationReader struct {
	driver.Reader
	scenario   string
	historical bool
}

func (r historyMaterializationReader) Versions(ctx context.Context, ref driver.Ref, req driver.PageReq) (driver.Page[driver.Version], error) {
	page, err := r.Reader.Versions(ctx, ref, req)
	if err != nil || len(page.Items) == 0 {
		return page, err
	}
	page.Items = append([]driver.Version(nil), page.Items...)
	v := &page.Items[len(page.Items)-1]
	v.Patch = []byte(`[{"op":"replace","path":"","value":` + string(v.Body) + `}]`)
	v.Body = nil
	switch r.scenario {
	case "missing-patch":
		v.Patch = nil
	case "malformed-patch":
		v.Patch = []byte(`[{`)
	case "non-array-patch":
		v.Patch = []byte(`{}`)
	case "missing-base-body":
		v.Base = true
	case "wrong-version-ref":
		v.Ref = model.RefByID("other", model.Natural64(0))
	}
	return page, nil
}

func (r historyMaterializationReader) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	rows, err := r.Reader.Get(ctx, refs)
	if !r.historical {
		return rows, err
	}
	switch r.scenario {
	case "missing-record":
		return nil, nil
	case "truncated-materialization":
		return nil, driver.ErrTruncated{Floor: model.Natural64(2)}
	case "changed-incarnation":
		return nil, driver.ErrIncarnationChanged{From: "old", To: "new"}
	}
	if err != nil || len(rows) == 0 {
		return rows, err
	}
	switch r.scenario {
	case "wrong-record-ref":
		rows[0].Ref = model.RefByID("other", model.Natural64(0))
	case "wrong-record-revision":
		rows[0].Revision = model.Natural64(0)
	case "missing-body":
		rows[0].Body = nil
	case "malformed-body":
		rows[0].Body = []byte(`[]`)
	case "duplicate-record":
		rows = append(rows, rows[0])
	}
	return rows, nil
}

func TestHistoryPatchMaterializationRefusesInconsistentEvidence(t *testing.T) {
	f := setup(t)
	ref, _ := create(t, f, "base")
	updated, err := f.svc.Update(context.Background(), anonymous, UpdateRequest{ID: ref, Fields: map[string]any{"title": "historical"}}, meta(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"missing-patch", "malformed-patch", "non-array-patch", "missing-base-body", "wrong-version-ref", "missing-record", "wrong-record-ref", "wrong-record-revision", "missing-body", "malformed-body", "duplicate-record", "truncated-materialization", "changed-incarnation"} {
		t.Run(scenario, func(t *testing.T) {
			svc := New(historyMaterializationStore{Store: f.store, scenario: scenario}, f.lease, f.writer)
			value, err := svc.HistoryGet(context.Background(), anonymous, HistoryVersionRequest{Ref: ref, AsOf: AsOf{Revision: updated.Revision}})
			code := model.ErrorCode("internal_error")
			if scenario == "wrong-version-ref" {
				// #117 validates every version page before materialization and
				// hides identity uncertainty as not_found.
				code = "not_found"
			}
			if scenario == "truncated-materialization" {
				code = "history_gone"
			}
			if scenario == "changed-incarnation" {
				code = "incarnation_changed"
			}
			wantCode(t, err, code)
			if !reflect.DeepEqual(value, HistoryVersion{}) {
				t.Fatalf("refusal returned a partial history value: %+v", value)
			}
		})
	}
}
