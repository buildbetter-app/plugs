package composition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/ontology"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These wrappers count actual calls and forward the real storage operation.
// They deliberately expose Selection and startup coverage independently.
type compositionSelectionStore struct {
	driver.Store
	driver.CheckpointStore
	selectable  bool
	selects     atomic.Int64
	queries     atomic.Int64
	unavailable atomic.Int64
	afterQuery  func()
	afterSelect func()
}

func (s *compositionSelectionStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	counted := &compositionSelectionReader{Reader: r, store: s}
	if s.selectable {
		return &compositionSelectionSelector{compositionSelectionReader: counted}, nil
	}
	return counted, nil
}

type compositionCoverageStore struct{ *compositionSelectionStore }

func (s *compositionCoverageStore) BuildGraphSourceIndexes(ctx context.Context, lease driver.ActiveLease, decode driver.GraphSourceDecoder, projections ...driver.GraphSourceProjection) error {
	return s.Store.(driver.GraphSourceIndexBuilder).BuildGraphSourceIndexes(ctx, lease, decode, projections...)
}

type compositionSelectionReader struct {
	driver.Reader
	store *compositionSelectionStore
}

func (r *compositionSelectionReader) Query(ctx context.Context, query driver.Query) (driver.Page[driver.Record], error) {
	r.store.queries.Add(1)
	page, err := r.Reader.Query(ctx, query)
	if hook := r.store.afterQuery; hook != nil && err == nil {
		r.store.afterQuery = nil
		hook()
	}
	return page, err
}

type compositionSelectionSelector struct{ *compositionSelectionReader }

func (r *compositionSelectionSelector) Select(ctx context.Context, plan driver.SelectionPlan) (driver.SelectionResult, error) {
	r.store.selects.Add(1)
	result, err := r.Reader.(driver.SelectionReader).Select(ctx, plan)
	if errors.Is(err, driver.ErrSelectionUnavailable) {
		r.store.unavailable.Add(1)
	}
	if hook := r.store.afterSelect; hook != nil && err == nil {
		r.store.afterSelect = nil
		hook()
	}
	return result, err
}

type compositionSelectionView struct{ definition viewops.Definition }

func (v compositionSelectionView) GetView(context.Context, model.PrincipalRef, string) (viewops.Definition, error) {
	return v.definition, nil
}

func compositionSelectionRef(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func TestComposedViewSelectionCapabilitiesCoverageAndCheckOnly(t *testing.T) {
	for _, kind := range []string{"memory", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			for _, mode := range []string{"eligible", "unsupported_reader", "unsupported_coverage", "uncovered_history", "saved", "unbounded_label", "extra_host", "no_plugins", "caveated", "unsupported_reader_revoke", "unsupported_coverage_revoke", "fixed_refs", "empty_refs", "unsupported_caveat"} {
				t.Run(mode, func(t *testing.T) {
					ctx := context.Background()
					config := Config{Driver: kind, Project: "visible-name", Address: DefaultAddress}
					if kind == "sqlite" {
						config.DSN = filepath.Join(t.TempDir(), "selection.db")
					}
					raw, err := openStore(ctx, config)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = raw.Close() })
					observed := &compositionSelectionStore{Store: raw, CheckpointStore: raw.(driver.CheckpointStore), selectable: mode != "unsupported_reader" && mode != "unsupported_reader_revoke"}
					var selected driver.Store = &compositionCoverageStore{observed}
					if mode == "unsupported_coverage" || mode == "unsupported_coverage_revoke" {
						selected = observed
					}
					config.StoreFactory = func(context.Context, string, driver.Config) (driver.Store, error) { return selected, nil }
					by := model.PrincipalRef{Kind: "user", ID: "user:selection"}
					if mode == "caveated" {
						by.Caveats = map[string]any{"projects": []model.Ref{compositionSelectionRef("visible-project")}}
					}
					binds, fences := 0, 0
					var facts *authzrpc.MutableFacts
					var base authzfixed.Facts
					if mode != "no_plugins" {
						provider := authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }})
						config.AuthMode = "authenticated"
						config.Auth = authn.Config{Provider: provider, AuthorizationInstalled: func() bool { return true }}
						config.SelectionFactory = func(ctx context.Context, store driver.Store, lease driver.ActiveLease) (*viewops.SelectionConfig, error) {
							meta, err := store.Meta(ctx)
							if err != nil {
								return nil, err
							}
							ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: compositionSelectionRef("authority")}
							base = authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{
								{Ref: compositionSelectionRef("visible-project"), Name: "visible-name", InstallationID: "fixed", Active: true},
								{Ref: compositionSelectionRef("hidden-project"), Name: "hidden-name", InstallationID: "fixed", Active: true},
							}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: by.ID}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: compositionSelectionRef("visible-project")}, Role: "involved"}}}
							facts, err = authzrpc.NewMutableFacts(base)
							if err != nil {
								return nil, err
							}
							app, err := authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: provider, Facts: facts, Namespace: ns})
							if err != nil {
								return nil, err
							}
							if err := facts.Replace(base); err != nil {
								return nil, err
							}
							adapter := app.Service.ViewSelection()
							bind := adapter.Bind
							adapter.Bind = func(ctx context.Context, by model.PrincipalRef, inline string) (*viewops.SelectionAuthority, error) {
								binds++
								authority, err := bind(ctx, by, inline)
								if err != nil || authority == nil {
									return authority, err
								}
								fence := authority.Fence
								authority.Fence = func(ctx context.Context) error {
									fences++
									return fence(ctx)
								}
								return authority, nil
							}
							return adapter, nil
						}
					} else {
						by = model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
					}
					if mode == "extra_host" {
						config.AuthorizeRead = func(context.Context, model.PrincipalRef, model.ResourceRef, string) error {
							return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
						}
					}
					def := viewops.Definition{Filter: &viewops.Filter{Query: "project=visible-name"}}
					config.ViewReader = compositionSelectionView{definition: def}
					req := viewops.RunRequest{Definition: &def, CountOnly: true}
					if mode == "uncovered_history" {
						// Real legacy records have no typed selection projections.
						// Startup may upgrade current graph domains; it must not
						// invent selection coverage for this earlier content range.
						lease, err := raw.Lease(ctx, "legacy-selection", driver.LeaseTTL)
						if err != nil {
							t.Fatal(err)
						}
						for i, project := range []string{"visible-name", "hidden-name"} {
							id := fmt.Sprintf("legacy-%d", i)
							body, err := json.Marshal(map[string]any{"id": id, "type": "issue", "project": project, "fields": map[string]any{"title": id}})
							if err != nil {
								t.Fatal(err)
							}
							commit, err := conformance.Seed(ctx, raw, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: compositionSelectionRef(id), Kind: "bead", Op: model.OpCreate, Body: body}})
							if err != nil {
								t.Fatal(err)
							}
							if i == 0 {
								req.AsOf = &viewops.AsOf{Revision: commit.Revision}
							}
						}
						if err := lease.Release(ctx); err != nil {
							t.Fatal(err)
						}
					}
					runtime, err := Open(ctx, config)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := runtime.Close(context.Background()); err != nil {
							t.Error(err)
						}
					})
					seededRefs := []model.Ref{}
					if mode != "uncovered_history" {
						// Seed through the actual composed Writer, which owns the
						// complete source and projection publication.
						for i, project := range []string{"visible-name", "hidden-name"} {
							commit, err := runtime.Service.Create(ctx, model.PrincipalRef{Kind: "user", ID: "user:fixture"}, service.CreateRequest{Type: "issue", Fields: map[string]any{"title": project}}, model.WriteMeta{Project: project, Agent: "selection-fixture", IdempotencyKey: fmt.Sprintf("54011150-1986-4bcf-9a97-%012d", i+1)})
							if err != nil {
								t.Fatal(err)
							}
							if len(commit.Results) == 0 || commit.Results[0].Ref.ID == "" {
								t.Fatal("composed Writer did not return its created source ref")
							}
							seededRefs = append(seededRefs, commit.Results[0].Ref)
						}
					}
					want := 1
					switch mode {
					case "saved":
						req.Definition, req.ViewID = nil, "saved"
					case "unbounded_label":
						def.Filter.Query = "label=absent"
						want = 0
					case "extra_host":
						want = 0
					case "no_plugins":
						def.Filter.Query = ""
						want = 2
					case "fixed_refs":
						// Real Writer results define the exact fixed scope; include
						// the hidden-project ref so the view predicate still applies.
						by.Caveats = map[string]any{"refs": seededRefs}
					case "empty_refs":
						by.Caveats = map[string]any{"refs": []model.Ref{}}
						want = 0
					case "unsupported_caveat":
						by.Caveats = map[string]any{"provider": true}
						want = 0
					}
					observed.selects.Store(0)
					observed.queries.Store(0)
					observed.unavailable.Store(0)
					// This is the service built by production Open; no test calls
					// viewops.New or installs a second view service.
					result, err := runtime.Views.Run(ctx, by, req)
					if err != nil || result.Count == nil || *result.Count != want || len(result.Items) != 0 {
						t.Fatalf("composed count=%+v want=%d error=%v", result, want, err)
					}
					selects, queries, unavailable := observed.selects.Load(), observed.queries.Load(), observed.unavailable.Load()
					switch mode {
					case "eligible", "fixed_refs", "empty_refs":
						if selects != 1 || queries != 0 || unavailable != 0 || binds != 1 {
							t.Fatalf("eligible work selects=%d queries=%d unavailable=%d binds=%d", selects, queries, unavailable, binds)
						}
					case "uncovered_history":
						if selects != 1 || queries == 0 || unavailable != 1 || binds != 1 {
							t.Fatalf("uncovered work selects=%d queries=%d unavailable=%d binds=%d", selects, queries, unavailable, binds)
						}
					default:
						if selects != 0 || queries == 0 || unavailable != 0 {
							t.Fatalf("ordinary work selects=%d queries=%d unavailable=%d binds=%d", selects, queries, unavailable, binds)
						}
					}
					if mode != "no_plugins" && (binds != 1 || fences != 1) {
						t.Fatalf("authority lifetime binds=%d fences=%d", binds, fences)
					}
					if mode == "fixed_refs" {
						pageDefinition := viewops.Definition{Filter: &viewops.Filter{Query: "project=visible-name"}, Fields: []string{"id", "fields.title"}, Sort: []string{"id"}}
						page, err := runtime.Views.Run(ctx, by, viewops.RunRequest{Definition: &pageDefinition})
						if err != nil || page.Count != nil || len(page.Items) != 1 || len(page.Groups) != 0 || page.NextCursor != nil || page.Items[0].ID != seededRefs[0].ID || page.Items[0].Fields["title"] != "visible-name" || observed.selects.Load() != 2 || observed.queries.Load() != 0 || fences != 2 || binds != 2 {
							t.Fatalf("composed fixed refs changed page, work or authority: %+v error=%v selects=%d queries=%d binds=%d fences=%d", page, err, observed.selects.Load(), observed.queries.Load(), binds, fences)
						}
						changedFacts := base
						changedFacts.Grants = nil
						observed.afterSelect = func() {
							if err := facts.Replace(changedFacts); err != nil {
								t.Fatal(err)
							}
						}
						changed, err := runtime.Views.Run(ctx, by, req)
						if err == nil || changed.Count != nil || len(changed.Items) != 0 || len(changed.Groups) != 0 || changed.NextCursor != nil || observed.afterSelect != nil || observed.selects.Load() != 3 || observed.queries.Load() != 0 || binds != 3 || fences != 3 {
							t.Fatalf("compiled fixed refs escaped changed-source final fence: %+v error=%v selects=%d queries=%d binds=%d fences=%d hook_pending=%t", changed, err, observed.selects.Load(), observed.queries.Load(), binds, fences, observed.afterSelect != nil)
						}
					}
					if mode == "unsupported_reader_revoke" || mode == "unsupported_coverage_revoke" {
						// Both authority domains are published for real. Revoking
						// the grant must invalidate the bound source even when the
						// fixed user-read predicate itself is unchanged.
						budget := model.Natural64(1)
						pageDefinition := viewops.Definition{Filter: &viewops.Filter{}, Fields: []string{"id"}, Sort: []string{"id"}, BudgetTokens: &budget}
						pageRequest := viewops.RunRequest{Definition: &pageDefinition}
						first, err := runtime.Views.Run(ctx, by, pageRequest)
						if err != nil || len(first.Items) != 1 || first.NextCursor == nil || first.NextCursor.AuthorityBinding == "" {
							t.Fatalf("ordinary first page lost authority cursor: %+v error=%v", first, err)
						}
						revoked := base
						revoked.Grants = nil
						if err := facts.Replace(revoked); err != nil {
							t.Fatal(err)
						}
						pageRequest.Cursor = first.NextCursor
						next, err := runtime.Views.Run(ctx, by, pageRequest)
						var typed *model.Error
						if !errors.As(err, &typed) || typed.Data.Code != "cursor_invalid" || len(next.Items) != 0 || len(next.Groups) != 0 || next.Count != nil || next.NextCursor != nil {
							t.Fatalf("grant-changed continuation escaped: %+v error=%v", next, err)
						}
						if err := facts.Replace(base); err != nil {
							t.Fatal(err)
						}
						beforeFence := fences
						observed.afterQuery = func() {
							if err := facts.Replace(revoked); err != nil {
								t.Fatal(err)
							}
						}
						changed, err := runtime.Views.Run(ctx, by, req)
						if err == nil || len(changed.Items) != 0 || len(changed.Groups) != 0 || changed.Count != nil || changed.NextCursor != nil || observed.afterQuery != nil || fences != beforeFence+1 {
							t.Fatalf("mid-read grant change escaped final fence: %+v error=%v fences=%d before=%d hook_pending=%t", changed, err, fences, beforeFence, observed.afterQuery != nil)
						}
						if observed.selects.Load() != 0 || observed.queries.Load() == 0 {
							t.Fatal("capability-fallback authority regression did not use the ordinary path")
						}
					}
					t.Logf("COMPOSED_VIEW_SELECTION engine=%s mode=%s count=%d selects=%d queries=%d coverage_refusals=%d binds=%d qualification=COMPOSITION_ROUTING_ONLY", kind, mode, want, selects, queries, unavailable, binds)
				})
			}
		})
	}
}

// These adapters deliberately supply only view authority. Event and protected
// ontology capabilities must still be explicitly supplied by their own host.
func compositionSelectionPolicyOnly(config *Config, mode string) {
	adapter := &viewops.SelectionConfig{
		Bind: func(context.Context, model.PrincipalRef, string) (*viewops.SelectionAuthority, error) {
			return nil, &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		},
		AuthorizeRead: func(context.Context, model.PrincipalRef, model.ResourceRef, string) error {
			return &model.Error{Data: model.ErrorData{Code: "forbidden"}}
		},
	}
	switch mode {
	case "direct":
		config.Selection = adapter
	case "factory":
		config.SelectionFactory = func(context.Context, driver.Store, driver.ActiveLease) (*viewops.SelectionConfig, error) {
			return adapter, nil
		}
	}
}

func TestComposedSelectionOnlyPolicyPreservesOtherBoundaries(t *testing.T) {
	for _, mode := range []string{"direct", "factory", "no_policy"} {
		t.Run(mode, func(t *testing.T) {
			t.Run("events", func(t *testing.T) {
				ctx := context.Background()
				config := Config{Driver: "memory", Project: "selection-policy", Address: DefaultAddress}
				compositionSelectionPolicyOnly(&config, mode)
				if config.Auth.AuthorizationInstalled != nil || config.AuthorizeRead != nil || config.EventAuthorization != nil {
					t.Fatal("control must have only the new optional view policy")
				}
				runtime, err := Open(ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := runtime.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				request := []byte(`{"jsonrpc":"2.0","id":1,"method":"event.list","params":{"filter":{},"limit":1,"_meta":{"project":"selection-policy"}}}`)
				response := runtime.RPC.Dispatch(ctx, request)
				var envelope struct {
					Result json.RawMessage `json:"result"`
					Error  *struct {
						Data model.ErrorData `json:"data"`
					} `json:"error"`
				}
				if err := json.Unmarshal(response, &envelope); err != nil {
					t.Fatal(err)
				}
				if mode == "no_policy" {
					if envelope.Error != nil || len(envelope.Result) == 0 || !runtime.Service.EventListConfigured() {
						t.Fatalf("explicit no-policy event path changed: %s", response)
					}
				} else if envelope.Error == nil || envelope.Error.Data.Code != "method_not_found" || runtime.Service.EventListConfigured() {
					t.Fatalf("view-only policy exposed event.list without event authorization: %s", response)
				}
			})
			t.Run("ontology", func(t *testing.T) {
				ctx := context.Background()
				config := Config{Driver: "memory", Project: "selection-policy", Address: DefaultAddress}
				store, err := openStore(ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				lease, err := store.Lease(ctx, "selection-policy-seed", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				body, err := service.MarshalOntologySource(service.OntologySource{Base: ontology.DefaultDocument()})
				if err != nil {
					t.Fatal(err)
				}
				ref := compositionSelectionRef("ontology/selection-policy")
				_, seedErr := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "ontology", Op: model.OpCreate, Body: body}})
				releaseErr := lease.Release(ctx)
				if seedErr != nil || releaseErr != nil {
					t.Fatalf("valid protected source seed=%v release=%v", seedErr, releaseErr)
				}
				config.StoreFactory = func(context.Context, string, driver.Config) (driver.Store, error) { return store, nil }
				config.Ontology = &OntologyConfig{Source: service.StaticOntologySource(ref)}
				compositionSelectionPolicyOnly(&config, mode)
				if config.Auth.AuthorizationInstalled != nil || config.AuthorizeRead != nil || config.Ontology.AuthorizeRead != nil || config.Ontology.Authorizer != nil {
					t.Fatal("control must have only the new optional view policy")
				}
				runtime, err := Open(ctx, config)
				if mode != "no_policy" {
					if runtime != nil {
						_ = runtime.Close(context.Background())
					}
					if err == nil || !strings.Contains(err.Error(), "requires a current read authorizer") {
						t.Fatalf("view-only policy admitted ontology without its read/apply authority: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("explicit no-policy ontology path changed: %v", err)
				}
				if err := runtime.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}
