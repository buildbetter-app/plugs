package authzrpc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	fixture "github.com/buildbetter-app/plugs/internal/testfixture/readyalternative"
)

// These counters count executed finite-policy calls, not SQL executor rows.
// The independent SQL alternative consumes the same fixture in its real store.
type alternativePolicySlice struct {
	ns                                                    authz.Namespace
	project                                               authz.ProjectBinding
	team                                                  bool
	contexts, records, projects, grants, groups, activity int
}

func (s *alternativePolicySlice) Namespace() authz.Namespace { return s.ns }
func (s *alternativePolicySlice) Record(context.Context, authz.Dependency) error {
	s.records++
	return nil
}
func (s *alternativePolicySlice) CheckContext(context.Context, authz.Caller) (bool, error) {
	s.contexts++
	return true, nil
}
func (s *alternativePolicySlice) ProjectForBead(context.Context, model.Ref) (authz.ProjectBinding, error) {
	s.projects++
	return s.project, nil
}
func (s *alternativePolicySlice) ProjectActive(context.Context, model.Ref) (bool, error) {
	s.activity++
	return true, nil
}
func (s *alternativePolicySlice) TeamActive(context.Context, model.Ref) (bool, error) {
	s.activity++
	return true, nil
}
func (s *alternativePolicySlice) Groups(context.Context, authz.SubjectRef) ([]model.Ref, error) {
	s.groups++
	return []model.Ref{model.RefByID("cp7-team", model.Natural64(0))}, nil
}
func (s *alternativePolicySlice) HasGrant(_ context.Context, subject authz.SubjectRef, scope authz.Scope, role string) (bool, error) {
	s.grants++
	if role == "member" {
		return s.team && subject.ID == "user:team" && scope.Ref.ID == "cp7-team", nil
	}
	if role == "involved" && scope.Ref.ID == fixture.ProjectName {
		return (!s.team && subject.ID == "user:direct") || (s.team && subject.ID == "cp7-team"), nil
	}
	return false, nil
}

func TestReadyProjectFilterUnchangedCorpus(t *testing.T) {
	ctx := context.Background()
	digest, err := fixture.BodiesDigest()
	if err != nil {
		t.Fatal(err)
	}
	corpus := make([]service.Bead, fixture.CorpusSize)
	closureRows := 0
	for n := 1; n <= fixture.CorpusSize; n++ {
		d := fixture.Document(n)
		corpus[n-1] = service.Bead{ID: d.ID, Type: d.Type, Project: d.Project, Status: "todo", Revision: model.Natural64(7), Fields: d.Fields}
		closureRows += (n-1)%11 + 1
	}
	if closureRows != 599995 {
		t.Fatalf("unchanged depth/closure count=%d", closureRows)
	}
	t.Logf("READY_ALTERNATIVE_INPUT source=%s corpus=%d body_sha256=%s closure_rows=%d seed=%s raw_priority=0 policy_probe_only=true storage_executor=UNMEASURED", fixture.SourceCommit, fixture.CorpusSize, digest, closureRows, fixture.Seed)
	ns := authz.Namespace{StoreID: "ready-alternative", Incarnation: "ready-alternative", InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	rules, err := authzfixed.New(ns)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"ready", "claim_direct", "claim_team"} {
		for _, stride := range []int{1, 100, 1000} {
			t.Run(fmt.Sprintf("%s_stride_%d", mode, stride), func(t *testing.T) {
				principal := model.PrincipalRef{Kind: "user", ID: "user:reader"}
				action, method := authz.Read, "bead.ready"
				if mode != "ready" {
					sponsor := "user:direct"
					if mode == "claim_team" {
						sponsor = "user:team"
					}
					principal = model.PrincipalRef{Kind: "agent", ID: "agent:worker", Sponsor: &model.PrincipalRef{Kind: "user", ID: sponsor}}
					action, method = authz.Modify, "bead.claimNext"
				}
				if stride != 1 {
					principal.Caveats = map[string]any{"refs": fixture.Refs(stride)}
				}
				q := authz.ListRequest{Caller: authz.Caller{Principal: principal, VerificationID: "verified", CapturedEpoch: model.Natural64(1)}, Action: action, ResourceKind: authz.Bead, Method: method}
				template, err := (authz.Chain{Namespace: ns, Rules: []authz.RuleSet{rules}}).Compile(q)
				if err != nil {
					t.Fatal(err)
				}
				s := &alternativePolicySlice{ns: ns, project: authz.ProjectBinding{Name: fixture.ProjectName, Ref: model.RefByID(fixture.ProjectName, model.Natural64(0)), Found: true}, team: mode == "claim_team"}
				bound, err := template.BindProject(ctx, s, s.project)
				if err != nil {
					t.Fatal(err)
				}
				refs, restricted := bound.References()
				if (stride == 1) != bound.Uniform() || restricted != (stride != 1) {
					t.Fatal("uniform/ref classification mismatch")
				}
				selected := make([]service.Bead, 0, fixture.CorpusSize/stride)
				if bound.Uniform() {
					selected = append(selected, corpus...)
				} else {
					byID := make(map[string]service.Bead, len(corpus))
					for _, row := range corpus {
						byID[row.ID] = row
					}
					for _, ref := range refs {
						row, exists := byID[ref.ID]
						if !exists {
							t.Fatal("classified identity outside corpus")
						}
						selected = append(selected, row)
					}
				}
				// The scalar oracle visits all 100k unchanged raw candidates. It is
				// independent of the classifier's set enumeration and policy AST.
				want := make([]service.Bead, 0, fixture.CorpusSize/stride)
				for n, row := range corpus {
					allowed, err := bound.Allows(ctx, model.RefByID(row.ID, model.Natural64(0)), s.project)
					if err != nil || allowed != ((n+1)%stride == 0) {
						t.Fatalf("executed compiled membership at %d = %t/%v", n+1, allowed, err)
					}
					if (n+1)%stride == 0 {
						want = append(want, row)
					}
				}
				if !reflect.DeepEqual(selected, want) {
					t.Fatal("ordinary authorized membership/order mismatch")
				}
				if readyMembershipDigest(selected) != readyMembershipDigest(want) {
					t.Fatal("full authorized fingerprint differs")
				}
				// Cross-check the real interpreter at both admitted and denied
				// identities, including both ends; full AST/caveat cases live in authz.
				for _, n := range []int{1, stride, min(stride+1, fixture.CorpusSize), fixture.CorpusSize - 1, fixture.CorpusSize} {
					ok, err := template.Match(ctx, &alternativePolicySlice{ns: ns, project: s.project, team: s.team}, authz.Resource{Kind: authz.Bead, Ref: fixture.Ref(n)})
					if err != nil || ok != (n%stride == 0) {
						t.Fatalf("reference policy n%d=%t/%v", n, ok, err)
					}
				}
				if mode != "ready" {
					seed := sha256.Sum256([]byte(fixture.Seed + "\x000"))
					pivot := int(binary.BigEndian.Uint64(seed[:8]) % uint64(fixture.CorpusSize))
					rawRank := map[string]int{}
					for n, row := range corpus {
						rawRank[row.ID] = n
					}
					rotated := slices.Clone(selected)
					sort.Slice(rotated, func(i, j int) bool {
						return (rawRank[rotated[i].ID]-pivot+fixture.CorpusSize)%fixture.CorpusSize < (rawRank[rotated[j].ID]-pivot+fixture.CorpusSize)%fixture.CorpusSize
					})
					expected := make([]service.Bead, 0, len(want))
					for i := 0; i < fixture.CorpusSize; i++ {
						n := (pivot + i) % fixture.CorpusSize
						if (n+1)%stride == 0 {
							expected = append(expected, corpus[n])
						}
					}
					if !reflect.DeepEqual(rotated, expected) || readyMembershipDigest(rotated) != readyMembershipDigest(expected) {
						t.Fatal("seeded raw-count pivot/order/fingerprint differs")
					}
					// This explicitly excludes the attractive but wrong authorized-size pivot.
					if stride != 1 && rotated[0].ID == want[int(binary.BigEndian.Uint64(seed[:8])%uint64(len(want)))].ID {
						t.Fatal("seed witness does not distinguish authorized-cardinality rotation")
					}
				}
				baseline := readyMembershipDigest(selected)
				tail := slices.Clone(selected)
				tail[len(tail)-1].Revision = model.Natural64(8)
				if readyMembershipDigest(tail) == baseline {
					t.Fatal("authorized tail revision omitted from fingerprint")
				}
				tail = slices.Clone(selected)
				tail[len(tail)-1].Fields = map[string]any{"title": "changed authorized tail", "priority": 0, "status": "todo"}
				if readyMembershipDigest(tail) == baseline {
					t.Fatal("authorized tail fields omitted from fingerprint")
				}
				if stride != 1 {
					changed := slices.Clone(corpus)
					changed[0].Fields = map[string]any{"title": "hidden mutation"}
					selectedAgain := make([]service.Bead, 0, len(selected))
					for _, row := range changed {
						allowed, err := bound.Allows(ctx, model.RefByID(row.ID, model.Natural64(0)), s.project)
						if err != nil {
							t.Fatal(err)
						}
						if allowed {
							selectedAgain = append(selectedAgain, row)
						}
					}
					if readyMembershipDigest(selectedAgain) != baseline {
						t.Fatal("hidden row changed authorized fingerprint")
					}
				}
				projected := slices.Clone(selected[:min(1000, len(selected))])
				for i := range projected {
					projectBeadParent(&projected[i], ParentDisclosure{Exists: true})
				}
				for _, row := range projected {
					if row.Parent != "" || !row.ParentHidden {
						t.Fatal("hidden parent disclosure changed")
					}
				}
				if s.contexts != 1 || s.projects != 0 {
					t.Fatalf("classifier repeated context/project reads %+v", s)
				}
				t.Logf("READY_COMPILED_POLICY mode=%s stride=%d raw_candidates=%d authorized=%d enumerated_refs=%d context_checks=%d policy_project_reads=%d grant_reads=%d group_reads=%d activity_reads=%d fingerprint_full_sequence=true SQL_executor=UNMEASURED", mode, stride, fixture.CorpusSize, len(selected), len(refs), s.contexts, s.projects, s.grants, s.groups, s.activity)
			})
		}
	}
}
