package authz

import (
	"strings"
	"testing"

	cedar "github.com/cedar-policy/cedar-go"
)

func TestCP7ParsesAndCompilesSubset(t *testing.T) {
	set, err := ParseCP7()
	if err != nil {
		t.Fatalf("ParseCP7() error = %v", err)
	}
	if got, want := len(set.Policies), 3; got != want {
		t.Fatalf("parsed %d policies, want %d", got, want)
	}
	for _, spec := range CP7Policies {
		if spec.Relationship == "" || spec.Cedar == "" {
			t.Errorf("%s has an empty policy representation", spec.Name)
		}
		if _, ok := set.Templates[spec.Name]; !ok {
			t.Errorf("%s did not produce a subset template", spec.Name)
		}
	}
	if _, err := cedar.NewPolicySetFromBytes("cp7.cedar", []byte(CP7CedarPolicies)); err != nil {
		t.Fatalf("combined CP7 Cedar source does not parse: %v", err)
	}
}

func TestCP7CedarDecisions(t *testing.T) {
	set, err := ParseCP7()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		request   EvaluationRequest
		wantAllow bool
	}{
		{
			name: "admin can import bulk history",
			request: EvaluationRequest{
				Principal: Principal{ID: "alice", Kind: "User", Admin: true},
				Action:    "importBulkHistory",
				Resource:  Resource{ID: "bulk", Kind: "History"},
			},
			wantAllow: true,
		},
		{
			name: "non-admin cannot import bulk history",
			request: EvaluationRequest{
				Principal: Principal{ID: "bob", Kind: "User"},
				Action:    "importBulkHistory",
				Resource:  Resource{ID: "bulk", Kind: "History"},
			},
			wantAllow: false,
		},
		{
			name: "users read every bead",
			request: EvaluationRequest{
				Principal: Principal{ID: "bob", Kind: "User"},
				Action:    "read",
				Resource:  Resource{ID: "b-1", Kind: "Bead"},
			},
			wantAllow: true,
		},
		{
			name: "sponsored agent modifies involved project",
			request: EvaluationRequest{
				Principal: Principal{ID: "agent-1", Kind: "Agent", SponsorID: "alice", InvolvedProjects: []string{"p-1"}},
				Action:    "modify",
				Resource:  Resource{ID: "p-1", Kind: "Project"},
			},
			wantAllow: true,
		},
		{
			name: "sponsored agent cannot modify uninvolved project",
			request: EvaluationRequest{
				Principal: Principal{ID: "agent-1", Kind: "Agent", SponsorID: "alice", InvolvedProjects: []string{"p-1"}},
				Action:    "modify",
				Resource:  Resource{ID: "p-2", Kind: "Project"},
			},
			wantAllow: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := set.AuthorizeRequest(tc.request)
			if err != nil {
				t.Fatalf("AuthorizeRequest() error = %v", err)
			}
			if got != tc.wantAllow {
				t.Fatalf("AuthorizeRequest() = %v, want %v", got, tc.wantAllow)
			}
		})
	}
}

func TestCompilePolicySubsetRejectsUnsupportedCondition(t *testing.T) {
	var policy cedar.Policy
	if err := policy.UnmarshalCedar([]byte(`permit (principal, action, resource) when { context.ip == "127.0.0.1" };`)); err != nil {
		t.Fatal(err)
	}
	if _, err := CompilePolicySubset("unsupported", &policy); err == nil {
		t.Fatal("CompilePolicySubset() accepted a non-contract condition")
	}
}

func TestCP7ReadFilterBindsPrincipalAtRequestTime(t *testing.T) {
	set, err := ParseCP7()
	if err != nil {
		t.Fatal(err)
	}
	resources := []Resource{{ID: "b-1", Kind: "Bead"}, {ID: "b-2", Kind: "Bead"}}
	got, err := set.Filter(Principal{ID: "alice", Kind: "User"}, "read", resources)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(resources) {
		t.Fatalf("read filter returned %d rows, want %d", len(got), len(resources))
	}
	got, err = set.Filter(Principal{ID: "agent-1", Kind: "Agent", SponsorID: "alice"}, "read", resources)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("agent read filter returned %d rows, want deny-all", len(got))
	}
}

func TestDifferentialCP7(t *testing.T) {
	set, err := ParseCP7()
	if err != nil {
		t.Fatal(err)
	}
	if err := DifferentialCheck(set); err != nil {
		t.Fatal(err)
	}
}

func TestDifferentialCP7LoadsCedarRelationshipsIndependently(t *testing.T) {
	compiled, err := loadCompiledDifferentialCorpus()
	if err != nil {
		t.Fatal(err)
	}
	cedarCorpus, err := loadCedarDifferentialCorpus()
	if err != nil {
		t.Fatal(err)
	}
	if got := compiled.Principals[2].InvolvedProjects; len(got) != 2 || got[0] != "p-1" || got[1] != "p-3" {
		t.Fatalf("compiled grant projection = %v, want [p-1 p-3]", got)
	}
	if cedarCorpus.Principals[2].InvolvedProjects != nil {
		t.Fatalf("Cedar fixture unexpectedly reused compiled relationship projection: %v", cedarCorpus.Principals[2].InvolvedProjects)
	}
	if &compiled.Grants[0] == &cedarCorpus.Grants[0] {
		t.Fatal("compiled and Cedar grant rows share storage")
	}

	// Supplying a conflicting caller attribute must not affect the Cedar path:
	// its sponsor membership comes only from the independently loaded rows.
	set, err := ParseCP7()
	if err != nil {
		t.Fatal(err)
	}
	request := EvaluationRequest{
		Principal: Principal{ID: "agent-1", Kind: "Agent", SponsorID: "alice", InvolvedProjects: []string{"p-2"}},
		Action:    "modify",
		Resource:  Resource{ID: "p-1", Kind: "Project"},
	}
	decision, diag, err := set.authorizeWithGrants(request, cedarCorpus.Grants)
	if err != nil {
		t.Fatal(err)
	}
	if len(diag.Errors) != 0 {
		t.Fatalf("Cedar diagnostic = %v", diag.Errors)
	}
	if decision != cedar.Allow {
		t.Fatalf("Cedar decision = %v, want allow from grant row", decision)
	}
}

func TestCP7ReadabilityInputs(t *testing.T) {
	if strings.Count(CP7CedarPolicies, "permit") != 3 {
		t.Fatalf("CP7 Cedar source has unexpected policy count")
	}
	if strings.Count(CP7RelationshipSchema, "relation") != 3 {
		t.Fatalf("CP7 relationship schema has unexpected relation count")
	}
	for _, spec := range CP7Policies {
		if len(strings.TrimSpace(spec.Cedar)) == 0 || len(strings.TrimSpace(spec.Relationship)) == 0 {
			t.Errorf("%s lacks one readability input", spec.Name)
		}
	}
}
