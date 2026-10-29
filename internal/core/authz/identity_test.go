package authz

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
func namespace() Namespace {
	return Namespace{StoreID: "store", Incarnation: "inc", InstallationID: "fixed", Authority: ref("authority")}
}

func TestCanonicalRefsAndInjectiveIdentities(t *testing.T) {
	n := namespace()
	n.Authority = model.Ref{ID: "authority"}
	if !sameNamespace(n, namespace()) {
		t.Fatal("equivalent ID-arm/zero-branch namespace representations differ")
	}
	for _, r := range []model.Ref{{}, model.RefByID("", model.Natural64(0)), model.RefByKey("key"), model.RefByID("p", model.Natural64(1)), ref(" p"), ref("p\x00q"), ref(string([]byte{0xff}))} {
		if CanonicalRef(r) == nil {
			t.Errorf("accepted invalid reference %+v", r)
		}
	}
	seen := map[string]bool{}
	for _, id := range []string{"p", "a:b", "web/api", "é", "s1_abc"} {
		for _, kind := range []ScopeKind{AuthorityScope, TeamScope, ProjectScope} {
			s, err := ScopeComponent(namespace(), Scope{Kind: kind, Ref: ref(id)})
			if err != nil {
				t.Fatal(err)
			}
			if seen[s] || strings.ContainsAny(s, "/\x00") {
				t.Fatalf("invalid/aliased key %q", s)
			}
			seen[s] = true
		}
	}
	if Encode("ab", "c") == Encode("a", "bc") || Encode("é") != "323ac3a9" {
		t.Fatal("encoding must use injective UTF-8 byte lengths")
	}
}

func TestNoPluginAllowsWithoutFacts(t *testing.T) {
	c := Chain{}
	d, err := c.Check(context.Background(), nil, []CheckRequest{{}})
	if err != nil || len(d) != 1 || d[0].Effect != Allow {
		t.Fatalf("%+v %v", d, err)
	}
	f, err := c.Compile(ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := f.Match(context.Background(), nil, Resource{})
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	r := ListRequest{Caller: Caller{Principal: model.PrincipalRef{Kind: "agent", ID: "a", Caveats: map[string]any{"actions": []string{"read"}}}}}
	if _, err := c.Compile(r); err == nil {
		t.Fatal("scoped token must refuse without authz")
	}
}
