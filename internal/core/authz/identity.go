package authz

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// Encode is lowercase hex of canonical decimal UTF-8 byte lengths and bytes.
func Encode(parts ...string) string {
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(strconv.Itoa(len(p)))
		b.WriteByte(':')
		b.WriteString(p)
	}
	return hex.EncodeToString([]byte(b.String()))
}

func ScopeComponent(n Namespace, s Scope) (string, error) {
	if err := n.Validate(); err != nil {
		return "", err
	}
	if err := s.Validate(); err != nil {
		return "", err
	}
	return "s1_" + Encode(n.StoreID, n.InstallationID, string(s.Kind), "id", s.Ref.ID, "0"), nil
}

func ACLDependency(n Namespace) Dependency {
	return Dependency{"read_fact", "az2/acl/" + Encode(n.StoreID, n.InstallationID)}
}
func ContextDependency(n Namespace) Dependency {
	return Dependency{"read_fact", "az2/context/" + Encode(n.StoreID)}
}
func GrantDependency(n Namespace, s Scope) (Dependency, error) {
	c, err := ScopeComponent(n, s)
	return Dependency{"sentinel", "grant/" + c}, err
}

func GrantIdentity(n Namespace, g Grant) (string, error) {
	if err := g.Subject.Validate(); err != nil {
		return "", err
	}
	if !ValidID(g.Role) {
		return "", fmt.Errorf("invalid grant role")
	}
	c, err := ScopeComponent(n, g.Scope)
	if err != nil {
		return "", err
	}
	return Encode(n.InstallationID, g.Subject.Kind, g.Subject.ID, g.Role, c), nil
}
