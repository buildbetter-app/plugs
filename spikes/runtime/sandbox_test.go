package runtime

import (
	"context"
	"net"
	"strings"
	"testing"
)

func TestEgressPolicyRequiresExactPublicHTTPSTarget(t *testing.T) {
	policy := EgressPolicy{
		Hosts: []string{"api.example.test:443"},
		Resolve: func(_ context.Context, host string) ([]net.IP, error) {
			if host != "api.example.test" {
				t.Fatalf("resolver host=%q", host)
			}
			return []net.IP{net.ParseIP("203.0.113.8")}, nil
		},
	}
	ctx := context.Background()
	if err := policy.ValidateURL(ctx, "https://api.example.test/v1"); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		"http://api.example.test/v1",
		"https://sub.api.example.test/v1",
		"https://api.example.test:8443/v1",
		"https://user:password@api.example.test/v1",
	} {
		if err := policy.ValidateURL(ctx, raw); err == nil {
			t.Fatalf("unsafe URL accepted: %s", raw)
		}
	}
}

func TestEgressPolicyRejectsPrivateAndRedirectTargets(t *testing.T) {
	ctx := context.Background()
	for name, address := range map[string]string{
		"loopback":        "127.0.0.1",
		"rfc1918":         "10.0.0.4",
		"link-local":      "169.254.10.4",
		"multicast":       "224.0.0.4",
		"unspecified":     "0.0.0.0",
		"ipv6-ula":        "fd00::4",
		"ipv6-link-local": "fe80::4",
	} {
		t.Run(name, func(t *testing.T) {
			private := EgressPolicy{
				Hosts: []string{"api.example.test"},
				Resolve: func(context.Context, string) ([]net.IP, error) {
					return []net.IP{net.ParseIP(address)}, nil
				},
			}
			if err := private.ValidateURL(ctx, "https://api.example.test"); err == nil || !strings.Contains(err.Error(), "private or local") {
				t.Fatalf("private target result=%v", err)
			}
		})
	}
	public := EgressPolicy{
		Hosts: []string{"api.example.test", "cdn.example.test"},
		Resolve: func(_ context.Context, host string) ([]net.IP, error) {
			return []net.IP{net.ParseIP(map[string]string{"api.example.test": "203.0.113.8", "cdn.example.test": "203.0.113.9"}[host])}, nil
		},
	}
	if err := public.ValidateRedirects(ctx, []string{"https://api.example.test/one", "https://cdn.example.test/two"}); err != nil {
		t.Fatal(err)
	}
	if err := public.ValidateSecretTarget(ctx, "https://cdn.example.test/two", "api.example.test"); err == nil {
		t.Fatal("secret crossed an exact host redirect")
	}
}

func TestEgressPolicyRejectsResolverFailureAndMissingAddresses(t *testing.T) {
	for name, resolve := range map[string]func(context.Context, string) ([]net.IP, error){
		"failure": func(context.Context, string) ([]net.IP, error) { return nil, context.DeadlineExceeded },
		"empty":   func(context.Context, string) ([]net.IP, error) { return nil, nil },
	} {
		t.Run(name, func(t *testing.T) {
			policy := EgressPolicy{Hosts: []string{"api.example.test"}, Resolve: resolve}
			if err := policy.ValidateURL(context.Background(), "https://api.example.test"); err == nil {
				t.Fatal("resolver refusal accepted")
			}
		})
	}
}
