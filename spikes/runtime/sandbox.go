package runtime

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// EgressPolicy is the host-side allowlist used by the spike.  Resolution is
// injected so tests can prove private/link-local refusal without opening a
// network connection.  A production proxy must resolve and re-check every
// redirect itself before sending bytes.
type EgressPolicy struct {
	// Hosts are exact host:port declarations.  A declaration without a port is
	// normalized to port 443 and never grants a subdomain.
	Hosts   []string
	Resolve func(context.Context, string) ([]net.IP, error)
}

// ValidateURL checks scheme, exact host declaration, and all resolved target
// addresses.  It intentionally does not perform an HTTP request.
func (p EgressPolicy) ValidateURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("egress: invalid URL: %w", err)
	}
	if u.Scheme != "https" || u.User != nil || u.Hostname() == "" {
		return fmt.Errorf("egress: only HTTPS URLs without userinfo are allowed")
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	port := u.Port()
	if port == "" {
		port = "443"
	}
	if port != "443" || !p.allowed(host, port) {
		return fmt.Errorf("egress: host %s:%s is not declared", host, port)
	}
	resolver := p.Resolve
	if resolver == nil {
		resolver = func(ctx context.Context, host string) ([]net.IP, error) {
			return net.DefaultResolver.LookupIP(ctx, "ip", host)
		}
	}
	addresses, err := resolver(ctx, host)
	if err != nil {
		return fmt.Errorf("egress: resolve %s: %w", host, err)
	}
	if len(addresses) == 0 {
		return fmt.Errorf("egress: resolve %s returned no addresses", host)
	}
	for _, address := range addresses {
		if privateOrLocal(address) {
			return fmt.Errorf("egress: resolved target %s is private or local", address)
		}
	}
	return nil
}

// ValidateRedirects applies the same check to each hop.  The final URL is not
// allowed to escape the declared host set, and a redirect cannot carry a
// secret across a host boundary.  The proxy must call this before following a
// redirect; no automatic HTTP redirect behavior is supplied by the spike.
func (p EgressPolicy) ValidateRedirects(ctx context.Context, chain []string) error {
	if len(chain) == 0 {
		return fmt.Errorf("egress: redirect chain is empty")
	}
	for _, raw := range chain {
		if err := p.ValidateURL(ctx, raw); err != nil {
			return err
		}
	}
	return nil
}

func (p EgressPolicy) allowed(host, port string) bool {
	for _, declared := range p.Hosts {
		value := strings.ToLower(strings.TrimSpace(declared))
		if value == "" {
			continue
		}
		declaredHost, declaredPort, err := net.SplitHostPort(value)
		if err != nil {
			// A bare hostname is an exact HTTPS/443 declaration.
			declaredHost, declaredPort = strings.TrimSuffix(value, "."), "443"
		}
		if strings.TrimSuffix(strings.ToLower(declaredHost), ".") == host && declaredPort == port {
			return true
		}
	}
	return false
}

func privateOrLocal(address net.IP) bool {
	if address == nil {
		return true
	}
	if v4 := address.To4(); v4 != nil {
		address = v4
	}
	return address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast() ||
		address.IsLinkLocalMulticast() || address.IsUnspecified() || address.IsMulticast()
}

// ValidateSecretTarget applies the same exact-host rule to a secret grant.
// A secret's host declaration deliberately excludes a wildcard or a port
// range; the proxy injects it only after ValidateURL has accepted the target.
func (p EgressPolicy) ValidateSecretTarget(ctx context.Context, raw, secretHost string) error {
	if err := p.ValidateURL(ctx, raw); err != nil {
		return err
	}
	u, _ := url.Parse(raw)
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	secretHost = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(secretHost)), ".")
	if _, port, err := net.SplitHostPort(secretHost); err == nil {
		if port != "443" {
			return fmt.Errorf("egress: secret host must use port 443")
		}
		secretHost, _, _ = net.SplitHostPort(secretHost)
	}
	if host != secretHost {
		return fmt.Errorf("egress: secret is not valid for redirected host %s", host)
	}
	return nil
}

// AddressForTest is a small helper used by the report generator to make the
// resolved-address decision explicit without depending on DNS formatting.
func AddressForTest(host string, port int) string { return net.JoinHostPort(host, strconv.Itoa(port)) }
