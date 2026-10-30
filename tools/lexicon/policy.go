package lexicon

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
)

// TreePolicy defines which retained paths are scanned and which provenance
// paths are protected until their owning migration tasks replace them.
// Protected paths are explicit so a new protected file fails closed. Binary
// files are accepted only when their exact SHA-256 is listed below.
type TreePolicy struct {
	// ReferencePrefixes contain immutable, retained reference inputs. Every
	// path below one of these prefixes must have an exact content hash in
	// ReferenceSHA256 before its bytes may bypass the vocabulary scan.
	ReferencePrefixes []string
	ReferenceSHA256   map[string]string
	ProtectedPrefixes []string
	// StrictProtectedPrefixes require every path to appear in ProtectedPaths or
	// BinarySHA256. This keeps retained CI and design trees explicit.
	StrictProtectedPrefixes []string
	ProtectedPaths          map[string]struct{}
	BinarySHA256            map[string]string
}

type pathDisposition uint8

const (
	dispositionScan pathDisposition = iota
	dispositionReference
	dispositionProtected
	dispositionBinary
)

// DefaultTreePolicy is the B1 policy for the inherited CI and design assets.
// The protected lists are intentionally exact: adding a retained file requires
// its owner to update this policy before the blocking gate can pass.
func DefaultTreePolicy() TreePolicy {
	return TreePolicy{
		ReferencePrefixes: []string{"spec/recorded/goldens.json"},
		ReferenceSHA256: map[string]string{
			"spec/recorded/goldens.json": "26a086334357d55ea847daa301f45c9435a71bba75fb6c324456aebf459fbf32",
		},
		ProtectedPrefixes:       []string{".github/", "engdocs/design/plug/"},
		StrictProtectedPrefixes: []string{".github/", "engdocs/design/plug/"},
		ProtectedPaths: map[string]struct{}{
			".github/CODEOWNERS":                                   {},
			".github/ISSUE_TEMPLATE/bug_report.yml":                {},
			".github/ISSUE_TEMPLATE/config.yml":                    {},
			".github/ISSUE_TEMPLATE/feature_request.yml":           {},
			".github/PULL_REQUEST_TEMPLATE.md":                     {},
			".github/allowed-actions.json":                         {},
			".github/copilot-instructions.md":                      {},
			".github/dependabot.yml":                               {},
			".github/images/agent-using-beads.jpg":                 {},
			".github/pages-redirect/index.html":                    {},
			".github/workflows/plug-bazel.yml":                     {},
			"engdocs/design/plug/adversarial-review-2026-10-28.md": {},
			"engdocs/design/plug/authorization-options.md":         {},
			"engdocs/design/plug/clean-room-plan.md":               {},
			"engdocs/design/plug/contracts/errors.md":              {},
			"engdocs/design/plug/contracts/events.md":              {},
			"engdocs/design/plug/contracts/go-sketches.md":         {},
			"engdocs/design/plug/contracts/openrpc.json":           {},
			"engdocs/design/plug/current-and-desired-state.md":     {},
			"engdocs/design/plug/decisions-needed.md":              {},
			"engdocs/design/plug/design.md":                        {},
			"engdocs/design/plug/review-2026-10-27.md":             {},
			"engdocs/design/plug/spec/behaviors.md":                {},
			"engdocs/design/plug/spec/behaviors/content.md":        {},
			"engdocs/design/plug/spec/behaviors/history.md":        {},
			"engdocs/design/plug/spec/behaviors/surfaces.md":       {},
			"engdocs/design/plug/spec/behaviors/work.md":           {},
			"engdocs/design/plug/spec/census.md":                   {},
			"engdocs/design/plug/spec/commands.md":                 {},
			"engdocs/design/plug/spec/ux-screens/blocked-100.png":  {},
			"engdocs/design/plug/spec/ux-screens/blocked-25.png":   {},
			"engdocs/design/plug/spec/ux-screens/blocked-50.png":   {},
			"engdocs/design/plug/spec/ux-screens/catalog.png":      {},
			"engdocs/design/plug/spec/ux-screens/changes-100.png":  {},
			"engdocs/design/plug/spec/ux-screens/changes-25.png":   {},
			"engdocs/design/plug/spec/ux-screens/changes-50.png":   {},
			"engdocs/design/plug/spec/ux-screens/epic-100.png":     {},
			"engdocs/design/plug/spec/ux-screens/epic-25.png":      {},
			"engdocs/design/plug/spec/ux-screens/epic-50.png":      {},
			"engdocs/design/plug/spec/ux-screens/home-100.png":     {},
			"engdocs/design/plug/spec/ux-screens/home-25.png":      {},
			"engdocs/design/plug/spec/ux-screens/home-50.png":      {},
			"engdocs/design/plug/spec/ux-screens/next-100.png":     {},
			"engdocs/design/plug/spec/ux-screens/next-25.png":      {},
			"engdocs/design/plug/spec/ux-screens/next-50.png":      {},
			"engdocs/design/plug/spec/ux-screens/people-100.png":   {},
			"engdocs/design/plug/spec/ux-screens/people-25.png":    {},
			"engdocs/design/plug/spec/ux-screens/people-50.png":    {},
			"engdocs/design/plug/spike-authz-results.md":           {},
			"engdocs/design/plug/spike-ux-decision.md":             {},
		},
		BinarySHA256: map[string]string{
			"tools/census/testdata/census.json":                                          "d005db62a2110979e7b3884a37fdb9c8b410a964184582776f0987457a85080f",
			".github/images/agent-using-beads.jpg":                                       "92f98f20c17f2ebb0c75815da94cc8a6fce98bb6d10b2bbf3555b706414ee778",
			"internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar": "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c",
			"engdocs/design/plug/spec/ux-screens/blocked-100.png":                        "62b21ea8dca4b02230c4cc008e662fdd88fd6602735d8885b486c456580c31a7",
			"engdocs/design/plug/spec/ux-screens/blocked-25.png":                         "ef6e3ad0e6233c3bed3bd0abfeb2d7fffa51386ae6b20f22a1ea9a5bd58d86d4",
			"engdocs/design/plug/spec/ux-screens/blocked-50.png":                         "2502f4e818e593e3a95242eba6afb74db4722d3fb28e9b694c1a1b7f963c756d",
			"engdocs/design/plug/spec/ux-screens/catalog.png":                            "4de25d130f70a6630407b9fb46584fbdf6a2baee9ea273a9c91ef0cd92db03bd",
			"engdocs/design/plug/spec/ux-screens/changes-100.png":                        "741bb442e9d77d2f3816b5debe13e25f2fbab70c9b7793bd0b77c70ccdd7fe64",
			"engdocs/design/plug/spec/ux-screens/changes-25.png":                         "f7ca431729e53b775718cf65d5faa1652ef115db63e6d2aacc7a8252a63432da",
			"engdocs/design/plug/spec/ux-screens/changes-50.png":                         "16f4fc3947073e0732e8c31e61698f820594cde83df5db99be696109d058ae41",
			"engdocs/design/plug/spec/ux-screens/epic-100.png":                           "fefccaf63191d49d1d8bd8db6c1c49983edc79225dbf6f48cad45cce3d896934",
			"engdocs/design/plug/spec/ux-screens/epic-25.png":                            "ecc55e6f1429269b23119e7e02607e786d4f93c4b6e477dd8b57131b565822de",
			"engdocs/design/plug/spec/ux-screens/epic-50.png":                            "beed825360ed9950c2bd7b89e391e2f98ba0f7c2333b9d5fcc45581a6d9e4923",
			"engdocs/design/plug/spec/ux-screens/home-100.png":                           "b906068644e3e07884fbb1a1f861cbc9b2d19c925b316917d0c5bef6be9a71be",
			"engdocs/design/plug/spec/ux-screens/home-25.png":                            "9a5bb2580040ef834f209d691b3de5bc686d1b29ca9e7e1d244f52ee7a895095",
			"engdocs/design/plug/spec/ux-screens/home-50.png":                            "052b7d2fb5d3176ae47d39d90530732a0a24d78b7830655d6dcb0eede603d7f8",
			"engdocs/design/plug/spec/ux-screens/next-100.png":                           "d410b096427f7ae828994093e71b56bc3cb16b3321b2e84ebf8a1eeca6f503eb",
			"engdocs/design/plug/spec/ux-screens/next-25.png":                            "03561d1af3fe4df3da6f27f72fca74470e9d41ac2cbcf9e6039820e48e0dfb2b",
			"engdocs/design/plug/spec/ux-screens/next-50.png":                            "001ff60f6873ddf7c6e12c526937a3fa5e43efbc66b0935d59ce8368e28ff1ff",
			"engdocs/design/plug/spec/ux-screens/people-100.png":                         "e4d184895fd5f75eec8ad795e77719bbd7f3a70e7c611ce7349a1ed8107bc959",
			"engdocs/design/plug/spec/ux-screens/people-25.png":                          "0ea11b85ea18550898688e3ed96f32d79596525be2fd68c35bc0aa069c007d89",
			"engdocs/design/plug/spec/ux-screens/people-50.png":                          "1bf5e5823ec5e997eb9ca6829fc5173b179987eec3e213b13916ed38ca9c4a6d",
		},
	}
}

func (p TreePolicy) disposition(name string, content []byte) (pathDisposition, error) {
	name = path.Clean(strings.ReplaceAll(name, "\\", "/"))
	if name == "." || strings.HasPrefix(name, "/") || name == ".." || strings.HasPrefix(name, "../") {
		return dispositionScan, fmt.Errorf("invalid repository path %q", name)
	}
	for _, prefix := range p.ReferencePrefixes {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		expected, ok := p.ReferenceSHA256[name]
		if !ok {
			return dispositionScan, fmt.Errorf("unlisted immutable reference path %q", name)
		}
		actual := sha256.Sum256(content)
		if hex.EncodeToString(actual[:]) != expected {
			return dispositionScan, fmt.Errorf("immutable reference %q does not match its declared SHA-256", name)
		}
		return dispositionReference, nil
	}
	for _, prefix := range p.ProtectedPrefixes {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		strict := false
		for _, strictPrefix := range p.StrictProtectedPrefixes {
			if prefix == strictPrefix {
				strict = true
				break
			}
		}
		if strict {
			_, pathListed := p.ProtectedPaths[name]
			_, binaryListed := p.BinarySHA256[name]
			if !pathListed && !binaryListed {
				return dispositionScan, fmt.Errorf("unlisted protected path %q", name)
			}
		}
		if expected, ok := p.BinarySHA256[name]; ok {
			actual := sha256.Sum256(content)
			if hex.EncodeToString(actual[:]) != expected {
				return dispositionScan, fmt.Errorf("protected binary %q does not match its declared SHA-256", name)
			}
			return dispositionBinary, nil
		}
		return dispositionProtected, nil
	}
	if expected, ok := p.BinarySHA256[name]; ok {
		actual := sha256.Sum256(content)
		if hex.EncodeToString(actual[:]) != expected {
			return dispositionScan, fmt.Errorf("binary %q does not match its declared SHA-256", name)
		}
		return dispositionBinary, nil
	}
	return dispositionScan, nil
}
