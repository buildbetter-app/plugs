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
	policy := TreePolicy{
		ReferencePrefixes: []string{
			"tools/roundtrip/testdata/fresh-native/recipe-profile.json",
			"tools/roundtrip/testdata/fresh-native/continuation-fields.json",
			"tools/roundtrip/testdata/fresh-native/beads-1a75f7.git-tree",
			"tools/roundtrip/testdata/fresh-native/beads-ef3f15.git-tree",
			"tools/roundtrip/testdata/typed-reader/beads-e25cf4.git-tree",
			"tools/roundtrip/testdata/readback-8f19/memory-complete-reverse-live.jsonl",
			"tools/roundtrip/testdata/readback-8f19/memory-partial-reverse-live.jsonl",
			"internal/core/dumpformat/testdata/commit-schema-registry-c8ab0ce7.json",
			"internal/core/dumpformat/testdata/commit-schema-registry-ff8006bf.json",
			"spec/s1-b09-native/testdata/beads-9d6fff.git-tree",
			"spec/s1-b09-native/testdata/beads-446dbb.git-tree",
			"spec/recorded/goldens.json",
			"spec/s1-b09-b12/reference-v2/cb40-41563023339.packet.json",
			"spec/s1-b17-b20/reference/b18/record.py",
		},
		ReferenceSHA256: map[string]string{
			"tools/roundtrip/testdata/fresh-native/continuation-fields.json":            "ff5514c3f022d2c1ea09843e0eec2c380e69a085e9e9265a52b07fbc0113893b",
			"tools/roundtrip/testdata/fresh-native/beads-1a75f7.git-tree":               "566e102e8909114fb20043da8438264e90d664ac273ec12d4be2fc07dc484954",
			"tools/roundtrip/testdata/fresh-native/recipe-profile.json":                 "5dded2584223f7043fdf9967c7741092c305d438e06385de7a0d2e7534d93392",
			"tools/roundtrip/testdata/fresh-native/beads-ef3f15.git-tree":               "c1d92931dfce093264a1ba155869b2bffbb9dd184e9778e2c75abeb081ab1bdb",
			"tools/roundtrip/testdata/typed-reader/beads-e25cf4.git-tree":               "0a31c98fdd08dc93af919d3bb0f4c8470bb4a126d1c7388c49dff7a222889f55",
			"tools/roundtrip/testdata/readback-8f19/memory-complete-reverse-live.jsonl": "3ddad50712c0f10cf59a5ea60c573c75e2ee2a3957d5a01bfa9d22e5410d8182",
			"tools/roundtrip/testdata/readback-8f19/memory-partial-reverse-live.jsonl":  "2b455dd7d71f77443adee5ea422daecd0ca8ef17594b2af7f1e398860eedfbc1",
			"internal/core/dumpformat/testdata/commit-schema-registry-c8ab0ce7.json":    "900320651fedf60f29686f928c4febba7513f0cb802c8f85399526d058b3aa44",
			"internal/core/dumpformat/testdata/commit-schema-registry-ff8006bf.json":    "cf71387bc62853dd48aa9e7e96dfe67cf1126cdd47c920758d1d4234b80cd253",
			"spec/s1-b09-native/testdata/beads-446dbb.git-tree":                         "a2456b25849b281d86ac57a461e21e0cc244cd80602c20269d88e7b9e28bc246",
			"spec/s1-b09-native/testdata/beads-9d6fff.git-tree":                         "23d84a56302ad19c50d9d25af733b2da50f9137aaf9308b273cab039b6023ae3",
			"spec/recorded/goldens.json":                                                "26a086334357d55ea847daa301f45c9435a71bba75fb6c324456aebf459fbf32",
			"spec/s1-b09-b12/reference-v2/cb40-41563023339.packet.json":                 "33cd8fdcb66b5b93f4a36329fc0598cb9c5d012fc350804809da5eb5a38d4067",
			"spec/s1-b17-b20/reference/b18/record.py":                                   "375c516727d1dac47c395a7b117171e638f25a1e1f7b95a1c78f61d0552f973f",
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
			".github/runs-on.yml":                                  {},
			".github/scripts/BUILD.bazel":                          {},
			".github/scripts/beads-ci-contract.py":                 {},
			".github/scripts/beads_ci_contract_test.go":            {},
			".github/workflows/BUILD.bazel":                        {},
			".github/workflows/plug-beads-bazel.yml":               {},
			".github/workflows/plug-bazel.yml":                     {},
			".github/workflows/plug-cp7-latency.yml":               {},
			".github/workflows/plug-nightly-restore-diffs.yml":     {},
			".github/workflows/plug-public-bundle.yml":             {},
			".github/workflows/plug-runs-on-smoke.yml":             {},
			".github/workflows/plug-remote-bazel.yml":              {},
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
			"tools/roundtrip/testdata/fresh-native/beads-1a75f7.tar.gz":                             "76ae171a1c7c1d2904a258807d94cbe15bba15bce24742dd5d5f5cbd01340925",
			"tools/roundtrip/testdata/fresh-native/independent-row-replay.tar.gz":                   "b826c9e069e000007cc1f3a7c5c137a2d5113cb51276b84ceec433636076ff02",
			"tools/roundtrip/testdata/fresh-native/beads-ef3f15.tar.gz":                             "034e39dba83ce13eaa4af01feb753bf4f2fe360551f49de40094573d7dd330cd",
			"tools/roundtrip/testdata/typed-reader/beads-e25cf4.tar.gz":                             "89d43f0bc3e8f0ed58393d8982bbd5f290739c9af1228683c5338c08efa36fb6",
			"tools/roundtrip/testdata/readback-8f19/memory-complete-beads-raw-readback.tar":         "dc58689a485ec1c37a85afef956c05d07f328e51062bd4112e14db20e669abae",
			"tools/roundtrip/testdata/readback-8f19/memory-partial-beads-raw-readback.tar":          "cb51c880fe8cbc69a562799a4eabf1993f18ce6eb4ecda9dea035fdd6299b91c",
			"internal/integration/converterroundtrip/testdata/public-history-boundary-e25cf419.tar": "46677d8d6021ee35611e81238fd037d33739e4e44eb5308430b90d5f0ba5f93a",
			"internal/integration/converterroundtrip/testdata/public-multiversion-072a1a8f.tar":     "72df289c1e94535815764be53bc9f01530e683d6c7480ee06fc743e7a69a230b",
			"internal/integration/converterroundtrip/testdata/public-own-version-d7f83c6f.tar":      "de2a032098a8b17aa3e06ac7fa445ab18bb13427a59606e4a4d66959650e62c5",
			"spec/s1-b09-native/testdata/beads-446dbb.tar.gz":                                       "32034b6d242f252144f6353f0d9d2f8b7d50739faf7973d62de93cafe25f3959",
			"spec/s1-b09-native/testdata/beads-9d6fff.tar.gz":                                       "8aa8d07655ec6903703a4c94a4b1aa12d4fb509ab647cd1958f73f8342d3fe6e",
			"internal/core/dumpformat/testdata/native-tables-synthetic.tar":                         "5afe9a2225a573000d1d5cd6245ee58ac07e4ff4b928f2b174df2ff99ef9a68e",
			"internal/core/dumpformat/testdata/producer-0f44-reprocody.tar":                         "fe136810685c5d7c438ea7ebc3b868caf130ace167be24f671816ef69f9889c8",
			"internal/core/dumpformat/testdata/producer-0f44-reprosignals.tar":                      "09fa02c4f96ede9b2c4207064c9cfeed0c732958696d7c3841636a88f888d309",
			"internal/core/dumpformat/testdata/producer-e615-reprocody-gap.tar":                     "9f82b8405d44acab48649f7815a69027238dccea0ae4c9ddb2a871b8a1a8c8ff",
			"internal/integration/converterroundtrip/testdata/public-complete-root-990eb800.tar":    "a2d933a7a54b53303d33a20d5b96eaac0a85c079c5b08cae553aa2a654d820b5",
			"internal/integration/converterroundtrip/testdata/public-partial-990eb800.tar":          "2dd0bc5cb41ee5ae410659fdff91c4ee6b865098b371a2e1be9b819187271e53",
			"internal/integration/heartbeads/testdata/heartbeads-source.tar":                        "81c30981b245f5516d94486ba8a5d7bc6f008f749b00ea5e1ae3dff548967690",
			"internal/integration/offlinehistory/testdata/dolt-source-partial.tar":                  "b56f1de4a36c22cca379647d5fe1b2962e4358d9976923e76007ea84b7712582",
			"internal/integration/offlinehistory/testdata/full.tar":                                 "a96f0134097751e981ec6c46b7d412fb0dd92938a04df0838564b71c1064a212",
			"internal/integration/offlinehistory/testdata/partial.tar":                              "ab25df11463f68dbfa1ee1ce781a252cfc1b802abac8bb2f1cbc7bed5932cf9c",
			"tools/census/testdata/census.json":                                                     "d005db62a2110979e7b3884a37fdb9c8b410a964184582776f0987457a85080f",
			".github/images/agent-using-beads.jpg":                                                  "92f98f20c17f2ebb0c75815da94cc8a6fce98bb6d10b2bbf3555b706414ee778",
			"internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar":            "c510f8e0b301086e1c9fdab6ed3dc60ea8da73597c4058a03d0abc478482ae3c",
			"internal/server/composition/testdata/server-representative-plug-dump-1.tar":            "1b70534a5ff3e1da6cd9826fbdfae6ffea3436853b5e26162c588ab58fda5f67",
			"engdocs/design/plug/spec/ux-screens/blocked-100.png":                                   "80c8143cc8b8949ba95a8bd9ce047e2ff1b54a61e739ed194c8c225f87bf9c9b",
			"engdocs/design/plug/spec/ux-screens/blocked-25.png":                                    "08ba6926aaf2949a8cbbf54911fd0dd23a13509817b135fe924e0c67ac421ed8",
			"engdocs/design/plug/spec/ux-screens/blocked-50.png":                                    "a94f3bde0a221afe96cbfae7c53d259e4e24c094b95d5b649e49017068ffcf48",
			"engdocs/design/plug/spec/ux-screens/catalog.png":                                       "7aea347fff607ee6561f7c992668777cc735b0579ea6a8dc18d0c35aada6cfb5",
			"engdocs/design/plug/spec/ux-screens/changes-100.png":                                   "9f4403ef5bf29879f8d851c2c4b16e527d6a3eedb7a23919c7be26eeca426d0a",
			"engdocs/design/plug/spec/ux-screens/changes-25.png":                                    "e8cc4c249912fbe9ccf39807b3ce5e87bf3486831f2a54da636d009d62802242",
			"engdocs/design/plug/spec/ux-screens/changes-50.png":                                    "eb4650cecc922a448cf9d42e856f6938c4af619cc0455e92b7c62e76c2bb6ac7",
			"engdocs/design/plug/spec/ux-screens/epic-100.png":                                      "386584834339d9a4cf93a99d7347375576d8694ebb53b4781b074f4b8fe69ade",
			"engdocs/design/plug/spec/ux-screens/epic-25.png":                                       "4885dcfd10f168f7a8073524299fdeda27f344289887777e9cbf4af6ed0c64f6",
			"engdocs/design/plug/spec/ux-screens/epic-50.png":                                       "20835e013ffa87b51626955fad38435d7a7e82d43da8458e8c9a3192da67b630",
			"engdocs/design/plug/spec/ux-screens/home-100.png":                                      "05d313e72611f537e61d6e44f94a178a80b43cba9bfbba5fbba417991052ac9a",
			"engdocs/design/plug/spec/ux-screens/home-25.png":                                       "72ef28bd87a4927cb10c0fac152bc7942bce374541b04537238cf4ecc8da53bf",
			"engdocs/design/plug/spec/ux-screens/home-50.png":                                       "761e08767810c2a8fd5ee02e8a241a59a3690a9affcd46336284916c8f893c84",
			"engdocs/design/plug/spec/ux-screens/next-100.png":                                      "ec593cb97865ad53f71f52439db092752b073081a4aa75e05f48680fda3ba704",
			"engdocs/design/plug/spec/ux-screens/next-25.png":                                       "2d0bdcc176729186ffcf501bcc7aca4291b9fa10761fda4cabd370d9a5db3e2f",
			"engdocs/design/plug/spec/ux-screens/next-50.png":                                       "8b3f7faa870f147023e4d8bdf2a9ba7de99bfe3673f083b3b4091bd536b0a1fe",
			"engdocs/design/plug/spec/ux-screens/people-100.png":                                    "f5af9f447f6e60fe644e1496807e163958b9b9057dedfe29b179075a42285b4c",
			"engdocs/design/plug/spec/ux-screens/people-25.png":                                     "0473d7c5ff19c91a59c579bc4ce74d7d408eb84620300d56d2395e2a94d4cff3",
			"engdocs/design/plug/spec/ux-screens/people-50.png":                                     "f620d28fba6d16c9e104b351d61915f5ecd913f42e8ca3f8d14a8e8c066cc4cd",
		},
	}
	policy.BinarySHA256["internal/integration/converterroundtrip/testdata/public-native-delta-small-0e4d5da4.tar.gz"] = "66e2a1410cb6de0532e8bd453f06353d3fde8159ed9d6ffe4b0ed17f20bf6ef8"

	// The numbered native schema corpus is immutable fixture input. Every
	// file, including siblings without vocabulary matches, has an exact hash;
	// a new sibling or changed byte continues to fail closed.
	policy.ReferencePrefixes = append(policy.ReferencePrefixes, "internal/testutil/nativeschema/migrations/")
	for name, digest := range undoSQLReferences() {
		policy.ReferenceSHA256[name] = digest
	}
	return policy
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
