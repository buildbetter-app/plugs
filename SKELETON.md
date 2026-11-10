# Initial tree inventory

This commit intentionally retains only the files needed to establish a clean
module and its review boundary.

| Path | Purpose |
| --- | --- |
| `.bazelversion` | Pins the Bazel tool used by the bootstrap lane. |
| `LICENSE`, `NOTICE.md` | Carries the MIT terms and required attribution. |
| `.gitignore`, `go.mod`, `MODULE.bazel`, `BUILD.bazel`, `doc.go` | Defines the new module, ignores generated outputs, and provides its empty buildable root. |
| `OWNERS.toml` | Provides neutral package ownership until contracts exist. |
| `tools/lexicon/rules.d/` | Declares vocabulary, area allowances and provenance zones. |
| `tools/lexicon/` | Implements the fail-closed tree check and focused tests. |
| `cmd/plug/`, `pluginapi/`, `sdk/go/`, `internal/` | Empty package seams for later contract work. |
| `engdocs/adr/0005-plug-design-decisions.md` | Retains the scrubbed ADR decision record used as stable provenance; source recordings and transcripts are excluded. |
| `engdocs/design/plug/` | Retains the reviewed planning records and visual references, with provenance links repaired; source recordings and transcripts are excluded. |
| `.github/` | Kept byte-for-byte for the CI owners; replacement is a separate task. |

All prior product source, build metadata, contributor guidance, and release
automation were removed from the working tree. No source package is carried
forward as an implementation dependency.
