# Beads fork provenance

This repository is a source fork of [Beads](https://github.com/gastownhall/beads), intended as the starting point for a plugin system. The original `steveyegge/beads` URL redirects to that repository.

- Imported upstream branch: `main`.
- Imported commit: `9d6fff760b7a314b476539198504d77ce0959656`.
- Import date: 2026-10-22.
- Destination remote: `https://github.com/buildbetter-app/plugs.git`.
- Upstream remote: `https://github.com/gastownhall/beads.git`.
- License and attribution: upstream MIT [LICENSE](LICENSE) and existing authors/history retained.
- Commit ancestry: all 11,399 reachable commits retained; this is not a shallow repository. Historical file blobs are fetched from the promisor `upstream` remote on demand.
- Go module remains `github.com/steveyegge/beads` to preserve internal imports and upstream mergeability.

Start with the [architecture guide](engdocs/architecture-guide/README.md), or generate its offline HTML edition at `engdocs/architecture-guide/site/index.html`. The guide is pinned to the commit above. The plugin runtime chapter is a proposal, not implemented behavior.

The BuildBetter repository is private and is now the publication target for this fork. Runtime code and upstream integration assets are unchanged by this documentation work. No `bd init` or `bd setup` is run in the contributor checkout.

## Updating upstream later

Fetch `upstream`, review changes against the pinned commit, and merge them on a working branch. Re-run the guide inventory/render/validation tools and update source claims before changing the pin. A moving Git branch is not a durable architecture reference.

This local import and new documentation do not by themselves publish to GitHub. Remote publication should use this repository's intended branch/review policy.
