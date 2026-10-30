#!/usr/bin/env bash
# Exercise target selection in a disposable repository with a deterministic
# Bazel-query stub. This keeps the CI contract test fast while covering the
# package, resource, reverse-dependent, repository-graph, and CI-only branches.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
resolver="$repo_root/scripts/ci/plug-bazel-targets.sh"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/plug-bazel-targets-test.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/repo/pkg/static" "$fixture/repo/docs" "$fixture/repo/scripts/ci"
git -C "$fixture/repo" init -q
git -C "$fixture/repo" config user.name "Plug CI test"
git -C "$fixture/repo" config user.email "plug-ci-test@example.invalid"
cat >"$fixture/repo/pkg/BUILD.bazel" <<'EOF'
go_library(
    name = "pkg",
    srcs = ["foo.go"],
    embedsrcs = ["catalog.json", "static/index.html"],
)
EOF
printf '%s\n' 'package pkg' >"$fixture/repo/pkg/foo.go"
printf '%s\n' '{"version":1}' >"$fixture/repo/pkg/catalog.json"
printf '%s\n' '<html>v1</html>' >"$fixture/repo/pkg/static/index.html"
printf '%s\n' 'unowned documentation' >"$fixture/repo/docs/guide.md"
printf '%s\n' '#!/usr/bin/env bash' >"$fixture/repo/scripts/ci/ci.sh"
git -C "$fixture/repo" add .
git -C "$fixture/repo" commit -q -m base
base="$(git -C "$fixture/repo" rev-parse HEAD)"

cat >"$fixture/fake-bazel" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
query="$*"
case "$query" in
  *'rdeps(//..., set(//pkg:foo.go))'*|*'rdeps(//..., set(//pkg:catalog.json //pkg:static/index.html))'*)
    printf '%s\n' '//consumer:consumer_test'
    ;;
  *'kind("source file", deps(//...))'*)
    printf '%s\n' '//pkg:foo.go' '//pkg:catalog.json' '//pkg:static/index.html'
    ;;
  *'kind(".*_test rule", //...)'*)
    printf '%s\n' '//pkg:pkg_test'
    ;;
  *'kind(".*_test rule", //pkg:all)'*)
    printf '%s\n' '//pkg:pkg_test'
    ;;
  *)
    echo "unexpected Bazel query: $query" >&2
    exit 1
    ;;
esac
EOF
chmod 755 "$fixture/fake-bazel"

printf '%s\n' '// change the package implementation' >>"$fixture/repo/pkg/foo.go"
git -C "$fixture/repo" add pkg/foo.go
git -C "$fixture/repo" commit -q -m package-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/package")
grep -Fxq '//pkg:pkg_test' "$fixture/package/touched-targets.txt"
grep -Fxq '//consumer:consumer_test' "$fixture/package/reverse-targets.txt"
grep -Fxq '0' "$fixture/package/full-graph.txt"

base="$head"
printf '%s\n' '{"version":2}' >"$fixture/repo/pkg/catalog.json"
printf '%s\n' '<html>v2</html>' >"$fixture/repo/pkg/static/index.html"
git -C "$fixture/repo" add pkg/catalog.json pkg/static/index.html
git -C "$fixture/repo" commit -q -m resource-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/resource")
grep -Fxq '//pkg' "$fixture/resource/packages.txt"
grep -Fxq 'pkg/catalog.json=//pkg:catalog.json' "$fixture/resource/resource-labels.txt"
grep -Fxq 'pkg/static/index.html=//pkg:static/index.html' "$fixture/resource/resource-labels.txt"
grep -Fxq '//pkg:pkg_test' "$fixture/resource/touched-targets.txt"
grep -Fxq '//consumer:consumer_test' "$fixture/resource/reverse-targets.txt"
grep -Fxq '0' "$fixture/resource/full-graph.txt"
[[ ! -s "$fixture/resource/unknown-files.txt" ]]

base="$head"
printf '%s\n' 'changed documentation' >>"$fixture/repo/docs/guide.md"
git -C "$fixture/repo" add docs/guide.md
git -C "$fixture/repo" commit -q -m unknown-resource-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/unknown")
grep -Fxq 'docs/guide.md' "$fixture/unknown/unknown-files.txt"
grep -Fxq '//pkg:pkg_test' "$fixture/unknown/touched-targets.txt"
grep -Fxq '1' "$fixture/unknown/full-graph.txt"
[[ ! -s "$fixture/unknown/reverse-targets.txt" ]]

base="$head"
printf '%s\n' '# graph change' >>"$fixture/repo/pkg/BUILD.bazel"
git -C "$fixture/repo" add pkg/BUILD.bazel
git -C "$fixture/repo" commit -q -m graph-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/graph")
grep -Fxq '//pkg:pkg_test' "$fixture/graph/touched-targets.txt"
grep -Fxq '1' "$fixture/graph/full-graph.txt"
[[ ! -s "$fixture/graph/reverse-targets.txt" ]]

base="$head"
printf '%s\n' '# CI-only change' >>"$fixture/repo/scripts/ci/ci.sh"
git -C "$fixture/repo" add scripts/ci/ci.sh
git -C "$fixture/repo" commit -q -m ci-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/ci")
grep -Fxq '//:lexicon_gate' "$fixture/ci/touched-targets.txt"
grep -Fxq '0' "$fixture/ci/full-graph.txt"
grep -Fq 'fallback=//:lexicon_gate' "$fixture/ci/selection.txt"

echo "plug-ci-targets: package, owned resource, unknown resource, reverse, graph, and CI-only cases passed"
