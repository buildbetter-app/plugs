#!/usr/bin/env bash
# Exercise target selection in a disposable repository with a deterministic
# Bazel-query stub. This keeps the CI contract test fast while covering the
# package, resource, reverse-dependent, repository-graph, and CI-only branches.
set -euo pipefail

source_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
resolver="$source_dir/plug-bazel-targets.sh"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/plug-bazel-targets-test.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/repo/pkg/static" "$fixture/repo/pkg/cmd/nested" "$fixture/repo/docs" "$fixture/repo/scripts/ci"
git -C "$fixture/repo" init -q
git -C "$fixture/repo" config user.name "Plug CI test"
git -C "$fixture/repo" config user.email "plug-ci-test@example.invalid"
cat >"$fixture/repo/pkg/BUILD.bazel" <<'EOF'
go_library(
    name = "pkg",
    srcs = ["foo.go"],
    embedsrcs = ["catalog.json", "static/index.html"],
)
go_binary(
    name = "nested",
    srcs = ["cmd/nested/main.go"],
)
EOF
printf '%s\n' 'package pkg' >"$fixture/repo/pkg/foo.go"
printf '%s\n' 'package main' 'func main() {}' >"$fixture/repo/pkg/cmd/nested/main.go"
printf '%s\n' '{"version":1}' >"$fixture/repo/pkg/catalog.json"
printf '%s\n' '<html>v1</html>' >"$fixture/repo/pkg/static/index.html"
printf '%s\n' 'unowned documentation' >"$fixture/repo/docs/guide.md"
printf '%s\n' '#!/usr/bin/env bash' >"$fixture/repo/scripts/ci/ci.sh"
git -C "$fixture/repo" add .
git -C "$fixture/repo" commit -q -m base
base="$(git -C "$fixture/repo" rev-parse HEAD)"

cat >"$fixture/fake-bazel" <<'EOF'
#!/usr/bin/python3
import json
import re
import sys

expression = sys.argv[sys.argv.index("query") + 1]
scope = expression
pattern = None
boundary = ') except (attr("tags", '
if expression.startswith("(") and boundary in expression:
    scope, tail = expression[1:].split(boundary, 1)
    pattern, end = json.JSONDecoder().raw_decode(tail)
    assert tail[end:] == ', ' + scope + ') except set(//tools/roundtrip:public_beads_runtime_test))', expression
count = "//spec/s1-b21-b24:ordinary_count_capture_test"
runtime = "//tools/roundtrip:public_beads_runtime_test"
tags = {count: ["local", "manual"], "//pkg:manual_capture_test": ["manual", "local"],
        runtime: ["local", "manual"], "//pkg:manual_policy_test": ["manual-policy"],
        "//pkg:premanual_test": ["premanual"]}
ordinary = ["//pkg:pkg_test", "//pkg:manual_policy_test", "//pkg:premanual_test"]
if scope == 'kind("source file", deps(//...))':
    result = ["//pkg:foo.go", "//pkg:cmd/nested/main.go", "//pkg:catalog.json", "//pkg:static/index.html"]
elif 'rdeps(//..., set(' in scope:
    result = ["//consumer:consumer_test", count, "//pkg:manual_capture_test", runtime]
elif scope == 'kind(".*_test rule", //...)':
    result = [*ordinary, "//consumer:consumer_test", count, "//pkg:manual_capture_test", runtime]
elif scope == 'kind(".*_test rule", //pkg:all)':
    result = [*ordinary, "//pkg:manual_capture_test"]
else:
    raise AssertionError(expression)
if pattern is not None:
    # Model attr over rendered lists and set subtraction independently of
    # target names: a substring manual tag must not lose ordinary coverage.
    matcher = re.compile(pattern)
    result = [label for label in result if label == runtime
              or not matcher.search("[" + ", ".join(tags.get(label, [])) + "]")]
print("\n".join(result))
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
printf '%s\n' '// nested source owned by the parent BUILD package' >>"$fixture/repo/pkg/cmd/nested/main.go"
git -C "$fixture/repo" add pkg/cmd/nested/main.go
git -C "$fixture/repo" commit -q -m nested-source-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/nested")
[[ "$(cat "$fixture/nested/packages.txt")" == '//pkg' ]]
[[ "$(cat "$fixture/nested/seeds.txt")" == '//pkg:cmd/nested/main.go' ]]
grep -Fxq '//pkg:pkg_test' "$fixture/nested/touched-targets.txt"
grep -Fxq '//consumer:consumer_test' "$fixture/nested/reverse-targets.txt"
grep -Fxq '0' "$fixture/nested/full-graph.txt"
[[ ! -s "$fixture/nested/unknown-files.txt" ]]

base="$head"
printf '%s\n' '// combined graph and nested source change' >>"$fixture/repo/pkg/cmd/nested/main.go"
printf '%s\n' '# graph and source changed together' >>"$fixture/repo/pkg/BUILD.bazel"
git -C "$fixture/repo" add pkg/cmd/nested/main.go pkg/BUILD.bazel
git -C "$fixture/repo" commit -q -m nested-source-graph-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/nested-graph")
# Delivery reuses packages.txt for owning-library queries even in full-graph mode.
[[ "$(cat "$fixture/nested-graph/packages.txt")" == '//pkg' ]]
[[ "$(cat "$fixture/nested-graph/seeds.txt")" == '//pkg:cmd/nested/main.go' ]]
grep -Fxq '//pkg:pkg_test' "$fixture/nested-graph/touched-targets.txt"
grep -Fxq '1' "$fixture/nested-graph/full-graph.txt"
[[ ! -s "$fixture/nested-graph/reverse-targets.txt" ]]

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
printf '%s\n' 'package pkg' >"$fixture/repo/pkg/unowned.go"
git -C "$fixture/repo" add pkg/unowned.go
git -C "$fixture/repo" commit -q -m unknown-go-source
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/unknown-go")
grep -Fxq 'pkg/unowned.go' "$fixture/unknown-go/unknown-files.txt"
grep -Fxq '//pkg:pkg_test' "$fixture/unknown-go/touched-targets.txt"
grep -Fxq '1' "$fixture/unknown-go/full-graph.txt"
[[ ! -s "$fixture/unknown-go/packages.txt" ]]
[[ ! -s "$fixture/unknown-go/seeds.txt" ]]

base="$head"
printf '%s\n' '# graph change' >>"$fixture/repo/pkg/BUILD.bazel"
git -C "$fixture/repo" add pkg/BUILD.bazel
git -C "$fixture/repo" commit -q -m graph-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/graph")
grep -Fxq '//pkg:pkg_test' "$fixture/graph/touched-targets.txt"
grep -Fxq '1' "$fixture/graph/full-graph.txt"
[[ ! -s "$fixture/graph/reverse-targets.txt" ]]

# Both complete-graph and source/reverse paths preserve every ordinary test,
# including tags containing the word manual, plus the prepared runtime.
python3 - "$fixture" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
ordinary = {"//pkg:pkg_test", "//pkg:manual_policy_test", "//pkg:premanual_test"}
runtime = "//tools/roundtrip:public_beads_runtime_test"
for name in ("package", "nested", "resource"):
    assert set((root / name / "touched-targets.txt").read_text().splitlines()) == ordinary, name
    assert set((root / name / "reverse-targets.txt").read_text().splitlines()) == {
        "//consumer:consumer_test", runtime}, name
for name in ("nested-graph", "unknown", "unknown-go", "graph"):
    assert set((root / name / "touched-targets.txt").read_text().splitlines()) == {
        *ordinary, "//consumer:consumer_test", runtime}, name
PY

# An ambient reference marker grants no automatic manual admission.
(cd "$fixture/repo" && PLUG_PUBLIC_REFERENCE=caller-claim BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/claimed")
cmp "$fixture/graph/touched-targets.txt" "$fixture/claimed/touched-targets.txt"

base="$head"
printf '%s\n' '# CI-only change' >>"$fixture/repo/scripts/ci/ci.sh"
git -C "$fixture/repo" add scripts/ci/ci.sh
git -C "$fixture/repo" commit -q -m ci-change
head="$(git -C "$fixture/repo" rev-parse HEAD)"
(cd "$fixture/repo" && BAZEL_BIN="$fixture/fake-bazel" "$resolver" --base "$base" --head "$head" --output "$fixture/ci")
grep -Fxq '//:lexicon_gate' "$fixture/ci/touched-targets.txt"
grep -Fxq '0' "$fixture/ci/full-graph.txt"
grep -Fq 'fallback=//:lexicon_gate' "$fixture/ci/selection.txt"

echo "plug-ci-targets: ordinary coverage, manual opt-in, prepared runtime, package/resource/reverse/graph and CI-only cases passed"
