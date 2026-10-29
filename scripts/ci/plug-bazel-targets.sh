#!/usr/bin/env bash
# Resolve the bounded Bazel test sets for a Plug change.
#
# The workflow runs the tests owned by changed packages and then every test
# that depends on those packages. Repository graph changes use the complete
# test graph because a package-level approximation would be unsound. Changes
# to the CI harness itself fall back to the lexicon gate: the workflow's
# contract test and the unconditional lexicon lane cover those files without
# turning a workflow edit into an unbounded product test run.
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/ci/plug-bazel-targets.sh --output DIR [--base SHA] [--head SHA]

Write changed-files.txt, touched-targets.txt, reverse-targets.txt, and
full-graph.txt to DIR. Bazel queries use BAZEL_BIN (default: bazel) and BAZEL_RC
when set. Missing revisions fall back to the current checkout's tracked files.
EOF
}

base=""
head=""
output=""
while (($# > 0)); do
  case "$1" in
    --base)
      (($# >= 2)) || { echo "targets: --base needs a value" >&2; exit 2; }
      base="$2"
      shift 2
      ;;
    --head)
      (($# >= 2)) || { echo "targets: --head needs a value" >&2; exit 2; }
      head="$2"
      shift 2
      ;;
    --output)
      (($# >= 2)) || { echo "targets: --output needs a value" >&2; exit 2; }
      output="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "targets: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n "$output" ]] || { echo "targets: --output is required" >&2; exit 2; }
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
mkdir -p "$output"

valid_revision() {
  [[ -n "$1" ]] && git rev-parse --verify "$1^{commit}" >/dev/null 2>&1
}

changed=()
if valid_revision "$base" && valid_revision "$head"; then
  mapfile -t changed < <(git diff --name-only --diff-filter=ACDMRTUXB "$base...$head")
elif valid_revision "origin/main" && valid_revision HEAD; then
  mapfile -t changed < <(git diff --name-only --diff-filter=ACDMRTUXB origin/main...HEAD)
else
  mapfile -t changed < <(git ls-files)
fi

printf '%s\n' "${changed[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/changed-files.txt"

bazel_bin="${BAZEL_BIN:-bazel}"
bazel_args=(--nosystem_rc --nohome_rc --noworkspace_rc)
if [[ -n "${BAZEL_RC:-}" ]]; then
  bazel_args+=("--bazelrc=$BAZEL_RC")
fi

query() {
  "$bazel_bin" "${bazel_args[@]}" query "$1" --lockfile_mode=error --output=label
}

full_graph=0
ci_contract=0
packages=()
seeds=()
while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  case "$path" in
    .bazelversion|.bazelrc|.bazelignore|MODULE.bazel|MODULE.bazel.lock|WORKSPACE|WORKSPACE.bazel|go.mod|go.sum|*/BUILD|*/BUILD.bazel|BUILD|BUILD.bazel|*.bzl)
      full_graph=1
      ;;
    .github/*|scripts/ci/*)
      ci_contract=1
      ;;
    *.go)
      # A deleted source file no longer has a Bazel label. The complete graph
      # is the safe approximation because its former reverse dependents may
      # still be affected by the package removal.
      if valid_revision "$head" && ! git cat-file -e "$head:$path" 2>/dev/null; then
        full_graph=1
        continue
      fi
      if [[ "$path" == */* ]]; then
        package="${path%/*}"
        file="${path##*/}"
        packages+=("//$package")
        seeds+=("//$package:$file")
      else
        packages+=("//")
        seeds+=("//:$path")
      fi
      ;;
  esac
done <"$output/changed-files.txt"

printf '%s\n' "${packages[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/packages.txt"
printf '%s\n' "${seeds[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/seeds.txt"

if ((full_graph)); then
  mapfile -t touched < <(query 'kind(".*_test rule", //...)' | grep -E '^//' | LC_ALL=C sort -u)
  reverse=()
else
  touched=()
  while IFS= read -r package; do
    [[ -n "$package" ]] || continue
    mapfile -t package_tests < <(query "kind(\".*_test rule\", ${package}:all)" | grep -E '^//' | LC_ALL=C sort -u)
    touched+=("${package_tests[@]}")
  done <"$output/packages.txt"

  reverse=()
  if [[ -s "$output/seeds.txt" ]]; then
    mapfile -t seed_values <"$output/seeds.txt"
    seed_set="set(${seed_values[*]})"
    mapfile -t reverse < <(query "kind(\".*_test rule\", rdeps(//..., ${seed_set}))" | grep -E '^//' | LC_ALL=C sort -u)
  fi
fi

printf '%s\n' "${touched[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/touched-targets.txt"
printf '%s\n' "${reverse[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/reverse-targets.txt"
printf '%s\n' "$full_graph" >"$output/full-graph.txt"

if [[ ! -s "$output/touched-targets.txt" && ! -s "$output/reverse-targets.txt" ]]; then
  printf '%s\n' '//:lexicon_gate' >"$output/touched-targets.txt"
  if ((ci_contract)); then
    printf '%s\n' 'fallback=//:lexicon_gate (CI-only change; contract and lexicon lanes cover it)' >"$output/selection.txt"
  else
    printf '%s\n' 'fallback=//:lexicon_gate (no Bazel test target is owned by the changed files)' >"$output/selection.txt"
  fi
else
  if ((full_graph)); then
    printf '%s\n' 'selection=complete Bazel test graph (repository graph change)' >"$output/selection.txt"
  else
    printf '%s\n' 'selection=package tests plus transitive reverse-dependent tests' >"$output/selection.txt"
  fi
fi

for file in touched-targets.txt reverse-targets.txt; do
  if grep -Ev '^//[^[:space:]]+$|^$' "$output/$file" >/dev/null; then
    echo "targets: invalid label in $file" >&2
    exit 1
  fi
done

printf 'changed=%s touched=%s reverse=%s full_graph=%s\n' \
  "$(wc -l <"$output/changed-files.txt")" \
  "$(wc -l <"$output/touched-targets.txt")" \
  "$(wc -l <"$output/reverse-targets.txt")" \
  "$full_graph"
