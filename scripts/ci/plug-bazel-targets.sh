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
full-graph.txt to DIR. Resource ownership evidence is written to
resource-labels.txt and unknown-files.txt. Bazel queries use BAZEL_BIN
(default: bazel) and BAZEL_RC when set. Missing revisions fall back to the
current checkout's tracked files.
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
  local receipt code
  mkdir -p "$output/queries"
  receipt="$(mktemp -d "$output/queries/query.XXXXXX")"
  local command=("$bazel_bin" "${bazel_args[@]}" query "$1" --lockfile_mode=error --output=label)
  printf '%s\0' "${command[@]}" >"$receipt/argv.nul"
  printf '%q ' "${command[@]}" >"$receipt/command.txt"
  printf '\n' >>"$receipt/command.txt"
  if "${command[@]}" >"$receipt/stdout.log" 2>"$receipt/stderr.log"; then
    code=0
  else
    code=$?
  fi
  printf '%s\n' "$code" >"$receipt/exit.txt"
  cat "$receipt/stdout.log"
  cat "$receipt/stderr.log" >&2
  return "$code"
}

full_graph=0
ci_contract=0
packages=()
seeds=()
source_paths=()
unknown_paths=()
resource_labels=()
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
      source_paths+=("$path")
      ;;
    *)
      if valid_revision "$head" && ! git cat-file -e "$head:$path" 2>/dev/null; then
        full_graph=1
        unknown_paths+=("$path")
        continue
      fi
      source_paths+=("$path")
      ;;
  esac
done <"$output/changed-files.txt"

if ((${#source_paths[@]} > 0)); then
  declare -A source_label_by_path=()
  # //... selects rules, not their source-file inputs. Walk the rule closure
  # for actual ownership: Go sources and resources can live below the BUILD
  # package directory. Resolve even in full-graph mode because delivery also
  # uses packages.txt to query the owning libraries' reverse dependents.
  if source_inventory="$(query 'kind("source file", deps(//...))')"; then :; else
    code=$?
    echo "targets: cannot query Bazel source-file ownership" >&2
    exit "$code"
  fi
  mapfile -t source_labels < <(printf '%s\n' "$source_inventory" | grep -E '^//' | LC_ALL=C sort -u)
  for label in "${source_labels[@]}"; do
    label_body="${label#//}"
    [[ "$label_body" == *:* ]] || continue
    package="${label_body%%:*}"
    file="${label_body#*:}"
    if [[ -n "$package" ]]; then
      source_path="$package/$file"
    else
      source_path="$file"
    fi
    source_label_by_path["$source_path"]="$label"
  done

  for path in "${source_paths[@]}"; do
    label="${source_label_by_path[$path]:-}"
    if [[ -n "$label" ]]; then
      label_body="${label#//}"
      package="${label_body%%:*}"
      if [[ -n "$package" ]]; then
        packages+=("//$package")
      else
        packages+=("//")
      fi
      seeds+=("$label")
      if [[ "$path" != *.go ]]; then
        resource_labels+=("$path=$label")
      fi
    else
      # An unowned file may still affect a custom rule, a generated input, or
      # a package declaration outside this diff. Complete-graph selection is
      # the safe fail-closed approximation when ownership is unknown.
      full_graph=1
      unknown_paths+=("$path")
    fi
  done
fi

printf '%s\n' "${packages[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/packages.txt"
printf '%s\n' "${seeds[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/seeds.txt"
printf '%s\n' "${resource_labels[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/resource-labels.txt"
printf '%s\n' "${unknown_paths[@]}" | sed '/^$/d' | LC_ALL=C sort -u >"$output/unknown-files.txt"

if ((full_graph)); then
  if touched_query="$(query 'kind(".*_test rule", //...)')"; then :; else
    code=$?
    echo "targets: cannot query complete Bazel test graph" >&2
    exit "$code"
  fi
  mapfile -t touched < <(printf '%s\n' "$touched_query" | grep -E '^//' | LC_ALL=C sort -u)
  reverse=()
else
  touched=()
  while IFS= read -r package; do
    [[ -n "$package" ]] || continue
    if package_query="$(query "kind(\".*_test rule\", ${package}:all)")"; then :; else
      code=$?
      echo "targets: cannot query tests for $package" >&2
      exit "$code"
    fi
    mapfile -t package_tests < <(printf '%s\n' "$package_query" | grep -E '^//' | LC_ALL=C sort -u)
    touched+=("${package_tests[@]}")
  done <"$output/packages.txt"

  reverse=()
  if [[ -s "$output/seeds.txt" ]]; then
    mapfile -t seed_values <"$output/seeds.txt"
    seed_set="set(${seed_values[*]})"
    if reverse_query="$(query "kind(\".*_test rule\", rdeps(//..., ${seed_set}))")"; then :; else
      code=$?
      echo "targets: cannot query reverse-dependent tests" >&2
      exit "$code"
    fi
    mapfile -t reverse < <(printf '%s\n' "$reverse_query" | grep -E '^//' | LC_ALL=C sort -u)
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
    printf '%s\n' 'selection=complete Bazel test graph (repository graph change or unknown resource ownership)' >"$output/selection.txt"
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
