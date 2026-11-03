#!/usr/bin/env python3
"""Offline N1 evaluator for the original nightly artifact and saved REST metadata."""

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
import tarfile
import xml.etree.ElementTree as ET
import zipfile


REPOSITORY = "buildbetter-app/plugs"
WORKFLOW = ".github/workflows/plug-nightly-restore-diffs.yml"
TARGET = "//internal/integration/nightlyrestore:nightlyrestore_test"
DRIVERS = ("memory", "sqlite", "postgres")
HISTORY = ("strict-default-full", "explicit-full", "explicit-partial", "snapshot-full", "snapshot-partial")
REFUSALS = {
    "default-refuses-partial": "non-contiguous", "full-refuses-partial": "non-contiguous",
    "missing-binding": "unmappable", "changed-digest": "unmappable", "changed-time": "unmappable",
}
FIXTURES = {
    "source": "internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar",
    "full": "internal/integration/offlinehistory/testdata/full.tar",
    "partial": "internal/integration/offlinehistory/testdata/partial.tar",
}
SNAPSHOT = "TestNightlyFreshStoreDiffs"
HISTORY_TEST = "TestNightlyHistoryFreshStoreDiffs"
REFUSAL_TEST = "TestNightlyHistoryRefusalsNoWrites"
OUTPUTS = "test-results/test.outputs/"
MAX_BYTES = 64 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def read_json(data):
    def invalid(value):
        raise ValueError("non-finite JSON number: " + value)
    return json.loads(data, object_pairs_hook=object_pairs, parse_constant=invalid)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def hex_digest(value, length=64):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % length, value) is not None


def utc_time(value):
    require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z", value),
            "missing or non-UTC GitHub timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def git(repository, *args):
    # Full object IDs only; no fetch, checkout, build, or ambient source-root override.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_NO_LAZY_FETCH="1", GIT_ALLOW_PROTOCOL="", GIT_TERMINAL_PROMPT="0",
               GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    return subprocess.check_output(["git", "--no-replace-objects", "-C", str(repository), *args],
                                   stderr=subprocess.PIPE, env=env)


def required_tests():
    names = {SNAPSHOT, HISTORY_TEST, REFUSAL_TEST}
    names.update(f"{SNAPSHOT}/{driver}/fresh-{i}" for driver in DRIVERS for i in (1, 2))
    names.update(f"{HISTORY_TEST}/{case}/{driver}/fresh-{i}"
                 for case in HISTORY for driver in DRIVERS for i in (1, 2))
    names.update(f"{REFUSAL_TEST}/{driver}/{case}" for driver in DRIVERS for case in REFUSALS)
    return Counter(names)


def validate_test_evidence(read):
    root = ET.fromstring(read("test-results/test.xml"))
    require(root.tag == "testsuites", "unexpected test XML root")
    for tag in ("failure", "error", "skipped"):
        require(root.find(".//" + tag) is None, "test XML contains " + tag)
    suites = list(root.iter("testsuite"))
    require(len(suites) == 3, "incomplete test suites")
    for suite in suites:
        require(all(suite.get(key) == "0" for key in ("errors", "failures", "skipped")),
                "non-success or incomplete suite counters")
        require(suite.get("tests") == str(len(suite.findall("testcase"))), "test XML count mismatch")
    observed = Counter(case.get("name") for case in root.iter("testcase"))
    require(observed == required_tests(), "missing, duplicate, or unexpected named XML comparisons")
    log = read("test-results/test.log").decode("utf-8")
    require(Counter(re.findall(r"^=== RUN\s+(\S+)\s*$", log, re.M)) == required_tests(),
            "missing or duplicate named RUN evidence")
    require(Counter(re.findall(r"^\s*--- PASS:\s+(\S+)\s+\(", log, re.M)) == required_tests(),
            "missing or duplicate named PASS evidence")
    require(not re.search(r"^\s*(?:--- (?:FAIL|SKIP):|FAIL(?:\s|$))", log, re.M), "failed or skipped test log")
    bazel = read("bazel.log").decode("utf-8")
    require(re.search(re.escape(TARGET) + r"\s+PASSED in ", bazel)
            and "Executed 1 out of 1 test: 1 test passes." in bazel,
            "missing uncached Bazel execution evidence")
    require(read("exit_code").strip() == b"0", "Bazel exit is not zero")


def fixture_members(data):
    members = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        for member in archive:
            require(member.isfile() and member.name not in members and 0 <= member.size <= MAX_BYTES,
                    "invalid source fixture inventory")
            members[member.name] = sha(archive.extractfile(member).read())
    require("manifest.json" in members and len(members) > 1, "incomplete source fixture inventory")
    return members


def census(value, archive, members, snapshot=False):
    keys = {"counts", "ids", "record_sha256", "version_sha256", "dependencies",
            "source_allocators", "target_allocators", "archive_member_sha256", "archive_sha256"}
    require(isinstance(value, dict) and keys <= value.keys(), "incomplete census")
    require(value["archive_sha256"] == archive, "census archive digest mismatch")
    require(isinstance(value["counts"], dict) and value["counts"]
            and all(type(n) is int and n >= 0 for n in value["counts"].values()), "invalid census counts")
    ids = value["ids"]
    require(isinstance(ids, list) and ids and all(isinstance(i, str) and i for i in ids)
            and len(set(ids)) == len(ids) and sum(value["counts"].values()) == len(ids), "incomplete census IDs")
    records, versions, actual_members = (value[key] for key in ("record_sha256", "version_sha256", "archive_member_sha256"))
    require(isinstance(records, dict) and set(records) == set(ids)
            and all(hex_digest(d) for d in records.values()), "incomplete record digests")
    require(isinstance(versions, dict) and (not snapshot or versions == records), "incomplete snapshot versions")
    require(isinstance(actual_members, dict) and set(actual_members) == set(members)
            and all(hex_digest(d) for d in actual_members.values())
            and (snapshot or actual_members == members), "incomplete archive member digests")
    require(isinstance(value["dependencies"], list) and isinstance(value["source_allocators"], list)
            and value["source_allocators"] and isinstance(value["target_allocators"], list)
            and len(value["target_allocators"]) == len(value["source_allocators"]), "incomplete allocator/dependency census")
    require(all(isinstance(item, str) and item for item in value["dependencies"])
            and len(set(value["dependencies"])) == len(value["dependencies"]), "invalid dependency identities")
    source, target = value["source_allocators"], value["target_allocators"]
    for allocator in source:
        require(isinstance(allocator, dict) and isinstance(allocator.get("prefix"), str) and allocator["prefix"]
                and allocator.get("id_mode") in {"hash", "sequential"}
                and type(allocator.get("last_id")) is int and allocator["last_id"] >= 0
                and isinstance(allocator.get("child_counters"), dict)
                and all(isinstance(key, str) and key and type(n) is int and n >= 0
                        for key, n in allocator["child_counters"].items()), "incomplete source allocator")
        require(allocator["id_mode"] != "hash" or
                (type(allocator.get("hash_length")) is int and allocator["hash_length"] > 0), "missing source hash profile")
    for allocator in target:
        require(isinstance(allocator, dict) and isinstance(allocator.get("prefix"), str) and allocator["prefix"]
                and allocator.get("id_mode") == "random" and type(allocator.get("random_bytes")) is int
                and allocator["random_bytes"] == 16 and allocator.get("suffix_encoding") == "lower-hex"
                and allocator.get("top_level_shape") == "prefix-random" and allocator.get("child_shape") == "parent-random",
                "incomplete target allocator")
    prefixes = [item["prefix"] for item in source]
    require(len(set(prefixes)) == len(prefixes) and Counter(prefixes) == Counter(item["prefix"] for item in target),
            "allocator project identities differ")


def history_census(value, head, floor):
    images, versions = value["as_of_record_sha256"], value["versions"]
    require(set(images) == {str(i) for i in range(1, head + 1)}, "incomplete history revision census")
    expected, changes, previous, last = {}, {}, {}, {}
    for revision in range(1, head + 1):
        image = images[str(revision)]
        require(isinstance(image, dict) and image and all(isinstance(key, str) and key and hex_digest(d)
                                                        for key, d in image.items()), "incomplete as-of image digests")
        changes[revision] = Counter()
        for identity in set(previous) | set(image):
            before, after = previous.get(identity), image.get(identity)
            if before == after:
                continue
            expected.setdefault(identity, []).append((revision, after))
            op = "delete" if after is None else ("create" if before is None else "patch")
            changes[revision][(identity, op, last.get(identity, 0))] += 1
            last[identity] = revision
        previous = image
    require(images[str(head)] == value["current"]["record_sha256"], "head image/current census differs")
    require(set(versions) == set(expected), "incomplete history version identities")
    for identity, wanted in expected.items():
        samples = versions[identity]
        require(isinstance(samples, list) and len(samples) == len(wanted), "incomplete nested version list")
        for index, (sample, (revision, image)) in enumerate(zip(samples, wanted)):
            require(isinstance(sample, dict) and type(sample.get("revision")) is int and sample["revision"] == revision
                    and type(sample.get("base")) is bool and (index != 0 or sample["base"])
                    and type(sample.get("deleted")) is bool and sample["deleted"] == (image is None)
                    and ("image_sha256" not in sample if image is None else sample.get("image_sha256") == image),
                    "version revision/image continuity mismatch")
    entries = value["changesets_after_floor"]
    require(isinstance(entries, list) and [entry.get("revision") for entry in entries] == list(range(floor + 1, head + 1)),
            "incomplete or duplicate changeset revisions")
    receipt = value["restore_receipt"]
    require(not entries or entries[-1] == receipt, "final changeset/receipt differs")
    for entry, revision in [(entry, i) for i, entry in enumerate(entries, floor + 1)] + [(receipt, head)]:
        require(type(entry.get("revision")) is int and entry["revision"] == revision
                and type(entry.get("branch")) is int and entry["branch"] == 0 and entry.get("incarnation") == ""
                and entry.get("kind") == "restore" and entry.get("time") == "0001-01-01T00:00:00Z"
                and entry.get("principal") == {"kind": "user", "id": "n1-test-restore-admin"}
                and entry.get("chain") == [] and not entry.get("events") and not entry.get("mutators")
                and isinstance(entry.get("changes"), list), "incomplete restore changeset/receipt")
        require(hex_digest(entry.get("idempotencyKey"), 32) if revision == head else "idempotencyKey" not in entry,
                "missing or misplaced restore receipt key")
        actual = Counter()
        for change in entry["changes"]:
            ref = change.get("ref", {})
            require(isinstance(ref.get("id"), str) and ref["id"] and type(ref.get("branch")) is int and ref["branch"] == 0
                    and type(change.get("fromRevision")) is int and 0 <= change["fromRevision"] < revision
                    and isinstance(change.get("changedPaths"), list) and change["changedPaths"]
                    and all(isinstance(path, str) for path in change["changedPaths"]), "incomplete change identity")
            actual[(ref["id"], change.get("op"), change["fromRevision"])] += 1
        require(actual == changes[revision], "changeset/version transition identities differ")


def replay_evidence(value, partial):
    require(isinstance(value, dict) and value.get("source") == "beads-dolt-images/1"
            and value.get("mode") == ("partial" if partial else "full") and value.get("complete") is (not partial)
            and all(type(value.get(key)) is int for key in ("first", "head", "floor"))
            and (value["first"], value["head"], value["floor"]) == ((4, 4, 1) if partial else (1, 4, 0))
            and value.get("gaps") == ([{"first": 1, "last": 3, "reason": "not-retained"}] if partial else []),
            "incomplete replay source/range/gaps")
    mapping = value.get("mapping")
    wanted = [(4, 2)] if partial else [(i, i) for i in range(1, 5)]
    require(isinstance(mapping, list) and [(item.get("sequence"), item.get("revision")) for item in mapping] == wanted,
            "incomplete replay revision mapping")
    for item in mapping:
        require(type(item.get("sequence")) is int and type(item.get("revision")) is int
                and all(isinstance(item.get(key), list) and item[key]
                        and all(isinstance(identity, str) and identity for identity in item[key])
                        and len(set(item[key])) == len(item[key]) for key in ("eventIds", "commitIds")),
                "incomplete replay event/commit identities")


def validate_reports(read, fixtures, members):
    def report(filename, schema, status):
        value = read_json(read(OUTPUTS + filename))
        require(value.get("schema") == schema and value.get("status") == status,
                "failed or unknown report: " + filename)
        return value

    snapshot = report("nightly-restore-diffs.json", "plug-nightly-restore-diffs/v1", "bounded_current_state")
    require(snapshot.get("source_fixture_sha256") == fixtures["source"]
            and hex_digest(snapshot.get("snapshot_fixture_sha256")), "snapshot fixture/source mismatch")
    history = report("nightly-history-restore-diffs.json", "plug-nightly-history-restore-diffs/v1", "bounded_public_history")
    require(history.get("fixtures") == {key: fixtures[key] for key in ("full", "partial")}, "history fixture/source mismatch")
    incarnations = set()
    for case, report_value in [("snapshot", snapshot)] + [(case, history) for case in HISTORY]:
        rows = report_value.get("results")
        require(isinstance(rows, list) and all(isinstance(row, dict) for row in rows), "missing comparison results")
        if case != "snapshot":
            require(all(row.get("case") in HISTORY for row in rows), "unknown history comparison")
            rows = [row for row in rows if row["case"] == case]
        require(Counter((row.get("driver"), row.get("fresh_restore")) for row in rows)
                == Counter((driver, i) for driver in DRIVERS for i in (1, 2)), "incomplete/duplicate driver matrix: " + case)
        baseline = None
        for row in rows:
            require(type(row["fresh_restore"]) is int, "invalid fresh restore ordinal")
            incarnation = row.get("incarnation")
            require(isinstance(incarnation, str) and incarnation and incarnation not in incarnations,
                    "missing or reused restore incarnation")
            incarnations.add(incarnation)
            value = row.get("census")
            if case == "snapshot":
                require(row.get("status") == "pass" and not row.get("boundary"), "snapshot comparison did not pass")
                census(value, snapshot["snapshot_fixture_sha256"], members["source"], snapshot=True)
            else:
                require(isinstance(value, dict), "missing history census")
                head, floor = ((1, 1) if case.startswith("snapshot-") else
                               ((3, 1) if case == "explicit-partial" else (5, 0)))
                require(all(type(value.get(key)) is int for key in ("head", "floor", "changesets_unavailable_through"))
                        and value.get("head") == head and value.get("floor") == floor
                        and value.get("changesets_unavailable_through") == floor, "history head/floor mismatch")
                fixture = "partial" if case.endswith("partial") else "full"
                census(value.get("current"), fixtures[fixture], members[fixture])
                require(all(isinstance(value.get(key), dict) and value[key] for key in
                            ("as_of_record_sha256", "versions", "restore_receipt"))
                        and isinstance(value.get("changesets_after_floor"), list), "incomplete history comparison evidence")
                history_census(value, head, floor)
                if case.startswith("snapshot-"):
                    require("history_evidence" not in value, "snapshot invented replay evidence")
                else:
                    require(isinstance(value.get("history_evidence"), dict) and value["history_evidence"], "missing replay evidence")
                    replay_evidence(value["history_evidence"], case == "explicit-partial")
            require(baseline is None or baseline == value, "driver comparison census differs: " + case)
            baseline = value
    refusals = report("nightly-history-refusals.json", "plug-nightly-history-refusals/v1", "pass")
    controls = refusals.get("typed_no_write_controls")
    require(isinstance(controls, list) and Counter(controls) == Counter(
        f"{driver}/{case}/{reason}" for driver in DRIVERS for case, reason in REFUSALS.items()),
        "incomplete/duplicate typed no-write refusals")


def validate_receipt(directory, repository, synthetic_tests=False):
    run = read_json((directory / "workflow-run.json").read_bytes())
    artifact = read_json((directory / "artifact.json").read_bytes())
    synthetic = ("synthetic_evaluator_fixture" in run
                 or "synthetic evaluator" in str(run.get("display_title", "")).lower())
    require(synthetic_tests == synthetic and (not synthetic_tests or run.get("synthetic_evaluator_fixture") is True),
            "synthetic evaluator records cannot qualify actual nights")
    require(run.get("repository", {}).get("full_name") == REPOSITORY
            and run.get("head_repository", {}).get("full_name") == REPOSITORY
            and run.get("path") == WORKFLOW and run.get("event") == "schedule"
            and run.get("head_branch") == "main" and run.get("status") == "completed"
            and run.get("conclusion") == "success" and type(run.get("run_attempt")) is int
            and run["run_attempt"] == 1, "not a successful first-attempt scheduled-main nightly run")
    run_id, artifact_id = run.get("id"), artifact.get("id")
    require(type(run_id) is int and run_id > 0 and type(artifact_id) is int and artifact_id > 0, "invalid run/artifact ID")
    repository_id = run["repository"].get("id")
    require(type(repository_id) is int and repository_id > 0
            and run["head_repository"].get("id") == repository_id, "repository identity mismatch")
    head = run.get("head_sha")
    require(hex_digest(head, 40), "missing full source SHA")
    created, started, updated = (utc_time(run.get(key)) for key in ("created_at", "run_started_at", "updated_at"))
    require(created <= started <= updated, "inconsistent run timestamps")
    binding = artifact.get("workflow_run", {})
    require(binding.get("id") == run_id and type(binding.get("id")) is int
            and binding.get("head_sha") == head and binding.get("head_branch") == "main"
            and binding.get("repository_id") == repository_id and binding.get("head_repository_id") == repository_id
            and artifact.get("name") == f"plug-nightly-restore-diffs-{run_id}-1"
            and artifact.get("expired") is False, "artifact/run identity mismatch or expired artifact")
    require(type(artifact.get("size_in_bytes")) is int and 0 < artifact["size_in_bytes"] <= MAX_BYTES,
            "invalid artifact size")
    archive = directory / "artifact.zip"
    require(archive.stat().st_size == artifact["size_in_bytes"], "artifact byte size mismatch")
    require(artifact.get("digest") == "sha256:" + sha(archive.read_bytes()), "artifact digest mismatch")
    require(git(repository, "cat-file", "-t", head).strip() == b"commit", "source SHA is not a commit")
    tree = git(repository, "rev-parse", "--verify", head + "^{tree}").decode().strip()
    require(hex_digest(tree, 40), "unresolved source tree")
    blobs = {key: git(repository, "show", head + ":" + path) for key, path in FIXTURES.items()}
    fixtures = {key: sha(data) for key, data in blobs.items()}
    members = {key: fixture_members(data) for key, data in blobs.items()}
    git(repository, "cat-file", "-e", head + ":" + WORKFLOW)
    with zipfile.ZipFile(archive) as zipped:
        names = set()
        total = 0
        for info in zipped.infolist():
            path = PurePosixPath(info.filename)
            require(info.filename not in names and not path.is_absolute() and ".." not in path.parts
                    and str(path) == info.filename.rstrip("/") and "\\" not in info.filename,
                    "unsafe or duplicate artifact member")
            require(not stat.S_ISLNK(info.external_attr >> 16), "artifact contains symlink")
            names.add(info.filename)
            total += info.file_size
        require(total <= MAX_BYTES, "artifact contents too large")
        def read(name):
            require(name in names, "missing artifact evidence: " + name)
            return zipped.read(name)
        checkout = read_json(read("run.json"))
        require(checkout.get("schema") == "plug-nightly-restore-run/v1"
                and checkout.get("expected_sha") == head and checkout.get("head") == head
                and checkout.get("tree") == tree and checkout.get("checkout_verified") is True
                and checkout.get("run_id") == str(run_id) and checkout.get("run_attempt") == "1"
                and checkout.get("event") == "schedule", "source/tree/checkout/run binding mismatch")
        validate_test_evidence(read)
        validate_reports(read, fixtures, members)
    return {"run_id": run_id, "artifact_id": artifact_id, "date": created.date().isoformat(),
            "repository_id": repository_id, "head": head, "tree": tree, "artifact_digest": artifact["digest"]}


def evaluate(directories, repository, *, synthetic_tests=False):
    result = {"schema": "plug-nightly-restore-streak/v1", "status": "BLOCK", "qualified": False,
              "scope": "N1 scheduled-main public fixture streak only", "nights": [],
              "evidence_kind": "synthetic_evaluator_tests" if synthetic_tests else "saved_github_metadata_and_artifacts"}
    try:
        for directory in directories:
            result["nights"].append(validate_receipt(Path(directory), repository, synthetic_tests))
        nights = sorted(result["nights"], key=lambda night: night["date"])
        result["nights"] = nights
        for key in ("run_id", "artifact_id", "date"):
            require(len({night[key] for night in nights}) == len(nights), "duplicate " + key)
        require(len({night["repository_id"] for night in nights}) <= 1, "mixed immutable repository identities")
        require(all(datetime.fromisoformat(right["date"]) - datetime.fromisoformat(left["date"]) == timedelta(days=1)
                    for left, right in zip(nights, nights[1:])), "missing consecutive UTC night")
        if len(nights) < 3:
            result.update(status="WAIT", reason=f"{len(nights)} complete distinct consecutive nights; three required")
        elif synthetic_tests:
            result.update(status="TEST_PASS", reason="synthetic evaluator test only; no actual night qualification")
        else:
            result.update(status="PASS", qualified=True, reason="three consecutive successful scheduled-main UTC nights",
                          streak=nights[-3:])
    except (ValueError, TypeError, KeyError, AttributeError, OSError, subprocess.SubprocessError,
            zipfile.BadZipFile, tarfile.TarError, ET.ParseError) as error:
        result["reason"] = str(error)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-repository", type=Path, required=True, help="offline Plug Git objects for all run heads")
    parser.add_argument("receipts", type=Path, nargs="+", help="directories containing workflow-run.json, artifact.json, artifact.zip")
    args = parser.parse_args(argv)
    result = evaluate(args.receipts, args.source_repository)
    print(json.dumps(result, indent=2, sort_keys=True))
    return {"PASS": 0, "BLOCK": 1, "WAIT": 2}[result["status"]]


if __name__ == "__main__":
    sys.exit(main())
