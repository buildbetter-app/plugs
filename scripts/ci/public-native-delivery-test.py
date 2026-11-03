#!/usr/bin/env python3
"""Proposed boundary cases; this file has not been run in the proposal unit."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from unittest import mock

spec = importlib.util.spec_from_file_location("native_delivery", Path(__file__).with_name("public-native-delivery.py"))
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
LABEL = "b1c-native-d70c-20261102"
RUN = 99999999999
CONTROL = "c" * 40  # Explicit synthetic fixture, never a prospective real SHA.
BRANCH = "codex/b1c-native-producer-d70c-20261102"


def refused(function) -> None:
    try:
        function()
    except (helper.Stop, FileExistsError):
        return
    raise AssertionError("required refusal did not occur")


def page(records, *, count=None, extra_headers="") -> bytes:
    body = {"total_count": len(records) if count is None else count, "artifacts": records}
    return ("HTTP/2.0 200 OK\r\nContent-Type: application/json\r\n" + extra_headers +
            "\r\n" + json.dumps(body)).encode()


def raw_response(body: bytes) -> bytes:
    # Literal wire bodies preserve duplicate members; a dict/json.dumps round
    # trip would erase the ambiguity this test must present to the decoder.
    return b"HTTP/2.0 200 OK\r\nContent-Type: application/json\r\n\r\n" + body


def duplicate_member_refusals(name: str) -> None:
    encoded_name = json.dumps(name)
    candidate = '{"name":' + encoded_name + ',"id":90000000000}'
    foreign = '{"name":' + encoded_name + ',"id":90000000001}'
    preflight = (
        # Actual exact-name candidate is hidden by duplicate count/list keys.
        '{"total_count":1,"artifacts":[' + candidate + '],"total_count":0,"artifacts":[]}',
        '{"total_count":0,"artifacts":[' + candidate + '],"artifacts":[]}',
        '{"total_count":1,"total_count":0,"artifacts":[]}',
        # Even equal values and escape-equivalent spellings are ambiguous.
        '{"total_count":0,"total_count":0,"artifacts":[]}',
        r'{"total_count":0,"artifacts":[],"art\u0069facts":[]}',
        '{"total_count":0,"artifacts":[],"extra":1,"extra":2}',
    )
    for body in preflight:
        refused(lambda: helper.exact_page(raw_response(body.encode()), name, 0))
    postdispatch = (
        # A foreign candidate cannot be discarded in favor of the last list.
        '{"total_count":2,"artifacts":[' + foreign + ',' + candidate +
        '],"total_count":1,"artifacts":[' + candidate + ']}',
        '{"total_count":1,"artifacts":[' + foreign + '],"artifacts":[' + candidate + ']}',
        '{"total_count":1,"artifacts":[{"name":"foreign","name":' + encoded_name + '}]}',
        '{"total_count":1,"artifacts":[{"name":' + encoded_name +
        ',"workflow_run":{"id":' + str(RUN + 1) + ',"id":' + str(RUN) + '}}]}',
        '{"total_count":1,"artifacts":[{"name":' + encoded_name +
        ',"workflow_run":{"id":' + str(RUN + 1) + '},"workflow_run":{"id":' + str(RUN) + '}}]}',
        '{"total_count":1,"artifacts":[{"name":' + encoded_name +
        ',"digest":"sha256:foreign","digest":"sha256:expected"}]}',
    )
    for body in postdispatch:
        refused(lambda: helper.exact_page(raw_response(body.encode()), name, 1))
    # The same strict decoder must reject nested/unknown duplicate members in
    # direct run/artifact reply documents, before any identity binder sees them.
    for body in (
        '{"id":' + str(RUN + 1) + ',"id":' + str(RUN) + '}',
        '{"actor":{"login":"personal","login":' + json.dumps(helper.ACTOR) + '}}',
        '{"workflow_run":{"head_sha":"foreign","head_sha":' + json.dumps(CONTROL) + '}}',
        '{"ignored":{"nested":{"key":1,"key":1}}}',
    ):
        refused(lambda: helper.one_response(raw_response(body.encode())))
    for body in (
        b'{"total_count":0,"artifacts":[],"extra":NaN}',
        b'{"total_count":0,"artifacts":[],"extra":Infinity}',
        b'{"total_count":0,"artifacts":[],"extra":"\xff"}',
        b'{"total_count":0,"artifacts":[]}{}',
    ):
        refused(lambda: helper.exact_page(raw_response(body), name, 0))


def repository_identity_refusals(run: dict, pages: dict) -> None:
    # All IDs here are synthetic. No current real repository ID is guessed.
    missing = object()
    invalid_ids = (missing, None, False, True, 0, -1, 11.0, "11", [], {})
    for repository_key in ("repository", "head_repository"):
        for invalid in invalid_ids:
            wrong = copy.deepcopy(run)
            if invalid is missing:
                del wrong[repository_key]["id"]
            else:
                wrong[repository_key]["id"] = invalid
            refused(lambda: helper.bind_run(wrong, RUN, CONTROL, BRANCH, terminal=True))
            # The artifact binder must revalidate even if a caller omitted bind_run.
            refused(lambda: helper.bind_artifacts(pages, wrong, LABEL))
        for invalid in (None, [], "unknown", {}):
            wrong = copy.deepcopy(run)
            wrong[repository_key] = invalid
            refused(lambda: helper.bind_run(wrong, RUN, CONTROL, BRANCH, terminal=True))
    wrong = copy.deepcopy(run)
    wrong["head_repository"]["id"] = 12
    refused(lambda: helper.bind_run(wrong, RUN, CONTROL, BRANCH, terminal=True))
    refused(lambda: helper.bind_artifacts(pages, wrong, LABEL))
    for role in pages:
        for origin_key in ("id", "repository_id", "head_repository_id"):
            for invalid in invalid_ids:
                wrong = copy.deepcopy(pages)
                origin = wrong[role][0]["workflow_run"]
                if invalid is missing:
                    del origin[origin_key]
                else:
                    origin[origin_key] = invalid
                refused(lambda: helper.bind_artifacts(wrong, run, LABEL))
        for origin_key in ("repository_id", "head_repository_id"):
            wrong = copy.deepcopy(pages)
            wrong[role][0]["workflow_run"][origin_key] = 12
            refused(lambda: helper.bind_artifacts(wrong, run, LABEL))
    # Exact r1 counterexample: both run IDs null; both origin IDs null or absent.
    for origin_missing in (False, True):
        wrong_run, wrong_pages = copy.deepcopy(run), copy.deepcopy(pages)
        wrong_run["repository"]["id"] = None
        wrong_run["head_repository"]["id"] = None
        for records in wrong_pages.values():
            for key in ("repository_id", "head_repository_id"):
                if origin_missing:
                    del records[0]["workflow_run"][key]
                else:
                    records[0]["workflow_run"][key] = None
        refused(lambda: helper.bind_run(wrong_run, RUN, CONTROL, BRANCH, terminal=True))
        refused(lambda: helper.bind_artifacts(wrong_pages, wrong_run, LABEL))
    for key in ("id", "workflow_id", "run_attempt"):
        for invalid in invalid_ids:
            wrong = copy.deepcopy(run)
            if invalid is missing:
                del wrong[key]
            else:
                wrong[key] = invalid
            refused(lambda: helper.bind_run(wrong, RUN, CONTROL, BRANCH, terminal=True))
    # Python numeric equality must not accept float IDs or bool attempt1.
    for key in ("id", "workflow_id", "run_attempt"):
        wrong = dict(run, **{key: float(run[key])})
        refused(lambda: helper.bind_run(wrong, RUN, CONTROL, BRANCH, terminal=True))
    refused(lambda: helper.bind_run(dict(run, run_attempt=True), RUN, CONTROL, BRANCH, terminal=True))
    wrong = copy.deepcopy(pages)
    wrong["bundle"][0]["workflow_run"]["id"] = float(RUN)
    refused(lambda: helper.bind_artifacts(wrong, run, LABEL))


def spent_marker_refusal(directory: Path) -> None:
    # Assert the actual independent budget guard, not another pre-existing file.
    try:
        helper.dispatch_once(directory, BRANCH, LABEL)
    except FileExistsError as error:
        assert Path(error.filename) == directory / "dispatch.spent"
        return
    raise AssertionError("spent marker did not refuse the dispatch attempt")


def dispatch_boundary_cases(url: bytes) -> None:
    expected_argv = [helper.GH_APP, "workflow", "run", str(helper.WORKFLOW_ID),
                     "--repo", helper.REPOSITORY, "--ref", BRANCH,
                     "-f", "profile=" + helper.PROFILE, "-f", "label=" + LABEL]
    # These exercise real dispatch_once + recorded_call with only Popen mocked.
    # No real child, gh, API, workflow or producer is invoked by these fixtures.
    scenarios = (
        ("success", 0, False, url + b"\n", b""),
        ("spawn-error", None, False, b"", b""),
        ("nonzero", 23, False, url + b"\n", b"synthetic command failure\n"),
        ("timeout", -9, True, url + b"\n", b"synthetic partial output\n"),
        ("missing-ack", 0, False, b"Created workflow_dispatch event\n", b""),
        ("ambiguous-ack", 0, False, url + b"\n" + url + b"\n", b""),
    )
    for scenario, exit_code, timed_out, stdout, stderr in scenarios:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            calls = []
            children = []
            class Child:
                def __init__(self):
                    self.returncode = None if timed_out else exit_code
                    self.killed = False
                    self.communications = 0
                def communicate(self, timeout=None):
                    self.communications += 1
                    if timed_out and self.communications == 1:
                        assert timeout == 120
                        raise helper.subprocess.TimeoutExpired(expected_argv, timeout)
                    if timed_out:
                        assert self.killed and timeout is None and self.communications == 2
                    else:
                        assert timeout == 120 and self.communications == 1
                    return stdout, stderr
                def kill(self):
                    assert timed_out and not self.killed
                    self.killed = True
                    self.returncode = exit_code
            def spawn(argv, **kwargs):
                calls.append(argv)
                assert argv == expected_argv and "shell" not in kwargs
                if scenario == "spawn-error":
                    raise OSError(5, "synthetic spawn failure")
                child = Child()
                children.append(child)
                return child
            with mock.patch.object(helper, "PAUSE", directory / "PAUSE"), mock.patch.object(helper.subprocess, "Popen", side_effect=spawn):
                if scenario == "success":
                    acknowledged, acknowledged_url, receipt = helper.dispatch_once(directory, BRANCH, LABEL)
                    assert (acknowledged, acknowledged_url) == (RUN, url.decode())
                    assert receipt["exit_code"] == 0
                else:
                    try:
                        helper.dispatch_once(directory, BRANCH, LABEL)
                    except helper.Stop as error:
                        expected_error = ("command spawn failed" if scenario == "spawn-error" else
                                          "command failed/timed out" if scenario in ("nonzero", "timeout") else
                                          "missing/ambiguous actual dispatch URL")
                        assert expected_error in str(error)
                    else:
                        raise AssertionError("required integrated dispatch refusal did not occur: " + scenario)
                assert len(calls) == 1
                assert (directory / "dispatch.spent").read_bytes() == b"one dispatch consumed; never remove or reuse\n"
                argv_bytes = b"\0".join(item.encode() for item in expected_argv) + b"\0"
                assert (directory / "dispatch.argv.nul").read_bytes() == argv_bytes
                assert json.loads((directory / "dispatch.argv.json").read_bytes()) == expected_argv
                assert json.loads((directory / "dispatch.inputs.json").read_bytes()) == {
                    "ref": BRANCH, "inputs": {"profile": helper.PROFILE, "label": LABEL}}
                assert json.loads((directory / "dispatch.started.json").read_bytes())["started_at"]
                if scenario == "spawn-error":
                    assert json.loads((directory / "dispatch.spawn-error.json").read_bytes()) == {
                        "type": "OSError", "errno": 5}
                    assert not (directory / "dispatch.receipt.json").exists()
                    assert not children
                else:
                    assert (directory / "dispatch.stdout").read_bytes() == stdout
                    assert (directory / "dispatch.stderr").read_bytes() == stderr
                    receipt_bytes = (directory / "dispatch.receipt.json").read_bytes()
                    receipt = json.loads(receipt_bytes)
                    assert receipt["exit_code"] == exit_code and receipt["timed_out"] is timed_out
                    assert receipt["argv_sha256"] == hashlib.sha256(argv_bytes).hexdigest()
                    assert receipt["stdout_sha256"] == hashlib.sha256(stdout).hexdigest()
                    assert receipt["stderr_sha256"] == hashlib.sha256(stderr).hexdigest()
                    assert (directory / "dispatch.receipt.sha256").read_text() == hashlib.sha256(receipt_bytes).hexdigest() + "\n"
                    assert len(children) == 1 and children[0].killed is timed_out
                saved = {path.name: path.read_bytes() for path in directory.iterdir()}
                spent_marker_refusal(directory)
                assert len(calls) == 1
                assert saved == {path.name: path.read_bytes() for path in directory.iterdir()}
    # No inputs/argv file exists: only the budget marker can prevent this child.
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        marker = directory / "dispatch.spent"
        marker.write_bytes(b"already consumed by this unit\n")
        with mock.patch.object(helper, "PAUSE", directory / "PAUSE"), mock.patch.object(helper.subprocess, "Popen") as spawn:
            spent_marker_refusal(directory)
            spawn.assert_not_called()
        assert list(directory.iterdir()) == [marker]
        assert marker.read_bytes() == b"already consumed by this unit\n"


def main() -> None:
    names = helper.native_names(LABEL)
    assert names == {"bundle": "plug-public-bundle-" + LABEL,
                     "verification": "plug-public-bundle-verification-" + LABEL}
    for label in ("", "has space", "a\nBUNDLE_ARTIFACT_NAME=forged", "*", "x" * 49):
        refused(lambda: helper.native_names(label))
    for name in names.values():
        duplicate_member_refusals(name)
        argv = helper.exact_query(name)
        assert "name=" + name in argv and "per_page=100" in argv and "page=1" in argv
        assert "--paginate" not in argv
        assert helper.exact_page(page([]), name, 0) == []
        # One foreign matching artifact blocks before dispatch; no workflow-only exemption.
        refused(lambda: helper.exact_page(page([{"name": name}]), name, 0))
        refused(lambda: helper.exact_page(page([{"name": name}] * 2), name, 1))
        refused(lambda: helper.exact_page(page([], count=1), name, 0))
        refused(lambda: helper.exact_page(page([], count=101), name, 0))
        refused(lambda: helper.exact_page(page([], extra_headers='Link: <anything>; rel="next"\r\n'), name, 0))
        refused(lambda: helper.exact_page(page([{"name": name + "-foreign"}]), name, 1))
    url = f"https://github.com/buildbetter-app/plugs/actions/runs/{RUN}".encode()
    assert helper.direct_ack(url + b"\n", b"") == (RUN, url.decode())
    for stdout, stderr in ((b"Created workflow_dispatch event", b""), (url + b"\n" + url, b""),
                           (url, url), (b"", url), (url + b"?attempt=2", b""),
                           (url.replace(b"buildbetter-app/plugs", b"other/project"), b"")):
        refused(lambda: helper.direct_ack(stdout, stderr))
    run = {"id": RUN, "workflow_id": helper.WORKFLOW_ID, "head_sha": CONTROL,
           "head_branch": BRANCH, "run_attempt": 1, "event": "workflow_dispatch",
           "html_url": url.decode(), "actor": {"login": helper.ACTOR},
           "triggering_actor": {"login": helper.ACTOR},
           "repository": {"id": 11, "full_name": helper.REPOSITORY},
           "head_repository": {"id": 11, "full_name": helper.REPOSITORY},
           "status": "completed", "conclusion": "success"}
    helper.bind_run(run, RUN, CONTROL, BRANCH, terminal=True)
    for key, value in (("id", RUN + 1), ("workflow_id", 1), ("head_sha", "d" * 40),
                       ("head_branch", "other"), ("run_attempt", 2), ("event", "push"),
                       ("conclusion", "failure"), ("actor", {"login": "personal"})):
        wrong = dict(run, **{key: value})
        refused(lambda: helper.bind_run(wrong, RUN, CONTROL, BRANCH, terminal=True))
    payload = b"synthetic ZIP bytes; no executable or official artifact claim"
    pages = {}
    for index, (role, name) in enumerate(names.items()):
        artifact = {"name": name, "id": 90000000000 + index, "expired": False,
                    "size_in_bytes": len(payload), "digest": "sha256:" + hashlib.sha256(payload).hexdigest(),
                    "workflow_run": {"id": RUN, "head_sha": CONTROL, "head_branch": BRANCH,
                                     "repository_id": 11, "head_repository_id": 11}}
        pages[role] = helper.exact_page(page([artifact]), name, 1)
    repository_identity_refusals(run, pages)
    dispatch_boundary_cases(url)
    selected = helper.bind_artifacts(pages, run, LABEL)
    for item in selected.values():
        helper.verify_official_zip(item, payload)
        refused(lambda: helper.verify_official_zip(item, payload + b"changed"))
    for change in ("foreign-run", "foreign-control", "expired", "duplicate-id", "legacy-id"):
        wrong = copy.deepcopy(pages)
        item = wrong["verification"][0]
        if change == "foreign-run":
            item["workflow_run"]["id"] += 1
        elif change == "foreign-control":
            item["workflow_run"]["head_sha"] = "d" * 40
        elif change == "expired":
            item["expired"] = True
        elif change == "duplicate-id":
            item["id"] = wrong["bundle"][0]["id"]
        else:
            item["id"] = next(iter(helper.OLD_ARTIFACTS))
        refused(lambda: helper.bind_artifacts(wrong, run, LABEL))


if __name__ == "__main__":
    main()
