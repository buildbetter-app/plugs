#!/usr/bin/env python3
"""Execute rendered public caller commands, using disposable synthetic inputs."""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import threading

import prepare
from render import PROJECTS, render

ROOT = Path(__file__).resolve().parent
DISPOSABLE = Path("/tmp/plug-swarm")


def public_snapshot(root: Path) -> Path:
    """Build only public synthetic census inputs; never consult host defaults."""
    project, home = root / "source/project", root / "source/home"
    for parent in (project, home):
        (parent / ".claude").mkdir(parents=True)
        (parent / ".codex").mkdir()
    claude = {"hooks": {"SessionStart": [{"matcher": "", "hooks": [{"type": "command", "command": "bd prime --hook-json"}]}]}}
    (project / ".claude/settings.json").write_text(json.dumps(claude))
    matchers = {"PostCompact": "manual|auto", "PreCompact": "manual|auto", "SessionStart": "startup|resume|clear"}
    hooks = {event: [{"hooks": [{"type": "command", "command": "bd prime --hook-json", "statusMessage": "Refresh " + event}],
                       **({"matcher": matchers[event]} if event in matchers else {})}] for event in prepare.EVENTS}
    (project / ".codex/hooks.json").write_text(json.dumps({"hooks": hooks}))
    (project / ".codex/config.toml").write_text("[features]\nhooks = true\n")
    (project / "AGENTS.md").write_text("bd prime\nbd ready\n")
    (project / "CLAUDE.md").write_text("bd show <id>\n")
    (home / ".claude/settings.json").write_text('{"enabledPlugins":{"public@test":true}}')
    (home / ".codex/config.toml").write_text('[features]\nhooks = true\nplugins = true\n[plugins."public@test"]\n')
    (home / ".claude/CLAUDE.md").write_text("Synthetic guidance without caller stanzas.\n")
    (home / ".codex/AGENTS.md").write_text("Synthetic guidance without caller stanzas.\n")
    skill = root / "source/SKILL.md"
    skill.write_text("---\nname: catch-me-up\ndescription: Public synthetic workflow\n---\n\n"
                     "Run `BEADS_DIR=/home/dev/beads/cody-ops/.beads bd list --status open,in_progress,blocked --json`.\n\n"
                     "## 2. Layout (strict order)\n\nUpdate `state.json` after reading the results.\n"
                     + "\n".join(f"Public workflow instruction {n}." for n in range(60)) + "\n")
    snapshot = root / "snapshot"
    with contextlib.redirect_stdout(io.StringIO()):
        prepare.prepare(argparse.Namespace(output=str(snapshot), cody_root=str(project), home_root=str(home), catch_skill=str(skill)))
    return snapshot


def shell_run(command: str, cwd: Path, payload: str | None = None) -> subprocess.CompletedProcess:
    env = dict(os.environ, BEADS_DIR="/synthetic/ambient-must-not-be-read", PLUG_SERVER="http://127.0.0.1:1",
               PYTHONDONTWRITEBYTECODE="1")
    env["PATH"] = "/tmp/plug-swarm/bin/worker-safe:" + env["PATH"]
    return subprocess.run(["/bin/sh", "-c", "export PATH=/tmp/plug-swarm/bin/worker-safe:$PATH\n" + command],
                          input=payload, text=True, capture_output=True, cwd=cwd, env=env, timeout=30, check=False)


def hook_commands(prepared: Path, project: str):
    for provider, name in (("claude", ".claude/settings.json"), ("codex", ".codex/hooks.json")):
        config = json.loads((prepared / f"prepared/{project}/{name}").read_text())
        for event, stanzas in config["hooks"].items():
            yield provider, event, stanzas[0]["hooks"][0]["command"]


def skill_commands(prepared: Path) -> dict[str, str]:
    # Read code spans from the emitted full skill, including variable-length
    # CommonMark delimiters when a shell path itself contains backticks.
    text = (prepared / "prepared/catch-me-up/SKILL.md").read_text()
    spans = re.findall(r"(?<!`)(`+)(?!`)(.+?)(?<!`)\1(?!`)", text)
    result = {}
    for _, span in spans:
        if "catch_me_up_adapter.py" in span:
            argv = shlex.split(span)
            assert argv[-2] == "--project" and argv[-1] in PROJECTS and argv[-1] not in result
            result[argv[-1]] = span
    assert set(result) == set(PROJECTS), result
    return result


def unchanged_settings(snapshot: Path, prepared: Path):
    for project in PROJECTS:
        for name in (".claude/settings.json", ".codex/hooks.json"):
            before = json.loads((snapshot / f"prepared/{project}/{name}").read_text())
            after = json.loads((prepared / f"prepared/{project}/{name}").read_text())
            for event in before["hooks"]:
                after["hooks"][event][0]["hooks"][0]["command"] = before["hooks"][event][0]["hooks"][0]["command"]
            assert after == before, (project, name)  # matchers, scheduling, status messages, all fields
        for name in (".codex/config.toml",):
            assert (snapshot / f"prepared/{project}/{name}").read_bytes() == (prepared / f"prepared/{project}/{name}").read_bytes()
    assert (snapshot / "prepared/plugin-settings.json").read_bytes() == (prepared / "prepared/plugin-settings.json").read_bytes()
    assert (snapshot / "preimage/catch-me-up-full.md").read_bytes() == (prepared / "preimage/catch-me-up-full.md").read_bytes()


def main():
    from fixture_test import _Server, _assert_catch_up, _assert_hook, _canonical_payload, INCARNATION, PRINCIPAL, AGENT
    DISPOSABLE.mkdir(exist_ok=True)
    server = _Server()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="i8-render-", dir=DISPOSABLE) as directory:
            root = Path(directory)
            snapshot = public_snapshot(root)
            # Literal single/double quotes, spaces, substitution, separators and
            # backticks must reach argv as data without executing shell syntax.
            callers = root / "callers ' \" $(touch injected-dollar) `touch injected-tick`; semi"
            callers.mkdir()
            for name in ("hook_adapter.py", "catch_me_up_adapter.py", "binding.py"):
                shutil.copyfile(ROOT / name, callers / name)
            binding = root / "binding ' \" $(touch injected-binding) `touch injected-btick`; name.json"
            binding.write_text(json.dumps({"schema": "plug-host-binding/1", "endpoint": f"http://127.0.0.1:{server.server_port}",
                                           "incarnation": INCARNATION, "principal": PRINCIPAL, "agent": AGENT,
                                           "projects": {p: {"workspace": str(root / p), "legacy_ids": {}} for p in PROJECTS}}))
            # Deliberately not JSON: rendering must not inspect binding contents.
            original_binding = binding.read_bytes()
            binding.write_text("RENDER MUST NOT READ THIS\n")
            prepared = root / "rendered"
            receipt = render(snapshot, prepared, callers, binding)
            assert receipt["binding_contents_read"] is False
            binding.write_bytes(original_binding)
            unchanged_settings(snapshot, prepared)
            applied = subprocess.run(["python3", "-B", str(ROOT / "application_check.py"), "--prepared", str(prepared)],
                                     text=True, capture_output=True, check=False, timeout=30)
            assert applied.returncode == 0, applied.stderr
            application = json.loads(applied.stdout)
            catch_commands = skill_commands(prepared)
            hook_count, catch_count, refusals = 0, 0, 0
            for project in PROJECTS:
                commands = list(hook_commands(prepared, project))
                assert len(commands) == 5
                for provider, event, emitted in commands:
                    argv = shlex.split(emitted)
                    assert argv == ["python3", "-B", str(callers / "hook_adapter.py"), "--binding", str(binding),
                                    "--project", project, "--provider", provider, "--event", event]
                    _assert_hook(shell_run(emitted, root, json.dumps(_canonical_payload(provider, event, project))), event, project)
                    hook_count += 1
                _assert_catch_up(shell_run(catch_commands[project], root), project)
                catch_count += 1
                command = next(c for _, event, c in commands if event == "PreCompact")
                for payload, error in (("{", "hook input is not JSON"), ('{"hookEventName":"UserPromptSubmit"}', "hook event input does not match configured event")):
                    refused = shell_run(command, root, payload)
                    assert refused.returncode == 1 and refused.stdout == "" and error in refused.stderr
                    refusals += 1
            server.state.unknown_category = True
            refused = shell_run(catch_commands["cody-ops"], root)
            assert refused.returncode == 1 and refused.stdout == "" and "ontology_category_unknown" in refused.stderr, refused.stderr
            server.state.unknown_category = False
            refusals += 1
            assert not server.state.failures, server.state.failures
            assert not any((root / name).exists() for name in ("injected-dollar", "injected-tick", "injected-binding", "injected-btick"))

            # Refuse corrupt or escaping snapshots before creating any output.
            bad_cases = 0
            for case in ("hash", "symlink", "unlisted", "reuse"):
                source = root / ("bad-" + case)
                shutil.copytree(snapshot, source)
                manifest = json.loads((source / "manifest.json").read_text())
                target = source / "preimage/codex-hooks.json"
                out = root / ("refused-" + case)
                if case == "hash":
                    target.write_text("{}")
                elif case == "symlink":
                    target.unlink()
                    target.symlink_to(snapshot / "preimage/codex-hooks.json")
                elif case == "unlisted":
                    manifest["files"][0]["path"] = "../uncaptured"
                    (source / "manifest.json").write_text(json.dumps(manifest))
                else:
                    out.mkdir()
                try:
                    render(source, out, callers, binding)
                except ValueError:
                    pass
                else:
                    raise AssertionError("renderer accepted " + case)
                assert (out.is_dir() and not list(out.iterdir())) if case == "reuse" else not out.exists()
                bad_cases += 1
            print(json.dumps({"status": "PASS", "scope": "synthetic rendered commands", "projects": list(PROJECTS),
                              "hook_commands": hook_count, "catch_up_commands": catch_count, "exit_refusals": refusals,
                              "snapshot_refusals": bad_cases, "shell_side_effects": 0,
                              "settings_preserved": True, "zero_fuzz_application": application,
                              "rpc_requests": len(server.state.requests)}, sort_keys=True))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
