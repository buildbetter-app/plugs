#!/usr/bin/env python3
"""Apply the generated projection diffs to a disposable preimage copy.

This check never writes a source or host path.  It proves that the exact
preimage/patch artifacts in a prepared output reproduce the candidate
templates with zero fuzz, then parses the resulting JSON settings.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import shlex
import subprocess
import tempfile


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_templates(root: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    rendered_path = root / "rendered.json"
    rendered = json.loads(rendered_path.read_text()) if rendered_path.exists() else None
    if rendered:
        assert rendered["schema"] == "plug-rendered-callers/1"
        assert rendered["binding_contents_read"] is False and rendered["live_configuration_changed"] is False

    def verify_command(value, project, script, *extra):
        if rendered:
            assert shlex.split(value) == ["python3", "-B", str(Path(rendered["caller_root"]) / script),
                                           "--binding", rendered["binding_path"], "--project", project, *extra]
        else:
            assert value == (f"python3 {{PLUG_CALLER_ROOT}}/{script} --binding {{HOST_BINDING}} "
                             f"--project {project}" + (" " + " ".join(extra) if extra else ""))

    for project in ("cody-ops", "signals-v2"):
        claude = json.loads((root / f"prepared/{project}/.claude/settings.json").read_text(encoding="utf-8"))
        session = claude["hooks"]["SessionStart"][0]
        command = session["hooks"][0]["command"]
        assert session["matcher"] == "" and f"--project {project} " in command
        verify_command(command, project, "hook_adapter.py", "--provider", "claude", "--event", "SessionStart")
        codex = json.loads((root / f"prepared/{project}/.codex/hooks.json").read_text(encoding="utf-8"))
        expected_matchers = {
            "PostCompact": "manual|auto",
            "PreCompact": "manual|auto",
            "SessionStart": "startup|resume|clear",
            "UserPromptSubmit": None,
        }
        for event, matcher in expected_matchers.items():
            stanza = codex["hooks"][event][0]
            hook = stanza["hooks"][0]
            assert f"--project {project} " in hook["command"]
            verify_command(hook["command"], project, "hook_adapter.py", "--provider", "codex", "--event", event)
            if matcher is None:
                assert "matcher" not in stanza
            else:
                assert stanza["matcher"] == matcher
        config = (root / f"prepared/{project}/.codex/config.toml").read_text(encoding="utf-8")
        assert "hooks = true" in config
        guidance = (root / f"prepared/{project}/guidance.md").read_text(encoding="utf-8")
        assert f"--project {project} " in guidance
        assert "documentation only" in guidance
        checks[f"{project}_settings"] = True
    skill = (root / "prepared/catch-me-up/SKILL.md").read_text(encoding="utf-8")
    legacy = "BEADS_DIR=/home/dev/beads/cody-ops/.beads bd list --status open,in_progress,blocked --json"
    def catch_command(project):
        if rendered:
            return shlex.join(["python3", "-B", str(Path(rendered["caller_root"]) / "catch_me_up_adapter.py"),
                               "--binding", rendered["binding_path"], "--project", project])
        return f"python3 {{PLUG_CALLER_ROOT}}/catch_me_up_adapter.py --binding {{HOST_BINDING}} --project {project}"
    cody_adapter, signals_adapter = (catch_command(project) for project in ("cody-ops", "signals-v2"))
    assert legacy not in skill
    assert "--project PROJECT" not in skill
    assert cody_adapter in skill and signals_adapter in skill
    if rendered:
        assert "{PLUG_CALLER_ROOT}" not in skill and "{HOST_BINDING}" not in skill
        checks["rendered_commands"] = True
    assert "JSON-array caller envelope" not in skill
    assert "## 2. Layout (strict order)" in skill and ("Update " + chr(96) + "state.json" + chr(96)) in skill
    assert len(skill.splitlines()) > 50
    checks["catch_up_template"] = True
    plugins = json.loads((root / "prepared/plugin-settings.json").read_text(encoding="utf-8"))
    assert set(plugins) == {"claude", "codex"}
    assert all(isinstance(value, bool) for value in plugins["claude"]["enabledPlugins"].values())
    assert plugins["codex"]["features"] == {"hooks": True, "plugins": True}
    assert all(isinstance(value, str) and "@" in value for value in plugins["codex"]["plugins"])
    checks["plugin_projection"] = True
    global_projection = json.loads((root / "preimage/global-guidance-projection.json").read_text(encoding="utf-8"))
    assert all(entry["selected_public_stanzas"] == [] for entry in global_projection)
    checks["global_guidance_withheld"] = True
    return checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepared", required=True)
    args = parser.parse_args()
    root = Path(args.prepared)
    if not root.is_absolute():
        parser.error("--prepared must be absolute")
    pre = root / "preimage"
    patches = root / "patches"
    required = [
        pre / "claude-session-start.json",
        pre / "codex-hooks.json",
        pre / "catch-me-up-full.md",
        pre / "cody-agents-public-commands.txt",
        pre / "cody-claude-public-commands.txt",
        patches / "claude-session-start.diff",
        patches / "codex-hooks.diff",
        patches / "catch-me-up.diff",
        patches / "guidance.diff",
    ]
    if any(not path.is_file() for path in required):
        missing = [str(path) for path in required if not path.is_file()]
        raise SystemExit("missing prepared artifact(s): " + ", ".join(missing))
    with tempfile.TemporaryDirectory(prefix="plug-i8-apply-check-") as directory:
        disposable = Path(directory)
        # The generated Claude patch is against the exact public settings
        # projection, while the catch-up patch below uses the complete public
        # skill body so its executable caller is actually replaced.
        shutil.copy2(pre / "claude-session-start.json", disposable / "claude-session-start")
        shutil.copy2(pre / "codex-hooks.json", disposable / "codex-hooks")
        shutil.copy2(pre / "catch-me-up-full.md", disposable / "catch-me-up")
        guidance = (
            (pre / "cody-agents-public-commands.txt").read_text(encoding="utf-8")
            + (pre / "cody-claude-public-commands.txt").read_text(encoding="utf-8")
        )
        (disposable / "guidance").write_text(guidance, encoding="utf-8")
        (disposable / "guidance-signals-v2").write_text(guidance, encoding="utf-8")
        patch_names = ("claude-session-start", "codex-hooks", "guidance", "guidance-signals-v2", "catch-me-up")
        for name in patch_names:
            patch_file = patches / f"{name}.diff"
            completed = subprocess.run(
                ["patch", "-p1", "--fuzz=0", "--forward", "--batch", "-i", str(patch_file)],
                cwd=disposable,
                text=True,
                capture_output=True,
                check=False,
            )
            if completed.returncode != 0:
                raise SystemExit(f"patch failed for {name}: {completed.stdout}{completed.stderr}")
        expected = {
            "claude-session-start": root / "prepared/cody-ops/.claude/settings.json",
            "codex-hooks": root / "prepared/cody-ops/.codex/hooks.json",
            "guidance": root / "prepared/cody-ops/guidance.md",
            "guidance-signals-v2": root / "prepared/signals-v2/guidance.md",
            "catch-me-up": root / "prepared/catch-me-up/SKILL.md",
        }
        matches = {}
        for name, candidate in expected.items():
            actual = disposable / name
            matches[name] = digest(actual) == digest(candidate)
            if not matches[name]:
                raise SystemExit(f"patched {name} does not match {candidate}")
            if name == "catch-me-up":
                legacy = "BEADS_DIR=/home/dev/beads/cody-ops/.beads bd list --status open,in_progress,blocked --json"
                if legacy in actual.read_text(encoding="utf-8"):
                    raise SystemExit("patched catch-me-up still contains the old bd list caller")
                if "--project PROJECT" in actual.read_text(encoding="utf-8"):
                    raise SystemExit("patched catch-me-up still contains an unresolved project placeholder")
        json.loads((disposable / "claude-session-start").read_text(encoding="utf-8"))
        json.loads((disposable / "codex-hooks").read_text(encoding="utf-8"))
    template_checks = verify_templates(root)
    print(json.dumps({"status": "PASS", "zero_fuzz": True, "matches": matches, "template_checks": template_checks}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
