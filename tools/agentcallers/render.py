#!/usr/bin/env python3
"""Render captured public caller templates in a fresh disposable /tmp tree.

This consumes the existing public preparation packet only. It never rereads
live host settings, the binding's contents, credentials or guidance prose.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import shlex

from prepare import EVENTS, pretty, unified

PROJECTS = ("cody-ops", "signals-v2")
FILES = {
    "patches/catch-me-up.diff", "patches/claude-session-start.diff", "patches/codex-hooks.diff",
    "patches/guidance-signals-v2.diff", "patches/guidance.diff",
    "preimage/catch-me-up-frontmatter.txt", "preimage/catch-me-up-full.md",
    "preimage/claude-session-start-stanza.json", "preimage/claude-session-start.json",
    "preimage/codex-features.txt", "preimage/codex-hooks.json",
    "preimage/cody-agents-public-commands.txt", "preimage/cody-claude-public-commands.txt",
    "preimage/global-guidance-projection.json", "preimage/plugin-settings.json",
    "preimage/project-codex-features.txt", "prepared/catch-me-up/SKILL.md",
    "prepared/plugin-settings.json",
    *(f"prepared/{p}/{name}" for p in PROJECTS for name in (
        ".claude/settings.json", ".codex/hooks.json", ".codex/config.toml", "guidance.md")),
}


def absolute(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute() or any(ch in str(value) for ch in ("\0", "\r", "\n")):
        raise ValueError("caller paths must be absolute single-line paths")
    return path


def command(caller_root: Path, binding: Path, project: str, script: str, *extra: str) -> str:
    if project not in PROJECTS or script not in ("hook_adapter.py", "catch_me_up_adapter.py"):
        raise ValueError("unknown prepared caller")
    return shlex.join(["python3", "-B", str(absolute(caller_root) / script),
                       "--binding", str(absolute(binding)), "--project", project, *extra])


def inline(value: str) -> str:
    """CommonMark code span, including paths which contain backticks."""
    fence = "`" * (1 + max((len(x) for x in re.findall(r"`+", value)), default=0))
    return fence + value + fence


def render(snapshot: Path, output: Path, caller_root: Path, binding: Path) -> dict:
    snapshot, output = absolute(snapshot), absolute(output)
    caller_root, binding = absolute(caller_root), absolute(binding)
    for root in (snapshot, output):
        if not root.resolve().is_relative_to(Path("/tmp/plug-swarm")):
            raise ValueError("snapshot and output must be disposable /tmp/plug-swarm paths")
    if output.exists() or output.is_symlink():
        raise ValueError("render output must not exist")
    source_manifest = snapshot / "manifest.json"
    if source_manifest.is_symlink():
        raise ValueError("snapshot manifest must not be a symlink")
    raw_manifest = source_manifest.read_bytes()
    manifest = json.loads(raw_manifest)
    if (not isinstance(manifest, dict) or manifest.get("schema") != "plug-prepared-callers/1"
            or manifest.get("live_settings_changed") is not False
            or manifest.get("secrets_or_private_prose_copied") is not False):
        raise ValueError("expected the captured public caller snapshot")
    entries = manifest.get("files", [])
    if (not isinstance(entries, list) or len(entries) != len(FILES)
            or not all(isinstance(item, dict) for item in entries)
            or {item.get("path") for item in entries} != FILES):
        raise ValueError("snapshot must contain exactly the declared public caller files")
    content = {}
    for item in entries:
        path = snapshot / item["path"]
        # Reject symlinks at any component; never escape into a live file.
        if any(part.is_symlink() for part in (path, *path.parents)) or not path.is_file():
            raise ValueError("snapshot file must be a regular captured file")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != item.get("sha256"):
            raise ValueError("snapshot preimage hash mismatch")
        content[item["path"]] = data.decode("utf-8")

    def hook(project, provider, event):
        return command(caller_root, binding, project, "hook_adapter.py", "--provider", provider, "--event", event)

    for project in PROJECTS:
        for provider, name in (("claude", ".claude/settings.json"), ("codex", ".codex/hooks.json")):
            path = f"prepared/{project}/{name}"
            config = json.loads(content[path])
            expected_events = {"SessionStart"} if provider == "claude" else set(EVENTS)
            if set(config["hooks"]) != expected_events:
                raise ValueError("snapshot hook event census differs")
            for event, stanzas in config["hooks"].items():
                if len(stanzas) != 1 or len(stanzas[0]["hooks"]) != 1:
                    raise ValueError("snapshot hook command census differs")
                original = stanzas[0]["hooks"][0]
                expected = (f"python3 {{PLUG_CALLER_ROOT}}/hook_adapter.py --binding {{HOST_BINDING}} "
                            f"--project {project} --provider {provider} --event {event}")
                if original["type"] != "command" or original["command"] != expected:
                    raise ValueError("unexpected captured hook template")
                original["command"] = hook(project, provider, event)
            content[path] = pretty(config)

        old_hook = (f"python3 {{PLUG_CALLER_ROOT}}/hook_adapter.py --binding {{HOST_BINDING}} "
                    f"--project {project} --provider claude --event SessionStart")
        old_catch = (f"python3 {{PLUG_CALLER_ROOT}}/catch_me_up_adapter.py --binding {{HOST_BINDING}} "
                     f"--project {project}")
        replacements = ((old_hook, hook(project, "claude", "SessionStart")),
                        (old_catch, command(caller_root, binding, project, "catch_me_up_adapter.py")))
        guidance = f"prepared/{project}/guidance.md"
        for old, new in replacements:
            if content[guidance].count(inline(old)) != 1:
                raise ValueError("expected one public guidance caller span")
            content[guidance] = content[guidance].replace(inline(old), inline(new))
        skill = "prepared/catch-me-up/SKILL.md"
        if content[skill].count(inline(old_catch)) != 1:
            raise ValueError("expected one public skill caller per project")
        content[skill] = content[skill].replace(inline(old_catch), inline(replacements[1][1]))

    guidance_pre = content["preimage/cody-agents-public-commands.txt"] + content["preimage/cody-claude-public-commands.txt"]
    for name, before, after in (
        ("claude-session-start", content["preimage/claude-session-start.json"], content["prepared/cody-ops/.claude/settings.json"]),
        ("codex-hooks", content["preimage/codex-hooks.json"], content["prepared/cody-ops/.codex/hooks.json"]),
        ("guidance", guidance_pre, content["prepared/cody-ops/guidance.md"]),
        ("guidance-signals-v2", guidance_pre, content["prepared/signals-v2/guidance.md"]),
        ("catch-me-up", content["preimage/catch-me-up-full.md"], content["prepared/catch-me-up/SKILL.md"]),
    ):
        content[f"patches/{name}.diff"] = unified(before, after, name)
    for name, text in content.items():
        if name.startswith("prepared/") and ("{PLUG_CALLER_ROOT}" in text or "{HOST_BINDING}" in text):
            raise ValueError("unresolved caller path placeholder")

    rendered = copy.deepcopy(manifest)
    rendered["rendered_from"] = {"snapshot": str(snapshot), "manifest_sha256": hashlib.sha256(raw_manifest).hexdigest()}
    rendered["catch_me_up_replacement"]["new_caller"] = " or ".join(
        inline(command(caller_root, binding, project, "catch_me_up_adapter.py")) for project in PROJECTS)
    rendered["files"] = [{"path": name, "sha256": hashlib.sha256(text.encode()).hexdigest()} for name, text in sorted(content.items())]
    receipt = {"schema": "plug-rendered-callers/1", "status": "PASS", "snapshot": str(snapshot),
               "output": str(output), "caller_root": str(caller_root), "binding_path": str(binding),
               "binding_contents_read": False, "live_configuration_changed": False,
               "source_manifest_sha256": hashlib.sha256(raw_manifest).hexdigest()}
    # Validate everything before creating output. Only prepared /tmp copies are written.
    output.mkdir(parents=True, exist_ok=False)
    for name, text in content.items():
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    (output / "manifest.json").write_text(pretty(rendered))
    (output / "rendered.json").write_text(pretty(receipt))
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("snapshot", "output", "caller-root", "binding"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        result = render(args.snapshot, args.output, args.caller_root, args.binding)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"plug caller render: {exc}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
