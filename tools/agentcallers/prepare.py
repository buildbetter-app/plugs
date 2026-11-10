#!/usr/bin/env python3
"""Prepare unapplied caller templates and exact public preimage/patch evidence.

Only the named hook objects, public guidance command lines, catch-me-up
frontmatter and plugin enablement flags are read.  State files, environment
values, credentials, stores and private agent/task prose are never copied.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys


ROOT = Path(__file__).resolve().parent
EVENTS = ("PostCompact", "PreCompact", "SessionStart", "UserPromptSubmit")
GUIDANCE_COMMANDS = re.compile(
    r"^\s*(?:BEADS_DIR=[^ ]+\s+)?bd\s+(?:prime|ready|show\s+<id>|update\s+<id>\s+--claim|close\s+<id>|remember|list\s+--status[^#]*)(?:.*)?$"
)


def read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot read public caller JSON: {path}") from exc


def pretty(value) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write(path: Path, data: str | bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        data = data.encode()
    path.write_bytes(data)


def unified(pre: str, post: str, name: str) -> str:
    return "".join(difflib.unified_diff(
        pre.splitlines(True), post.splitlines(True),
        fromfile=f"a/{name}", tofile=f"b/{name}", n=3,
    ))


def hook_stanzas(cody_root: Path):
    claude_path = cody_root / ".claude/settings.json"
    codex_path = cody_root / ".codex/hooks.json"
    claude = read_json(claude_path)
    codex = read_json(codex_path)
    try:
        session = claude["hooks"]["SessionStart"][0]
        codex_hooks = {event: codex["hooks"][event][0] for event in EVENTS}
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("census hook stanza is incomplete") from exc
    if session.get("matcher") != "":
        raise RuntimeError("Claude SessionStart matcher changed")
    for event, stanza in codex_hooks.items():
        hooks = stanza.get("hooks")
        if not isinstance(hooks, list) or len(hooks) != 1 or hooks[0].get("type") != "command":
            raise RuntimeError(f"Codex {event} command stanza is incomplete")
    return session, codex_hooks


def projected_guidance(path: Path, *, required: bool = True) -> str:
    """Keep exact command stanzas while dropping surrounding private prose."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RuntimeError(f"cannot read guidance source: {path}") from exc
    selected = [line for line in lines if GUIDANCE_COMMANDS.match(line)]
    # A source may have no list command, but the exact public prime/ready/etc.
    # stanzas are required for a useful preimage.
    if required and not selected:
        raise RuntimeError(f"no public caller stanzas found in {path}")
    return "\n".join(selected) + ("\n" if selected else "")


def plugin_projection(home_root: Path):
    claude = read_json(home_root / ".claude/settings.json")
    enabled_source = claude.get("enabledPlugins", {})
    enabled = {
        key: value
        for key, value in enabled_source.items()
        if isinstance(key, str) and isinstance(value, bool)
    } if isinstance(enabled_source, dict) else {}
    # The Codex TOML is intentionally parsed line-by-line.  Only feature and
    # plugin identifiers are copied; URLs, tokens and project settings stay out.
    codex = (home_root / ".codex/config.toml").read_text(encoding="utf-8")
    features = {}
    in_features = False
    plugins = []
    for line in codex.splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            in_features = stripped == "[features]"
        elif in_features and re.match(r"^(hooks|plugins)\s*=\s*(true|false)\s*$", stripped):
            key, value = [x.strip() for x in stripped.split("=", 1)]
            features[key] = value == "true"
        match = re.match(r'^\[plugins\."([^"]+)"\]$', stripped)
        if match:
            plugins.append(match.group(1))
    return {"claude": {"enabledPlugins": enabled}, "codex": {"features": features, "plugins": sorted(plugins)}}


def feature_projection(path: Path) -> dict[str, bool]:
    """Read only public hook/plugin feature toggles from a Codex TOML file."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RuntimeError(f"cannot read project feature source: {path}") from exc
    features: dict[str, bool] = {}
    in_features = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("["):
            in_features = stripped == "[features]"
            continue
        match = re.match(r"^(hooks|plugins)\s*=\s*(true|false)\s*$", stripped) if in_features else None
        if match:
            features[match.group(1)] = match.group(2) == "true"
    if features.get("hooks") is not True:
        raise RuntimeError(f"project Codex hooks feature is not enabled in {path}")
    return features


def prepare(args: argparse.Namespace):
    out = Path(args.output)
    if not out.is_absolute():
        raise RuntimeError("--output must be an absolute disposable path")
    cody_root = Path(args.cody_root)
    home_root = Path(args.home_root)
    session, codex_hooks = hook_stanzas(cody_root)
    project_features = feature_projection(cody_root / ".codex/config.toml")
    guidance = projected_guidance(cody_root / "AGENTS.md")
    claude_guidance = projected_guidance(cody_root / "CLAUDE.md")
    global_guidance = []
    for path in (home_root / ".codex/AGENTS.md", home_root / ".claude/CLAUDE.md"):
        source = path.read_bytes()
        # Global guidance is inspected only for public caller stanzas.  The
        # files currently contain identity/auth/task instructions, so their
        # content is withheld and only the empty projection plus hash is kept.
        selected = projected_guidance(path, required=False)
        global_guidance.append({
            "path": str(path),
            "sha256": sha(source),
            "selected_public_stanzas": selected.splitlines(),
        })
    catch_path = Path(args.catch_skill)
    catch_source = catch_path.read_text(encoding="utf-8")
    catch_lines = catch_source.splitlines(True)
    if not catch_lines or catch_lines[0].strip() != "---":
        raise RuntimeError("catch-me-up public frontmatter is not the census source")
    try:
        frontmatter_end = next(index for index, line in enumerate(catch_lines[1:], 1) if line.strip() == "---")
    except StopIteration as exc:
        raise RuntimeError("catch-me-up public frontmatter is not closed") from exc
    frontmatter = "".join(catch_lines[:frontmatter_end + 1])
    if "name: catch-me-up" not in frontmatter:
        raise RuntimeError("catch-me-up public frontmatter is not the census source")
    plugins = plugin_projection(home_root)

    # The generated settings are unapplied templates.  {{HOST_BINDING}} is a
    # required absolute path substitution at a later, separately approved
    # installation step; no live setting is edited here.
    def command(project, provider, event):
        return f"python3 {{PLUG_CALLER_ROOT}}/hook_adapter.py --binding {{HOST_BINDING}} --project {project} --provider {provider} --event {event}"

    prepared_claude = {"hooks": {"SessionStart": [{
        "hooks": [{"command": command("cody-ops", "claude", "SessionStart"), "type": "command"}],
        "matcher": session["matcher"],
    }]}}
    prepared_codex = {"hooks": {}}
    for event in EVENTS:
        original = codex_hooks[event]
        hook = original["hooks"][0]
        new_hook = {"command": command("cody-ops", "codex", event), "statusMessage": hook.get("statusMessage"), "type": "command"}
        new_hook = {k: v for k, v in new_hook.items() if v is not None}
        prepared_codex["hooks"][event] = [{"hooks": [new_hook], **({"matcher": original["matcher"]} if "matcher" in original else {})}]

    def guidance_post(project: str) -> str:
        return f"""# Plug caller guidance (prepared, unapplied)\n\nUse the explicit two-project binding for every caller. Never infer a project from cwd, BEADS_DIR, labels or a sidecar path.\n\n## Context refresh\n\n`python3 {{PLUG_CALLER_ROOT}}/hook_adapter.py --binding {{HOST_BINDING}} --project {project} --provider claude --event SessionStart`\n\nThe adapter preserves the caller's hookSpecificOutput envelope and includes the Plug brief's claims, ready records, recent changes, conventions and memories.\n\n## Catch-me-up\n\n`python3 {{PLUG_CALLER_ROOT}}/catch_me_up_adapter.py --binding {{HOST_BINDING}} --project {project}`\n\nThe adapter returns the legacy JSON array after the explicit Plug view and the `nikhil OR decision` selection.\n\nThe source action stanzas below are retained verbatim as a review preimage only. They are fenced documentation, not executable prepared instructions; the separately owned six-script packet supplies the typed write/read adapters for those actions.\n\n## Public source stanzas (verbatim, documentation only)\n\n```text\n""" + guidance + claude_guidance + "```\n"

    # Replace the one public executable list instruction in the complete
    # skill body. The rest of the skill remains byte-for-byte identical in the
    # disposable preimage, including its document/memory workflow.
    legacy_list = "BEADS_DIR=/home/dev/beads/cody-ops/.beads bd list --status open,in_progress,blocked --json"
    # The source line already supplies the first inline-code fence. Close and
    # reopen it around the second explicit project command so the generated
    # public skill remains valid Markdown and contains no unresolved token.
    replacement = (
        "python3 {PLUG_CALLER_ROOT}/catch_me_up_adapter.py --binding {HOST_BINDING} --project cody-ops"
        "` or `"
        "python3 {PLUG_CALLER_ROOT}/catch_me_up_adapter.py --binding {HOST_BINDING} --project signals-v2"
    )
    if catch_source.count(legacy_list) != 1:
        raise RuntimeError("catch-me-up public list caller was not unique")
    catch_post = catch_source.replace(legacy_list, replacement)
    if legacy_list in catch_post or replacement not in catch_post:
        raise RuntimeError("catch-me-up adapter replacement did not remove the old caller")


    pre = out / "preimage"
    prepared = out / "prepared"
    patches = out / "patches"
    # Exact source projections, never settings secrets or private guidance.
    # The public catch-me-up skill is retained in a disposable full-body
    # preimage because its executable caller is embedded in the workflow text.
    write(pre / "claude-session-start-stanza.json", pretty(session))
    write(pre / "claude-session-start.json", pretty({"hooks": {"SessionStart": [session]}}))
    write(pre / "codex-hooks.json", pretty({"hooks": codex_hooks}))
    write(pre / "cody-agents-public-commands.txt", guidance)
    write(pre / "cody-claude-public-commands.txt", claude_guidance)
    write(pre / "catch-me-up-frontmatter.txt", frontmatter)
    write(pre / "catch-me-up-full.md", catch_source)
    write(pre / "global-guidance-projection.json", pretty(global_guidance))
    write(pre / "project-codex-features.txt", "[features]\n" + "".join(f"{key} = {str(value).lower()}\n" for key, value in project_features.items()))
    write(pre / "plugin-settings.json", pretty(plugins))
    write(pre / "codex-features.txt", "[features]\n" + "".join(f"{k} = {str(v).lower()}\n" for k, v in plugins["codex"]["features"].items()))

    write(prepared / "cody-ops/.claude/settings.json", pretty(prepared_claude))
    write(prepared / "cody-ops/.codex/hooks.json", pretty(prepared_codex))
    write(prepared / "cody-ops/.codex/config.toml", "[features]\n" + "".join(f"{key} = {str(value).lower()}\n" for key, value in project_features.items()))
    write(prepared / "signals-v2/.claude/settings.json", pretty(prepared_claude | {"hooks": {"SessionStart": [{"hooks": [{"command": command("signals-v2", "claude", "SessionStart"), "type": "command"}], "matcher": ""}]}}))
    write(prepared / "signals-v2/.codex/hooks.json", pretty({"hooks": {event: [{"hooks": [{"command": command("signals-v2", "codex", event), "statusMessage": codex_hooks[event]["hooks"][0].get("statusMessage"), "type": "command"}], **({"matcher": codex_hooks[event]["matcher"]} if "matcher" in codex_hooks[event] else {})}] for event in EVENTS}}))
    write(prepared / "signals-v2/.codex/config.toml", "[features]\n" + "".join(f"{key} = {str(value).lower()}\n" for key, value in project_features.items()))
    write(prepared / "cody-ops/guidance.md", guidance_post("cody-ops"))
    write(prepared / "signals-v2/guidance.md", guidance_post("signals-v2"))
    write(prepared / "catch-me-up/SKILL.md", catch_post)
    write(prepared / "plugin-settings.json", pretty(plugins))

    pairs = [
        ("claude-session-start", (pre / "claude-session-start.json").read_text(), (prepared / "cody-ops/.claude/settings.json").read_text()),
        ("codex-hooks", (pre / "codex-hooks.json").read_text(), (prepared / "cody-ops/.codex/hooks.json").read_text()),
        ("guidance", guidance + claude_guidance, guidance_post("cody-ops")),
        ("guidance-signals-v2", guidance + claude_guidance, guidance_post("signals-v2")),
        ("catch-me-up", catch_source, catch_post),
    ]
    for name, before, after in pairs:
        write(patches / f"{name}.diff", unified(before, after, name))

    manifest = {
        "schema": "plug-prepared-callers/1",
        "source_scope": "public caller stanzas plus complete public catch-me-up skill body",
        "live_settings_changed": False,
        "secrets_or_private_prose_copied": False,
        "source": {
            "cody_root": str(cody_root),
            "home_root": str(home_root),
            "catch_skill": str(catch_path),
        },
        "source_inputs": [
            {"path": str(cody_root / ".claude/settings.json"), "sha256": sha((cody_root / ".claude/settings.json").read_bytes()), "projection": "preimage/claude-session-start.json"},
            {"path": str(cody_root / ".codex/hooks.json"), "sha256": sha((cody_root / ".codex/hooks.json").read_bytes()), "projection": "preimage/codex-hooks.json"},
            {"path": str(cody_root / "AGENTS.md"), "sha256": sha((cody_root / "AGENTS.md").read_bytes()), "projection": "preimage/cody-agents-public-commands.txt"},
            {"path": str(cody_root / "CLAUDE.md"), "sha256": sha((cody_root / "CLAUDE.md").read_bytes()), "projection": "preimage/cody-claude-public-commands.txt"},
            {"path": str(catch_path), "sha256": sha(catch_path.read_bytes()), "projection": "preimage/catch-me-up-full.md"},
            {"path": str(home_root / ".claude/settings.json"), "sha256": sha((home_root / ".claude/settings.json").read_bytes()), "projection": "preimage/plugin-settings.json"},
            {"path": str(home_root / ".codex/config.toml"), "sha256": sha((home_root / ".codex/config.toml").read_bytes()), "projection": "preimage/plugin-settings.json"},
            {"path": str(cody_root / ".codex/config.toml"), "sha256": sha((cody_root / ".codex/config.toml").read_bytes()), "projection": "preimage/project-codex-features.txt"},
            *global_guidance,
        ],
        "catch_me_up_replacement": {
            "old_caller": legacy_list,
            "new_caller": replacement,
            "old_removed": legacy_list not in catch_post,
            "full_body_preimage": "preimage/catch-me-up-full.md",
        },
        "files": [],
    }
    for path in sorted(out.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            manifest["files"].append({"path": str(path.relative_to(out)), "sha256": sha(path.read_bytes())})
    write(out / "manifest.json", pretty(manifest))
    print(json.dumps({"output": str(out), "files": len(manifest["files"]), "live_settings_changed": False}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cody-root", default="/home/dev/beads/cody-ops")
    parser.add_argument("--home-root", default="/home/dev")
    parser.add_argument("--catch-skill", default="/home/dev/.claude/skills/catch-me-up/SKILL.md")
    args = parser.parse_args()
    try:
        prepare(args)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"prepare caller templates: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
