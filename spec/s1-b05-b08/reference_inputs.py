"""Admission checks only; accepted inputs are not proof of a recording."""

import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess


COMMIT = "9d6fff760b7a314b476539198504d77ce0959656"
TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
SOURCE_MANIFEST = "23d84a56302ad19c50d9d25af733b2da50f9137aaf9308b273cab039b6023ae3"
SOURCE_ENTRIES = 5087


def ordinary_path(value, *, directory=False):
    if not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise RuntimeError("input path is missing or malformed")
    path = Path(value)
    if (not path.is_absolute() or str(path) != value
            or path.resolve(strict=True) != path
            or (not path.is_dir() if directory else not path.is_file())):
        raise RuntimeError("input must be an absolute ordinary path without symlinks")
    return path


def neutral_environment():
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8", "TZ": "UTC",
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0", "GIT_NO_REPLACE_OBJECTS": "1",
        "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
    }


def git(source, *arguments):
    result = subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
         "-c", "core.logAllRefUpdates=false", "-c", "gc.auto=0", "-c", "credential.helper=",
         "-C", str(source), *arguments], env=neutral_environment(),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=120,
    )
    if result.returncode:
        raise RuntimeError("cannot verify pinned reference Git source")
    return result.stdout


def verify_source(source):
    source = ordinary_path(str(source), directory=True)
    metadata = source / ".git"
    if metadata.is_symlink() or not metadata.is_dir() or any(p.is_symlink() for p in metadata.rglob("*")):
        raise RuntimeError("reference source must have private ordinary Git metadata")
    for name in ("commondir", "gitdir", "FETCH_HEAD", "logs", "refs/replace",
                 "objects/info/alternates", "objects/info/http-alternates"):
        if (metadata / name).exists():
            raise RuntimeError("reference source retains external or unexpected Git state")
    config = git(source, "config", "--local", "--list").decode().splitlines()
    allowed = {"core.repositoryformatversion", "core.filemode", "core.bare", "core.logallrefupdates"}
    if (any(line.split("=", 1)[0] not in allowed for line in config)
            or "core.filemode=true" not in config or "core.bare=false" not in config):
        raise RuntimeError("reference source has unsupported Git configuration")
    if (git(source, "rev-parse", "HEAD").decode().strip() != COMMIT
            or git(source, "rev-parse", "HEAD^{tree}").decode().strip() != TREE):
        raise RuntimeError("reference source is not the pinned commit/tree")
    if (git(source, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching")
            or any(row[:2] != b"H " for row in git(source, "ls-files", "-v").splitlines())):
        raise RuntimeError("reference source is dirty or has hidden index entries")
    manifest = git(source, "ls-tree", "-r", "--full-tree", "HEAD")
    if hashlib.sha256(manifest).hexdigest() != SOURCE_MANIFEST or len(manifest.splitlines()) != SOURCE_ENTRIES:
        raise RuntimeError("reference source manifest differs from the pin")
    # An index stat cache can hide same-size, same-time edits after copying.
    # Compare actual blob bytes and executable flags, independently of status.
    for entry in git(source, "ls-tree", "-rz", "--full-tree", "HEAD").split(b"\0"):
        if not entry:
            continue
        header, name = entry.split(b"\t", 1)
        mode, kind, object_id = header.split()
        relative = Path(os.fsdecode(name))
        if kind != b"blob" or relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("reference tree contains an unsupported entry")
        path = source / relative
        if not path.resolve(strict=True).is_relative_to(source):
            raise RuntimeError("reference source link leaves its private checkout")
        actual_mode = path.lstat().st_mode
        if mode in {b"100644", b"100755"} and stat.S_ISREG(actual_mode):
            if bool(actual_mode & 0o111) != (mode == b"100755"):
                raise RuntimeError("reference source executable flag differs from its tree")
            contents = path.read_bytes()
        elif mode == b"120000" and stat.S_ISLNK(actual_mode):
            contents = os.fsencode(os.readlink(path))
        else:
            raise RuntimeError("reference source file type differs from its tree")
        blob = b"blob " + str(len(contents)).encode("ascii") + b"\0" + contents
        if hashlib.sha1(blob).hexdigest().encode("ascii") != object_id:
            raise RuntimeError("reference source bytes differ from its pinned tree")
    return {"sourceCommit": COMMIT, "sourceTree": TREE, "sourceManifestSha256": SOURCE_MANIFEST}


def verify_clock_proof(path, binary_digest):
    path = ordinary_path(str(path))
    if path.stat().st_size > 1024 * 1024:
        raise RuntimeError("clock proof exceeds its bound")
    raw = path.read_bytes()
    proof = json.loads(raw)
    if (not isinstance(proof, dict) or proof.get("sourceCommit") != COMMIT
            or proof.get("sourceTree") != TREE or proof.get("binarySha256") != binary_digest):
        raise RuntimeError("clock proof is not bound to the pinned source/executable")
    if proof.get("mode") != "go-sql-controlled":
        raise RuntimeError("clock proof does not declare Go+SQL control")
    for name in ("go", "sql"):
        clock = proof.get(name)
        reads = clock.get("controlledReads") if isinstance(clock, dict) else None
        if type(reads) is not int or reads <= 0:
            raise RuntimeError(f"clock proof has no controlled {name} clock reads")
    schedule = proof.get("scheduleSha256")
    if not isinstance(schedule, str) or len(schedule) != 64 or any(c not in "0123456789abcdef" for c in schedule):
        raise RuntimeError("clock proof has no schedule digest")
    return proof, hashlib.sha256(raw).hexdigest()
