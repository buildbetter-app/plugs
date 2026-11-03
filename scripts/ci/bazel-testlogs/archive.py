#!/usr/bin/env python3
"""Retain Bazel logs and actual undeclared outputs in the existing testlog tar.

The inventory describes bytes found after the test, including failed tests.
It does not infer an expected sink, manufacture a transcript, or qualify the
test result. Original outputs.zip containers and unzipped files are preserved.
"""

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import stat
import sys
import tarfile
import tempfile
import zipfile
import zlib


INVENTORY = "undeclared-outputs.json"
CAPTURE_ERROR = 86


def digest(data):
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def read_regular(root, relative):
    # Constrain every component below Bazel's resolved convenience link. A
    # test must not redirect collection through a nested symlink, including a
    # link swapped in between discovery and opening the actual output file.
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        parts = PurePosixPath(relative).parts
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(descriptor, "rb") as source:
            mode = os.fstat(source.fileno()).st_mode
            if not stat.S_ISREG(mode):
                raise ValueError("selected evidence is not a regular file")
            return source.read(), stat.S_IMODE(mode)
    finally:
        os.close(directory)


def output_location(path):
    parts = PurePosixPath(path).parts
    for index, part in enumerate(parts):
        if part.endswith(".outputs"):
            parent = PurePosixPath(*parts[:index])
            stem = part.removesuffix(".outputs")
            identity = parent if stem == "test" else parent / stem
            return index, identity.as_posix()
    return None


def output_record(path, data, *, container=None, member_index=None, name=None):
    parts = PurePosixPath(path).parts
    index, identity = output_location(path)
    row = {
        "test": identity,
        "name": name if name is not None else PurePosixPath(*parts[index + 1:]).as_posix(),
        "path": path,
        **digest(data),
    }
    if container is not None:
        row.update(container=container, member_index=member_index)
    return row


def inspect_zip(path, data, inventory):
    """Hash original member bytes without extracting or rewriting the ZIP."""
    prefix = str(PurePosixPath(path).parent)
    seen = set()
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for index, member in enumerate(archive.infolist()):
                if member.is_dir():
                    continue
                name = PurePosixPath(member.filename)
                if (name.is_absolute() or ".." in name.parts or
                        "\\" in member.filename or not name.parts or
                        member.filename in seen or
                        stat.S_ISLNK(member.external_attr >> 16)):
                    raise ValueError("ambiguous or non-file output member: " + member.filename)
                seen.add(member.filename)
                raw = archive.read(member)  # Includes the original ZIP CRC check.
                inventory["outputs"].append(output_record(
                    prefix + "/" + member.filename, raw,
                    container=path, member_index=index, name=member.filename,
                ))
    except (OSError, ValueError, RuntimeError, EOFError, zipfile.BadZipFile, zlib.error, NotImplementedError) as exc:
        inventory["errors"].append({"path": path, "error": str(exc)})


def archive_testlogs(testlogs, output):
    # Bazel's workspace-level convenience symlink is the normal entry point.
    root = testlogs.resolve()
    inventory = {"version": 1, "files": [], "outputs": [], "errors": []}
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=output.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        with temporary_path.open("wb") as raw_archive:
            with gzip.GzipFile(filename="", fileobj=raw_archive, mode="wb", mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                    if root.exists() and not root.is_dir():
                        inventory["errors"].append({"path": ".", "error": "testlogs is not a directory"})
                    paths = sorted(root.rglob("*")) if root.is_dir() else []
                    for source in paths:
                        relative = source.relative_to(root).as_posix()
                        parts = PurePosixPath(relative).parts
                        try:
                            mode = source.lstat().st_mode
                            if stat.S_ISDIR(mode):
                                continue
                            original_link = None
                            read_path = relative
                            if stat.S_ISLNK(mode):
                                target = source.resolve(strict=True)
                                if not target.is_relative_to(root) or not target.is_file():
                                    raise ValueError("evidence link is not a regular file inside this testlog tree")
                                original_link = os.readlink(source)
                                read_path = target.relative_to(root).as_posix()
                            elif not stat.S_ISREG(mode):
                                raise ValueError("selected evidence is not a regular file")
                            data, original_mode = read_regular(root, read_path)
                            member = tarfile.TarInfo("./" + relative)
                            member.size = len(data)
                            member.mode = original_mode
                            archive.addfile(member, io.BytesIO(data))
                            file_record = {"path": relative, **digest(data)}
                            if original_link is not None:
                                file_record.update(original_symlink=original_link, snapshot_source=read_path)
                            inventory["files"].append(file_record)
                            location = output_location(relative)
                            if location is None:
                                continue
                            index, _ = location
                            if parts[index + 1:] == ("outputs.zip",):
                                inspect_zip(relative, data, inventory)
                            else:
                                inventory["outputs"].append(output_record(relative, data))
                        except (OSError, ValueError) as exc:
                            inventory["errors"].append({"path": relative, "error": str(exc)})
                    inventory["complete"] = not inventory["errors"]
                    body = (json.dumps(inventory, ensure_ascii=False, indent=2) + "\n").encode()
                    member = tarfile.TarInfo(INVENTORY)
                    member.size, member.mode = len(body), 0o644
                    archive.addfile(member, io.BytesIO(body))
        # Readers get one complete tarball, including the incomplete inventory
        # and any surviving original files when output inspection has failed.
        temporary_path.replace(output)
    finally:
        temporary_path.unlink(missing_ok=True)
    return inventory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--testlogs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    inventory = archive_testlogs(args.testlogs, args.output)
    for error in inventory["errors"]:
        print("Bazel output archive: " + error["path"] + ": " + error["error"], file=sys.stderr)
    return 0 if inventory["complete"] else CAPTURE_ERROR


if __name__ == "__main__":
    sys.exit(main())
