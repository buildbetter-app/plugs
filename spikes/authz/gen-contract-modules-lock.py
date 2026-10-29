#!/usr/bin/env python3
"""Pin the offline contract exporter's proxy files to the unchanged go.sum.

Run manually with network access, never during acceptance:
  python3 spikes/authz/gen-contract-modules-lock.py --download-dir /tmp/authz-proxy

Each .mod and .zip named by go.sum is checked against its Go h1 checksum
before its raw SHA-256 enters the deterministic lock. --check compares with
the existing lock instead of writing it. --download-dir retains the verified
proxy files for maintenance/testing; it is not an acceptance prerequisite.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
from pathlib import Path
import sys
import urllib.request
import zipfile


SOURCE = Path(__file__).resolve().parent
PROXY = "https://proxy.golang.org"


def sha256(contents):
    return hashlib.sha256(contents).hexdigest()


def escape(path):
    return "".join("!" + char.lower() if char.isupper() else char for char in path)


def h1(files):
    # golang.org/x/mod/sumdb/dirhash.Hash1: sorted names, per-file hex digest,
    # two spaces, name and newline; hash that list and encode it as h1.
    entries = []
    for name, contents in sorted(files):
        if "\n" in name:
            raise ValueError("newline in module filename")
        entries.append(f"{sha256(contents)}  {name}\n")
    result = hashlib.sha256("".join(entries).encode()).digest()
    return "h1:" + base64.b64encode(result).decode()


def read_sums(contents):
    modules = {}
    for line in contents.decode().splitlines():
        path, version, checksum = line.split()
        extension = "mod" if version.endswith("/go.mod") else "zip"
        version = version.removesuffix("/go.mod")
        key = (path, version)
        record = modules.setdefault(key, {})
        if extension in record:
            raise ValueError(f"duplicate checksum: {path}@{version}.{extension}")
        if not checksum.startswith("h1:") or len(base64.b64decode(checksum[3:], validate=True)) != 32:
            raise ValueError(f"invalid h1 checksum: {path}@{version}")
        record[extension] = checksum
    for (path, version), record in modules.items():
        if "mod" not in record:
            raise ValueError(f"no go.mod checksum for {path}@{version}")
    return modules


def fetch_verified(path, version, extension, expected_h1, download_dir):
    relative = f"{escape(path)}/@v/{escape(version)}.{extension}"
    retained = download_dir / relative if download_dir else None
    if retained and retained.is_file():
        contents = retained.read_bytes()
    else:
        with urllib.request.urlopen(f"{PROXY}/{relative}", timeout=60) as response:
            contents = response.read()
    if extension == "mod":
        actual_h1 = h1([("go.mod", contents)])
    else:
        with zipfile.ZipFile(io.BytesIO(contents)) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)):
                raise ValueError(f"duplicate archive member: {path}@{version}")
            prefix = f"{path}@{version}/"
            if any(not name.startswith(prefix) for name in names):
                raise ValueError(f"unexpected archive prefix: {path}@{version}")
            actual_h1 = h1((name, archive.read(name)) for name in names)
    if actual_h1 != expected_h1:
        raise ValueError(f"go.sum checksum mismatch: {path}@{version}.{extension}")
    if retained:
        retained.parent.mkdir(parents=True, exist_ok=True)
        retained.write_bytes(contents)
    return sha256(contents)


def module_record(item, download_dir):
    (path, version), sums = item
    record = {"path": path, "version": version}
    for extension, checksum in sorted(sums.items()):
        record[f"{extension}_sha256"] = fetch_verified(path, version, extension, checksum, download_dir)
    if download_dir:
        info = download_dir / f"{escape(path)}/@v/{escape(version)}.info"
        info.write_text(json.dumps({"Version": version}, separators=(",", ":")) + "\n")
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download-dir", type=Path)
    parser.add_argument("--output", type=Path, default=SOURCE / "contract-modules.lock.json")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    go_mod = (SOURCE / "go.mod").read_bytes()
    go_sum = (SOURCE / "go.sum").read_bytes()
    sums = read_sums(go_sum)
    # Parallel downloads do not affect sorted records or serialized bytes.
    with ThreadPoolExecutor(max_workers=4) as pool:
        modules = list(pool.map(lambda item: module_record(item, args.download_dir), sorted(sums.items())))
    lock = {
        "schema": "plug-authz-contract-modules/v1",
        "source": {
            "module": go_mod.decode().splitlines()[0].removeprefix("module "),
            "go_mod_sha256": sha256(go_mod),
            "go_sum_sha256": sha256(go_sum),
        },
        "toolchain": {"version": "go1.26.10", "goos": "linux", "goarch": "amd64"},
        "modules": modules,
    }
    output = json.dumps(lock, indent=2, sort_keys=True) + "\n"
    if args.check:
        if args.output.read_text() != output:
            raise ValueError("contract module lock is stale")
    else:
        args.output.write_text(output)
    if args.download_dir:
        (args.download_dir / "ROOT").write_text(output)
    print(f"verified {len(modules)} modules, {sum('zip_sha256' in m for m in modules)} archives against go.sum")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, zipfile.BadZipFile) as error:
        sys.exit(f"contract module lock: {error}")
