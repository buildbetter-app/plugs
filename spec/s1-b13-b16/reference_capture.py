#!/usr/bin/env python3
"""RunsOn-only capture of the pinned public B13-B16 reference packet."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
import zipfile


PINNED_COMMIT = "9d6fff760b7a314b476539198504d77ce0959656"
PINNED_TREE = "cde121d6e4119e454857ad1c3ca293e2791be840"
ARTIFACT_RUN = "41361573924"
ARTIFACT_ID = "13105007501"
ARTIFACT_URLS = [
    f"https://api.github.com/repos/buildbetter-app/plugs/actions/artifacts/{ARTIFACT_ID}/zip",
    f"https://github.com/buildbetter-app/plugs/actions/artifacts/{ARTIFACT_ID}/zip",
    f"https://github.com/buildbetter-app/plugs/actions/runs/{ARTIFACT_RUN}/artifacts/{ARTIFACT_ID}/zip",
]
BINARY_SHA256 = "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4"
BINARY_SIZE = 112341314
ARCHIVE_SHA256 = "63f31a9cec53d7fd0ef538795814822c809b0398627c1c4c4ed5bca1258b95dc"
ARCHIVE_SIZE = 132280320
RECORDER_RELATIVE = "spec/s1-b13-b16/record_reference.py"
EXPECTED_FILES = {
    "bd-version.json",
    "bin/bd",
    "bin/plug",
    "bundle.tar",
    "manifest.json",
    "plug-help.txt",
    "receipt.json",
}
ARTIFACT_OBJECT_HOSTS = {
    "objects.githubusercontent.com",
    "release-assets.githubusercontent.com",
    *(f"productionresultssa{index}.blob.core.windows.net" for index in range(10)),
}
REDIRECT_CODES = {301, 302, 303, 307, 308}


def artifact_destination(url: str) -> str:
    """Admit only the pinned API route and trusted public artifact origins."""
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127
           for character in url) or "\\" in url:
        raise RuntimeError("unsupported artifact destination")
    try:
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.username is not None or parsed.password is not None
                or parsed.fragment or parsed.port not in (None, 443)):
            raise ValueError("unsupported route")
    except ValueError:
        raise RuntimeError("unsupported artifact destination") from None
    if parsed.hostname == "api.github.com" and parsed.path == urlsplit(ARTIFACT_URLS[0]).path and not parsed.query:
        return "api"
    if parsed.hostname == "github.com" and not parsed.query and parsed.path in {
        urlsplit(value).path for value in ARTIFACT_URLS[1:]
    }:
        return "public"
    if parsed.hostname in ARTIFACT_OBJECT_HOSTS:
        return "public"
    raise RuntimeError("unsupported artifact destination")


class NoArtifactRedirect(urllib.request.HTTPRedirectHandler):
    def http_error_302(self, request, fp, code, msg, headers):
        # Return the untouched response; urllib must never construct the next request.
        return fp

    http_error_301 = http_error_302
    http_error_303 = http_error_302
    http_error_307 = http_error_302
    http_error_308 = http_error_302


def artifact_response(opener, url: str, token: str | None):
    for hop in range(6):
        route = artifact_destination(url)
        if hop and route == "api":
            raise RuntimeError("artifact redirect to the authenticated API is unsupported")
        request = urllib.request.Request(url, headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "plug-s1-b13-b16-reference-capture",
        })
        if route == "api" and token:
            request.add_unredirected_header("Authorization", f"Bearer {token}")
        try:
            response = opener.open(request, timeout=180)
        except HTTPError as error:
            if error.code not in REDIRECT_CODES:
                raise
            response = error
        status = response.code
        if status not in REDIRECT_CODES:
            return response
        try:
            location = response.headers.get("Location")
            if not location:
                raise RuntimeError("artifact redirect has no destination")
            next_url = urljoin(url, location)
            artifact_destination(next_url)
        finally:
            response.close()
        url = next_url
    raise RuntimeError("artifact redirect limit exceeded")


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def command_receipt(argv: list[str], cwd: Path, env: dict[str, str], receipts: list[dict[str, object]]) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, check=False)
    receipts.append(
        {
            "argv": argv,
            "cwd": str(cwd),
            "exitCode": result.returncode,
            "stdoutSha256": digest(result.stdout),
            "stderrSha256": digest(result.stderr),
        }
    )
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(argv)}")
    return result


def safe_zip_name(name: str) -> bool:
    path = PurePosixPath(name)
    return bool(name) and not path.is_absolute() and str(path) == name and all(part not in {"", ".", ".."} for part in path.parts)


def locate_recorder() -> Path:
    candidates = [Path(__file__).resolve().with_name("record_reference.py")]
    runfiles = os.environ.get("RUNFILES_DIR")
    if runfiles:
        candidates.append(Path(runfiles) / "plugs" / RECORDER_RELATIVE)
    manifest = os.environ.get("RUNFILES_MANIFEST_FILE")
    if manifest and Path(manifest).is_file():
        for line in Path(manifest).read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition(" ")
            if key.endswith(RECORDER_RELATIVE):
                candidates.append(Path(value))
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise RuntimeError("record_reference.py is not present in the reference_capture runfiles")


def download_bundle(root: Path, provided_url: str | None) -> tuple[Path, dict[str, object]]:
    archive_zip = root / "public-artifact.zip"
    download_attempts = []
    # The Actions runtime bearer belongs to a different service and is never used here.
    auth_env_name = next((name for name in ("GITHUB_TOKEN", "GH_TOKEN") if os.environ.get(name)), None)
    token = os.environ[auth_env_name] if auth_env_name else None
    url_entries = ([("provided-immutable-url", provided_url)] if provided_url is not None
                   else [("public-url", url) for url in ARTIFACT_URLS])
    # Disable environment-selected proxies and automatic redirect/header propagation.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoArtifactRedirect())
    for url_ref, url in url_entries:
        route = artifact_destination(url)
        auth_env_names = [auth_env_name] if auth_env_name and route == "api" else []
        try:
            with artifact_response(opener, url, token) as response, archive_zip.open("wb") as stream:
                shutil.copyfileobj(response, stream)
            download_attempts.append({"urlRef": url_ref, "status": "downloaded", "authEnvNames": auth_env_names})
            break
        except (HTTPError, URLError) as error:
            if isinstance(error, HTTPError):
                detail = f"http-{error.code}"
            else:
                detail = f"url-error-{type(error.reason).__name__}"
            download_attempts.append({"urlRef": url_ref, "status": "refused", "error": detail, "authEnvNames": auth_env_names})
    else:
        raise RuntimeError(f"all immutable public artifact download URLs refused: {download_attempts}")
    zip_bytes = archive_zip.read_bytes()
    extracted = root / "public-bundle"
    extracted.mkdir()
    with zipfile.ZipFile(archive_zip) as bundle:
        members = [item for item in bundle.infolist() if not item.is_dir()]
        if not members:
            raise RuntimeError("public artifact ZIP is empty")
        names = [item.filename for item in members]
        if any(not safe_zip_name(name) for name in names):
            raise RuntimeError(f"public artifact ZIP has unsafe member names: {names}")
        roots = {PurePosixPath(name).parts[0] for name in names if len(PurePosixPath(name).parts) > 1}
        prefix = next(iter(roots)) + "/" if len(roots) == 1 and all(len(PurePosixPath(name).parts) > 1 for name in names) else ""
        normalized: dict[str, zipfile.ZipInfo] = {}
        for item in members:
            relative = item.filename[len(prefix):] if prefix and item.filename.startswith(prefix) else item.filename
            if relative in normalized or relative not in EXPECTED_FILES:
                raise RuntimeError(f"public artifact ZIP members are not the expected exact set: {names}")
            mode = (item.external_attr >> 16) & 0o170000
            if mode not in (0, stat.S_IFREG):
                raise RuntimeError(f"public artifact ZIP contains a non-regular file: {item.filename}")
            normalized[relative] = item
        if set(normalized) != EXPECTED_FILES:
            raise RuntimeError(f"public artifact ZIP members are incomplete: {sorted(normalized)}")
        for relative, item in normalized.items():
            destination = extracted / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(item) as source, destination.open("xb") as target:
                shutil.copyfileobj(source, target)

    manifest_bytes = (extracted / "manifest.json").read_bytes()
    receipt_bytes = (extracted / "receipt.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    receipt = json.loads(receipt_bytes)
    if manifest != receipt:
        raise RuntimeError("public artifact manifest and receipt differ")
    if manifest.get("schema") != "plug-public-bundle/1":
        raise RuntimeError("unexpected public artifact schema")
    source = manifest.get("source", {}).get("beads", {})
    if source.get("commit") != PINNED_COMMIT or source.get("tree") != PINNED_TREE:
        raise RuntimeError(f"public artifact source is {source}")
    archive_record = manifest.get("archive", {})
    archive = extracted / "bundle.tar"
    if archive_record.get("name") != "bundle.tar" or archive_record.get("sha256") != ARCHIVE_SHA256 or archive_record.get("size") != ARCHIVE_SIZE:
        raise RuntimeError(f"public artifact archive manifest is {archive_record}")
    if digest(archive.read_bytes()) != ARCHIVE_SHA256 or archive.stat().st_size != ARCHIVE_SIZE:
        raise RuntimeError("public artifact archive digest or size mismatch")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != {"bin/bd", "bin/plug"}:
        raise RuntimeError("public artifact executable manifest set is not exact")
    artifact_record = artifacts.get("bin/bd", {})
    if (not isinstance(artifact_record, dict) or artifact_record.get("sha256") != BINARY_SHA256
            or artifact_record.get("size") != BINARY_SIZE or artifact_record.get("mode") != "0o755"):
        raise RuntimeError(f"public artifact binary manifest is {artifact_record}")
    binary = extracted / "bin" / "bd"
    # Artifact ZIPs strip executable mode. Validate their bytes, then reconstruct
    # modes only from the independently hash-pinned tar, never from ZIP metadata.
    for name, record in artifacts.items():
        destination = extracted / name
        if (not isinstance(record, dict) or record.get("mode") != "0o755"
                or destination.is_symlink() or not destination.is_file()
                or destination.stat().st_size != record.get("size")
                or digest(destination.read_bytes()) != record.get("sha256")):
            raise RuntimeError(f"public artifact loose executable hash, size or mode record mismatch: {name}")
    executable_payloads = {}
    with tarfile.open(archive, "r") as stream:
        members = stream.getmembers()
        if {item.name for item in members} != {"bin/bd", "bin/plug"} or len(members) != 2:
            raise RuntimeError("public artifact archive members are not exact")
        for item in members:
            if not item.isreg() or stat.S_IMODE(item.mode) != 0o755 or not safe_zip_name(item.name):
                raise RuntimeError(f"unsafe public archive member: {item.name}")
            payload = stream.extractfile(item)
            if payload is None:
                raise RuntimeError(f"archive member has no payload: {item.name}")
            member_bytes = payload.read()
            record = artifacts[item.name]
            if len(member_bytes) != record.get("size") or digest(member_bytes) != record.get("sha256"):
                raise RuntimeError(f"archive executable hash or size mismatch: {item.name}")
            executable_payloads[item.name] = member_bytes
    version = json.loads((extracted / "bd-version.json").read_bytes())
    if version.get("commit") != PINNED_COMMIT:
        raise RuntimeError(f"bd version metadata is {version}")
    resolution = manifest.get("resolution", {})
    if digest((extracted / "bd-version.json").read_bytes()) != resolution.get("beads_version_sha256"):
        raise RuntimeError("bd version metadata digest mismatch")
    if digest((extracted / "plug-help.txt").read_bytes()) != resolution.get("plug_help_sha256"):
        raise RuntimeError("plug help digest mismatch")
    # All member, source, archive, binary and resolver checks precede any execute bit.
    for name, payload in executable_payloads.items():
        destination = extracted / name
        destination.write_bytes(payload)
        destination.chmod(0o755)
        if destination.is_symlink() or not destination.is_file() or stat.S_IMODE(destination.stat().st_mode) != 0o755:
            raise RuntimeError("public artifact executable reconstruction failed")
    return binary, {
        "runId": ARTIFACT_RUN,
        "artifactId": ARTIFACT_ID,
        "downloadUrls": ARTIFACT_URLS,
        "providedUrlSha256": digest(provided_url.encode()) if provided_url else None,
        "downloadSha256": digest(zip_bytes),
        "downloadSize": len(zip_bytes),
        "downloadAttempts": download_attempts,
        "manifestSha256": digest(manifest_bytes),
        "receiptSha256": digest(receipt_bytes),
        "archive": archive_record,
        "binary": artifact_record,
        "source": source,
        "build": manifest.get("build", {}),
        "toolchain": manifest.get("toolchain", {}),
        "mode": "public-pinned-go-packaged",
        "canonicalBazel": False,
    }


def fetch_source(root: Path, receipts: list[dict[str, object]], env: dict[str, str]) -> Path:
    source = root / "beads-source"
    source.mkdir()
    command_receipt(["git", "init", "-q"], source, env, receipts)
    command_receipt(["git", "-C", str(source), "remote", "add", "origin", "https://github.com/buildbetter-app/plugs.git"], root, env, receipts)
    command_receipt(["git", "-C", str(source), "fetch", "--no-tags", "--depth=1", "origin", PINNED_COMMIT], root, env, receipts)
    command_receipt(["git", "-C", str(source), "checkout", "--detach", "FETCH_HEAD"], root, env, receipts)
    commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], env=env, text=True).strip()
    tree = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"], env=env, text=True).strip()
    clean = subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"], env=env, text=True)
    if commit != PINNED_COMMIT or tree != PINNED_TREE or clean:
        raise RuntimeError(f"fetched source is not the exact clean pin: {commit}/{tree} clean={not clean}")
    return source


def main() -> int:
    args = sys.argv[1:]
    if not args:
        provided_url = None
    elif len(args) == 2 and args[0] == "--artifact-url":
        provided_url = args[1]
    else:
        print("usage: reference_capture.py [--artifact-url IMMUTABLE_SIGNED_URL]", file=sys.stderr)
        return 64
    root = Path(tempfile.mkdtemp(prefix="s1-b13-b16-reference-"))
    receipts: list[dict[str, object]] = []
    try:
        binary, bundle = download_bundle(root, provided_url)
        env = dict(os.environ)
        source = fetch_source(root, receipts, env)
        recorder = locate_recorder()
        recorder_bytes = recorder.read_bytes()
        output = root / "record.json"
        workspace = root / "recorder-work"
        command = [
            sys.executable,
            str(recorder),
            "--reference-binary",
            str(binary),
            "--binary-sha256",
            BINARY_SHA256,
            "--source",
            str(source),
            "--workspace",
            str(workspace),
            "--output",
            str(output),
        ]
        result = subprocess.run(command, cwd=root, env=env, capture_output=True, check=False, text=True)
        recorder_receipt = {
            "argv": command,
            "cwd": str(root),
            "exitCode": result.returncode,
            "stdoutSha256": digest(result.stdout.encode()),
            "stderrSha256": digest(result.stderr.encode()),
        }
        if result.returncode:
            raise RuntimeError(f"reference recorder failed: {result.returncode}: {result.stderr[-2000:]}")
        record = json.loads(output.read_bytes())
        recordings = record.get("recordings")
        if not isinstance(recordings, list) or len(recordings) != 2:
            raise RuntimeError("reference recorder did not produce two recordings")
        projection_digests = [item.get("projectionSha256") for item in recordings]
        if recordings[0].get("projection") != recordings[1].get("projection") or projection_digests[0] != projection_digests[1]:
            raise RuntimeError("independent reference projections differ")
        if any(item.get("exitCode", 1) != 0 for run in recordings for item in run.get("commands", [])):
            raise RuntimeError("reference command receipt contains a nonzero command")
        packet = {
            "schema": "plug.s1-b13-b16.reference-packet.v1",
            "cards": record.get("cards"),
            "bundle": bundle,
            "source": {
                "commit": record.get("sourceCommit"),
                "tree": record.get("sourceTree"),
                "fetchReceipts": receipts,
            },
            "recorder": {
                "path": RECORDER_RELATIVE,
                "sha256": digest(recorder_bytes),
                "receipt": recorder_receipt,
                "recordSha256": digest(output.read_bytes()),
            },
            "recordings": recordings,
            "projectionTwinSha256": projection_digests,
            "projectionEqual": True,
            "limits": record.get("limits"),
        }
        payload = canonical(packet)
        print("S1_REFERENCE_PACKET_BEGIN")
        print(payload.decode())
        print("S1_REFERENCE_PACKET_END")
        return 0
    except Exception as error:
        blocker = {
            "schema": "plug.s1-b13-b16.reference-blocker.v1",
            "artifact": {
                "runId": ARTIFACT_RUN,
                "artifactId": ARTIFACT_ID,
                "downloadUrls": ARTIFACT_URLS,
                "providedUrlSupplied": bool(provided_url),
            },
            "commands": receipts,
            "error": str(error),
        }
        print("S1_REFERENCE_BLOCKER_BEGIN")
        print(canonical(blocker).decode())
        print("S1_REFERENCE_BLOCKER_END")
        return 1
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
