#!/usr/bin/env python3
"""Synthetic transport/extraction tests; never downloads or executes a reference."""
from __future__ import annotations

from contextlib import contextmanager
from email.message import Message
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import stat
import sys
import tarfile
import tempfile
import unittest
from unittest import mock
import urllib.request
from urllib.response import addinfourl
import zipfile

spec = importlib.util.spec_from_file_location("capture_under_test", sys.argv[1])
assert spec is not None and spec.loader is not None
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)

API = capture.ARTIFACT_URLS[0]
OBJECT = "https://productionresultssa0.blob.core.windows.net/fixture/bundle.zip?sig=synthetic"
OTHER_OBJECT = "https://objects.githubusercontent.com/fixture/bundle.zip?signature=synthetic"
FAKE_ENV = {
    "GITHUB_TOKEN": "fixture-github-marker",
    "GH_TOKEN": "fixture-gh-marker",
    "ACTIONS_RUNTIME_TOKEN": "fixture-runtime-marker",
}
BD_BYTES = b"synthetic bd payload; never executed\n"
PLUG_BYTES = b"synthetic plug payload; never executed\n"


def sha(value):
    return hashlib.sha256(value).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True).encode()


class Bundle:
    """Tiny public test bytes with test-only binary/archive pins."""
    def __init__(self, members=None):
        if members is None:
            members = [("bin/bd", BD_BYTES, 0o755, tarfile.REGTYPE),
                       ("bin/plug", PLUG_BYTES, 0o755, tarfile.REGTYPE)]
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w", format=tarfile.USTAR_FORMAT) as stream:
            for name, payload, mode, kind in members:
                item = tarfile.TarInfo(name)
                item.mode = mode
                item.type = kind
                item.size = len(payload) if item.isreg() else 0
                if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                    item.linkname = "../outside"
                stream.addfile(item, io.BytesIO(payload) if item.isreg() else None)
        archive_bytes = archive.getvalue()
        self.pins = {"ARCHIVE_SHA256": sha(archive_bytes), "ARCHIVE_SIZE": len(archive_bytes),
                     "BINARY_SHA256": sha(BD_BYTES), "BINARY_SIZE": len(BD_BYTES)}
        self.files = {
            "bin/bd": BD_BYTES, "bin/plug": PLUG_BYTES, "bundle.tar": archive_bytes,
            "bd-version.json": encoded({"commit": capture.PINNED_COMMIT}),
            "plug-help.txt": b"synthetic help; not a runtime probe\n",
        }
        self.manifest = {
            "schema": "plug-public-bundle/1",
            "source": {"beads": {"commit": capture.PINNED_COMMIT, "tree": capture.PINNED_TREE}},
            "archive": {"name": "bundle.tar", "sha256": sha(archive_bytes), "size": len(archive_bytes)},
            "artifacts": {name: {"sha256": sha(payload), "size": len(payload), "mode": "0o755"}
                          for name, payload in (("bin/bd", BD_BYTES), ("bin/plug", PLUG_BYTES))},
            "resolution": {"beads_version_sha256": sha(self.files["bd-version.json"]),
                           "plug_help_sha256": sha(self.files["plug-help.txt"])},
            "build": {"beads": "synthetic fixture only"},
        }

    def zip_bytes(self, *, prefix="", mode=0o644, duplicate=False, unsafe=None):
        files = dict(self.files)
        files.setdefault("manifest.json", encoded(self.manifest))
        files.setdefault("receipt.json", encoded(self.manifest))
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as bundle:
            entries = list(files.items())
            if duplicate:
                entries.append(("bin/bd", BD_BYTES))
            for name, payload in entries:
                item = zipfile.ZipInfo(prefix + name)
                item.create_system = 3
                item.external_attr = ((unsafe.get(name, stat.S_IFREG) if unsafe else stat.S_IFREG) | mode) << 16
                bundle.writestr(item, payload)
        return stream.getvalue()


class InterceptedTransport(urllib.request.BaseHandler):
    handler_order = 100

    def __init__(self, routes):
        self.routes = {url: list(responses) for url, responses in routes.items()}
        self.calls = []

    def https_open(self, request):
        authorization = request.get_header("Authorization")
        self.calls.append({"url": request.full_url, "hasAuthorization": authorization is not None,
                           "githubMarker": authorization == "Bearer " + FAKE_ENV["GITHUB_TOKEN"],
                           "ghMarker": authorization == "Bearer " + FAKE_ENV["GH_TOKEN"],
                           "runtimeMarker": authorization == "Bearer " + FAKE_ENV["ACTIONS_RUNTIME_TOKEN"]})
        if request.full_url not in self.routes or not self.routes[request.full_url]:
            raise AssertionError("unexpected intercepted transport destination")
        status, header_values, payload = self.routes[request.full_url].pop(0)
        headers = Message()
        for name, value in header_values.items():
            headers[name] = value
        response = addinfourl(io.BytesIO(payload), headers, request.full_url, status)
        response.msg = "synthetic response"
        return response

    http_open = https_open
    ftp_open = https_open


@contextmanager
def offline_transport(routes, *, env=None):
    transport = InterceptedTransport(routes)
    build_opener = urllib.request.build_opener

    def intercepted_opener(*handlers):
        return build_opener(urllib.request.ProxyHandler({}), transport, *handlers)

    def intercepted_urlopen(request, *args, **kwargs):
        return intercepted_opener().open(request, *args, **kwargs)

    with (mock.patch.dict(os.environ, FAKE_ENV if env is None else env, clear=True),
          mock.patch.object(socket, "socket", side_effect=AssertionError("real network forbidden")),
          mock.patch.object(socket, "create_connection", side_effect=AssertionError("real network forbidden")),
          mock.patch.object(urllib.request, "build_opener", side_effect=intercepted_opener),
          mock.patch.object(urllib.request, "urlopen", side_effect=intercepted_urlopen)):
        yield transport


class CaptureCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-capture-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bundle = Bundle()

    def download(self, url, routes, *, env=None):
        with (mock.patch.multiple(capture, **self.bundle.pins),
              offline_transport(routes, env=env) as transport):
            self.transport = transport
            return capture.download_bundle(self.root, url)

    def ok(self, payload=None):
        return (200, {}, self.bundle.zip_bytes() if payload is None else payload)

    def assert_refused(self, url, routes, *, env=None):
        try:
            self.download(url, routes, env=env)
        except RuntimeError as error:
            self.assertTrue(str(error), "refusal must have a reason")
        else:
            self.fail("unsafe fixture was accepted")
        for path in (self.root / "public-bundle" / "bin").glob("*"):
            self.assertEqual(stat.S_IMODE(path.lstat().st_mode) & 0o111, 0,
                             "refusal granted executable mode")
        return self.transport


class CaptureTransportTests(CaptureCase):
    def observe(self, url, routes, *, env=None):
        # Transport assertions must still run if the old extraction mode guard fails.
        try:
            self.download(url, routes, env=env)
        except RuntimeError:
            pass
        return self.transport.calls

    def test_exact_api_uses_github_token(self):
        calls = self.observe(API, {API: [self.ok()]})
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0]["githubMarker"], "exact API request lost its selected GitHub credential")

    def test_exact_api_can_use_gh_token(self):
        calls = self.observe(API, {API: [self.ok()]}, env={"GH_TOKEN": FAKE_ENV["GH_TOKEN"]})
        self.assertTrue(calls[0]["ghMarker"], "exact API request lost its GitHub CLI credential")

    def test_runtime_token_is_not_a_github_api_credential(self):
        calls = self.observe(API, {API: [self.ok()]}, env={"ACTIONS_RUNTIME_TOKEN": FAKE_ENV["ACTIONS_RUNTIME_TOKEN"]})
        self.assertFalse(calls[0]["hasAuthorization"], "runner runtime credential was sent to the GitHub API")

    def test_signed_object_has_no_ambient_credential(self):
        calls = self.observe(OBJECT, {OBJECT: [self.ok()]})
        self.assertFalse(calls[0]["hasAuthorization"], "signed object request carried ambient authorization")

    def test_public_web_route_has_no_ambient_credential(self):
        web = capture.ARTIFACT_URLS[1]
        calls = self.observe(web, {web: [self.ok()]})
        self.assertFalse(calls[0]["hasAuthorization"], "public web request carried ambient authorization")

    def test_api_redirect_to_object_strips_authorization(self):
        calls = self.observe(API, {API: [(302, {"Location": OBJECT}, b"")], OBJECT: [self.ok()]})
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[0]["githubMarker"])
        self.assertFalse(calls[1]["hasAuthorization"], "redirect forwarded an API credential to object storage")

    def test_public_redirect_stays_credential_free(self):
        calls = self.observe(OBJECT, {OBJECT: [(307, {"Location": OTHER_OBJECT}, b"")], OTHER_OBJECT: [self.ok()]})
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(not call["hasAuthorization"] for call in calls), "public redirect acquired authorization")

    def test_unsupported_routes_refuse_before_transport(self):
        base = self.root
        routes = ["https://untrusted.invalid/bundle.zip?sig=synthetic",
                  "http://api.github.com/repos/buildbetter-app/plugs/actions/artifacts/13105007501/zip",
                  "https://api.github.com:444/repos/buildbetter-app/plugs/actions/artifacts/13105007501/zip",
                  "https://user:password@api.github.com/repos/buildbetter-app/plugs/actions/artifacts/13105007501/zip",
                  "https://api.github.com.evil.invalid/repos/buildbetter-app/plugs/actions/artifacts/13105007501/zip",
                  "https://api.github.com/repos/other/repository/actions/artifacts/13105007501/zip",
                  "https://github.com/buildbetter-app/plugs/settings",
                  "https://objects.githubusercontent.com/fixture#fragment"]
        for index, url in enumerate(routes):
            with self.subTest(route=index):
                self.root = base / str(index)
                self.root.mkdir()
                calls = self.observe(url, {url: [self.ok()]})
                self.assertEqual(len(calls), 0, "unsupported destination reached transport")

    def test_redirect_to_unknown_origin_refuses_before_second_request(self):
        unknown = "https://untrusted.invalid/bundle.zip?sig=synthetic"
        calls = self.observe(API, {API: [(302, {"Location": unknown}, b"")], unknown: [self.ok()]})
        self.assertEqual(len(calls), 1, "redirect reached an unsupported origin")

    def test_redirect_downgrade_refuses_before_second_request(self):
        insecure = "http://objects.githubusercontent.com/fixture"
        calls = self.observe(API, {API: [(302, {"Location": insecure}, b"")], insecure: [self.ok()]})
        self.assertEqual(len(calls), 1, "redirect downgraded to HTTP")

    def test_public_redirect_cannot_acquire_api_authority(self):
        calls = self.observe(OBJECT, {OBJECT: [(302, {"Location": API}, b"")], API: [self.ok()]})
        self.assertEqual(len(calls), 1, "public redirect acquired API authority")
        self.assertFalse(calls[0]["hasAuthorization"])

    def test_redirect_loop_is_bounded_and_never_carries_ambient_auth(self):
        calls = self.observe(OBJECT, {OBJECT: [(302, {"Location": OBJECT}, b"")] * 12})
        self.assertLessEqual(len(calls), 6, "redirect count is not bounded")
        self.assertTrue(all(not call["hasAuthorization"] for call in calls), "redirect loop carried ambient authorization")

    def test_supplied_route_failure_does_not_fall_back_or_log_credentials(self):
        routes = {url: [(404, {}, b"")] for url in [OBJECT, *capture.ARTIFACT_URLS]}
        with (mock.patch.multiple(capture, **self.bundle.pins), offline_transport(routes) as transport):
            with self.assertRaises(RuntimeError) as refused:
                capture.download_bundle(self.root, OBJECT)
        self.assertEqual(len(transport.calls), 1, "supplied-route failure silently contacted fallback destinations")
        diagnostic = str(refused.exception)
        self.assertTrue(all(marker not in diagnostic for marker in FAKE_ENV.values()), "diagnostic contains a credential")
        self.assertNotIn("sig=synthetic", diagnostic, "diagnostic contains the supplied signed URL")


class CaptureExtractionTests(CaptureCase):
    def test_zip_stripped_modes_reconstructed_from_verified_tar(self):
        binary, receipt = self.download(OBJECT, {OBJECT: [self.ok()]})
        self.assertEqual(binary.read_bytes(), BD_BYTES)
        self.assertEqual(stat.S_IMODE(binary.stat().st_mode), 0o755)
        plug = binary.with_name("plug")
        self.assertEqual(plug.read_bytes(), PLUG_BYTES)
        self.assertEqual(stat.S_IMODE(plug.stat().st_mode), 0o755)
        self.assertFalse(receipt["canonicalBazel"])
        self.assertEqual(receipt["mode"], "public-pinned-go-packaged")
        self.assertTrue(all(marker not in json.dumps(receipt) for marker in FAKE_ENV.values()), "receipt contains a credential")

    def test_prefixed_zip_special_modes_are_not_restored(self):
        payload = self.bundle.zip_bytes(prefix="download/", mode=0o7777)
        binary, _ = self.download(OBJECT, {OBJECT: [self.ok(payload)]})
        self.assertEqual(stat.S_IMODE(binary.stat().st_mode), 0o755)
        for name in ("manifest.json", "receipt.json", "bundle.tar", "bd-version.json", "plug-help.txt"):
            self.assertEqual(stat.S_IMODE((binary.parent.parent / name).stat().st_mode) & 0o7111, 0,
                             "unverified ZIP metadata granted special or executable mode")

    def test_archive_digest_mismatch_never_grants_execute(self):
        self.bundle.files["bundle.tar"] += b"tampered"
        self.assert_refused(OBJECT, {OBJECT: [self.ok()]})

    def test_loose_binary_hash_mismatch_never_grants_execute(self):
        self.bundle.files["bin/bd"] = b"forged loose executable"
        self.assert_refused(OBJECT, {OBJECT: [self.ok()]})

    def test_tar_hash_member_set_and_modes_refuse_before_execute(self):
        variants = [
            [("bin/bd", BD_BYTES, 0o755, tarfile.SYMTYPE), ("bin/plug", PLUG_BYTES, 0o755, tarfile.REGTYPE)],
            [("bin/bd", BD_BYTES, 0o755, tarfile.LNKTYPE), ("bin/plug", PLUG_BYTES, 0o755, tarfile.REGTYPE)],
            [("bin/bd", BD_BYTES, 0o4755, tarfile.REGTYPE), ("bin/plug", PLUG_BYTES, 0o755, tarfile.REGTYPE)],
            [("bin/bd", BD_BYTES, 0o644, tarfile.REGTYPE), ("bin/plug", PLUG_BYTES, 0o755, tarfile.REGTYPE)],
            [("../bin/bd", BD_BYTES, 0o755, tarfile.REGTYPE), ("bin/plug", PLUG_BYTES, 0o755, tarfile.REGTYPE)],
            [("bin/bd", BD_BYTES, 0o755, tarfile.REGTYPE)] * 2,
            [("bin/bd", BD_BYTES, 0o755, tarfile.REGTYPE)],
            [("bin/bd", b"forged tar bd", 0o755, tarfile.REGTYPE), ("bin/plug", PLUG_BYTES, 0o755, tarfile.REGTYPE)],
            [("bin/bd", BD_BYTES, 0o755, tarfile.REGTYPE), ("bin/plug", b"forged tar plug", 0o755, tarfile.REGTYPE)],
        ]
        base = self.root
        for index, members in enumerate(variants):
            with self.subTest(variant=index):
                self.root = base / str(index)
                self.root.mkdir()
                self.bundle = Bundle(members)
                self.assert_refused(OBJECT, {OBJECT: [self.ok()]})

    def test_zip_links_devices_duplicates_and_traversal_refuse(self):
        base = self.root
        variants = [{"unsafe": {"bin/bd": stat.S_IFLNK}},
                    {"unsafe": {"bin/bd": stat.S_IFCHR}}, {"prefix": "../"}, {"duplicate": True}]
        for index, options in enumerate(variants):
            with self.subTest(variant=index):
                self.root = base / str(index)
                self.root.mkdir()
                self.assert_refused(OBJECT, {OBJECT: [self.ok(self.bundle.zip_bytes(**options))]})

    def test_source_pin_manifest_and_resolver_refusals_never_grant_execute(self):
        base = self.root
        for index in range(6):
            with self.subTest(variant=index):
                self.root = base / str(index)
                self.root.mkdir()
                self.bundle = Bundle()
                if index == 0:
                    self.bundle.manifest["source"]["beads"]["commit"] = "0" * 40
                elif index == 1:
                    self.bundle.manifest["source"]["beads"]["tree"] = "0" * 40
                elif index == 2:
                    self.bundle.manifest["artifacts"]["bin/bd"]["mode"] = "0o777"
                elif index == 3:
                    self.bundle.files["bd-version.json"] = encoded({"commit": "0" * 40})
                elif index == 4:
                    self.bundle.files["plug-help.txt"] += b"tampered"
                else:
                    self.bundle.files["receipt.json"] = encoded({"schema": "different"})
                self.assert_refused(OBJECT, {OBJECT: [self.ok()]})


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0], sys.argv[2]], verbosity=2)
