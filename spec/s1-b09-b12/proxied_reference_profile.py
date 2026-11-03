"""Explicit external-runtime profile; the historical embedded profile is intact."""

from public_reference_profile import PROFILE as PUBLIC_BUILD


RUNTIME_SOURCE = {
    "tools/bazel/dolt.bzl": "699d49317af8c669b92ee2cc03fdaed75aeac2f873d7c1221dcd751cde20f49f",
    "cmd/bd/init_proxied_server.go": "aedda5be896a6575f4a2a0641c9a22064b06383b687cccf256c268656eb28654",
    "cmd/bd/uow_factory.go": "6e9e4264c9f77167aa7a12d78d19b5e349a465650ea78aa473b88313decd0af2",
    "cmd/bd/dolt.go": "856d86821d0131e0ca0cf5299637b8ed41f0f9e8c9c5fbfa3d4727a73bb400fb",
}
RUNTIME = {
    "mode": "managed-proxied-server",
    "version": "2.2.0",
    "platform": "linux-amd64",
    "url": "https://github.com/dolthub/dolt/releases/download/v2.2.0/dolt-linux-amd64.tar.gz",
    "archiveSha256": "1f7ad8c2622995789420a3fb0f2d16b4aa7430000a825dd91d5938f36480cbf6",
    "member": "dolt-linux-amd64/bin/dolt",
    "versionStdout": "dolt version 2.2.0\n",
    "binaryIdentity": "measured from the fixed verified archive before execution",
    "init": ["init", "--proxied-server", "--non-interactive", "--skip-hooks", "--skip-agents"],
    "shutdown": ["dolt", "stop", "--json"],
}
PROFILE = {
    "id": "s1-b09-b11-public9d6-proxied-dolt220-v1",
    "publicBuild": PUBLIC_BUILD,
    "runtime": RUNTIME,
    "runtimeSource": RUNTIME_SOURCE,
    "embeddedModeEquivalence": False,
}
CARDS = ("B09", "B10", "B11")
UNEXECUTED = {
    "B12": {
        "status": "not-executed; full-card-unqualified",
        "reason": "embedded compact/restore refusals are not proxied compaction evidence",
    },
}
QUALIFICATION = {
    "scope": "selected B09-B11 public managed-proxied-server cases only",
    "B12": "not executed; embedded apply/restore deviation and full-card obligations remain unqualified",
    "fullCards": "unqualified",
    "fullS1": "unqualified",
    "canonicalBazel": False,
    "legacyBinaryEquivalence": False,
    "embeddedModeEquivalence": False,
    "runtimeArchiveBinding": "fixed SHA256 checked before extracting executable bytes",
    "runtimeBinaryBinding": "measured executable SHA256; extraction requires trusted exact-source run receipt",
    "workflowControlBinding": "requires the same-run external receipt",
}
PACKET_SCHEMA = "plug.s1-b09-b12.proxied-public-reference-packet.v2"
RUN_SCHEMA = "plug.s1-b09-b11.proxied-beads-reference.v2"
ENGINE = "managed-proxied-dolt-server"
ENVIRONMENT_SHA256 = "d2669285b710d3b3f36226826d91b69892a6870e563861088a7f2d5570c91d42"
GIT_CONFIGURATION_SHA256 = "b7fb296ed6500a70246c1553af15597ffcb34efa413924eceabb4951d9468ddb"
MODULES = (
    "record_reference.py", "public_reference_profile.py", "reference_capture.py",
    "extract_reference_packet.py", "proxied_reference_profile.py",
    "reference_proxied_capture.py", "extract_proxied_reference_packet.py",
    "proxied_capture_harness_test.go", "proxied_runtime.go",
    "reference_proxied_capture_test.go",
)
