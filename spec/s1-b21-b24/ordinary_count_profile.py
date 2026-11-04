"""Pinned public runtime profile for the B23 ordinary-count reference.

The profile is intentionally separate from the retained PR288 Beads twins.
Those files use the older embedded-Dolt executable. This profile names the
managed-proxied runtime required by the public CGO-free executable and carries
the exact archive, source, executable, configuration and command bindings.
"""

PROFILE = {
    "schema": "plug.s1-b23.ordinary-count-profile.v2",
    "id": "s1-b23-public9d6-cgo0-managed-proxied-dolt220-v2",
    "classification": "original Go-built9d6; CGO0; managed-proxied-Dolt2.2.0; not canonical Bazel",
    "canonicalBazel": False,
    "descriptor": "41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2",
    "producer": {"run": "41399124974", "artifact": "13121145530", "workflowSha": "c761c579a91c38263187afd5d30fdfd38eadd97c", "branch": "codex/v4-b1c-public-companion-20261102"},
    "source": {
        "commit": "9d6fff760b7a314b476539198504d77ce0959656",
        "tree": "cde121d6e4119e454857ad1c3ca293e2791be840",
        "manifestSha256": "23d84a56302ad19c50d9d25af733b2da50f9137aaf9308b273cab039b6023ae3",
        "manifestEntries": 5087,
    },
    "publicBundle": {
        "zipSha256": "c8ace91c3494db905a4c64dbd4e733e5ebb863b1987d66b4907e8d21a0ba3b4c",
        "zipBytes": 119364059,
        "manifestSha256": "8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2",
        "archiveSha256": "e4b67e5456f91419ec9c1290ca3cc620ef27f71812a821677a3ea5647a70d412",
        "archiveBytes": 132331520,
        "member": "bin/bd",
        "binarySha256": "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4",
        "binaryBytes": 112341314,
        "binaryMode": "0o755",
    },
    "runtime": {
        "mode": "managed-proxied-server",
        "version": "2.2.0",
        "platform": "linux-amd64",
        "archiveURL": "https://github.com/dolthub/dolt/releases/download/v2.2.0/dolt-linux-amd64.tar.gz",
        "archiveSha256": "1f7ad8c2622995789420a3fb0f2d16b4aa7430000a825dd91d5938f36480cbf6",
        "archiveBytes": 43985479,
        "member": "dolt-linux-amd64/bin/dolt",
        "binarySha256": "72352f1d882472a7a8d6124e9c4dc12f80e2c2359ea95f1424c032e9250e35e6",
        "binaryBytes": 126592264,
        "binaryMode": "0o700",
        "configBytes": '{"metrics.disabled":"true","versioncheck.disabled":"true"}\n',
        "configDirectoryMode": "0o700",
        "configFileMode": "0o600",
        "configProbeArgv": ["dolt", "config", "--global", "--get", "versioncheck.disabled"],
        "configProbeStdout": "true\n",
        "versionArgv": ["dolt", "version"],
        "versionStdout": "dolt version 2.2.0\n",
        "initArgv": ["bd", "init", "--proxied-server", "--non-interactive", "--skip-hooks", "--skip-agents"],
        "shutdownArgv": ["bd", "dolt", "stop", "--json"],
        "lifecycle": "private HOME and DOLT_ROOT_PATH per fresh workspace; exact config before any Dolt child; config probe then exact version before init; bd dolt stop --json; remove workspace only after stopped=true and verified=true; retain failed/partial streams and workspace",
    },
    "runtimeSource": {
        "tools/bazel/dolt.bzl": "699d49317af8c669b92ee2cc03fdaed75aeac2f873d7c1221dcd751cde20f49f",
        "cmd/bd/init_proxied_server.go": "aedda5be896a6575f4a2a0641c9a22064b06383b687cccf256c268656eb28654",
        "cmd/bd/uow_factory.go": "6e9e4264c9f77167aa7a12d78d19b5e349a465650ea78aa473b88313decd0af2",
        "cmd/bd/dolt.go": "856d86821d0131e0ca0cf5299637b8ed41f0f9e8c9c5fbfa3d4727a73bb400fb",
        "internal/testutil/localdoltserver.go": "2515e188b89e1676921d3a7dd215b8fa824f1206a72aa81da9aa85e491f6f3df",
        "tools/bazel/test_env.sh": "8bb3e5f57e2d8816753a4e0e9f08be5d64e51b89bde61d0d799f3bf04e1dd3d0",
        "cmd/bd/count.go": "1b4c36f67fab79960f6475873cb5ed0da32723f0d80ecb3949bb460c3e099277",
        "cmd/bd/count_proxied_integration_test.go": "de03381a30e8fe7914a1d2b6e3d9922db694f54d32e9420a2231ddb154055cd4",
    },
    "capture": {
        "timeoutSeconds": 120,
        "frameBegin": "S1_B23_ORDINARY_COUNT_V2_PACKET_BEGIN",
        "frameEnd": "S1_B23_ORDINARY_COUNT_V2_PACKET_END",
        "outerFrameBegin": "S1_B23_ORDINARY_COUNT_OUTER_BEGIN",
        "outerFrameEnd": "S1_B23_ORDINARY_COUNT_OUTER_END",
        "publicDescriptorEnv": "PLUG_PUBLIC_REFERENCE",
        "publicBuildRunEnv": "PLUG_PUBLIC_REFERENCE_BUILD_RUN",
        "publicZipEnv": "PLUG_PUBLIC_REFERENCE_ZIP",
        "publicManifestEnv": "PLUG_PUBLIC_REFERENCE_MANIFEST",
        "publicSourceEnv": "PLUG_PUBLIC_REFERENCE_SOURCE",
        "sourceRootEnv": "PLUG_SOURCE_MODULE_ROOT",
        "doltArchiveEnv": "S1_B23_DOLT_ARCHIVE",
    },
    "limitations": [
        "The public CGO0 executable cannot initialize embedded Dolt; this packet explicitly uses the pinned managed-proxied Dolt 2.2.0 profile.",
        "Beads has no Plug authorization, current-caveat, HTTP, idempotency, or SQL-clock semantics; the projection compares ordinary count inputs and results only.",
        "No binary equivalence to retained PR288 reference-a/reference-b is claimed; those bytes remain unchanged.",
        "The B24 untimed defer deviation remains separate and is not promoted by this count packet.",
    ],
}

COUNT_VECTOR = {"openTaskCount": 4, "openNeedleTaskCount": 3, "closedTaskCount": 1, "absentNeedleTaskCount": 0, "openTaskStatusTotal": 4, "afterUpdateOpenNeedleTaskCount": 2, "afterUpdateChangedTaskCount": 1}

MODULES = (
    "record_ordinary_count.py", "ordinary_count_profile.py", "ordinary_count_capture.py",
    "extract_ordinary_count_packet.py", "ordinary_count_capture_policy_test.py",
)
SUPPORT_FILES = (
    "ordinary_count_capture_harness_test.go", "ordinary_count_runtime.go",
    "ordinary_count_capture_test.go", "ordinary_count_capture_policy_test.go", "BUILD.bazel",
)

ROWS = (("b23-alpha", "needle-alpha"), ("b23-zeta", "needle-zeta"),
        ("b23-secret", "needle-secret"), ("b23-done", "done-needle"), ("b23-other", "other-beta"))
INPUTS = {
    "rows": [{"id": key, "title": title, "referenceType": "task"} for key, title in ROWS],
    "closed": "b23-secret", "updated": {"id": "b23-zeta", "title": "changed-zeta"},
    "mapping": {"type": {"task": "issue"}, "status": {"open": "todo", "closed": "done"}},
    "filters": {"openTask": {"status": "open", "type": "task"},
                "openNeedleTask": {"status": "open", "type": "task", "title": "needle"},
                "closedTask": {"status": "closed", "type": "task"},
                "absentNeedleTask": {"status": "open", "type": "task", "title": "absent"}},
}
# Full argv suffixes, checked independently against every raw execution.
BD_GLOBAL_ARGS = ["--sandbox", "--actor", "cardb23", "--json"]
COMMANDS = [
    ("git-init", "git", ["init", "-q"]),
    ("dolt-config-probe", "dolt", ["config", "--global", "--get", "versioncheck.disabled"]),
    ("dolt-version", "dolt", ["version"]),
    ("init", "bd", ["init", "--proxied-server", "--non-interactive", "--skip-hooks", "--skip-agents", "--prefix", "b23", "--quiet"]),
] + [("create-" + key, "bd", ["create", "--id", key, "--title", title, "--type", "task"]) for key, title in ROWS] + [
    ("close-secret", "bd", ["close", "b23-secret", "--reason", "complete"]),
    ("open-task-count", "bd", ["count", "--status", "open", "--type", "task"]),
    ("open-needle-count", "bd", ["count", "--status", "open", "--type", "task", "--title", "needle"]),
    ("closed-task-count", "bd", ["count", "--status", "closed", "--type", "task"]),
    ("absent-needle-count", "bd", ["count", "--status", "open", "--type", "task", "--title", "absent"]),
    ("open-task-by-status", "bd", ["count", "--status", "open", "--type", "task", "--by-status"]),
    ("update-zeta", "bd", ["update", "b23-zeta", "--title", "changed-zeta"]),
    ("after-update-needle-count", "bd", ["count", "--status", "open", "--type", "task", "--title", "needle"]),
    ("after-update-changed-count", "bd", ["count", "--status", "open", "--type", "task", "--title", "changed"]),
    ("cleanup-stop", "bd", ["dolt", "stop", "--json"]),
]
COUNT_STEPS = [item[0] for item in COMMANDS]
ENVIRONMENT = {
    "PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "TZ": "UTC", "TERM": "dumb",
    "CI": "true", "NO_COLOR": "1", "BD_NON_INTERACTIVE": "1", "BEADS_ACTOR": "cardb23",
    "USER": "s1-b23-reference", "LOGNAME": "s1-b23-reference",
    "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_TERMINAL_PROMPT": "0", "GIT_NO_LAZY_FETCH": "1", "GIT_ASKPASS": "/bin/false",
    "SSH_ASKPASS": "/bin/false", "GIT_SSH_COMMAND": "/bin/false",
    "GIT_AUTHOR_NAME": "s1-b23-reference", "GIT_AUTHOR_EMAIL": "s1-b23-reference@invalid",
    "GIT_COMMITTER_NAME": "s1-b23-reference", "GIT_COMMITTER_EMAIL": "s1-b23-reference@invalid",
    "AWS_EC2_METADATA_DISABLED": "true", "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
}
GIT_CONFIGURATION = {
    "credential.helper": "", "credential.interactive": "false", "core.askPass": "/bin/false",
    "core.sshCommand": "/bin/false", "core.hooksPath": "/dev/null", "core.fsmonitor": "false",
    "core.untrackedCache": "false", "init.templateDir": "/dev/null", "init.defaultBranch": "main",
    "protocol.allow": "never", "user.name": "s1-b23-reference", "user.email": "s1-b23-reference@invalid",
}
