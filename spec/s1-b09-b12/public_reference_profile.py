"""One fixed public build profile; never an alias for the legacy recorder pin."""

PROFILE = {
    "id": "s1-b09-b12-public9d6-original-go-v1",
    "classification": "original Go-built9d6; not canonical Bazel",
    "canonicalBazel": False,
    "descriptor": "41399124974:13121145530:8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2",
    "producer": {
        "run": "41399124974", "artifact": "13121145530",
        "workflowSha": "c761c579a91c38263187afd5d30fdfd38eadd97c",
        "branch": "codex/v4-b1c-public-companion-20261102",
    },
    "source": {
        "commit": "9d6fff760b7a314b476539198504d77ce0959656",
        "tree": "cde121d6e4119e454857ad1c3ca293e2791be840",
        "manifestSha256": "23d84a56302ad19c50d9d25af733b2da50f9137aaf9308b273cab039b6023ae3",
        "manifestEntries": 5087,
    },
    "zip": {
        "sha256": "c8ace91c3494db905a4c64dbd4e733e5ebb863b1987d66b4907e8d21a0ba3b4c",
        "size": 119364059,
    },
    "manifestSha256": "8c3d155f3940d21bd270fb14fe0292de0db1b3f377214756db59edf53eec5fa2",
    "binary": {
        "sha256": "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4",
        "size": 112341314, "mode": "0o755",
    },
    "archive": {
        "name": "bundle.tar", "size": 132331520,
        "sha256": "e4b67e5456f91419ec9c1290ca3cc620ef27f71812a821677a3ea5647a70d412",
    },
    "build": {
        "command": "CGO_ENABLED=0 go build -tags gms_pure_go -trimpath ./cmd/bd",
        "go": "go version go1.26.10 linux/amd64",
    },
    "legacyBinarySha256": "33cdadbe3bd7a82e2c3c40f4b5da4403d822acc00d544a321df24e277c0836a3",
    "reviewedRecorderSha256": "5aad875792b19f70a0bdc69cb3f0349cbf08b163b0f811e7e1c5c377080d4d35",
}

MODULES = (
    "record_reference.py", "public_reference_profile.py", "reference_capture.py",
    "extract_reference_packet.py", "reference_capture_test.go", "capture_harness_test.go",
)
CARDS = ("B09", "B10", "B11", "B12")
SOURCE_TESTS = {
    "B09": [
        "internal/idgen/hash_test.go::TestGenerateHashIDMatchesJiraVector",
        "internal/storage/dolt/queries_test.go::TestCreateIssue_HashModeDefault",
    ],
    "B10": [
        "cmd/bd/protocol/child_id_test.go::TestProtocol_ChildIDCounterNeverReusesNumbers",
        "internal/storage/embeddeddolt/child_id_test.go::TestGetNextChildID",
    ],
    "B11": [
        "internal/storage/depid/depid_test.go::TestNewGolden",
        "cmd/bd/protocol/dep_identity_test.go::TestProtocol_DepReAddSameTypeIsIdempotent",
        "cmd/bd/protocol/dep_identity_test.go::TestProtocol_DepReAddDifferentTypeIsAnError",
    ],
    "B12": [
        "cmd/bd/compact_tier2_embedded_test.go::TestEmbeddedAdminCompactTier2Honesty",
        "cmd/bd/restore_embedded_test.go::TestEmbeddedRestore",
        "internal/compact/compactor_test.go::TestCompactTier1_DryRun",
    ],
}
QUALIFICATION = {
    "scope": "selected public reference cases only",
    "B12": "embedded apply/restore unsupported; full-card-unqualified",
    "fullS1": "unqualified",
    "legacyBinaryEquivalence": False,
    "workflowControlBinding": "requires the same-run external receipt",
}
PACKET_SCHEMA = "plug.s1-b09-b12.public-reference-packet.v2"
BEGIN = b"S1_B09_B12_V2_PACKET_BEGIN"
END = b"S1_B09_B12_V2_PACKET_END"
MAX_PACKET_BYTES = 32 * 1024 * 1024
