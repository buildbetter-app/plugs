#!/usr/bin/env python3
"""Execute one public proof using exact remotely built executables.

The external 1200/1614-commit originals stay outside the repository. Runtime
requires the current orchestrator's accounting/rehearsal hold to be cleared;
this helper does not grant that authorization or build any executable.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time


POLICY_SHA256 = "3afb19b934e69e90b9f8e966d5e4e4d1fab27a47686eacf38543e23d1c7542d9"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def exact_executable(path, expected):
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or not os.access(path, os.X_OK) or digest(path) != expected:
        raise ValueError("public executable binding differs")
    return before


def write(path, value):
    with path.open("xb") as stream:
        stream.write(json.dumps(value, indent=2).encode() + b"\n")


def stop(process):
    try:
        os.killpg(process.pid, 15)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, 9)
        except ProcessLookupError:
            pass
        process.wait()


def invoke(output, label, argv, deadline):
    directory = output / label
    directory.mkdir(mode=0o700)
    write(directory / "argv.json", argv)
    started = time.monotonic()
    with (directory / "stdout.json").open("xb") as stdout, (directory / "stderr.txt").open("xb") as stderr:
        process = subprocess.Popen(argv, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            status = process.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            stop(process)
            status = 124
        except BaseException:
            stop(process)
            write(directory / "wait.json", {"exit": process.returncode, "interrupted": True,
                                          "wall_seconds": time.monotonic() - started})
            raise
    write(directory / "wait.json", {"exit": status, "wall_seconds": time.monotonic() - started})
    if status != 0:
        raise ValueError("public command failed: " + label + "; original streams retained")
    with (directory / "stdout.json").open() as stream:
        return json.load(stream)


def canonical(value):
    # Bindings contain ASCII identities, paths and hexadecimal digests only.
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def check_clean_owner(result):
    usage = result["decoded_after_close"]
    if (usage["current_accounted_decoded_bytes"] != 0 or usage.get("current_categories")
            or usage.get("current_charges")):
        raise ValueError("public proof did not close its accounting owner")


def execute(args, output):
    deadline = time.monotonic() + 7200
    exact_executable(args.proof_binary, args.proof_sha256)
    write(output / "executable.json", {"proof": str(args.proof_binary), "proof_sha256": args.proof_sha256})
    base = [str(args.proof_binary), "--mode", args.case]
    for source in args.source:
        base += ["--source", source]
    if args.case in ("source", "collision"):
        for directory in args.directory:
            base += ["--directory", str(directory)]
        result = invoke(output, args.case, base, deadline)
        check_clean_owner(result)
        if result["reader_calls"] != [1] * len(args.source):
            raise ValueError("public source authentication call census differs")
        if args.case == "collision" and (result["refusal"]["code"] != "cross_project_id_collision" or result["refusal"]["count"] <= 0):
            raise ValueError("typed nonzero collision refusal was not observed")
        return result

    if len(args.directory) != 2 or args.receiver_binary is None or args.receiver_sha256 is None or args.policy is None:
        raise ValueError("positive proof requires two originals, exact receiver and policy")
    exact_executable(args.receiver_binary, args.receiver_sha256)
    write(output / "receiver.json", {"receiver": str(args.receiver_binary), "receiver_sha256": args.receiver_sha256})
    if digest(args.policy) != POLICY_SHA256:
        raise ValueError("public policy bytes differ")
    input_dir = output / "inputs"
    base[2] = "inputs"
    inputs = invoke(output, "inputs-construction", base + ["--output", str(input_dir)], deadline)
    binding = inputs["binding"]
    request_argv = [str(args.receiver_binary), "prepare-request", "--driver", args.driver,
                    "--expected-binary-sha256", args.receiver_sha256]
    originals = []
    for directory in args.directory:
        originals += ["--capture-set", str(directory)]
    originals += ["--policy", str(args.policy), "--bindings", str(input_dir / "binding.json"),
                  "--profile", str(input_dir / "profile.json")]
    request = invoke(output, "prepare", request_argv + originals, deadline)
    archive_sha = hashlib.sha256(canonical(binding)).hexdigest()
    key = hashlib.sha256(canonical({"format": "plug-native-rehearsal-idempotency/2", "inputs": binding})).hexdigest()
    plan = request["source_plan"]
    if (request["idempotency_key"] != key or request["archive_sha256"] != archive_sha
            or request["original_inputs"] != binding or request["policy_sha256"] != POLICY_SHA256
            or request["collection_source_preparations"] != [1, 1]
            or len(plan["Providers"]) != 1 or len(plan["Principals"]) != 1 or plan.get("Runs")):
        raise ValueError("independent ordered intent or one-authority oracle differs")
    request_path = output / "prepare" / "stdout.json"
    receipt = invoke(output, "receive", [str(args.receiver_binary), "--driver", args.driver,
                     "--target", args.target, "--request", str(request_path),
                     "--expected-binary-sha256", args.receiver_sha256] + originals, deadline)
    verified = receipt["native_collection_verification"]
    report = receipt["committed_report"]
    if (receipt["request_sha256"] != digest(request_path) or receipt["binary_sha256"] != args.receiver_sha256
            or receipt["archive_sha256"] != archive_sha or receipt["policy_sha256"] != POLICY_SHA256
            or receipt["original_inputs"] != binding or receipt["cross_project_id_collisions"] != 0
            or verified["source_count"] != 2 or verified["mapped_commits"] != inputs["commits"]
            or not verified["imported_byte_readback"] or not verified["reopened_byte_readback"]
            or report["IdempotencyKey"] != key or report["Receipt"] is None
            or len(report["NativeHistories"]) != 2 or len(report["NativeSchemas"]) != 2
            or len(report["SourcePlanes"]["providers"]) != 1
            or int(report["Restored"]["Head"]) != sum(inputs["commits"]) + 1):
        raise ValueError("public joined receipt or all-revision conservation oracle differs")
    for name in ("receiving_source_preparations", "imported_readback_source_preparations", "later_readback_source_preparations"):
        if verified[name] != [1, 1]:
            raise ValueError("observed per-source preparation census differs")
    reopen = "same-process-memory-lease-reacquired" if args.driver == "memory" else "driver-reopened"
    if verified["reopen"] != reopen or receipt["session_ceiling_seconds"] != 7200:
        raise ValueError("public lifetime or session ceiling differs")
    return {"format": "plug-public-native-joined-proof/1", "driver": args.driver,
            "idempotency_key": key, "verified_commits": inputs["commits"],
            "receiver_sha256": args.receiver_sha256, "registry_sha256": inputs["registry_sha256"],
            "verification": verified, "measurements": receipt["capture_set_measurements"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("source", "collision", "positive"), required=True)
    parser.add_argument("--source", action="append", required=True)
    parser.add_argument("--directory", type=Path, action="append", required=True)
    parser.add_argument("--proof-binary", type=Path, required=True)
    parser.add_argument("--proof-sha256", required=True)
    parser.add_argument("--receiver-binary", type=Path)
    parser.add_argument("--receiver-sha256")
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--driver", choices=("memory", "sqlite", "postgres"), default="memory")
    parser.add_argument("--target", default="")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(mode=0o700)
    try:
        result = execute(args, args.output)
        exact_executable(args.proof_binary, args.proof_sha256)
        if args.case == "positive":
            exact_executable(args.receiver_binary, args.receiver_sha256)
        write(args.output / "PASS.json", result)
    except Exception as error:
        write(args.output / "FAIL.json", {"error": str(error)})
        raise


if __name__ == "__main__":
    main()
