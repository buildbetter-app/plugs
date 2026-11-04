#!/usr/bin/env python3
"""B06/B08 private native protocol; only genuine public operations on owned Dolt."""
import base64, hashlib, json, os, re, subprocess, sys, time
from pathlib import Path

OUTPUT = Path(sys.argv[3])
def sha(data): return hashlib.sha256(data).hexdigest()
def sidecar_inventory(root):
    entries = []
    def visit(directory):
        for entry in sorted(os.scandir(directory), key=lambda item: item.name):
            if entry.name == "poll.log":
                continue
            if entry.is_symlink():
                raise RuntimeError("native sidecar cannot be a symlink")
            if entry.is_dir(follow_symlinks=False):
                visit(Path(entry.path))
            elif entry.is_file(follow_symlinks=False):
                data = Path(entry.path).read_bytes()
                entries.append({"path": str(Path(entry.path).relative_to(root)), "bytes": len(data), "sha256": sha(data)})
            else:
                raise RuntimeError("native sidecar is not a regular file or directory")
    visit(root)
    return entries

def run(binary, root, commands, args, expected=None, env=None):
    started = time.time_ns()
    result = subprocess.run([str(binary), *args], cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    index = len(commands)
    entry = {"argv":args,"exitCode":result.returncode,"startedNs":started,"endedNs":time.time_ns(),
        "stdoutSha256":sha(result.stdout),"stderrSha256":sha(result.stderr),
        "stdoutBase64":base64.b64encode(result.stdout).decode(),"stderrBase64":base64.b64encode(result.stderr).decode()}
    commands.append(entry)
    (OUTPUT / f"protocol-{index:03d}.stdout").write_bytes(result.stdout)
    (OUTPUT / f"protocol-{index:03d}.stderr").write_bytes(result.stderr)
    with (OUTPUT / "protocol.jsonl").open("a") as out: out.write(json.dumps(entry,sort_keys=True)+"\n")
    if result.returncode != (0 if expected is None else expected): raise RuntimeError(f"unexpected actual native exit {args}: {result.returncode}")
    return result.stdout

def dependency_projection(stdout, source, target):
    row=json.loads(stdout)[0]
    return [{"from":source,"to":edge["id"],"kind":edge["dependency_type"]} for edge in row.get("dependencies",[]) if edge.get("id")==target]

def b06_paths(stdout, expected, label):
    """Check the actual native report without repairing its path or order."""
    try:
        report = json.loads(stdout.decode("utf-8"))
        if not isinstance(report, list):
            raise ValueError("report must be an array")
        paths = []
        for cycle in report:
            if cycle["partial"] is not False or not cycle["members"]:
                raise ValueError("these public fixtures have complete members")
            path = []
            for member in cycle["members"]:
                identifier = member["id"]
                if not isinstance(identifier, str) or not identifier or member["issue"]["id"] != identifier:
                    raise ValueError("member hydration differs from its edge ID")
                path.append(identifier)
            if len(set(path)) != len(path) or path[0] != min(path):
                raise ValueError("path must be distinct and start at its lowest ID")
            paths.append(path)
        if paths != expected:
            raise ValueError(f"paths {paths!r} differ from {expected!r}")
        return paths
    except (KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
        raise RuntimeError(f"B06 {label} report failed: {exc}") from exc


def b06_rendered(stdout, expected_paths, shown):
    # Keep the original streams; remove terminal decoration only for assertions.
    rendered = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", stdout.decode("utf-8"))
    total = len(expected_paths)
    headings = re.findall(r"^\d+\. Cycle involving:\s*$", rendered, re.MULTILINE)
    members = re.findall(r"^   - ([^:]+):", rendered, re.MULTILINE)
    expected_members = [identifier for path in expected_paths[:shown] for identifier in path]
    omitted = f"... and {total - shown} more not shown."
    if (f"Found {total} dependency cycles:" not in rendered or len(headings) != shown
            or members != expected_members or (omitted in rendered) != (shown < total)
            or (shown == total and "more not shown" in rendered)):
        raise RuntimeError("B06 native human limit changed the total, order, or rendered members")
    return {"total": total, "shown": shown}


def record_b06(binary, root, commands, env, ids, probe_start):
    """Use ordinary guarded CLI operations, never legacy-row or SQL injection.

    A blocks-only cycle cannot be made through the traced add/import routes:
    even bulk --no-cycle-check retains a final graph gate, and import records
    skipped cycle edges. Tracks can legitimately close a diagnostic path while
    leaving the scheduling graph acyclic. That supported case is useful real
    coverage, with the remaining native fixture dependencies stated explicitly.
    """
    probes = []
    refusals = []

    def create(identifier):
        run(binary, root, commands, ["create", "--id", identifier, "--type", "task", "--title", identifier,"--force","--json"], env=env)
        ids.append(identifier)

    def refuse(label, arguments, members, reason):
        before_indices = []
        before = []
        for identifier, parent in members:
            before_indices.append(len(commands))
            output = run(binary, root, commands, ["show", identifier, "--json"], env=env)
            row = json.loads(output)[0]
            if row.get("id") != identifier:
                raise RuntimeError("B06 refusal baseline differs from its public member")
            if parent is not None and dependency_projection(output, identifier, parent) != [
                    {"from": identifier, "to": parent, "kind": "parent-child"}]:
                raise RuntimeError("B06 refusal fixture lost its public parent-child edge")
            before.append(output)
        index = len(commands)
        output = run(binary, root, commands, arguments, expected=1, env=env)
        diagnostic = (output + base64.b64decode(commands[-1]["stderrBase64"])).lower()
        if (not re.search(reason, diagnostic)
                or re.search(rb"unknown command|unknown flag|usage:|unrecognized", diagnostic)):
            raise RuntimeError(f"B06 {label} did not identify its guard refusal")
        after_indices = []
        for (identifier, _), original in zip(members, before):
            after_indices.append(len(commands))
            if run(binary, root, commands, ["show", identifier, "--json"], env=env) != original:
                raise RuntimeError(f"B06 {label} changed a public member after refusal")
        refusals.append({"case": label, "commandIndex": index,
                         "beforeIndices": before_indices, "afterIndices": after_indices})

    def report(label, arguments, expected):
        index = len(commands)
        output = run(binary, root, commands, ["dep", "cycles", *arguments], env=env)
        probes.append({"case": label, "commandIndex": index, "paths": b06_paths(output, expected, label)})
        return output

    for kind in ("blocks", "tracks"):
        refuse("self-" + kind, ["dep", "add", ids[0], ids[0], "--type", kind],
               [(ids[0], None)], rb"self|itself")
    ancestor, child, descendant = ("b06-ancestor", "b06-child", "b06-descendant")
    for identifier in (ancestor, child, descendant):
        create(identifier)
    for source, target in ((child, ancestor), (descendant, child)):
        run(binary, root, commands, ["dep", "add", source, target, "--type", "parent-child"], env=env)
    members = [(ancestor, None), (child, ancestor), (descendant, child)]
    for label, source, target in (("ancestor-blocker", ancestor, descendant),
                                  ("descendant-blocker", descendant, ancestor)):
        refuse(label, ["dep", "add", source, target, "--type", "blocks"], members,
               rb"ancestor|descendant|hierarch|parent|child")

    run(binary, root, commands, ["dep", "add", ids[0], ids[1], "--type", "blocks"], env=env)
    refusal = run(binary, root, commands, ["dep", "add", ids[1], ids[0], "--type", "blocks"], expected=1, env=env)
    if b"cycle" not in (refusal + base64.b64decode(commands[-1]["stderrBase64"])).lower():
        raise RuntimeError("B06 reverse-edge refusal did not identify a cycle")
    report("acyclic-control", ["--json"], [])

    prefix = "b06"
    pure = [f"{prefix}-purea", f"{prefix}-purez"]
    rings = [[f"{prefix}-alpha", f"{prefix}-middle", f"{prefix}-zulu"],
             [f"{prefix}-beta", f"{prefix}-november", f"{prefix}-yankee"]]

    for identifier in pure:
        create(identifier)
    for source, target in (pure, list(reversed(pure))):
        run(binary, root, commands, ["dep", "add", source, target, "--type", "tracks"], env=env)
    report("pure-tracks-control", ["--include-tracks", "--json"], [])

    # Write the higher ring first, and each scheduling edge from its highest
    # member, so a correct answer must order and rotate the actual report.
    for low, middle, high in reversed(rings):
        for identifier in (low, middle, high):
            create(identifier)
        for source, target, kind in ((high, low, "blocks"), (low, middle, "tracks"), (middle, high, "tracks")):
            run(binary, root, commands, ["dep", "add", source, target, "--type", kind], env=env)
    report("default-excludes-tracks", ["--json"], [])
    expanded = report("tracks-expanded", ["--include-tracks", "--json"], rings)
    repeated = report("unchanged-repeat", ["--include-tracks", "--json"], rings)
    if repeated != expanded:
        raise RuntimeError("B06 unchanged native cycle report bytes differ")
    run(binary, root, commands, ["dep", "add", rings[0][2], rings[0][0], "--type", "blocks"], env=env)
    if report("same-edge-readd", ["--include-tracks", "--json"], rings) != expanded:
        raise RuntimeError("B06 same-type re-add changed the native report")
    # Native JSON returns before the renderer's cap; Plug's result envelope
    # has its own explicit count/truncated/items projection instead.
    if report("json-limit-uncapped", ["--include-tracks", "--limit", "1", "--json"], rings) != expanded:
        raise RuntimeError("B06 native JSON limit truncated or changed the report")
    for label, limit, shown in (("human-limit", "1", 1), ("human-unlimited", "0", 2)):
        index = len(commands)
        output = run(binary, root, commands, ["dep", "cycles", "--include-tracks", "--limit", limit], env=env)
        probes.append({"case": label, "commandIndex": index, **b06_rendered(output, rings, shown)})
    return {
        "probeResults": commands[probe_start:],
        "guardRefusals": refusals,
        "cycleProbes": probes,
        "cycleProbeCoverage": {
            "status": "partial",
            "supported": ["self-blocks-refusal", "self-tracks-refusal", "ancestor-blocker-refusal", "descendant-blocker-refusal",
                          "public-refusal-conservation", "nonempty-tracks-report", "lowest-id-rotation", "report-order", "pure-tracks-exclusion",
                          "unchanged-report-bytes", "same-type-readd", "native-json-uncapped", "native-human-limit"],
            "blocked": ["blocking-only-cycle-public-fixture", "parallel-rows-public-fixture", "unreadable-cycle-public-fixture"],
        },
    }


def record_b08(binary, root, commands, env):
    ids=["b08-parent","b08-child","b08-blocker","b08-dependent","b08-grandchild","b08-foreign","b08-pinned"]
    def call(args, expected=0): return run(binary,root,commands,args,expected,env)
    def row(identifier):
        value=json.loads(call(["show",identifier,"--json"]))
        if not isinstance(value,list) or len(value)!=1 or value[0]["id"]!=identifier: raise RuntimeError("native show identity mismatch")
        return value[0]
    def ready(): return sorted(x["id"] for x in json.loads(call(["ready","--limit","0","--json"])))
    def state(label):
        # Public read-only SQL retains full rows/history/head; no fixture SQL or writes.
        values={}
        for table,order in [("issues","id"),("dependencies","issue_id,depends_on_issue_id,type"),("events","id"),("dolt_log","commit_hash")]:
            values[table]=json.loads(call(["--readonly","sql",f"SELECT * FROM {table} ORDER BY {order}","--json"]))
        for identifier in ids: values[identifier]=json.loads(call(["show",identifier,"--json"]))
        (OUTPUT / (label+"-full-state.json")).write_text(json.dumps(values,sort_keys=True,indent=2)+"\n")
        return values
    def refusal(label,args,reason,expected=1):
        before=state(label+"-before")
        call(args,expected)
        diagnostic=base64.b64decode(commands[-1]["stdoutBase64"])+base64.b64decode(commands[-1]["stderrBase64"])
        if not re.search(reason,diagnostic,re.I) or re.search(rb"unknown command|unknown flag|usage:",diagnostic,re.I): raise RuntimeError("not the required close refusal")
        if state(label+"-after")!=before: raise RuntimeError("native refusal changed full rows, events, history or head")
    for identifier in ids:
        args=["create","--id",identifier,"--type","task","--title",identifier,"--force","--json"]
        if identifier=="b08-foreign": args += ["--assignee","another-actor"]
        if identifier=="b08-pinned": args += ["--status","pinned"]
        call(args)
    call(["dep","add",ids[1],ids[0],"--type","parent-child"])
    call(["dep","add",ids[3],ids[2],"--type","blocks"])
    call(["dep","add",ids[4],ids[3],"--type","blocks"])
    # Admit the entire seven-role initial image to the existing Plug receiver.
    # No reconstructed graph, selected-row export or archive rewrite is used.
    call(["dolt","commit","-m","B08 complete initial role corpus"])
    initial_inventory=OUTPUT / "initial-inventory.json"
    call(["--readonly","export","--format","beads-inventory/1","--plug-capture-public","-o",str(initial_inventory)])
    initial_census=json.loads(initial_inventory.read_text())
    if (initial_census.get("version")!="beads-inventory/1" or initial_census.get("quiesced") is not True
            or initial_census.get("working_set_clean") is not True):
        raise RuntimeError("initial seven-role inventory is not clean and quiesced")
    initial_before=sidecar_inventory(Path(str(initial_inventory)+".sources"))
    call(["--readonly","export","--format","plug-dump/1","--plug-inventory",str(initial_inventory),"-o",str(OUTPUT / "initial-readback.tar")])
    initial_after=sidecar_inventory(Path(str(initial_inventory)+".sources"))
    (OUTPUT / "initial-sidecar-closure.json").write_text(json.dumps({"before":initial_before,"after":initial_after,"equal":initial_before==initial_after},sort_keys=True,indent=2)+"\n")
    if initial_before!=initial_after or not (OUTPUT / "initial-readback.tar").stat().st_size:
        raise RuntimeError("initial native dump missing or original sidecars changed")
    before_ready=ready()
    refusal("open-child",["close",ids[0],"--json"],rb"child|children")
    refusal("live-blocker",["close",ids[3],"--json"],rb"block")
    refusal("foreign-assignee",["close",ids[5],"--json"],rb"assign|actor")
    refusal("pinned",["close",ids[6],"--json"],rb"pinned|pin")
    call(["close",ids[0],"--force","--reason","first forced reason","--json"])
    forced=row(ids[0]); child=row(ids[1])
    if forced["status"]!="closed" or child["status"]!="open": raise RuntimeError("native force changed the live child or failed to close")
    # The current native policy rechecks open children before its closed-row
    # no-op. Preserve that refusal and the complete original state.
    closed_state=state("closed-once")
    call(["close",ids[0],"--reason","must preserve first close","--json"],expected=1)
    diagnostic=base64.b64decode(commands[-1]["stdoutBase64"])+base64.b64decode(commands[-1]["stderrBase64"])
    if not re.search(rb"open child|children",diagnostic,re.I) or re.search(rb"unknown command|unknown flag|usage:",diagnostic,re.I):
        raise RuntimeError("native closed-parent repeat did not identify its current child guard")
    if state("closed-twice")!=closed_state: raise RuntimeError("native repeated close published a change")
    call(["close",ids[2],"--reason","settle blocker","--json"])
    # An unblocked already-closed issue reaches the ordinary no-op. Keep the
    # first reason and every row/event/history byte while repeating it.
    blocker_closed_state=state("blocker-closed-once")
    call(["close",ids[2],"--reason","must preserve blocker first close","--json"])
    if state("blocker-closed-twice")!=blocker_closed_state:
        raise RuntimeError("native unblocked repeated close changed its first reason or complete state")
    after_ready=ready()
    if ids[3] not in after_ready or ids[3] in before_ready or ids[4] in after_ready: raise RuntimeError("native direct/transitive readiness settlement differs")
    call(["reopen",ids[2],"--reason","reblock dependent","--json"])
    reopened_ready=ready()
    if ids[3] in reopened_ready or ids[4] in reopened_ready: raise RuntimeError("native reopen did not reblock dependers")
    call(["close",ids[5],"--force","--json"])
    foreign=row(ids[5])
    if foreign.get("assignee")!="another-actor": raise RuntimeError("native force close discarded assignee")
    call(["reopen",ids[5],"--json"])
    reopened=row(ids[5])
    if reopened.get("assignee")!="another-actor": raise RuntimeError("native reopen discarded assignee")
    return {"beforeReady":before_ready,"afterReady":after_ready,"reopenedReady":reopened_ready,
        "forcedStatus":forced["status"],"childStatus":child["status"],"foreignHolder":reopened.get("assignee"),
        "closedParentRepeat":"open-child-refusal-full-state-conserved",
        "blockerRepeat":"ordinary-close-noop-full-state-conserved",
        "finalRows":[row(i) for i in ids],"refusalConservation":"full issues/dependencies/events/dolt_log and public show bytes decoded without field stripping"}

card,binary,output=sys.argv[1:4]
if card not in ("B06","B08"): raise RuntimeError("only B06/B08 are authorized")
root=Path.cwd();commands=[];env=dict(os.environ)
if card=="B06":
    ids=["b06-source","b06-target"]
    for identifier in ids: run(binary,root,commands,["create","--id",identifier,"--type","task","--title",identifier,"--force","--json"],env=env)
    observations=record_b06(binary,root,commands,env,ids,len(commands))
else: observations=record_b08(binary,root,commands,env)
(OUTPUT / "observed-native.json").write_text(json.dumps({"card":card,"commands":commands,"observations":observations},sort_keys=True,indent=2)+"\n")
