"""Prepared six host script implementations; all data paths come from binding."""
import argparse
import copy
import json
from pathlib import Path
import sys

from plug_host import AdapterError, Client, Job, Writes, created_ref, expect, fields_for, read_json


DEFAULTS = {"plan.json": None, "ids.json": {}, "linear-pending.json": []}


def create_items(writes, plan, ids, additions, ontology):
    items = {x["key"]: x for x in plan["items"]}
    pending = copy.deepcopy(additions)
    if len({x["key"] for x in pending}) != len(pending) or any(x["key"] in items for x in pending):
        raise AdapterError("duplicate plan key")
    while pending:
        ready = [x for x in pending if not x.get("parent") or x["parent"] in ids]
        if not ready:
            raise AdapterError("unknown/cyclic create parents")
        for item in ready:
            typ, fields = fields_for(item, plan["doc"], writes.c.project)
            status = item.get("status", ontology.initial(typ))
            category = ontology.category(status, typ)
            # Terminal imports close after graph creation, in guarded order.
            if ontology.terminal(status, typ):
                fields.pop("status", None)
            else:
                fields["status"] = status
            parent = ids[item["parent"]] if item.get("parent") else None
            ids[item["key"]] = writes.create(typ, fields, parent)
            item.update(status=status, status_category=category)
            item.setdefault("deps", [])
            plan["items"].append(item)
            items[item["key"]] = item
            pending.remove(item)
    return items


def apply_body(w, plan, ids, change):
    additions = change.get("create", [])
    ontology = w.c.ontology()
    for item in additions:
        typ, _ = fields_for(item, plan["doc"], w.c.project)
        if ontology.terminal(item.get("status", ontology.initial(typ)), typ) and item["key"] not in change.get("close", {}):
            raise AdapterError("terminal create requires its explicit close reason")
    known = {x["key"] for x in plan["items"]} | {x["key"] for x in additions}
    for item in additions:
        if any(dep not in known for dep in item.get("deps", [])):
            raise AdapterError("unknown create dependency")
    for section in ("update", "dep_add", "dep_remove", "move", "close", "reopen"):
        if any(key not in known for key in change.get(section, {})):
            raise AdapterError("unknown change key")
    items = create_items(w, plan, ids, additions, ontology)
    for key, patch in change.get("update", {}).items():
        if set(patch) - {"title", "description", "priority", "design", "acceptance", "acceptance_criteria"}:
            raise AdapterError("unsupported update field")
        fields = copy.deepcopy(patch)
        if "description" in fields:
            fields["description"] = f"{fields['description']}\n\nPlan key: {key}. Design doc: {plan['doc']}"
        if "acceptance" in fields:
            fields["acceptance_criteria"] = fields.pop("acceptance")
        w.update(ids[key], fields)
        items[key].update(patch)
    for key, deps in change.get("dep_remove", {}).items():
        for dep in deps:
            if dep not in items[key]["deps"]:
                raise AdapterError("removal is not an existing plan dependency")
            w.link(ids[key], ids[dep], remove=True)
            items[key]["deps"].remove(dep)
    for item in additions:
        for dep in item.get("deps", []):
            w.link(ids[item["key"]], ids[dep])
    for key, deps in change.get("dep_add", {}).items():
        for dep in deps:
            w.link(ids[key], ids[dep])
            if dep not in items[key]["deps"]:
                items[key]["deps"].append(dep)
    for item in additions:
        milestone = item.get("milestone")
        if milestone:
            if items[milestone]["type"] != "milestone":
                raise AdapterError("milestone target has wrong type")
            w.labels([ids[item["key"]]], add=[f"milestone:{milestone.lower()}"])
            w.link(ids[milestone], ids[item["key"]])
            if item["key"] not in items[milestone]["deps"]:
                items[milestone]["deps"].append(item["key"])
    for key, target in change.get("move", {}).items():
        if target not in items or items[target]["type"] != "milestone":
            raise AdapterError("unknown milestone target")
        old = items[key].get("milestone")
        w.labels([ids[key]], add=[f"milestone:{target.lower()}"], remove=[f"milestone:{old.lower()}"] if old else [])
        if old:
            w.link(ids[old], ids[key], remove=True)
            items[old]["deps"].remove(key)
        w.link(ids[target], ids[key])
        if key not in items[target]["deps"]:
            items[target]["deps"].append(key)
        items[key]["milestone"] = target
    for key in change.get("reopen", []):
        commit = w.lifecycle(ids[key], close=False)
        items[key].update(w.c.committed_state(commit, ids[key]))
        items[key].pop("resolution", None)
    for key, reason in change.get("close", {}).items():
        desired = items[key].get("status") if key in {item["key"] for item in additions} else None
        if desired is not None and not ontology.terminal(desired):
            desired = None
        commit = w.lifecycle(ids[key], reason=reason, status=desired)
        items[key].update(w.c.committed_state(commit, ids[key]), resolution=reason)
    return items


def closure_order(items, ontology):
    """Prerequisites first and children before parents; retain all core guards."""
    by_key = {item["key"]: item for item in items}
    closed = set()
    for key, item in by_key.items():
        typ = "decision" if "decision" in item.get("labels", []) else item["type"]
        if ontology.terminal(item.get("status", ontology.initial(typ)), typ):
            closed.add(key)
    children = {k: [i["key"] for i in items if i.get("parent") == k] for k in by_key}
    dependencies = {}
    for key in closed:
        required = list(by_key[key].get("deps", [])) + children[key]
        if any(k not in by_key or k not in closed for k in required):
            raise AdapterError("closed item has an unknown or open prerequisite/child")
        dependencies[key] = required
    result, visiting, seen = [], set(), set()
    def visit(key):
        if key in seen:
            return
        if key in visiting:
            raise AdapterError("closure constraints contain a cycle")
        visiting.add(key)
        for required in dependencies[key]:
            visit(required)
        visiting.remove(key)
        seen.add(key)
        result.append(key)
    for item in items:
        if item["key"] in closed:
            visit(item["key"])
    return result


def load_plan(c, plan_path):
    plan_path = Path(plan_path).resolve()
    if plan_path.parent != c.workspace:
        raise AdapterError("plan must belong to the explicitly bound workspace")
    plan = read_json(plan_path)
    ontology = c.ontology()
    close_order = closure_order(plan["items"], ontology)
    with Job(c, "load_plan", plan, {"ids.json": {}}) as j:
        if j.done:
            return
        ids = j.data["ids.json"]
        if ids:
            raise AdapterError("load_plan requires empty ids.json; import mappings must be retained")
        w = Writes(c, j)
        working = {"doc": plan["doc"], "items": []}
        create_items(w, working, ids, plan["items"], ontology)
        for item in plan["items"]:
            for dep in item.get("deps", []):
                w.link(ids[item["key"]], ids[dep])
        by_key = {item["key"]: item for item in plan["items"]}
        for key in close_order:
            item = by_key[key]
            typ, _ = fields_for(item, plan["doc"], c.project)
            w.lifecycle(ids[key], reason=item["description"], status=item.get("status", ontology.initial(typ)))
        j.finish({"ids.json": ids})


def apply_change(c, path):
    change = read_json(path)
    with Job(c, "apply_change", change, DEFAULTS) as j:
        if j.done:
            return
        plan, ids, pending = (j.data[x] for x in ("plan.json", "ids.json", "linear-pending.json"))
        apply_body(Writes(c, j), plan, ids, change)
        pending.append({"applied_at": j.state["stamp"], "change": change,
                        "beads": {x["key"]: ids[x["key"]] for x in change.get("create", [])}})
        j.finish({"plan.json": plan, "ids.json": ids, "linear-pending.json": pending})


def restructure(c, payload):
    with Job(c, "restructure", payload, DEFAULTS) as j:
        if j.done:
            return
        plan, ids = j.data["plan.json"], j.data["ids.json"]
        additions = copy.deepcopy(payload["new_items"])
        for m in payload["milestones"]:
            additions.append({"key": m["key"], "type": "milestone", "priority": m["priority"], "parent": None,
                              "deps": m["chain"] + m["members"], "title": m["title"],
                              "description": m["description"] + "\n\n" + payload["holds_no_work"]})
        updates = {k: {"title": v[0], "description": v[1]} for k, v in payload["retitles"].items()}
        for key, priority in payload["priority_changes"].items():
            updates.setdefault(key, {})["priority"] = priority
        change = {"create": additions, "update": updates,
                  "dep_remove": {k: v[0] for k, v in payload["dep_changes"].items()},
                  "dep_add": {k: v[1] for k, v in payload["dep_changes"].items()}}
        w = Writes(c, j)
        items = apply_body(w, plan, ids, change)
        for m in payload["milestones"]:
            w.labels([ids[k] for k in m["members"]], add=[f"milestone:{m['key'].lower()}"])
            for key in m["members"]:
                items[key]["milestone"] = m["key"]
        j.finish({"plan.json": plan, "ids.json": ids})


def seed(c, items, resume=None):
    ontology = c.ontology()
    for it in items:
        typ, fields = fields_for(it, project=c.project)
        if ontology.terminal(fields.get("status", ontology.initial(typ)), typ) and not it.get("closed"):
            raise AdapterError("terminal seed requires its explicit close reason")
    with Job(c, "seed", {"items": items, "resume": resume}, {"ids.json": {}}) as j:
        if j.done:
            return
        ids = j.data["ids.json"]
        w = Writes(c, j)
        for it in sorted(items, key=lambda x: x["type"] != "epic"):
            if it["key"] not in ids:
                typ, fields = fields_for(it, project=c.project)
                if ontology.terminal(fields.get("status", ontology.initial(typ)), typ):
                    fields.pop("status", None)
                ids[it["key"]] = w.create(typ, fields, ids[it["parent"]] if it.get("parent") else None)
            else:
                c.get(ids[it["key"]])
        if resume is not None and resume not in {x["key"] for x in items}:
            raise AdapterError("unknown seed resume key")
        start = next((i for i, x in enumerate(items) if x["key"] == resume), 0)
        for it in items[start:]:
            for blocker in it.get("blocked_by", []):
                # Plug link is structurally idempotent; no string-matching of errors.
                w.link(ids[it["key"]], ids[blocker])
            if it.get("closed"):
                w.lifecycle(ids[it["key"]], reason=it["closed"], status=it.get("status"))
        j.finish({"ids.json": ids})


STEPS = ("implement", "verify", "self-review", "review", "land")


def molecule_mapping(mol):
    return {"id_mapping": {"mol-execplan-step": mol["root"],
            **{"mol-execplan-step." + k: v for k, v in mol["steps"].items()}}}


def pour(c, chunk, keys):
    with Job(c, "pour", {"chunk": chunk, "keys": keys}, {"ids.json": {}, "molecules.json": {}}) as j:
        if j.done:
            return molecule_mapping(j.state["outputs"]["molecules.json"][chunk])
        ids, mols = j.data["ids.json"], j.data["molecules.json"]
        if not keys or len(set(keys)) != len(keys) or any(k not in ids for k in keys) or chunk in mols:
            raise AdapterError("unknown/duplicate keys or chunk already poured")
        def build():
            ontology = c.ontology()
            ontology.initial("molecule")
            ontology.initial("task")
            conditions = [expect(c.get(ids[k])) for k in keys]
            ops = [{"kind": "create", "key": "root", "type": "molecule", "project": c.project,
                    "fields": {"title": chunk, "labels": [c.project], "priority": 2}}]
            for step in STEPS:
                ops.append({"kind": "create", "key": step, "type": "task", "project": c.project,
                            "parent": {"key": "root"}, "fields": {"title": f"{chunk}: {step}", "priority": 2,
                            "labels": [c.project, "step:" + step] + ["for:" + k.lower() for k in keys]}})
            for before, after in zip(STEPS, STEPS[1:]):
                ops.append({"kind": "link", "from": {"key": after}, "to": {"key": before}, "linkKind": "blocks"})
            for key in keys:
                ops.append({"kind": "link", "from": c.ref(ids[key]), "to": {"key": "root"}, "linkKind": "blocks"})
            return {"changeSet": {"ops": ops, "expect": conditions}}
        commit = j.mutate("tx.apply", build)
        root = created_ref(commit, 0)
        steps = {step: created_ref(commit, i + 1) for i, step in enumerate(STEPS)}
        c.generated.update([root, *steps.values()])
        mols[chunk] = {"root": root, "steps": steps, "keys": keys}
        j.finish({"molecules.json": mols})
        return molecule_mapping(mols[chunk])


def main(kind, payload=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--binding", required=True)
    if kind in {"apply", "restructure", "pour"}:
        parser.add_argument("workspace")
    if kind == "load":
        parser.add_argument("plan")
    elif kind == "apply":
        parser.add_argument("change")
    elif kind == "pour":
        parser.add_argument("chunk")
        parser.add_argument("keys", nargs="+")
    elif kind == "seed":
        parser.add_argument("resume", nargs="?")
    args = parser.parse_args()
    project = "cody-ops" if kind == "seed" else "signals-v2"
    workspace = str(Path(args.plan).resolve().parent) if kind == "load" else getattr(args, "workspace", None)
    c = Client(args.binding, project, workspace)
    if kind == "load":
        return load_plan(c, args.plan)
    if kind == "apply":
        return apply_change(c, args.change)
    if kind == "restructure":
        return restructure(c, payload)
    if kind == "seed":
        return seed(c, payload, args.resume)
    if kind == "pour":
        result = pour(c, args.chunk, args.keys)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return result
    raise AdapterError("unknown driver")
