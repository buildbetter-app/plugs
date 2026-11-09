"""Prepared explicitly bound Plug milestone view; no Beads export/store access."""
import argparse, functools, http.server, json, os, time
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from plug_host import Client
from projection import Projection
parser = argparse.ArgumentParser()
parser.add_argument("--binding", required=True)
parser.add_argument("--static", required=True)
parser.add_argument("--spec", default=None)
parser.add_argument("--listen", default="127.0.0.1")
parser.add_argument("--port", type=int, default=3001)
parser.add_argument("--data-once", action="store_true")
args = parser.parse_args()
client = Client(args.binding, "signals-v2")
WORKSPACE = str(client.workspace)
STATIC = str(Path(args.static).resolve())
PORT = args.port
projection = Projection(client)
ORDER = ["M0", "M1", "M2", "M3", "M3.5", "M4", "M5", "M6", "M7"]
UNSCHEDULED = "unscheduled"
SPEC_PATH = args.spec


def milestone_from_labels(labels):
    for label in labels or []:
        if label.startswith("milestone:"):
            return label.split(":", 1)[1].upper()
    return None


def fresh_export():
    return projection.fresh()


def build_data():
    ids = json.load(open(os.path.join(WORKSPACE, "ids.json")))
    linear = json.load(open(os.path.join(WORKSPACE, "linear-map.json")))
    key_of = {bead: key for key, bead in ids.items()}
    issues = [json.loads(line) for line in open(fresh_export()) if line.strip()]
    by_id = {issue["id"]: issue for issue in issues}

    nodes, edges, parent_of = [], [], {}
    for issue in issues:
        for dep in issue.get("dependencies") or []:
            if dep["depends_on_id"] in by_id:
                edges.append({"source": dep["depends_on_id"], "target": issue["id"], "type": dep["type"]})
                if dep["type"] == "parent-child":
                    parent_of[issue["id"]] = dep["depends_on_id"]

    # ExecPlan step molecules (implement, verify, self-review, review, land) carry a for:<task key> label.
    bead_of_key = {key.lower(): bead for key, bead in ids.items()}
    # A task bead is blocked by its molecule root; steps are the root's children.
    task_of_root = {}
    for issue in issues:
        for dep in issue.get("dependencies") or []:
            target = by_id.get(dep["depends_on_id"])
            if dep["type"] == "blocks" and target and target["issue_type"] == "molecule":
                task_of_root[target["id"]] = issue["id"]
    def step_task(issue):
        if issue["issue_type"] == "molecule":
            return task_of_root.get(issue["id"])
        root = parent_of.get(issue["id"])
        if root in task_of_root:
            return task_of_root[root]
        for label in issue.get("labels") or []:
            if label.startswith("for:"):
                return bead_of_key.get(label[4:])
        return None

    for issue in issues:
        key = key_of.get(issue["id"], issue["id"])
        is_milestone = issue["issue_type"] == "milestone"
        task_bead = step_task(issue)
        milestone = key if is_milestone else (milestone_from_labels(issue.get("labels")) or UNSCHEDULED)
        if task_bead and task_bead in by_id:
            milestone = milestone_from_labels(by_id[task_bead].get("labels")) or UNSCHEDULED
            step = next((l.split(":", 1)[1] for l in issue.get("labels") or [] if l.startswith("step:")), "plan")
            key = f"{key_of.get(task_bead, task_bead)}·{step}"
        nodes.append({
            "stepOf": task_bead,
            "design": issue.get("design") or None,
            "acceptance": issue.get("acceptance_criteria") or issue.get("acceptance") or None,
            "notes": issue.get("notes") or None,
            "id": issue["id"], "key": key, "title": issue["title"], "status": issue["status"], "statusCategory": issue["status_category"], "isClosed": issue["status_terminal"],
            "type": issue["issue_type"], "priority": issue.get("priority"), "milestone": milestone,
            "isMilestone": is_milestone, "parent": parent_of.get(issue["id"]),
            "linear": f"https://linear.app/buildbetter/issue/{linear[key]}" if key in linear else None,
            "description": (issue.get("description") or "").split("\n\nPlan key:")[0],
            "resolution": issue.get("close_reason") or None,
        })

    # Partitions keep ELK's layers in milestone order. Epics sit with their earliest child.
    partition = {n["id"]: (ORDER.index(n["milestone"]) if n["milestone"] in ORDER else len(ORDER)) for n in nodes}
    for n in nodes:
        if n["type"] == "epic":
            children = [e["target"] for e in edges if e["type"] == "parent-child" and e["source"] == n["id"]]
            partition[n["id"]] = min([partition[c] for c in children] or [0])
    for n in nodes:
        n["partition"] = partition[n["id"]]
    backward = [e for e in edges if e["type"] == "blocks" and partition[e["source"]] > partition[e["target"]]]

    groups = []
    for key in ORDER + [UNSCHEDULED]:
        members = [n for n in nodes if n["milestone"] == key and not n["isMilestone"] and n["type"] != "epic" and not n["stepOf"]]
        title = next((n["title"] for n in nodes if n["isMilestone"] and n["key"] == key), "Unscheduled")
        groups.append({"key": key, "title": title, "total": len(members),
                       "closed": sum(n["isClosed"] for n in members)})
    return {"projectionStale": projection.stale, "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S %Z"), "nodes": nodes, "edges": edges,
            "milestones": groups, "partitionSafe": not backward,
            "backwardEdges": [f'{key_of.get(e["source"])} -> {key_of.get(e["target"])}' for e in backward]}


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] == "/data.json":
            body = json.dumps(build_data()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.split("?")[0] == "/spec.md":
            # The custom signals spec, read live from its worktree so the page always shows the latest draft.
            try:
                body = open(SPEC_PATH, "rb").read()
            except (FileNotFoundError, TypeError):
                body = b"_The spec hasn't been written yet._"
            self.send_response(200)
            self.send_header("Content-Type", "text/markdown; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def end_headers(self):
        # Mocks and pages change often while we iterate; never let the browser serve a stale copy.
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def list_directory(self, path):
        self.send_error(404)


if __name__ == "__main__":
    if args.data_once:
        print(json.dumps(build_data()))
    else:
        handler = functools.partial(Handler, directory=STATIC)
        with http.server.ThreadingHTTPServer((args.listen, PORT), handler) as server:
            print("milestone view port", server.server_port, flush=True)
            server.serve_forever()
