"""Prepare zero-context host patches; never writes a live host path or payload copy."""
import ast
import difflib
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
HOST = Path('/home/dev/beads/scripts')
NAMES = ['apply_change.py', 'load_plan.py', 'restructure_milestones.py', 'cody-ops-seed.py', 'pour_step.py', 'milestone_view_server.py']
BOOT = '''import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from host_drivers import main
'''
manifest = []
for name in NAMES:
    raw = (HOST / name).read_bytes()
    before = raw.decode()
    if name in {'apply_change.py', 'load_plan.py', 'pour_step.py'}:
        kind = {'apply_change.py': 'apply', 'load_plan.py': 'load', 'pour_step.py': 'pour'}[name]
        after = '# Prepared Plug caller; explicit --binding is mandatory.\n' + BOOT + f'main({kind!r})\n'
        fixture = after
    elif name == 'cody-ops-seed.py':
        # Keep the private embedded seed in the candidate image IN MEMORY ONLY.
        # Both changed intervals are procedural; zero-context diff emits no seed.
        start = before.index('E = lambda')
        end = before.index('# Pass 1:')
        after = BOOT + '\n' + before[start:end] + '\nmain("seed", items)\n'
        fixture = BOOT + '\nitems = ' + repr([
            {'key':'OPS', 'title':'Synthetic ops epic', 'type':'epic', 'prio':'1','owner':'cody'},
            {'key':'OPS.1','parent':'OPS','title':'Synthetic ops task','type':'task','prio':'2','owner':'cody',
             'labels':['fixture'], 'due':'2026-11-06', 'ext':'fixture:ops','blocked_by':[]},
        ]) + '\nmain("seed", items)\n'
    elif name == 'restructure_milestones.py':
        start = before.index('HOLDS_NO_WORK =')
        end = sum(len(x) for x in before.splitlines(True)[:next(n.end_lineno for n in ast.parse(before).body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'milestones' for t in n.targets))])
        call = '''main("restructure", dict(new_items=new_items, milestones=milestones,
    retitles=retitles, priority_changes=priority_changes, dep_changes=dep_changes,
    holds_no_work=HOLDS_NO_WORK))
'''
        after = BOOT + '\n' + before[start:end] + '\n' + call
        # Entire embedded payload is substituted, including comments.
        fixture = BOOT + '''
HOLDS_NO_WORK = "Closes after all members."
new_items = [dict(key="E", type="epic", priority=1, parent=None, deps=[], title="Synthetic epic", description="fixture")]
retitles = {"A": ("Retitled fixture", "Updated fixture")}
priority_changes = {"A": 1}
dep_changes = {"B": (["A"], [])}
milestones = [dict(key="M1", title="Synthetic milestone", priority=1, description="fixture", chain=[], members=["A", "B"])]
''' + call
    else:
        start = before.index('ORDER =')
        after = '''"""Prepared explicitly bound Plug milestone view; no Beads export/store access."""
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
''' + before[start:]
        after = after.replace('SPEC_PATH = "/home/dev/.claude/worktrees/signal-sets/docs/specs/2026-10-23-signal-sets-spec.md"', 'SPEC_PATH = args.spec')
        left = after.index('FRESH =')
        right = after.index('\ndef build_data():', left)
        after = after[:left] + 'def fresh_export():\n    return projection.fresh()\n\n' + after[right:]
        after = after.replace('"generatedAt": time.strftime', '"projectionStale": projection.stale, "generatedAt": time.strftime')
        after = after.replace('except FileNotFoundError:', 'except (FileNotFoundError, TypeError):')
        left = after.index('if __name__ == "__main__":')
        after = after[:left] + '''if __name__ == "__main__":
    if args.data_once:
        print(json.dumps(build_data()))
    else:
        handler = functools.partial(Handler, directory=STATIC)
        with http.server.ThreadingHTTPServer((args.listen, PORT), handler) as server:
            print("milestone view port", server.server_port, flush=True)
            server.serve_forever()
'''
        fixture = after
    if name in {'cody-ops-seed.py', 'restructure_milestones.py'}:
        kept = before[start:end]
        pre_new, post_new = after.split(kept)
        def hunk(old, new, old_line, new_line):
            old_lines, new_lines = old.splitlines(True), new.splitlines(True)
            return f'@@ -{old_line},{len(old_lines)} +{new_line},{len(new_lines)} @@\n' + ''.join('-'+x for x in old_lines) + ''.join('+'+x for x in new_lines)
        patch = '--- a/scripts/'+name+'\n+++ b/scripts/'+name+'\n'
        patch += hunk(before[:start], pre_new, 1, 1)
        patch += hunk(before[end:], post_new, len(before[:end].splitlines())+1, len((pre_new+kept).splitlines())+1)
    else:
        patch = ''.join(difflib.unified_diff(before.splitlines(True), after.splitlines(True), fromfile='a/scripts/'+name, tofile='b/scripts/'+name, n=0))
    # The two private-file hunks are explicitly outside the retained interval.
    if name in {'cody-ops-seed.py', 'restructure_milestones.py'}:
        assert after == pre_new + kept + post_new
        assert before == before[:start] + kept + before[end:]
    (ROOT/'patches'/f'{name}.patch').write_text(patch)
    (ROOT/'fixtures'/name).write_text(fixture)
    ast.parse(after)
    ast.parse(fixture)
    manifest.append({'path':str(HOST/name),'source_sha256':hashlib.sha256(raw).hexdigest(),
        'prepared_full_post_sha256':hashlib.sha256(after.encode()).hexdigest(),
        'patch_sha256':hashlib.sha256(patch.encode()).hexdigest(),
        'private_payload_copied_to_disk':False,
        'fixture_payload':'synthetic replacement' if name in {'cody-ops-seed.py','restructure_milestones.py'} else 'generic source'})
for helper in ('plug_host.py', 'host_drivers.py', 'projection.py'):
    (ROOT/'patches'/(helper+'.patch')).write_text(''.join(difflib.unified_diff(
        [], (ROOT/helper).read_text().splitlines(True), fromfile='/dev/null', tofile='b/scripts/'+helper, n=0)))
(ROOT/'source-manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
print('prepared six zero-context patches; private payload retained only in live source and transient hash image')
