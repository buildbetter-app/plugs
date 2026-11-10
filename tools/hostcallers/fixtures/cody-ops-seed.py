import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from host_drivers import main

items = [{'key': 'OPS', 'title': 'Synthetic ops epic', 'type': 'epic', 'prio': '1', 'owner': 'cody'}, {'key': 'OPS.1', 'parent': 'OPS', 'title': 'Synthetic ops task', 'type': 'task', 'prio': '2', 'owner': 'cody', 'labels': ['fixture'], 'due': '2026-11-06', 'ext': 'fixture:ops', 'blocked_by': []}]
main("seed", items)
