import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from host_drivers import main

HOLDS_NO_WORK = "Closes after all members."
new_items = [dict(key="E", type="epic", priority=1, parent=None, deps=[], title="Synthetic epic", description="fixture")]
retitles = {"A": ("Retitled fixture", "Updated fixture")}
priority_changes = {"A": 1}
dep_changes = {"B": (["A"], [])}
milestones = [dict(key="M1", title="Synthetic milestone", priority=1, description="fixture", chain=[], members=["A", "B"])]
main("restructure", dict(new_items=new_items, milestones=milestones,
    retitles=retitles, priority_changes=priority_changes, dep_changes=dep_changes,
    holds_no_work=HOLDS_NO_WORK))
