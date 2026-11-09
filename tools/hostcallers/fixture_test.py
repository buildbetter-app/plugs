"""Synthetic only. Invoked remotely by the actual SQLite/RPC Go fixture."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import plug_host as ph
import host_drivers as hd
from projection import Projection


class HostCallerFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="plug-i8-fixture-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.ops, self.signals = self.root/'cody-ops', self.root/'signals-v2'
        self.ops.mkdir(); self.signals.mkdir()
        self.scripts = self.root/'scripts'; self.scripts.mkdir()
        source = Path(__file__).resolve().parent
        for p in (source/'fixtures').glob('*.py'):
            shutil.copyfile(p, self.scripts/p.name)
        for name in ('plug_host.py', 'host_drivers.py', 'projection.py'):
            shutil.copyfile(source/name, self.scripts/name)
        self.cfg = {'schema':'plug-host-binding/1', 'endpoint':os.environ['I8_TEST_ENDPOINT'],
                    'incarnation':os.environ['I8_TEST_INCARNATION'], 'principal':{'kind':'anonymous','id':'anonymous'},
                    'agent':'cody', 'projects':{'cody-ops':{'workspace':str(self.ops),'legacy_ids':{}},
                                              'signals-v2':{'workspace':str(self.signals),'legacy_ids':{}}}}
        self.binding = self.root/'binding.json'
        self.write_binding()
        self.c = ph.Client(self.binding, 'signals-v2')

    def write_binding(self):
        ph.atomic_json(self.binding, self.cfg)

    def run_script(self, name, *args):
        env = {**os.environ, 'BEADS_DIR':'/fixture-forbidden-ambient', 'PLUG_SERVER':'http://127.0.0.1:1'}
        proc = subprocess.run([sys.executable, '-I', str(self.scripts/name), '--binding', str(self.binding), *map(str,args)],
                              cwd=self.root, env=env, capture_output=True, text=True, timeout=120)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc.stdout

    def create(self, c=None, **fields):
        c = c or self.c
        with ph.Job(c, 'fixture-create', str(uuid.uuid4()), {}) as j:
            ident = ph.Writes(c,j).create('task', {'title':'synthetic fixture', 'labels':[], **fields})
            j.finish({})
            return ident

    def write(self, name, value, workspace=None):
        ph.atomic_json((workspace or self.signals)/name, value)

    def lifecycle(self, ident, close=True):
        return self.c.rpc('bead.close' if close else 'bead.reopen', {'id':self.c.ref(ident)}, str(uuid.uuid4()))

    def assert_refused(self, ident):
        with self.assertRaises(ph.RPCError) as e:
            self.lifecycle(ident)
        self.assertEqual(e.exception.code, 'ontology_guard_failed')

    def test_six_prepared_scripts_real_rpc(self):
        plan={'doc':'fixture.md','items':[
            dict(key='P',type='epic',priority=1,parent=None,deps=[],title='Synthetic parent',description='fixture'),
            dict(key='A',type='task',priority=2,parent='P',deps=[],title='Synthetic A',description='fixture',labels=['keep','milestone:m0'],milestone='M0'),
            dict(key='B',type='task',priority=3,parent='P',deps=['A'],title='Synthetic B',description='fixture'),
            dict(key='M0',type='milestone',priority=1,parent=None,deps=['A'],title='Synthetic M0',description='fixture')]}
        self.write('plan.json',plan)
        self.run_script('load_plan.py', self.signals/'plan.json')
        ids=ph.read_json(self.signals/'ids.json'); self.assertEqual(set(ids),{'P','A','B','M0'})
        self.assertEqual(self.c.get(ids['A'])['parent'],ids['P'])
        self.assertEqual(self.c.get(ids['B'])['links'][0]['to']['id'],ids['A'])
        change={'create':[dict(key='M2',type='milestone',priority=2,parent=None,deps=[],title='Synthetic M2',description='fixture'),
                          dict(key='C',type='task',priority=0,parent='P',deps=['B'],title='Synthetic decision',description='fixture',labels=['decision'],design='fixture design',acceptance='fixture acceptance')],
                'update':{'A':{'title':'Changed A','priority':0,'design':'updated design','acceptance':'updated acceptance'}},
                'dep_remove':{'B':['A']},'move':{'A':'M2'},'close':{'B':'fixture close'}}
        self.write('change.json',change)
        self.run_script('apply_change.py', self.signals,self.signals/'change.json')
        ids=ph.read_json(self.signals/'ids.json')
        self.assertEqual(self.c.get(ids['C'])['type'],'decision')
        a=self.c.get(ids['A']); self.assertEqual(a['fields']['acceptance_criteria'],'updated acceptance')
        self.assertEqual(a['fields']['priority'],0); self.assertIn('keep',a['labels'])
        self.assertIn('milestone:m2',a['labels']); self.assertNotIn('milestone:m0',a['labels'])
        self.assertEqual(self.c.get(ids['B'])['status'],'closed')
        self.write('amend.json',{'reopen':['B'],'close':{'B':'fixture amended close'}})
        self.run_script('apply_change.py',self.signals,self.signals/'amend.json')
        self.assertEqual(len(ph.read_json(self.signals/'linear-pending.json')),2)
        bprojected=Projection(self.c).row(self.c.get(ids['B']))
        self.assertEqual(bprojected['close_reason'],'fixture amended close')
        out=json.loads(self.run_script('pour_step.py',self.signals,'synthetic chunk','A'))
        mapping=out['id_mapping']; self.assertEqual(len(mapping),6)
        # Completed stdout-loss retry must return the same durable mapping.
        again=json.loads(self.run_script('pour_step.py',self.signals,'synthetic chunk','A'))
        self.assertEqual(again,out)
        mol=ph.read_json(self.signals/'molecules.json')['synthetic chunk']
        self.assertEqual(mapping['mol-execplan-step'],mol['root'])
        self.assertEqual(set(mol['steps']),set(hd.STEPS))
        for ident in (ids['A'],mol['root'],mol['steps']['land'],mol['steps']['verify']):
            self.assert_refused(ident)
        for i, step in enumerate(hd.STEPS):
            if i + 1 < len(hd.STEPS):
                self.assert_refused(mol['steps'][hd.STEPS[i + 1]])
            self.lifecycle(mol['steps'][step])
        self.lifecycle(mol['root']); self.lifecycle(ids['A'])
        self.write('prep-restructure.json',{'reopen':['B'],'dep_add':{'B':['A']}})
        self.run_script('apply_change.py',self.signals,self.signals/'prep-restructure.json')
        self.run_script('restructure_milestones.py',self.signals)
        ids=ph.read_json(self.signals/'ids.json')
        self.assertIn('E',ids); self.assertIn('M1',ids)
        for key in ('A','B'):
            self.assertIn('milestone:m1',self.c.get(ids[key])['labels'])
        self.assertEqual(self.c.get(ids['A'])['fields']['title'],'Retitled fixture')
        self.write('linear-map.json',{'A':'FIXTURE-1'})
        view=json.loads(self.run_script('milestone_view_server.py','--static',self.root,'--data-once'))
        node=next(n for n in view['nodes'] if n['id']==ids['A'])
        self.assertEqual(node['linear'],'https://linear.app/buildbetter/issue/FIXTURE-1')
        self.assertEqual(node['milestone'],'M1'); self.assertFalse(view['projectionStale'])
        bnode=next(n for n in view['nodes'] if n['id']==ids['B'])
        # Restructure reopened B; the old amended resolution must be absent.
        self.assertIsNone(bnode['resolution'])
        self.assertTrue(any(e['type']=='parent-child' and e['target']==ids['A'] and e['source']==ids['P'] for e in view['edges']))
        self.run_script('cody-ops-seed.py')
        opsids=ph.read_json(self.ops/'ids.json'); oc=ph.Client(self.binding,'cody-ops')
        op=oc.get(opsids['OPS.1']); self.assertEqual(op['fields']['assignee'],'cody')
        self.assertEqual(op['fields']['due_at'],'2026-11-06T00:00:00Z')
        self.assertEqual(op['fields']['external_ref'],'fixture:ops')
        self.assertEqual(op['parent'],opsids['OPS'])
        self.assertNotIn(opsids['OPS.1'],[b['id'] for b in self.c.snapshot()])

    def test_load_closed_graph_orders_prerequisites_and_children(self):
        plan = {"doc": "fixture.md", "items": [
            dict(key="P",type="epic",priority=1,parent=None,deps=[],title="Closed parent",description="parent close",status="closed"),
            dict(key="A",type="task",priority=1,parent="P",deps=[],title="Closed prerequisite",description="A close",status="closed"),
            dict(key="B",type="task",priority=1,parent="P",deps=["A"],title="Closed dependent",description="B close",status="closed"),
        ]}
        self.write('plan.json', plan)
        self.assertEqual(hd.closure_order(plan['items'], self.c.ontology()), ['A','B','P'])
        self.run_script('load_plan.py', self.signals/'plan.json')
        ids=ph.read_json(self.signals/'ids.json')
        for key in ('A','B','P'):
            bead=self.c.get(ids[key])
            self.assertEqual(bead['status'],'closed')
            self.assertEqual(Projection(self.c).row(bead)['close_reason'],key+' close' if key!='P' else 'parent close')
        self.run_script('load_plan.py', self.signals/'plan.json')
        invalid=copy.deepcopy(plan['items'])
        invalid[1]['status']='open'
        with self.assertRaises(ph.AdapterError): hd.closure_order(invalid, self.c.ontology())

    def test_binding_cwd_env_principal_and_incarnation(self):
        with self.assertRaises(ph.AdapterError):
            ph.Client(self.binding,'signals-v2',self.ops)
        self.cfg['principal']={'kind':'user','id':'wrong'}; self.write_binding()
        with self.assertRaises(ph.AdapterError): ph.Client(self.binding,'signals-v2')
        self.cfg['principal']={'kind':'anonymous','id':'anonymous'}
        self.cfg['incarnation']='wrong'; self.write_binding()
        with self.assertRaises(ph.AdapterError): ph.Client(self.binding,'signals-v2')

    def test_priorities_refuse_coercion_and_preserve_seed_integer_strings(self):
        before={b['id'] for b in self.c.snapshot()}
        before_ops={b['id'] for b in ph.Client(self.binding,'cody-ops').snapshot()}
        for n, priority in enumerate((True, False, 2.9, 2.0, None, '2.9', 'true', -1, 5)):
            for kind, project in (('load','signals-v2'),('seed','cody-ops')):
                with self.subTest(kind=kind, priority=priority):
                    workspace=self.root/(kind+'-priority-'+str(n)); workspace.mkdir()
                    self.cfg['projects'][project]['workspace']=str(workspace)
                    self.write_binding(); client=ph.Client(self.binding,project)
                    item=dict(key='K',type='task',title='Invalid priority fixture',description='fixture',parent=None,deps=[])
                    if kind=='load':
                        item['priority']=priority
                        plan=workspace/'plan.json'; ph.atomic_json(plan,{'doc':'fixture.md','items':[item]})
                        with self.assertRaisesRegex(ph.AdapterError,'priority'):
                            hd.load_plan(client,plan)
                    else:
                        item['prio']=priority
                        with self.assertRaisesRegex(ph.AdapterError,'priority'):
                            hd.seed(client,[item])
                    self.assertFalse((workspace/'ids.json').exists())
        self.assertEqual({b['id'] for b in self.c.snapshot()},before)
        self.assertEqual({b['id'] for b in ph.Client(self.binding,'cody-ops').snapshot()},before_ops)
        workspace=self.root/'valid-seed-priorities'; workspace.mkdir()
        self.cfg['projects']['cody-ops']['workspace']=str(workspace)
        self.write_binding(); client=ph.Client(self.binding,'cody-ops')
        before_ops={b['id'] for b in client.snapshot()}
        items=[dict(key=str(p),type='task',title='Seed integer priority',prio=str(p)) for p in range(5)]
        hd.seed(client,items)
        ids=ph.read_json(workspace/'ids.json')
        self.assertEqual({b['id'] for b in client.snapshot()}-before_ops,set(ids.values()))
        for p in range(5):
            self.assertEqual(client.get(ids[str(p)])['fields']['priority'],p)

    def test_lossless_import_ids_and_cross_project_refusal(self):
        signal=self.create(); oc=ph.Client(self.binding,'cody-ops'); ops=self.create(oc)
        original='Case.Same/İD:opaque'
        self.cfg['projects']['signals-v2']['legacy_ids']={original:{'id':signal}}
        self.cfg['projects']['cody-ops']['legacy_ids']={original:{'id':ops}}
        self.write_binding(); sc=ph.Client(self.binding,'signals-v2'); oc=ph.Client(self.binding,'cody-ops')
        self.assertEqual(sc.get(original)['id'],signal); self.assertEqual(oc.get(original)['id'],ops)
        self.assertEqual(sc.original(signal),original)
        self.write('ids.json',{'K':original})
        with ph.Job(sc,'import-label','fixture',{'ids.json':{}}) as j:
            ph.Writes(sc,j).labels([j.data['ids.json']['K']],add=['imported'])
            j.finish({'ids.json':j.data['ids.json']})
        self.assertEqual(ph.read_json(self.signals/'ids.json')['K'],original)
        with self.assertRaises(ph.AdapterError): sc.get(ops)
        # An otherwise valid canonical ID without mapping/receipt must not be
        # guessed from a legacy sidecar, even when it belongs to this project.
        isolated = self.root/'unmapped-signals'; isolated.mkdir()
        self.cfg['projects']['signals-v2']['workspace'] = str(isolated)
        self.cfg['projects']['signals-v2']['legacy_ids'] = {}
        self.write_binding()
        unmapped = ph.Client(self.binding, 'signals-v2')
        with self.assertRaises(ph.AdapterError): unmapped.get(signal)

    def test_multi_id_label_cas_preserves_competing_edit(self):
        a,b=self.create(labels=['base']),self.create(labels=['base'])
        original=self.c.rpc; raced=False
        def racing(method,params,key=None):
            nonlocal raced
            if method=='tx.apply' and not raced:
                raced=True
                original('bead.update',{'id':{'id':b},'fields':{'labels':['base','concurrent']}},str(uuid.uuid4()))
            return original(method,params,key)
        with patch.object(self.c,'rpc',racing):
            with ph.Job(self.c,'label-race','fixture',{}) as j:
                ph.Writes(self.c,j).labels([a,b],add=['new'],remove=['base']); j.finish({})
                self.assertEqual(j.state['calls'][0]['attempts'][0]['error'],'precondition_failed')
        self.assertEqual(self.c.get(a)['labels'],['new'])
        self.assertEqual(self.c.get(b)['labels'],['concurrent','new'])

    def test_lost_create_response_replays_exact_key_without_duplicate(self):
        before={b['id'] for b in self.c.snapshot()}
        original=self.c.rpc; dropped=False
        def dropping(method,params,key=None):
            nonlocal dropped
            result=original(method,params,key)
            if method=='bead.create' and not dropped:
                dropped=True
                raise ph.AdapterError('fixture dropped committed response')
            return result
        with patch.object(self.c,'rpc',dropping):
            with self.assertRaises(ph.AdapterError):
                with ph.Job(self.c,'lost-response','same',{}) as j:
                    ph.Writes(self.c,j).create('task',{'title':'lost response fixture'})
        book=ph.read_json(self.signals/'.plug-host-journal.json')
        intent=next(iter(book['jobs'].values()))['calls'][0]['attempts'][0]
        with ph.Job(self.c,'lost-response','same',{}) as j:
            ident=ph.Writes(self.c,j).create('task',{'title':'lost response fixture'}); j.finish({})
            self.assertEqual(j.state['calls'][0]['attempts'][0]['key'],intent['key'])
        self.assertEqual({b['id'] for b in self.c.snapshot()}-before,{ident})

    def test_sidecar_partial_replace_recovers_once(self):
        self.write('plan.json',{'doc':'fixture.md','items':[]})
        change={'create':[dict(key='NEW',type='task',priority=1,parent=None,deps=[],title='Recovery fixture',description='fixture')]}
        self.write('change.json',change)
        original=ph.atomic_json; failed=False
        def crashing(path,value):
            nonlocal failed
            if Path(path).name=='ids.json' and not failed:
                failed=True; raise OSError('fixture interrupted after first file')
            return original(path,value)
        with patch.object(ph,'atomic_json',crashing):
            with self.assertRaises(OSError): hd.apply_change(self.c,self.signals/'change.json')
        self.assertEqual(len(ph.read_json(self.signals/'plan.json')['items']),1)
        hd.apply_change(self.c,self.signals/'change.json')
        self.assertEqual(len(ph.read_json(self.signals/'ids.json')),1)
        self.assertEqual(len(ph.read_json(self.signals/'linear-pending.json')),1)
        hd.apply_change(self.c,self.signals/'change.json')
        self.assertEqual(len(ph.read_json(self.signals/'linear-pending.json')),1)

    def test_pending_job_and_external_sidecar_edit_fail_closed(self):
        self.write('ids.json',{'K':'initial'})
        with self.assertRaises(ph.AdapterError):
            with ph.Job(self.c,'unfinished','one',{'ids.json':{}}):
                raise ph.AdapterError('fixture interruption')
        with self.assertRaises(ph.AdapterError):
            with ph.Job(self.c,'other','two',{'ids.json':{}}): pass
        self.write('ids.json',{'K':'external edit'})
        with self.assertRaises(ph.AdapterError):
            with ph.Job(self.c,'unfinished','one',{'ids.json':{}}): pass

    def test_paginated_projection_is_pinned_during_competing_write(self):
        marker = str(uuid.uuid4())
        # The current view budget counts rows. Exceed the adapter's 1000-row
        # budget with six bounded transactions, rather than guessing token cost.
        expected = set()
        for batch in range(6):
            ops = [{"kind": "create", "key": "page-" + str(batch * 200 + i), "type": "task", "project": self.c.project,
                    "fields": {"title": "pagination fixture " + marker}} for i in range(200)]
            commit = self.c.rpc("tx.apply", {"changeSet": {"ops": ops}}, str(uuid.uuid4()))
            expected.update(ph.created_ref(commit, i) for i in range(200))
        original = self.c.rpc
        pages = 0
        late = None
        def competing(method, params, key=None):
            nonlocal pages, late
            result = original(method, params, key)
            if method == "view.run":
                pages += 1
                if pages == 1:
                    late = ph.created_ref(original("bead.create", {"type": "task", "fields": {"title": "after pinned snapshot"}}, str(uuid.uuid4())))
            return result
        with patch.object(self.c, "rpc", competing):
            snapshot = self.c.snapshot()
        got = {b["id"] for b in snapshot}
        self.assertGreater(pages, 1)
        self.assertTrue(expected <= got)
        self.assertNotIn(late, got)
        self.assertIn(late, {b["id"] for b in self.c.snapshot()})

    def test_projection_real_current_visibility_and_expiry(self):
        self.create()
        view=Projection(self.c)
        path=Path(view.fresh())
        self.assertTrue(path.read_bytes())
        def mode(value):
            with urllib.request.urlopen(self.cfg['endpoint']+'/fixture-visibility?mode='+str(value)) as response:
                self.assertEqual(response.status,204)
        try:
            mode(1)
            # Actual viewops current authorization silently removes denied rows.
            self.assertEqual(Path(view.fresh()).read_bytes(),b'')
            self.assertFalse(view.stale)
            mode(0); view.fresh(); self.assertTrue(path.read_bytes())
            mode(2)
            # Actual viewops non-redaction refusal must not use the fresh cache.
            with self.assertRaises(ph.RPCError) as error: view.fresh()
            self.assertEqual(error.exception.code,'token_expired')
            self.assertFalse(path.exists())
            with patch.object(self.c,'verify_identity',side_effect=ph.TransportError('fixture offline after expiry')):
                with self.assertRaises(ph.AdapterError): view.fresh()
        finally:
            mode(0)

    def test_projection_authority_refusals_bypass_fresh_and_stale_cache(self):
        self.create()
        view=Projection(self.c)
        path=Path(view.fresh())
        for age in (0, 30):
            for code in ('forbidden','unauthenticated','token_expired','not_found'):
                with self.subTest(age=age,code=code):
                    view.fresh()
                    if age:
                        os.utime(path,(0,0))
                    with patch.object(self.c,'snapshot',side_effect=ph.RPCError(code)):
                        with self.assertRaises(ph.RPCError): view.fresh()
                    self.assertFalse(path.exists())
                    self.assertFalse(view.stale)
        with patch.object(self.c,'verify_identity',side_effect=ph.AdapterError('principal changed')):
            with self.assertRaises(ph.AdapterError): view.fresh()

    def test_pour_recovers_mapping_after_sidecar_finish_interruption(self):
        ident=self.create(); self.write('ids.json',{'A':ident})
        original=ph.atomic_json; failed=False
        def interrupted(path,value):
            nonlocal failed
            if Path(path).name=='molecules.json' and not failed:
                failed=True
                raise OSError('fixture interrupted sidecar finish')
            return original(path,value)
        with patch.object(ph,'atomic_json',interrupted):
            with self.assertRaises(OSError): hd.pour(self.c,'recovered mapping',['A'])
        result=hd.pour(self.c,'recovered mapping',['A'])
        self.assertEqual(len(result['id_mapping']),6)
        self.assertEqual(hd.pour(self.c,'recovered mapping',['A']),result)

    def test_scoped_projection_cache_marks_stale_and_keeps_complete_file(self):
        ident=self.create(); view=Projection(self.c)
        path=Path(view.fresh()); previous=path.read_bytes()
        self.assertIn(ident,[json.loads(x)['id'] for x in path.read_text().splitlines()])
        os.utime(path,(0,0))
        with patch.object(self.c,'snapshot',side_effect=ph.TransportError('fixture offline')):
            self.assertEqual(view.fresh(),str(path)); self.assertTrue(view.stale)
        self.assertEqual(path.read_bytes(),previous)
        changed=copy.deepcopy(self.cfg); changed['agent']='other-agent'
        self.cfg=changed; self.write_binding(); fresh=Projection(ph.Client(self.binding,'signals-v2'))
        with patch.object(fresh.c,'snapshot',side_effect=ph.TransportError('fixture offline')):
            with self.assertRaises(ph.AdapterError): fresh.fresh()


if __name__=='__main__':
    unittest.main()
