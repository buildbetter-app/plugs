"""Public synthetic native Plug dump/restore -> SQLite/RPC -> prepared scripts.

The restored runtime uses literal native categories from the delivered B21 core.
Neither this native archive nor reader fault injections qualify private-store,
Beads-converter or source-history parity.
"""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request
import uuid

from fixture_test import HostCallerFixture
import host_drivers as hd
import plug_host as ph
from projection import Projection


class InstalledProjectCategories(unittest.TestCase):
    run_script = HostCallerFixture.run_script
    write_binding = HostCallerFixture.write_binding
    write = HostCallerFixture.write
    create = HostCallerFixture.create

    def setUp(self):
        HostCallerFixture.setUp(self)
        self.cfg['projects']['signals-v2']['legacy_ids'] = {
            'Imported.Exact/İD': {'id': 'fixture_import_signals'},
            'Imported.Named.Closed': {'id': 'fixture_named_closed'},
            'Imported.Alternate.Done': {'id': 'fixture_alternate_done'}}
        self.cfg['projects']['cody-ops']['legacy_ids'] = {'Imported.Exact/İD': {'id': 'fixture_import_ops'}}
        self.write_binding()
        self.c = ph.Client(self.binding, 'signals-v2')
        self.oc = ph.Client(self.binding, 'cody-ops')

    def test_imported_project_categories_preserve_same_native_name(self):
        for client, category, terminal in ((self.c, 'active', False), (self.oc, 'done', True)):
            bead = client.get('Imported.Exact/İD')
            self.assertEqual(bead['status'], 'todo')
            row = Projection(client).row(bead)
            self.assertEqual((row['id'], row['status'], row['status_category'], row['status_terminal']),
                             ('Imported.Exact/İD', 'todo', category, terminal))
        self.assertNotEqual(self.c.ontology().signature, self.oc.ontology().signature)

    def test_actual_http_admission_requires_marker(self):
        request = urllib.request.Request(self.cfg['endpoint']+'/rpc/v1', data=ph.encoded(
            {'jsonrpc': '2.0', 'id': 'unadmitted', 'method': 'server.hello', 'params': {}}),
            headers={'Content-Type': 'application/json'})
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(request, timeout=10)
        self.assertEqual(error.exception.code, 403)
        self.c.verify_identity()  # The same endpoint admits the actual typed caller.

    def test_actual_script_close_reopen_uses_commit_native_status(self):
        item = dict(key='K', type='task', title='Native lifecycle', description='Public fixture', priority=2, deps=[])
        self.write('plan.json', {'doc': 'fixture.md', 'items': [item]})
        self.run_script('load_plan.py', self.signals/'plan.json')
        self.write('close.json', {'close': {'K': 'native reason'}})
        self.run_script('apply_change.py', self.signals, self.signals/'close.json')
        plan = ph.read_json(self.signals/'plan.json')
        self.assertEqual((plan['items'][0]['status'], plan['items'][0]['status_category']), ('shipped', 'done'))
        ident = ph.read_json(self.signals/'ids.json')['K']
        row = Projection(self.c).row(self.c.get(ident))
        self.assertEqual(row['status'], 'shipped')
        self.assertEqual(row['close_reason'], 'native reason')
        self.write('reopen.json', {'reopen': ['K']})
        self.run_script('apply_change.py', self.signals, self.signals/'reopen.json')
        plan = ph.read_json(self.signals/'plan.json')
        self.assertEqual((plan['items'][0]['status'], plan['items'][0]['status_category']), ('todo', 'active'))
        self.assertIsNone(Projection(self.c).row(self.c.get(ident))['close_reason'])

    def test_nonterminal_named_closed_retains_name_and_hides_old_reason(self):
        row = Projection(self.c).row(self.c.get('Imported.Named.Closed'))
        self.assertEqual((row['status'], row['status_category'], row['status_terminal']), ('closed', 'active', False))
        self.assertIsNone(row['close_reason'])

    def test_native_plan_closure_order_and_milestone_progress(self):
        plan = {'doc': 'fixture.md', 'items': [
            dict(key='P', type='epic', priority=1, parent=None, deps=[], title='Parent', description='P', status='shipped'),
            dict(key='A', type='task', priority=2, parent='P', deps=[], title='Prerequisite', description='A', status='shipped', labels=['keep', 'milestone:m0']),
            dict(key='B', type='task', priority=2, parent='P', deps=['A'], title='Dependent', description='B', status='shipped', labels=['milestone:m0']),
        ]}
        self.assertEqual(hd.closure_order(plan['items'], self.c.ontology()), ['A', 'B', 'P'])
        self.write('plan.json', plan)
        self.run_script('load_plan.py', self.signals/'plan.json')
        ids = ph.read_json(self.signals/'ids.json')
        self.assertEqual(self.c.get(ids['A'])['status'], 'shipped')
        self.assertEqual(self.c.get(ids['B'])['status'], 'shipped')
        self.write('linear-map.json', {})
        static = self.root/'static'; static.mkdir()
        data = json.loads(self.run_script('milestone_view_server.py', '--static', static, '--data-once'))
        group = next(g for g in data['milestones'] if g['key'] == 'M0')
        self.assertEqual((group['total'], group['closed']), (2, 2))
        by_key = {row['key']: row for row in data['nodes']}
        self.assertEqual((by_key['A']['status'], by_key['A']['statusCategory'], by_key['A']['isClosed']), ('shipped', 'done', True))
        self.run_script('restructure_milestones.py', self.signals)
        data = json.loads(self.run_script('milestone_view_server.py', '--static', static, '--data-once'))
        group = next(g for g in data['milestones'] if g['key'] == 'M1')
        self.assertEqual((group['total'], group['closed']), (2, 2))
        self.assertEqual({label for label in self.c.get(ids['A'])['labels'] if label.startswith('milestone:')}, {'milestone:m1'})
        self.assertIn('keep', self.c.get(ids['A'])['labels'])
        self.assertEqual(self.c.get(ids['A'])['status'], 'shipped')
        self.assertEqual(self.c.get(ids['B'])['status'], 'shipped')

    def test_actual_ready_uses_native_category_without_status_renaming(self):
        active = self.c.ref('Imported.Named.Closed')['id']
        working = self.create()
        self.c.rpc('bead.claim', {'id': self.c.ref(working)}, str(uuid.uuid4()))
        frozen = self.create()
        self.c.rpc('bead.defer', {'id': self.c.ref(frozen)}, str(uuid.uuid4()))
        terminal = self.create()
        HostCallerFixture.lifecycle(self, terminal)
        ready = {row['id'] for row in self.c.rpc('bead.ready', {'limit': 1000})['items']}
        self.assertIn(active, ready)
        self.assertTrue({working, frozen, terminal}.isdisjoint(ready))
        self.assertEqual(self.c.get('Imported.Named.Closed')['status'], 'closed')
        self.assertEqual(self.c.get(terminal)['status'], 'shipped')

    def test_non_slot_targets_refuse_before_plan_writes_and_imported_names_survive(self):
        before = {bead['id'] for bead in self.c.snapshot()}
        for status, code in (('accepted', 'ontology_terminal_target_unsupported'),
                             ('closed', 'ontology_noninitial_create_unsupported')):
            with self.subTest(status=status):
                plan = {'doc': 'fixture.md', 'items': [dict(key='UNSUPPORTED', type='task',
                    title='Exact target', description='No normalization', priority=2, status=status, deps=[])]}
                self.write('plan.json', plan)
                with self.assertRaises(ph.OntologyError) as error:
                    hd.load_plan(self.c, self.signals/'plan.json')
                self.assertEqual(error.exception.code, code)
                with self.assertRaises(ph.OntologyError) as error:
                    hd.seed(self.c, [dict(plan['items'][0], closed='reason' if status == 'accepted' else None)])
                self.assertEqual(error.exception.code, code)
                with ph.Job(self.c, 'fixture-preflight', status, {}) as job:
                    with self.assertRaises(ph.OntologyError) as error:
                        hd.apply_body(ph.Writes(self.c, job), {'doc': 'fixture.md', 'items': []}, {},
                                      {'create': plan['items'], 'close': {'UNSUPPORTED': 'reason'}})
                    self.assertEqual(error.exception.code, code)
                    self.assertEqual(job.state['calls'], [])
                    job.finish({})
                self.assertFalse((self.signals/'ids.json').exists())
                self.assertEqual({bead['id'] for bead in self.c.snapshot()}, before)
        row = Projection(self.c).row(self.c.get('Imported.Alternate.Done'))
        self.assertEqual((row['status'], row['status_category'], row['status_terminal']), ('accepted', 'done', True))

    def test_project_seed_and_molecule_keep_guards_and_scoped_ids(self):
        self.run_script('cody-ops-seed.py')
        ids = ph.read_json(self.ops/'ids.json')
        self.assertEqual(self.oc.get(ids['OPS.1'])['status'], 'queued')
        ident = self.create()
        self.write('ids.json', {'A': ident})
        result = json.loads(self.run_script('pour_step.py', self.signals, 'native names', 'A'))
        self.assertEqual(len(result['id_mapping']), 6)
        mol = ph.read_json(self.signals/'molecules.json')['native names']
        with self.assertRaises(ph.RPCError) as error:
            HostCallerFixture.lifecycle(self, mol['root'])
        self.assertEqual(error.exception.code, 'ontology_guard_failed')
        for name in hd.STEPS:
            HostCallerFixture.lifecycle(self, mol['steps'][name])
        HostCallerFixture.lifecycle(self, mol['root'])
        self.assertEqual(self.c.get(mol['root'])['status'], 'shipped')

    def test_unknown_status_category_and_ontology_drift_invalidate_cache(self):
        self.create()
        projection = Projection(self.c)
        path = Path(projection.fresh())
        result = self.c.rpc('ontology.get', {})
        # Fault injection at the read boundary, not invented installed parity.
        bad = copy.deepcopy(result)
        bad['document']['statuses']['todo']['category'] = 'unrecognized'
        with patch.object(self.c, 'ontology', return_value=ph.InstalledOntology(bad)):
            with self.assertRaises(ph.OntologyError) as error:
                projection.fresh()
        self.assertEqual(error.exception.code, 'ontology_category_unsupported')
        self.assertFalse(path.exists())
        bead = self.c.get('Imported.Exact/İD')
        bead['status'] = 'not-in-ontology'
        with self.assertRaises(ph.OntologyError) as error:
            projection.row(bead)
        self.assertEqual(error.exception.code, 'ontology_status_unknown')
        projection.fresh()
        other = copy.deepcopy(result); other['revision'] += 1
        with patch.object(self.c, 'ontology', side_effect=[ph.InstalledOntology(result), ph.InstalledOntology(other)]):
            with self.assertRaises(ph.OntologyError) as error:
                projection.fresh()
        self.assertEqual(error.exception.code, 'ontology_changed_during_projection')
        self.assertFalse(path.exists())

    def test_ontology_revision_is_lossless_json_integer(self):
        result = self.c.rpc('ontology.get', {})
        self.assertIs(type(result['revision']), int)
        for invalid in (True, False, 1.5, '1', None, -1):
            with self.subTest(revision=invalid):
                result['revision'] = invalid
                with self.assertRaises(ph.OntologyError) as error:
                    ph.InstalledOntology(result)
                self.assertEqual(error.exception.code, 'ontology_revision_invalid')
        result['revision'] = 18446744073709551615
        schema = ph.InstalledOntology(json.loads(ph.encoded(result)))
        self.assertEqual(schema.signature, ph.InstalledOntology(result).signature)

    def test_reader_contract_literal_beads_categories_without_mapping(self):
        result = self.c.rpc('ontology.get', {})
        document = result['document']
        statuses = document['workflows']['host']['statuses']
        for category in ('active', 'wip', 'done', 'frozen'):
            native_name = 'Arbitrary.Native.' + category
            document['statuses'][native_name] = {'name': native_name, 'category': category}
            statuses.append(native_name)
        schema = ph.InstalledOntology(result)
        for category in ('active', 'wip', 'done', 'frozen'):
            native_name = 'Arbitrary.Native.' + category
            bead = self.c.get('Imported.Exact/İD')
            bead['status'] = native_name
            bead['metadata'] = {'i8.host.close_reason': {'schema': 'plug-host-close-reason/1', 'reason': 'current'}}
            row = Projection(self.c).row(bead, schema)
            self.assertEqual((row['status'], row['status_category']), (native_name, category))
            self.assertEqual(row['status_terminal'], category == 'done')
            self.assertEqual(row['close_reason'], 'current' if category == 'done' else None)


if __name__ == '__main__':
    # Imported base fixture class has no ownership of this successor run.
    unittest.main(defaultTest='InstalledProjectCategories')
