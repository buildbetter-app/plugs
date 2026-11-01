#!/usr/bin/env python3
"""Build only public synthetic history fixtures from the existing wire fixture."""
import copy
import hashlib
import io
import json
import pathlib
import tarfile

ROOT = pathlib.Path(__file__).resolve().parents[4]
OUT = pathlib.Path(__file__).resolve().parent

def encode(x):
    return (json.dumps(x, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n').encode()

def sha(b):
    return hashlib.sha256(b).hexdigest()

with tarfile.open(ROOT / 'internal/integration/converterroundtrip/testdata/synthetic-plug-dump-1.tar') as source:
    files = {m.name: source.extractfile(m).read() for m in source}
m = json.loads(files.pop('manifest.json'))
m['projects'][0].update(id_mode='hash', hash_length=1)
rows = {k['name']: [json.loads(x) for x in files[k['file']].splitlines()] for k in m['kind_registry']}
rows['beads-archive'] = [r for r in rows['beads-archive'] if r['source']['table'] not in ('issue_versions', 'events')]
rows['edge'][0]['body']['type'] = rows['edge'][0]['source']['fields']['type'] = 'blocks'
opaque = dict(agent_state='waiting\x00opaque', await_id='approval/byte-for-byte', await_type='human',
              ephemeral=1, event_kind=None, hook_bead='demo-8', is_template=1,
              mol_type='formula', payload={'n': 9007199254740993, 'null': None},
              role_bead='demo-7', role_type='agent', target='demo-8',
              timeout_ns=9007199254740993, waiters=['demo-8', None], wisp_type='run', work_type='workflow')
rows['bead'][0]['source']['fields'].update(opaque)
portable = [copy.deepcopy(r) for kind in ['bead', 'edge', 'comment', 'memory', 'config'] for r in rows.get(kind, [])]
frames = []
events = []
for seq in range(1, 5):
    image = copy.deepcopy(portable)
    if seq == 1:
        image = [r for r in image if r['kind'] not in ('edge', 'comment')]
    if seq == 3:
        image = [r for r in image if not (r['kind'] == 'bead' and r['id'] == 'demo-8')]
    if seq < 4:
        for r in image:
            if r['kind'] == 'bead' and r['id'] == 'demo-7':
                r['body']['title'] = r['source']['fields']['title'] = 'Historical title ' + str(seq)
    event = dict(id=seq, issue_id='demo-7', event_type='updated', actor='unverified-source-author',
                 old_value=None, new_value='source-commit-' + str(seq), comment=None, created_at='2026-01-02T00:00:00Z')
    events.append(event)
    frames.append(dict(sequence=seq, commit_id=chr(96+seq)*32, records=image, events=copy.deepcopy(events)))
for e in events:
    rows['beads-archive'].append(dict(kind='beads-archive', id='events/'+sha(encode(e)), project='',
        body=dict(read_only=True, redactable=True, history_verified=False, attribution_verified=False), source=dict(table='events', fields=e)))
for kind, records in rows.items():
    path = 'records/' + kind + '.ndjson'
    files[path] = b''.join(encode(r) for r in sorted(records, key=lambda r: r['project']+'\0'+r['id']))
    next(k for k in m['kind_registry'] if k['name']==kind)['count'] = len(records)
for s in m['sources']:
    if s['family'] == 'issue_versions': s['rows'] = 0
    if s['family'] == 'events': s['rows'] = len(events)
commits = [dict(id=chr(96+i)*32, parents=[] if i==1 else [chr(95+i)*32],
                metadata={'message': 'PUBLIC SYNTHETIC commit '+str(i), 'committer': 'source author; not a Plug principal'}) for i in range(1,5)]
for partial in [False, True]:
    capture = dict(format='beads-dolt-images/1', commits=commits, events=events,
        range=dict(status='verified', reason='', complete=not partial, first=4 if partial else 1, head=4,
                   baseline=frames[2]['records'] if partial else [], baseline_events=frames[2]['events'] if partial else [],
                   gaps=[dict(first=1,last=3,reason='not-retained')] if partial else [], frames=frames[3:] if partial else frames))
    members = dict(files)
    members['history/source.json'] = encode(capture)
    manifest = copy.deepcopy(m)
    manifest['history'] = 'available-unverified' if partial else 'verified'
    manifest['files'] = {p: dict(size=len(b),sha256=sha(b)) for p,b in members.items()}
    members['manifest.json'] = encode(manifest)
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode='w',format=tarfile.USTAR_FORMAT) as tar:
        for p in ['manifest.json'] + sorted(k for k in members if k != 'manifest.json'):
            info=tarfile.TarInfo(p);info.mode=0o600;info.size=len(members[p]);tar.addfile(info,io.BytesIO(members[p]))
    # Go's canonical writer emits exactly two terminal blocks, not Python's
    # record-size padding. Match the canonical source envelope.
    length=sum(512+((len(b)+511)//512)*512 for b in members.values())+1024
    data=output.getvalue()[:length]
    (OUT / ('partial.tar' if partial else 'full.tar')).write_bytes(data)
