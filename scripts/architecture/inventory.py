#!/usr/bin/env python3
"""Regenerate source atlas and inventory from the pinned upstream Git tree."""
from pathlib import Path
import re,json,collections,datetime
import subprocess,tarfile,tempfile,atexit
# Always inventory the pinned Git tree, never the evolving working directory.
dest=Path(__file__).resolve().parents[2]
guide=dest/'engdocs/architecture-guide'
pin=json.loads((guide/'source-pin.json').read_text())['upstream_sha']
staging=tempfile.TemporaryDirectory(prefix='beads-source-atlas-')
atexit.register(staging.cleanup)
archive=Path(staging.name)/'source.tar'
with archive.open('wb') as stream:
    subprocess.run(['git','archive','--format=tar',pin],cwd=dest,stdout=stream,check=True)
src=Path(staging.name)/'source';src.mkdir()
with tarfile.open(archive) as tree:
    tree.extractall(src,filter='data')
exclude={'upstream.tar.gz'}
paths=sorted(p for p in src.rglob('*') if p.is_file() and p.name not in exclude)
packages={}
for f in paths:
    if f.suffix!='.go': continue
    rel=f.relative_to(src); key=str(rel.parent); s=f.read_text(errors='replace')
    v=packages.setdefault(key,{'path':key,'files':[],'tests':[],'imports':set(),'exported_types':set(),'exported_functions':set()})
    v['tests' if f.name.endswith('_test.go') else 'files'].append(str(rel))
    if f.name.endswith('_test.go'): continue
    for block in re.findall(r'(?m)^import\s*\((.*?)\)',s,re.S):
        v['imports'].update(re.findall(r'"([^"]+)"',block))
    v['imports'].update(re.findall(r'(?m)^import\s+(?:\w+\s+)?"([^"]+)"',s))
    v['exported_types'].update(re.findall(r'(?m)^type\s+([A-Z]\w*)\b',s))
    v['exported_functions'].update(re.findall(r'(?m)^func\s+(?:\([^\n]*\)\s+)?([A-Z]\w*)\s*(?:\[|\()',s))
for p,v in packages.items():
    for k in ('imports','exported_types','exported_functions'): v[k]=sorted(v[k])
areas=collections.Counter(str(f.relative_to(src)).split('/')[0] if len(f.relative_to(src).parts)>1 else '(root)' for f in paths)
counts={'files':len(paths),'go_files':sum(f.suffix=='.go' for f in paths),'go_tests':sum(f.name.endswith('_test.go') for f in paths),'go_package_directories':len(packages),'markdown_files':sum(f.suffix in ('.md','.mdx') for f in paths)}
commands=[]
for f in (src/'cmd/bd').glob('*.go'):
    if f.name.endswith('_test.go'): continue
    for m in re.finditer(r'(?m)^\s*Use:\s*"([^"\n]+)"',f.read_text(errors='replace')):
        commands.append({'use':m.group(1),'file':str(f.relative_to(src)),'line':f.read_text(errors='replace')[:m.start()].count('\n')+1})
inventory={'upstream_sha':pin,'method':'Static textual inventory: file paths, package directories, direct import declarations, anchored exported declarations, literal Cobra Use fields. Not a semantic call graph; grouped type declarations, generated aliases and computed command definitions may not be captured.','counts':counts,'packages':list(packages.values()),'literal_commands':sorted(commands,key=lambda x:(x['use'],x['file']))}
(guide/'source-inventory.json').write_text(json.dumps(inventory,indent=2)+'\n')
meaning={
'cmd':'Cobra CLI, runtime dispatch, context setup, formatting and command implementations',
'internal':'Implementation packages: storage, contracts, discovery, adapters and operations',
'backend':'Public storage implementer API, conformance corpus and HTTP embedder API',
'issueops':'Public narrow issue operation contracts',
'journalops':'Public committed-journal reader contract',
'memoryops':'Public persistent keyed-memory contract',
'schema':'Public schema access surface',
'beadserrors':'Public error classification helpers',
'format':'Public presentation/formatting helpers',
'docs':'Release-pinned published user documentation and generated CLI reference',
'engdocs':'Contributor design documents, ADRs, testing policy and this dossier',
'plugins':'Agent-host plugin assets, skills, commands and lifecycle hook manifest',
'integrations':'Separate MCP integration and editor/agent setup assets',
'examples':'Runnable examples of extensions, configuration and workflows',
'npm-package':'npm binary-distribution wrapper',
'scripts':'Build, release, migration, documentation and repository checks',
'tools':'Build generators, lint tooling, protocol/doc tooling and test support',
'schema':'Public schema surface',
'release-gates':'Release qualification scripts and evidence helpers',
'platforms':'Platform-specific distribution assets',
'.github':'GitHub CI, release and repository automation',
'.githooks':'Contributor Git hook scripts',
'.beads':'Upstream contributor ledger/config artifacts; do not initialize this checkout as a user workspace',
'.claude':'Contributor agent skills and configuration',
'.claude-plugin':'Agent marketplace/plugin metadata',
'.agent':'Agent assets',
'.devcontainer':'Development container setup',
'test':'Cross-package, documentation, protocol and repository tests',
'tests':'Additional integration test assets',
'third_party':'Vendored build or third-party support assets',
'winget':'Windows package manifests',
}
internal={
'ado':'Azure DevOps adapter','atomicfile':'Atomic file replacement','audit':'Audit helpers','beads':'Internal compatibility/consumer API aliases','ceiling':'Resource ceiling helpers','compact':'Compaction logic','config':'Runtime configuration','configfile':'Workspace metadata and backend/mode configuration','creds':'Credential lookup and configuration','debug':'Debug facilities','doltremote':'Dolt remote and Git-ref transport','doltserver':'Local SQL server lifecycle','doltversion':'Dolt version checks','eventsjournal':'Published committed-change envelope and activation/retention','execenv':'Subprocess environment policy','fdhygiene':'File descriptor hygiene','formula':'Formula discovery, parsing and compilation','git':'Git operations','gitenv':'Git environment isolation','githooksenv':'Git hook environment','github':'GitHub integration','gitignore':'Ignore-file updates','gitlab':'GitLab integration','gittraceenv':'Git trace environment','hooks':'Executable workspace mutation hooks','httpapi':'HTTP v0 server, routes, auth, schemas and handler adapters','httpclient':'HTTP-backed store and wire negotiation','idgen':'Issue ID generation','initprocs':'Initialization process helpers','jira':'Jira adapter','linear':'Linear adapter','lockfile':'Local lockfile utility; not a distributed lease protocol','memoryapi':'Shared keyed-memory behavior','metrics':'Metrics helpers','migration':'Migration support','molecules':'Molecule utilities','notion':'Notion integration','procid':'Process identity helpers','query':'Query utilities','recipes':'Recipe support','remotecache':'Remote reference cache','routing':'Cross-workspace issue routing','storage':'Engine contracts, SQL operations, domain/UOW, schema and adapters','telemetry':'OpenTelemetry setup and decorated roles/stores','templates':'Built-in template assets','testutil':'Test-only fixtures and backend helpers','timeparsing':'Human time parsing','tracker':'Shared issue-tracker adapter contract, registry and sync engine','types':'Canonical model, filters and serialized field contracts','ui':'Terminal presentation','uimd':'Markdown UI rendering','utils':'Shared utilities','validation':'Input validation','versioncheck':'Release/version checks','versionedhistory':'History-mode policy','workapi':'Shared work-query request, defaults, builder and response logic','workspacegate':'Workspace concurrency/access gates','worktreeremove':'Worktree removal support'}
text='# Source atlas and navigation\n\nThis is a map of the exact imported source, including the broad areas that a plugin implementation is likely to cross. It is a directory/declaration inventory, not a claim to have traced every function. The detailed chapters trace the important behavior paths.\n\n'
text+=f"Snapshot: `{pin}`. Inventory: **{counts['files']:,} files**, **{counts['go_files']:,} Go files**, **{counts['go_tests']:,} Go test files**, **{counts['go_package_directories']:,} Go package directories**, **{counts['markdown_files']:,} Markdown/MDX files**. Tests and generated files are included in these counts.\n\n"
text+='## Repository map\n\n```mermaid\nflowchart TB\n  Root[Repository root and Go API] --> CLI[cmd/bd]\n  Root --> Public[backend, issueops, journalops, memoryops]\n  Root --> Internal[internal implementation]\n  Internal --> Application[workapi, types, memoryapi]\n  Internal --> Storage[storage contracts, SQL, domain and UOW]\n  Internal --> Adapter[tracker adapters, HTTP and hooks]\n  Root --> Agent[plugins and integrations]\n  Root --> Verification[Bazel, tests, tools and scripts]\n  Root --> Docs[docs: users; engdocs: contributors]\n```\n\n| Area | Files | Purpose |\n|---|---:|---|\n'
for area,n in sorted(areas.items()):
    link='[root](../../README.md)' if area=='(root)' else '`.beads`' if area=='.beads' else f'[{area}](../../{area})'
    text+=f'| {link} | {n:,} | {meaning.get(area,"Build/configuration assets; inspect the linked directory")} |\n'
text+='\n## Internal package families\n\nThe storage subtree is large enough to deserve its own [chapter](03-data-model-and-storage.md). This table covers each immediate implementation family.\n\n| Family | Responsibility |\n|---|---|\n'
for f in sorted((src/'internal').iterdir()):
    if f.is_dir(): text+=f'| [{f.name}](../../internal/{f.name}) | {internal.get(f.name,"See package source and tests")} |\n'
text+='\n## Public API entry points\n\n| Package | Intended consumer |\n|---|---|\n| [beads](../../beads.go) | Open workspaces, use exported types and storage without importing internal implementation |\n| [issueops](../../issueops) | Narrow query/lifecycle/graph/claim/config roles |\n| [memoryops](../../memoryops) | Keyed workspace memory operations |\n| [journalops](../../journalops) | Read committed journal records |\n| [backend](../../backend) | Implement and register a backend |\n| [backend/conformance](../../backend/conformance) | Prove portable behavior and explicit unsupported contracts |\n| [backend/http](../../backend/http) | Embed remote HTTP-backed workspaces and per-open credentials |\n| [schema](../../schema) | Public schema entry points |\n| [format](../../format) | Reuse formatting helpers |\n| [beadserrors](../../beadserrors) | Public error classification |\n\n'
text+='## Package dependency spine\n\n```mermaid\nflowchart LR\n  CLI[cmd/bd] --> Roles[Public issueops and memoryops contracts]\n  CLI --> Store[internal/storage]\n  CLI --> UOW[storage/uow and domain]\n  HTTP[internal/httpapi] --> Roles\n  Store --> SQL[storage/issueops and schema]\n  UOW --> Domain[domain/db repositories]\n  SQL --> Types[internal/types]\n  Domain --> Types\n  Backend[Public backend facade] --> Store\n  SDK[Root beads package] --> Store\n  SDK --> Roles\n```\n\nThis is the important architectural spine, not an exhaustive import DAG. The HTTP transport has guard-enforced restrictions on constructing filters or directly reaching domain use cases; its startup/context files have narrow value-type exceptions. [The HTTP package documentation](../../internal/httpapi/doc.go) specifies those boundaries.\n\n'
text+='## Full package inventory\n\nEvery directory containing Go files appears here. Production/test counts are file counts; declarations are an anchored textual approximation, so grouped declarations and computed aliases may need direct source inspection. The [machine-readable inventory](source-inventory.json) includes direct imports, declaration names, file paths and literal Cobra command definitions.\n\n| Package directory | Production files | Test files | Exported type declarations | Exported function/method names |\n|---|---:|---:|---:|---:|\n'
for k,v in sorted(packages.items()):
    path='../../'+k if k!='.' else '../../'
    text+=f'| [{k}]({path}) | {len(v["files"])} | {len(v["tests"])} | {len(v["exported_types"])} | {len(v["exported_functions"])} |\n'
text+='\n## Small generated contract graph\n\n[Interactive public backend graph](graphify-out/graph.html), [raw graph](graphify-out/graph.json), [audit report](graphify-out/GRAPH_REPORT.md), and [health diagnostic](graphify-out/health.txt) supplement the hand-authored diagrams. It covers only `backend/backend.go`, `backend/types.go`, and `backend/errors.go`. Go aliases and generic references are only partly captured: six unresolved endpoint edges and three repeated endpoint pairs were reported during extraction. Use it for navigation, not semantic authority or whole-repository coverage. No LLM tokens were used for that AST extraction.\n'
(guide/'01-source-atlas.md').write_text(text)
(guide/'source-pin.json').write_text(json.dumps({'upstream_repository':'https://github.com/gastownhall/beads','upstream_sha':pin,'upstream_ref':'main','import_date':'2026-10-22','destination_repository':'https://github.com/nikhilshinday/plugs','go_module_path':'github.com/steveyegge/beads','runtime_changes':False},indent=2)+'\n')
print(json.dumps(counts)); print('literal command definitions:',len(commands))
