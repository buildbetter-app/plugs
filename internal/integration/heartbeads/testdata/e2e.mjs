import assert from 'node:assert/strict';
import { readFile, writeFile, mkdtemp, cp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer, request as httpRequest } from 'node:http';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import vm from 'node:vm';
import { observeDependencyRemoval } from './activity-observer.mjs';
import { verifyWatchLifecycle } from './watch-lifecycle.mjs';
import { pathToFileURL } from 'node:url';

const [bindingPath, fixtureRoot, outputs] = process.argv.slice(2);
const binding = JSON.parse(await readFile(bindingPath, 'utf8'));
const actualEndpoint = binding.endpoint;
const temp = await mkdtemp(join(tmpdir(), 'i8-heartbeads-browser-'));
const ui = join(temp, 'ui');
const children = new Set(), servers = new Set();
const receipts = { checks: [], calls: [], responses: [], browserRequests: [], externalRecords: [], browserErrors: [], browserInterceptionFailures: [], browserNetworkFailures: [], boundaries: ['synthetic per-project protected ontology, native names/categories preserved', 'native head import', 'synthetic ATProto session/agent only; real Plug core/transport'] };
const save = async (name, value) => writeFile(join(outputs, name), typeof value === 'string' || Buffer.isBuffer(value) ? value : JSON.stringify(value, null, 2));
const check = (name, detail = {}) => { receipts.checks.push({name, ...detail}); console.log('PASS ' + name + ' ' + JSON.stringify(detail)); };
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, label, timeout = 25000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) { try { const value = await fn(); if (value) return value; } catch (e) { last = e; } await pause(150); }
  throw new Error('Timeout: ' + label, {cause:last});
}
async function listen(handler) {
  const server = createServer((req, res) => { Promise.resolve(handler(req,res)).catch(e => { res.statusCode=500;res.end(String(e.stack)); }); });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve)); servers.add(server);
  return {server, url:`http://127.0.0.1:${server.address().port}`};
}
async function payload(req) { let text='';for await(const chunk of req) text+=chunk;return text; }
function json(res, value, status=200) { res.writeHead(status, {'content-type':'application/json'}); res.end(JSON.stringify(value)); }
function launch(command, args, cwd=ui, extra={}) {
  const child=spawn(command,args,{cwd,env:{...process.env,PATH:'/tmp/plug-swarm/bin/worker-safe:'+process.env.PATH,...extra},stdio:['ignore','pipe','pipe']});
  children.add(child); return child;
}
async function run(command,args,cwd=ui,extra={}) {
  const child=launch(command,args,cwd,extra); let out='';child.stdout.on('data',b=>out+=b);child.stderr.on('data',b=>out+=b);
  const code=await new Promise((resolve,reject)=>{child.on('exit',resolve);child.on('error',reject)});children.delete(child);
  await save(command+'-'+args[0]+'.log',out); assert.equal(code,0,out.slice(-7000));
}
async function stop(child) {
  if (!child || !children.has(child)) return;
  const done=new Promise(resolve=>child.once('exit',resolve)); child.kill('SIGTERM');
  await Promise.race([done,pause(5000).then(()=>child.kill('SIGKILL'))]); children.delete(child);
}
let fault='none', rpcCount=0, helloGate;
function armHelloGate() {
  let release;
  const gate = { wait: new Promise(resolve => { release = resolve; }), release: () => release() };
  helloGate = gate;
  return gate;
}
const rpcProxy=await listen(async(req,res)=>{
  rpcCount++;
  const body=await payload(req); const request=JSON.parse(body);receipts.calls.push(request);
  if (helloGate && request.method === 'server.hello') {
    const gate = helloGate; helloGate = undefined;
    gate.request = structuredClone(request);
    await gate.wait;
  }
  if(fault==='outage'){res.writeHead(503);res.end('synthetic outage');return;}
  if(fault==='malformed'){res.writeHead(200,{'content-type':'application/json'});res.end('{broken');return;}
  const response=await fetch(actualEndpoint+'/rpc/v1',{method:'POST',headers:{'Content-Type':'application/json','Plug-RPC':'1'},body});
  const data=await response.json();receipts.responses.push({requestId:request.id,method:request.method,httpStatus:response.status,envelope:structuredClone(data)});
  if(fault==='envelope')data.jsonrpc='invalid-envelope';
  if(fault==='ontology' && request.method==='ontology.get')data.result.document.statuses.todo.category='unknown-future-category';
  if(fault==='incarnation' && request.method==='server.hello')data.result.incarnation='wrong-instance';
  if(fault==='principal' && request.method==='agent.brief')data.result.whoami.principal.id='wrong-principal';
  json(res,data,response.status);
});
binding.endpoint=rpcProxy.url+'/';
await writeFile(bindingPath,JSON.stringify(binding));
let sequence=0;
async function rpc(project,method,params={},allowError=false) {
  const response=await fetch(rpcProxy.url+'/rpc/v1',{method:'POST',headers:{'Content-Type':'application/json','Plug-RPC':'1'},body:JSON.stringify({jsonrpc:'2.0',id:++sequence,method,params:method==='server.hello'?params:{...params,_meta:{project,agent:binding.agent,...(!['bead.get','bead.ready','view.run','agent.brief','ontology.get'].includes(method)?{idempotencyKey:randomUUID()}: {})}}})});
  const envelope=await response.json();if(!allowError)assert.ok(!envelope.error,JSON.stringify(envelope));return allowError?envelope:envelope.result;
}
const stable=value=>Array.isArray(value)?'['+value.map(stable).join(',')+']':value&&typeof value==='object'?'{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+stable(value[k])).join(',')+'}':JSON.stringify(value);
async function journal(project,calls) {
  const scope=createHash('sha256').update(stable({endpoint:binding.endpoint,incarnation:binding.incarnation,principal:binding.principal,project})).digest('hex');
  await writeFile(join(binding.projects[project].workspace,'.plug-host-journal.json'),JSON.stringify({schema:'plug-host-journal/1',jobs:{e2e:{scope,calls}}}));
}
// External ATProto/indexer/auth are synthetic and remain outside Plug.
const did='did:plc:i8fixtureauthor';
const session={did,handle:'fixture.test',displayName:'Fixture author'};
const externalRecords=new Map();let recordSequence=0;
function putExternal(repo,collection,rkey,value){const uri=`at://${repo}/${collection}/${rkey}`;const entry={cid:'synthetic-cid-'+rkey,collection,did:repo,rkey,uri,value};externalRecords.set(uri,entry);return entry;}
putExternal(did,'org.impactindexer.review.comment','seed-cody',{project:'cody-ops',subject:{uri:'beads:SHARED'},text:'cody-only fixture comment',createdAt:'2026-11-05T00:00:00Z'});
putExternal('did:plc:i8other','org.impactindexer.review.comment','seed-signals',{project:'signals-v2',subject:{uri:'beads:SHARED'},text:'signals-only fixture comment',createdAt:'2026-11-05T00:00:00Z'});
putExternal(did,'org.impactindexer.review.comment','ambiguous',{subject:{uri:'beads:SHARED'},text:'ambiguous unqualified comment',createdAt:'2026-11-05T00:00:00Z'});
putExternal(did,'org.impactindexer.review.comment','ambiguous-claim',{subject:{uri:'beads:SHARED'},text:'@ambiguous.fixture.test',createdAt:'2026-11-05T00:00:01Z'});
putExternal(did,'org.impactindexer.review.like','ambiguous-like',{subject:{uri:`at://${did}/org.impactindexer.review.comment/ambiguous`},createdAt:'2026-11-05T00:00:02Z'});
putExternal(did,'org.impactindexer.review.like','contradictory-like',{project:'cody-ops',nodeId:'SHARED',subject:{uri:'at://did:plc:i8other/org.impactindexer.review.comment/seed-signals'},createdAt:'2026-11-05T00:00:03Z'});
function indexed(body){const collection=JSON.parse(body||'{}').variables?.collection;return {data:{records:{edges:[...externalRecords.values()].filter(r=>r.collection===collection).map(node=>({node})),pageInfo:{hasNextPage:false}}}};}
const externalAgent={com:{atproto:{repo:{
  createRecord:async({repo,collection,rkey,record})=>{const entry=putExternal(repo,collection,rkey||'created-'+(++recordSequence),record);receipts.externalRecords.push({method:'create',entry});return {data:{uri:entry.uri,cid:entry.cid}}},
  putRecord:async({repo,collection,rkey,record})=>{const entry=putExternal(repo,collection,rkey,record);receipts.externalRecords.push({method:'put',entry});return {data:{uri:entry.uri,cid:entry.cid}}},
  getRecord:async({repo,collection,rkey})=>{const entry=externalRecords.get(`at://${repo}/${collection}/${rkey}`);if(!entry)throw Error('external fixture record missing');return {data:{value:entry.value,uri:entry.uri,cid:entry.cid}}},
  deleteRecord:async({repo,collection,rkey})=>{const uri=`at://${repo}/${collection}/${rkey}`;externalRecords.delete(uri);receipts.externalRecords.push({method:'delete',uri});return {data:{}}}
}}}};
let recordModule;
async function recordRequest(url,method,body){const request=new Request(url,{method,...(body?{body,headers:{'content-type':'application/json'}}:{})});const response=await recordModule.namespace[method](request);return {status:response.status,body:await response.text()};}

const roots=Object.fromEntries(Object.entries(binding.projects).map(([p,v])=>[p,v.legacy_ids.SHARED.id]));
const ids={};
let nextLog=''; let next, chrome, ws, cdp, evaluate;
try {
  for (const project of Object.keys(binding.projects)) {
    const result=await rpc(project,'bead.create',{type:'task',fields:{title:'new child '+project,description:'new description',priority:2},...(project==='signals-v2'?{parent:{id:roots[project]}}:{})});
    ids[project]=result.results[0].ref.id;
    // Mixed transaction positions and a receipt for an imported bead must not
    // erase its original SHARED ID or mistake non-create ops for create refs.
    const mixedParams={changeSet:{ops:[{kind:'patch',target:{id:ids[project]},fields:{description:'receipt child'}},{kind:'create',key:'extra',type:'task',fields:{title:'extra '+project,description:'extra discussion '+project,priority:3}}]}};
    if(project==='signals-v2') {
      const bad=structuredClone(mixedParams);bad.changeSet.ops[1].fields.status='todo';
      const refusal=await rpc(project,'tx.apply',bad,true);assert.equal(refusal.error?.data?.code,'ontology_use_slot_verb');
      const unchanged=await rpc(project,'bead.get',{id:{id:ids[project]}});assert.equal(unchanged.bead.fields.description,'new description');check('actual noninitial create refusal preserves atomic patch');
    }
    const mixed=await rpc(project,'tx.apply',mixedParams); ids[project+'-extra']=mixed.results[1].ref.id;
    if(project==='signals-v2')await rpc(project,'bead.defer',{id:{id:ids[project+'-extra']}});
    await journal(project,[{method:'bead.create',attempts:[{params:{type:'task'}}],result},{method:'tx.apply',attempts:[{params:mixedParams}],result:mixed},{method:'bead.create',attempts:[{params:{type:'task'}}],result:{results:[{ref:{id:roots[project]}}]}}]);
  }
  const legacyId=ids['cody-ops-extra'];
  const oldComment=putExternal(did,'org.impactindexer.review.comment','legacy-comment',{subject:{uri:'beads:'+legacyId},text:'old projectless comment',createdAt:'2026-11-05T00:00:04Z'});
  putExternal(did,'org.impactindexer.review.comment','legacy-claim',{subject:{uri:'beads:'+legacyId},text:'@legacy.fixture.test',createdAt:'2026-11-05T00:00:05Z'});
  putExternal(did,'org.impactindexer.review.like','legacy-like',{subject:{uri:oldComment.uri},createdAt:'2026-11-05T00:00:06Z'});
  putExternal(did,'org.impactindexer.review.comment','legacy-adopt',{subject:{uri:'beads:'+legacyId},text:'old adoption comment',createdAt:'2026-11-05T00:00:07Z'});
  await rpc('cody-ops','bead.link',{from:{id:ids['cody-ops']},to:{id:roots['cody-ops']},kind:'blocks'});
  const guard=await rpc('cody-ops','bead.close',{id:{id:ids['cody-ops']}},true);
  assert.ok(guard.error);check('real lifecycle guard refusal',{code:guard.error.data?.code});
  await save('write-readback.json',{roots,ids,guard,beads:await Promise.all(Object.keys(binding.projects).map(p=>rpc(p,'bead.get',{id:{id:roots[p]}})))});

  await mkdir(ui,{recursive:true});
  await run('tar',['-xf',join(fixtureRoot,'heartbeads-source.tar'),'-C',ui],temp);
  await run('npm',['ci','--no-audit','--no-fund','--ignore-scripts']);
  const indexer=await listen(async(req,res)=>json(res,indexed(await payload(req))));
  const networkGuard=join(temp,'network-guard.cjs');
  await writeFile(networkGuard,`const realFetch=globalThis.fetch;globalThis.fetch=async(input,init)=>{const url=typeof input==='string'?input:input.url||String(input);if(url.startsWith('https://public.api.bsky.app/'))return Response.json({handle:'fixture.test',displayName:'Fixture profile'});if(!['127.0.0.1','localhost'].includes(new URL(url).hostname))throw Error('fixture blocked nonloopback fetch');return realFetch(input,init)};`);
  const fixtureEnv={HEARTBEADS_BINDING:bindingPath,INDEXER_URL:indexer.url,COOKIE_SECRET:'synthetic-fixture-cookie-secret-at-least-32-chars',NEXT_TELEMETRY_DISABLED:'1'};
  await run('npm',['run','build'],ui,fixtureEnv);
  const portServer=await listen((req,res)=>res.end('')); const nextPort=portServer.server.address().port;await new Promise(resolve=>portServer.server.close(resolve));servers.delete(portServer.server);
  const nextUrl=`http://127.0.0.1:${nextPort}`;
  const startNext=()=>{ const child=launch('node',['--require',networkGuard,'node_modules/next/dist/bin/next','start','--hostname','127.0.0.1','--port',String(nextPort)],ui,fixtureEnv);child.stdout.on('data',b=>nextLog+=b);child.stderr.on('data',b=>nextLog+=b);return child;};
  next=startNext();await until(async()=> (await fetch(nextUrl+'/api/config')).ok,'Next startup',45000);
  const adapter=await import(pathToFileURL(join(ui,'lib/plug-adapter.mjs')));
  const fixtureSource = JSON.parse(await readFile(join(fixtureRoot,'source-binding.json'),'utf8'));
  const parentSource = await readFile(join(fixtureRoot,'watch-parent.mjs'));
  assert.equal(createHash('sha256').update(parentSource).digest('hex'), fixtureSource.watch_regression.parent_adapter_sha256);
  const parentAdapter = await import(pathToFileURL(join(fixtureRoot,'watch-parent.mjs')));
  await verifyWatchLifecycle({ adapter, parentAdapter, binding, armHelloGate, setFault: value => { fault = value; }, receipts, pause, until, save, check });
  const client=new adapter.PlugClient(await adapter.loadBinding(bindingPath));const store=new adapter.ProjectionStore(client);
  const first=await store.refresh();assert.equal(first.stats.total,6);assert.equal(first.graphData.nodes.filter(n=>n.id==='SHARED').length,2);
  for(const node of first.graphData.nodes.filter(n=>n.id==='SHARED')){assert.equal(node.updatedAt,undefined);assert.ok(node.createdAt);}
  assert.equal(first.projects['cody-ops'].stats.actionable,2);assert.equal(first.projects['signals-v2'].stats.actionable,2);
  const exactTodo=first.graphData.nodes.filter(n=>n.status==='todo');
  assert.ok(exactTodo.some(n=>n.project==='cody-ops'&&n.statusCategory==='active'));
  assert.ok(exactTodo.some(n=>n.project==='signals-v2'&&n.statusCategory==='frozen'));
  check('per-project installed category preserves identical native todo name');
  assert.equal(new Set(first.graphData.nodes.map(n=>n.graphKey)).size,6);
  const cody=first.graphData.links.find(l=>l.type==='blocks');assert.equal(cody.source,'SHARED');assert.equal(cody.target,ids['cody-ops']);
  check('actual imported two-project IDs, receipt IDs, oriented graph and hierarchy readiness',{nodes:6});await save('direct-projection.json',first);
  const paged=await client.page('cody-ops',{filter:{},fields:['fields','project','status','parent'],sort:['id'],budgetTokens:1});
  assert.ok(paged.pages>=2);assert.equal(paged.items.length,3);check('actual pagination pins asOf',{pages:paged.pages,asOf:paged.asOf});

  async function http(path,expected=200){const r=await fetch(nextUrl+path);const data=await r.json();assert.equal(r.status,expected,JSON.stringify(data));return data;}
  const config=await http('/api/config');assert.deepEqual(config.repos,['cody-ops','signals-v2']);
  const all=await http('/api/v1/graph?include=comments,activity&limit=200');
  const ops=await http('/api/v1/graph?project=cody-ops&include=comments,activity&limit=200');
  const sig=await http('/api/v1/graph?project=signals-v2&include=comments,activity&limit=200');
  const ambiguous=await http('/api/v1/issues/SHARED',409);
  const issue=await http('/api/v1/issues/SHARED?project=cody-ops');
  const child=await http('/api/v1/issues/'+encodeURIComponent(ids['cody-ops'])+'?project=cody-ops');
  const ready=await http('/api/v1/ready?project=signals-v2&type=task&limit=1');
  await save('http-responses.json',{config,all,ops,sig,ambiguous,issue,child,ready});
  assert.equal(ops.issues.find(x=>x.id==='SHARED').comments?.[0]?.text,'cody-only fixture comment');
  assert.equal(sig.issues.find(x=>x.id==='SHARED').comments?.[0]?.text,'signals-only fixture comment');
  assert.equal(ops.issues.length,3);assert.equal(sig.issues.length,3);assert.ok(ops.issues.every(n=>n.project==='cody-ops'));
  assert.equal(child.issue.blockers[0].title,'Imported root cody-ops');
  assert.equal(Object.hasOwn(issue.issue,'updated_at'),false);assert.equal(Object.hasOwn(ops.issues.find(n=>n.id==='SHARED'),'updated_at'),false);
  assert.ok(ready.issues.length<=1);check('actual HTTP aggregate/project/ambiguous identity/filter/limit');
  for(const response of [all,ops,sig]) {
    assert.ok(!response.activity.some(e=>String(e.detail).includes('ambiguous')));
    assert.ok(!response.activity.some(e=>e.type==='like-added' && e.issue_id==='SHARED'));
  }
  assert.ok(ops.activity.some(e=>e.detail==='cody-only fixture comment'&&e.project==='cody-ops'));
  assert.equal(sig.issues.find(n=>n.id==='SHARED').comments[0].likes,0);
  assert.ok(!JSON.stringify([ops.issues,sig.issues]).includes('@ambiguous.fixture.test'));
  check('globally ambiguous comment/claim/like omitted from aggregate and each project activity; contradictory like omitted');


  // Cancellation during delayed initialization is driven from the exact route
  // source with only the async initialization dependency held behind a promise.
  const ts=await import(pathToFileURL(join(ui,'node_modules/typescript/lib/typescript.js')));
  const recordCode=ts.default.transpileModule(await readFile(join(ui,'app/api/records/route.ts'),'utf8'),{compilerOptions:{target:99,module:99}}).outputText;
  const recordContext=vm.createContext({Request,Response,URL,console});
  recordModule=new vm.SourceTextModule(recordCode,{context:recordContext});
  const deps={
    'next/server':{NextResponse:{json:(value,options)=>Response.json(value,options)}},
    '@/lib/session':{getSession:async()=>session},
    '@/lib/agent':{getAuthenticatedAgent:async()=>externalAgent},
    '@/lib/plug-adapter.mjs':{loadProjection:()=>store.refresh()}
  };
  await recordModule.link(specifier=>{const values=deps[specifier];if(!values)throw Error('unexpected records dependency '+specifier);return new vm.SyntheticModule(Object.keys(values),function(){for(const [key,value]of Object.entries(values))this.setExport(key,value)},{context:recordContext});});await recordModule.evaluate();
  const deletesBefore=receipts.externalRecords.filter(x=>x.method==='delete').length;
  const wrongDid=await recordRequest(nextUrl+'/api/records?collection=org.impactindexer.review.comment&rkey=seed-cody&project=cody-ops&nodeId=SHARED&did=did:plc:wrong','DELETE');assert.equal(wrongDid.status,403);
  const wrongProject=await recordRequest(nextUrl+'/api/records?collection=org.impactindexer.review.comment&rkey=seed-cody&project=signals-v2&nodeId=SHARED&did='+did,'DELETE');assert.notEqual(wrongProject.status,200);
  assert.equal(receipts.externalRecords.filter(x=>x.method==='delete').length,deletesBefore);check('exact record handler refuses wrong owner and stored-project identity');
  const writesBefore=receipts.externalRecords.length;
  const ambiguousDelete=await recordRequest(nextUrl+'/api/records?collection=org.impactindexer.review.comment&rkey=ambiguous&project=cody-ops&nodeId=SHARED&did='+did,'DELETE');assert.equal(ambiguousDelete.status,403);
  const ambiguousUnlike=await recordRequest(nextUrl+'/api/records?collection=org.impactindexer.review.like&rkey=ambiguous-like&project=cody-ops&nodeId=SHARED&did='+did,'DELETE');assert.equal(ambiguousUnlike.status,403);
  const crossLike=await recordRequest(nextUrl+'/api/records','POST',JSON.stringify({collection:'org.impactindexer.review.like',project:'cody-ops',nodeId:'SHARED',record:{subject:{uri:'at://did:plc:i8other/org.impactindexer.review.comment/seed-signals'},createdAt:new Date().toISOString()}}));assert.equal(crossLike.status,403);
  const crossPut=await recordRequest(nextUrl+'/api/records','PUT',JSON.stringify({collection:'org.impactindexer.review.like',rkey:'contradictory-like',project:'cody-ops',nodeId:'SHARED',record:{subject:{uri:'at://did:plc:i8other/org.impactindexer.review.comment/seed-signals'}}}));assert.equal(crossPut.status,403);
  const crossReply=await recordRequest(nextUrl+'/api/records','POST',JSON.stringify({collection:'org.impactindexer.review.comment',project:'cody-ops',nodeId:'SHARED',record:{subject:{uri:'beads:SHARED'},replyTo:'at://did:plc:i8other/org.impactindexer.review.comment/seed-signals',text:'wrong-project reply',createdAt:new Date().toISOString()}}));assert.equal(crossReply.status,403);
  assert.equal(receipts.externalRecords.length,writesBefore);
  const adopted=await recordRequest(nextUrl+'/api/records','PUT',JSON.stringify({collection:'org.impactindexer.review.comment',rkey:'legacy-adopt',project:'cody-ops',nodeId:legacyId,record:{subject:{uri:'beads:'+legacyId},text:'adopted legacy comment',createdAt:new Date().toISOString()}}));assert.equal(adopted.status,200);
  const adoptedRecord=externalRecords.get(`at://${did}/org.impactindexer.review.comment/legacy-adopt`);assert.equal(adoptedRecord.value.project,'cody-ops');assert.equal(adoptedRecord.value.nodeId,legacyId);
  await save('record-refusals-and-adoption.json',{wrongDid,wrongProject,ambiguousDelete,ambiguousUnlike,crossLike,crossPut,crossReply,adopted,adoptedRecord});
  check('legacy PUT adoption and explicit ambiguous/cross-project record refusals before any external write');

  const commentCollection='org.impactindexer.review.comment', likeCollection='org.impactindexer.review.like';
  const at=(collection,rkey)=>`at://${did}/${collection}/${rkey}`;
  const writeRecord=(method,collection,rkey,record,nodeId=legacyId,project='cody-ops')=>recordRequest(nextUrl+'/api/records',method,JSON.stringify({collection,rkey,record,nodeId,project}));
  const deleteRecord=(collection,rkey,nodeId=legacyId,project='cody-ops',owner=did)=>recordRequest(nextUrl+`/api/records?collection=${collection}&rkey=${rkey}&nodeId=${encodeURIComponent(nodeId)}&project=${project}&did=${owner}`,'DELETE');
  const replyValue=(parent,text)=>({subject:{uri:'beads:'+legacyId},replyTo:parent,text,createdAt:'2026-11-05T00:01:00Z'});
  const mixedReply=await writeRecord('POST',commentCollection,'mixed-scoped',replyValue(oldComment.uri,'mixed scoped reply'));
  assert.equal(mixedReply.status,200);
  const oldDeep=putExternal(did,commentCollection,'mixed-legacy-deep',replyValue(at(commentCollection,'mixed-scoped'),'mixed legacy deep reply'));
  const mixedDeep=await writeRecord('POST',commentCollection,'mixed-scoped-deep',replyValue(oldDeep.uri,'mixed scoped deepest reply'));
  assert.equal(mixedDeep.status,200);
  const mixedHTTP=await http('/api/v1/issues/'+encodeURIComponent(legacyId)+'?project=cody-ops');
  const mixedGraph=await http('/api/v1/graph?project=cody-ops&include=comments,activity&limit=200');
  const mixedReady=await http('/api/v1/ready?project=cody-ops');
  for(const comments of [mixedHTTP.issue.comments,mixedGraph.issues.find(n=>n.id===legacyId).comments,mixedReady.issues.find(n=>n.id===legacyId).comments]) {
    const parent=comments.find(c=>c.text==='old projectless comment');
    assert.equal(parent.replies[0].text,'mixed scoped reply');
    assert.equal(parent.replies[0].replies[0].text,'mixed legacy deep reply');
    assert.equal(parent.replies[0].replies[0].replies[0].text,'mixed scoped deepest reply');
    assert.ok(!comments.some(c=>c.text==='mixed scoped reply'));
  }
  await save('mixed-generation-http.json',{mixedReply,mixedDeep,mixedHTTP,mixedGraph,mixedReady});
  check('actual issue/graph/ready HTTP rebuild mixed legacy/scoped/deeper reply trees');

  const cleanupParent=await writeRecord('POST',commentCollection,'cleanup-parent',{subject:{uri:'beads:'+legacyId},text:'cleanup parent'});
  assert.equal(cleanupParent.status,200);
  const orphanValue=replyValue(at(commentCollection,'cleanup-parent'),'owned scoped orphan');
  const cleanupReply=await writeRecord('POST',commentCollection,'cleanup-reply',orphanValue);assert.equal(cleanupReply.status,200);
  const cleanupDelete=await deleteRecord(commentCollection,'cleanup-parent');assert.equal(cleanupDelete.status,200);
  const orphanAdopt=await writeRecord('PUT',commentCollection,'cleanup-reply',{...orphanValue,text:'owned adopted orphan'});assert.equal(orphanAdopt.status,200);
  const orphanLike=await writeRecord('POST',likeCollection,'cleanup-like',{subject:{uri:at(commentCollection,'cleanup-reply')}});assert.equal(orphanLike.status,200);
  const orphanUnlike=await deleteRecord(likeCollection,'cleanup-like');assert.equal(orphanUnlike.status,200);
  const orphanHTTP=await http('/api/v1/issues/'+encodeURIComponent(legacyId)+'?project=cody-ops');assert.ok(orphanHTTP.issue.comments.some(c=>c.text==='owned adopted orphan'));
  const orphanDelete=await deleteRecord(commentCollection,'cleanup-reply');assert.equal(orphanDelete.status,200);
  putExternal(did,likeCollection,'missing-target-like',{subject:{uri:at(commentCollection,'cleanup-parent')}});
  const afterCleanup=receipts.externalRecords.length;
  const missingLike=await deleteRecord(likeCollection,'missing-target-like');assert.equal(missingLike.status,403);
  const missingNewReply=await writeRecord('POST',commentCollection,'missing-parent-reply',orphanValue);assert.equal(missingNewReply.status,403);
  putExternal(did,commentCollection,'contradictory-orphan',{...orphanValue,project:'signals-v2',nodeId:legacyId});
  const contradictoryOrphan=await deleteRecord(commentCollection,'contradictory-orphan');assert.equal(contradictoryOrphan.status,403);
  const foreignOrphan=await deleteRecord(commentCollection,'cleanup-reply',legacyId,'cody-ops','did:plc:wrong');assert.equal(foreignOrphan.status,403);
  assert.equal(receipts.externalRecords.length,afterCleanup);
  await save('orphan-record-cleanup.json',{cleanupParent,cleanupReply,cleanupDelete,orphanAdopt,orphanLike,orphanUnlike,orphanHTTP,orphanDelete,missingLike,missingNewReply,contradictoryOrphan,foreignOrphan});
  check('owned orphan PUT/like/unlike/delete succeed; missing/contradictory/foreign targets refuse before writes');

  const streamCode=ts.default.transpileModule(await readFile(join(ui,'app/api/beads/stream/route.ts'),'utf8'),{compilerOptions:{target:99,module:99}}).outputText;
  let resolveStore, watches=0,stops=0,intervals=new Set();
  const context=vm.createContext({Request,Response,ReadableStream,TextEncoder,setInterval:(f,ms)=>{const t=setInterval(f,ms);intervals.add(t);return t},clearInterval:t=>{intervals.delete(t);clearInterval(t)}});
  const module=new vm.SourceTextModule(streamCode,{context});
  const dep=new vm.SyntheticModule(['projectionStore'],function(){this.setExport('projectionStore',()=>new Promise(resolve=>{resolveStore=resolve}));},{context});
  await module.link(()=>dep);await module.evaluate();
  const abort=new AbortController();const streamResponse=await module.namespace.GET(new Request(nextUrl+'/api/beads/stream',{signal:abort.signal}));
  await streamResponse.body.cancel();abort.abort();resolveStore({watch:()=>{watches++;return()=>stops++}});await pause(10);
  assert.equal(watches,0);assert.equal(intervals.size,0);check('cancel-before-initialization starts no watcher',{watches,activeIntervals:intervals.size});

  // Fresh responses are always from the public Plug process. The proxy only
  // injects explicit outage/result/authority faults for refusal checks.
  fault='outage';const joined=await Promise.all([store.refresh(),store.refresh(),store.refresh()]);
  assert.ok(joined.every(x=>x.freshness.state==='stale' && x.stats.total===6));check('concurrent transport failures share complete stale outcome');
  fault='malformed';await assert.rejects(store.refresh(),adapter.AuthorityError);assert.equal(store.current,null);check('malformed JSON invalidates cache');
  fault='none';await store.refresh();fault='envelope';await assert.rejects(store.refresh(),adapter.AuthorityError);assert.equal(store.current,null);check('malformed envelope invalidates cache');
  fault='none';await store.refresh();fault='ontology';await assert.rejects(store.refresh(),adapter.AuthorityError);assert.equal(store.current,null);check('unknown installed category typed-refuses and invalidates cache');
  fault='none';await store.refresh();fault='principal';await assert.rejects(store.refresh(),adapter.AuthorityError);assert.equal(store.current,null);fault='none';check('principal refusal invalidates cache');

  // Browser uses actual Next routes; unrelated external indexer/profile calls
  // are refused by the CDP fixture, so no private or live dependency is used.
  const profile=join(temp,'chrome');await mkdir(profile);
  chrome=launch('google-chrome',['--headless=new','--no-sandbox','--disable-gpu','--disable-background-networking','--remote-debugging-port=0','--user-data-dir='+profile,'about:blank'],temp);
  chrome.stdout.resume();chrome.stderr.resume();
  const devPort=await until(async()=>{const text=await readFile(join(profile,'DevToolsActivePort'),'utf8');return Number(text.split('\n')[0]);},'Chrome');
  const tabs=await (await fetch(`http://127.0.0.1:${devPort}/json/list`)).json();const tab=tabs.find(x=>x.type==='page');
  ws=new WebSocket(tab.webSocketDebuggerUrl);await new Promise((resolve,reject)=>{ws.onopen=resolve;ws.onerror=reject});
  let mid=0;const pending=new Map();
  cdp=(method,params={})=>new Promise((resolve,reject)=>{const id=++mid;pending.set(id,{resolve,reject});ws.send(JSON.stringify({id,method,params}));});
  const fulfill=async(requestId,body,status=200)=>cdp('Fetch.fulfillRequest',{requestId,responseCode:status,responseHeaders:[{name:'content-type',value:'application/json'},{name:'access-control-allow-origin',value:'*'},{name:'access-control-allow-methods',value:'POST, OPTIONS'},{name:'access-control-allow-headers',value:'content-type'}],body:Buffer.from(typeof body==='string'?body:JSON.stringify(body)).toString('base64')});
  async function intercepted(p){
    const request=p.request;receipts.browserRequests.push({url:request.url,method:request.method,body:request.postData});
    if(request.url===nextUrl+'/api/status')return fulfill(p.requestId,session);
    if(request.url.startsWith(nextUrl+'/api/records')){const result=await recordRequest(request.url,request.method,request.postData);return fulfill(p.requestId,result.body,result.status);}
    if(request.url.startsWith('https://public.api.bsky.app/'))return fulfill(p.requestId,{handle:'fixture.test',displayName:'Fixture profile'});
    if(request.url.includes('hypergoat')||request.url===indexer.url){
      if(request.method==='OPTIONS')return fulfill(p.requestId,'',204);
      assert.equal(request.method,'POST');return fulfill(p.requestId,indexed(request.postData));
    }
    const local=request.url.startsWith(nextUrl)||request.url.startsWith('http://127.0.0.1:')||request.url.startsWith('data:');
    return cdp(local?'Fetch.continueRequest':'Fetch.failRequest',local?{requestId:p.requestId}:{requestId:p.requestId,errorReason:'BlockedByClient'});
  }
  let closingBrowser=false;
  ws.onmessage=e=>{const m=JSON.parse(e.data);if(m.id){const p=pending.get(m.id);if(p){pending.delete(m.id);m.error?p.reject(Error(JSON.stringify(m.error))):p.resolve(m.result)}}else if(m.method==='Fetch.requestPaused'){void intercepted(m.params).catch(error=>{receipts.browserInterceptionFailures.push({intercept:error.message,url:m.params.request.url,networkId:m.params.networkId,duringBrowserClose:closingBrowser});});}else if(m.method==='Network.loadingFailed'){receipts.browserNetworkFailures.push(m.params);}else if(m.method==='Runtime.exceptionThrown'){receipts.browserErrors.push(m.params);}};

  evaluate=async expression=>{const r=await cdp('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value;};
  const detailText=()=>evaluate("document.querySelector('aside')?.innerText || ''");
  await cdp('Page.enable');await cdp('Runtime.enable');await cdp('Network.enable');await cdp('Emulation.setDeviceMetricsOverride',{width:1440,height:1000,deviceScaleFactor:1,mobile:false});await cdp('Fetch.enable',{patterns:[{urlPattern:'*',requestStage:'Request'}]});
  await cdp('Page.navigate',{url:nextUrl+'/?panel=tasks'});
  await until(async()=> (await evaluate('document.body.innerText')).includes('6 issues'),'browser two-project data');
  await save('browser-before.json',await evaluate('({text:document.body.innerText,status:document.querySelector("[data-testid=plug-freshness]")?.textContent})'));
  await type('input[placeholder="Search tasks..."]','SHARED');
  await until(async()=>await evaluate(`(()=>{const bs=[...document.querySelectorAll('[data-node-key]')];return bs.some(b=>b.textContent.includes('Imported root cody-ops'))&&bs.some(b=>b.textContent.includes('Imported root signals-v2'))})()`),'legacy-ID task search in both projects');
  assert.ok(first.graphData.nodes.filter(n=>n.id==='SHARED').every(n=>!n.title.includes('SHARED')));
  const codyRootKey=first.graphData.nodes.find(n=>n.project==='cody-ops'&&n.id==='SHARED').graphKey;
  await evaluate(`(()=>{const b=[...document.querySelectorAll('button[data-node-key]')].find(x=>x.getAttribute('data-node-key')===${JSON.stringify(codyRootKey)});if(!b)throw Error('scoped task button missing');b.click()})()`);
  await until(async()=>await evaluate(`Boolean(document.querySelector('button[title="Copy description"]'))`),'selected node description panel');
  await pause(200);assert.ok(await evaluate(`Boolean(document.querySelector('button[title="Copy description"]'))`));
  check('browser selects cody SHARED independently of signals SHARED');
  await evaluate(`Object.defineProperty(navigator,'clipboard',{configurable:true,value:{writeText:async(text)=>{window.__copied=text}}})`);
  await evaluate(`document.querySelector('button[title="Copy description"]').click()`);
  const copied=await until(async()=>await evaluate('window.__copied'),'legacy description copy');assert.ok(copied.includes('SHARED'));assert.ok(!copied.includes('\0'));await save('browser-copy.json',{copied});
  await searchFor('SHARED');
  await until(async()=>{const body=await evaluate('document.body.innerText');return body.includes('Imported root cody-ops')&&body.includes('Imported root signals-v2')},'legacy-ID global search in both projects');
  assert.ok(!(await evaluate('document.body.innerText')).includes('\0'));
  await closeSearch();
  check('browser searches original ID absent from titles and copies public ID without NUL');
  const missingTime=await evaluate(`(()=>{const detail=document.querySelector('aside');const row=[...detail.querySelectorAll('div')].find(el=>el.children.length===2&&el.children[0].textContent==='Updated');return {text:detail.innerText,updated:row?.innerText}})()`);
  assert.ok(missingTime.updated.includes('Unavailable'));assert.ok(!missingTime.text.includes('Invalid Date'));await save('browser-missing-update-time.json',missingTime);
  check('actual missing update time stays absent in HTTP and displays Unavailable without inventing a timestamp');

  await until(async()=> (await detailText()).includes('cody-only fixture comment'),'scoped detail comment load');
  const indexerTraffic=receipts.browserRequests.filter(r=>r.url.includes('hypergoat'));
  assert.ok(indexerTraffic.some(r=>r.method==='OPTIONS'));
  assert.ok(indexerTraffic.some(r=>r.method==='POST'&&JSON.parse(r.body).variables.collection==='org.impactindexer.review.comment'));
  assert.ok(indexerTraffic.some(r=>r.method==='POST'&&JSON.parse(r.body).variables.collection==='org.impactindexer.review.like'));
  await save('browser-indexer-preflight.json',{requests:indexerTraffic,selectedDetail:await detailText(),text:await evaluate('document.body.innerText'),boundary:'all external-shaped indexer requests fulfilled locally; browser CORS enforcement retained'});
  check('browser indexer successful synthetic preflight dispatches actual comment/like POSTs and loads scoped comments');
  assert.ok(!(await detailText()).includes('signals-only fixture comment'));
  assert.ok((await evaluate('document.body.innerText')).includes('signals-only fixture comment')); // Global All Comments intentionally spans both projects.
  check('selected cody detail isolates colliding-ID comments while the global panel retains both projects');
  assert.ok(!(await evaluate('document.body.innerText')).includes('ambiguous unqualified comment'));
  async function clickText(text){await evaluate(`(()=>{const b=[...document.querySelectorAll('button')].find(x=>x.textContent.trim()===${JSON.stringify(text)}&&!x.disabled);if(!b)throw Error('button missing: '+${JSON.stringify(text)});b.click()})()`);}
  async function type(selector,text){await evaluate(`(()=>{const input=document.querySelector(${JSON.stringify(selector)});input.focus();input.select?.()})()`);await cdp('Input.insertText',{text});}
  async function searchFor(query){
    await evaluate(`(()=>{if(!document.querySelector('input[placeholder="Search issues or profiles..."]'))document.querySelector('button[title="Search issues or profiles (Ctrl+F)"]').click()})()`);
    await until(async()=>await evaluate(`Boolean(document.querySelector('input[placeholder="Search issues or profiles..."]')?.getClientRects().length)`),'search input opens');
    await type('input[placeholder="Search issues or profiles..."]',query);
  }
  async function closeSearch(){
    await evaluate(`(()=>{const input=document.querySelector('input[placeholder="Search issues or profiles..."]');if(input)input.parentElement.querySelector('button').click()})()`);
    await until(async()=>await evaluate(`!document.querySelector('input[placeholder="Search issues or profiles..."]')`),'search input closes');
  }
  async function commentAction(index){await evaluate(`(()=>{const p=[...document.querySelector('aside').querySelectorAll('p')].find(x=>x.textContent==='cody-only fixture comment');if(!p)throw Error('comment missing');p.parentElement.querySelectorAll('button')[${index}].click()})()`);}
  await type('textarea[placeholder="Leave a comment..."]','browser new cody comment');await clickText('Comment');
  await until(()=>receipts.externalRecords.some(x=>x.entry?.value.text==='browser new cody comment'),'comment caller');
  await until(async()=> (await evaluate('document.body.innerText')).includes('browser new cody comment'),'comment refresh');
  await commentAction(0);await until(()=>receipts.externalRecords.some(x=>x.entry?.collection==='org.impactindexer.review.like'),'like caller');
  await until(async()=>await evaluate(`(()=>{const p=[...document.querySelector('aside').querySelectorAll('p')].find(x=>x.textContent==='cody-only fixture comment');return p?.parentElement.querySelector('button')?.textContent.trim()==='1'})()`),'like refresh');
  await commentAction(0);await until(()=>receipts.externalRecords.some(x=>x.method==='delete'&&x.uri.includes('review.like')),'unlike DELETE caller');
  await commentAction(1);await type('input[placeholder="Write a reply..."]','browser reply cody');await clickText('Reply');
  await until(()=>receipts.externalRecords.some(x=>x.entry?.value.text==='browser reply cody'&&x.entry.value.replyTo),'reply caller');
  await until(async()=> (await evaluate('document.body.innerText')).includes('browser reply cody'),'reply refresh');
  await evaluate(`(()=>{const p=[...document.querySelector('aside').querySelectorAll('p')].find(x=>x.textContent==='browser new cody comment');p.parentElement.querySelectorAll('button')[2].click()})()`);
  await until(()=>receipts.externalRecords.some(x=>x.method==='delete'&&x.uri.includes('review.comment/created-')),'comment DELETE caller');
  // Open the real graph context menu through its selected-node callback, then
  // use actual UI buttons for the claim/unclaim handlers. Canvas positions are
  // simulation-dependent; no product API/auth bypass is added to the package.
  async function graphMenu(publicId='SHARED'){await evaluate(`(()=>{const all=[...document.querySelectorAll('*')];for(const el of all){const key=Object.keys(el).find(k=>k.startsWith('__reactFiber$'));let f=key&&el[key];while(f){const p=f.memoizedProps;if(p?.onNodeRightClick&&p?.nodes){const n=p.nodes.find(n=>n.project==='cody-ops'&&n.legacyId===${JSON.stringify(publicId)});p.onNodeRightClick(n,new MouseEvent('contextmenu',{clientX:250,clientY:250}));return;}f=f.return}}throw Error('graph callback missing')})()`);}
  await graphMenu();await clickText('Claim task');await until(()=>receipts.externalRecords.some(x=>x.entry?.value.text==='@fixture.test'),'claim caller');
  await until(async()=> (await evaluate('document.body.innerText')).includes('@fixture.test'),'claim refresh');
  await graphMenu();await until(async()=>await evaluate(`Boolean([...document.querySelectorAll('button')].find(b=>b.textContent.trim()==='Unclaim task'&&!b.disabled))`),'unclaim menu render');await clickText('Unclaim task');await until(()=>!([...externalRecords.values()].some(x=>x.value.text==='@fixture.test')),'unclaim DELETE caller');
  const browserWrites=receipts.browserRequests.filter(r=>r.url.startsWith(nextUrl+'/api/records'));
  assert.ok(browserWrites.length>=7);assert.ok(browserWrites.filter(r=>r.method==='POST').every(r=>{const v=JSON.parse(r.body);return v.project==='cody-ops'&&v.nodeId==='SHARED'}));
  check('browser comment/reply/like/unlike/delete/claim/unclaim callers preserve original ID and project',{requests:browserWrites.length,externalAuth:'synthetic only'});
  await save('browser-records.json',{requests:browserWrites,records:receipts.externalRecords});
  await searchFor(legacyId);
  await until(async()=>await evaluate(`Boolean([...document.querySelectorAll('button')].find(b=>b.textContent.includes('extra cody-ops')&&b.textContent.includes(${JSON.stringify(legacyId)})))`),'legacy unique task search');
  await evaluate(`(()=>{const b=[...document.querySelectorAll('button')].find(b=>b.textContent.includes('extra cody-ops')&&b.textContent.includes(${JSON.stringify(legacyId)}));b.click()})()`);
  await until(async()=> (await evaluate('document.body.innerText')).includes('old projectless comment'),'legacy projectless comment load');
  await until(async()=> (await evaluate('document.body.innerText')).includes('mixed scoped deepest reply'),'mixed-generation issue UI');
  const threadDOM=await evaluate(`(()=>{const paragraph=text=>[...document.querySelector('aside').querySelectorAll('p')].find(p=>p.textContent===text);const names=['old projectless comment','mixed scoped reply','mixed legacy deep reply','mixed scoped deepest reply'];const ps=names.map(paragraph);if(ps.some(p=>!p))throw Error('missing mixed reply');for(let i=1;i<ps.length;i++){const parentCard=ps[i-1].parentElement.parentElement;const replyCard=ps[i].parentElement.parentElement;if(!parentCard.contains(replyCard)||!replyCard.classList.contains('border-l'))throw Error('reply is not nested under its parent');}return {text:document.body.innerText,names}})()`);
  await save('browser-mixed-generation.json',threadDOM);
  check('browser issue UI renders old parent/new scoped/deeper mixed-generation replies in their actual nested cards');
  await clickText('View in window');
  await until(async()=>await evaluate(`(()=>{const modal=document.querySelector('[class~="z-[100]"]');return Boolean(modal?.getClientRects().length&&modal.querySelector('button[title="Copy description"]'))})()`),'visible description window opens');
  const countThread=comments=>comments.reduce((sum,c)=>sum+1+countThread(c.replies),0);
  const expectedModalCount=countThread(mixedHTTP.issue.comments);
  const modalThread=await evaluate(`(()=>{const modal=document.querySelector('[class~="z-[100]"]');if(!modal)throw Error('description window missing');const names=['old projectless comment','mixed scoped reply','mixed legacy deep reply','mixed scoped deepest reply'];for(const name of names){const ps=[...modal.querySelectorAll('p')].filter(p=>p.textContent===name);if(ps.length!==1)throw Error('description thread duplicate/missing '+name+':'+ps.length);}const header=[...modal.querySelectorAll('h4')].find(h=>h.textContent.trim().startsWith('Comments'));const count=Number(header?.querySelector('span')?.textContent);if(count!==${expectedModalCount})throw Error('description thread count '+count+' expected '+${expectedModalCount});return {text:modal.innerText,count}})()`);
  await save('browser-description-thread.json',modalThread);
  await evaluate(`document.querySelector('[class~="z-[100]"]').click()`); // Real modal backdrop close callback.
  await until(async()=>await evaluate(`(()=>{const modal=document.querySelector('[class~="z-[100]"]');return !modal||modal.getClientRects().length===0})()`),'description window closes');
  check('browser description window shares exact roots without duplicated replies');
  await evaluate(`(()=>{const p=[...document.querySelector('aside').querySelectorAll('p')].find(p=>p.textContent==='old projectless comment');p.parentElement.querySelectorAll('button')[0].click()})()`);
  await until(()=>!externalRecords.has(`at://${did}/org.impactindexer.review.like/legacy-like`),'legacy unlike UI');
  await until(async()=>await evaluate(`(()=>{const p=[...document.querySelector('aside').querySelectorAll('p')].find(p=>p.textContent==='old projectless comment');return p?.parentElement.querySelector('button')?.textContent.trim()!=='1'})()`),'legacy unlike refresh');
  await evaluate(`(()=>{const p=[...document.querySelector('aside').querySelectorAll('p')].find(p=>p.textContent==='old projectless comment');p.parentElement.querySelectorAll('button')[2].click()})()`);
  await until(()=>!externalRecords.has(`at://${did}/org.impactindexer.review.comment/legacy-comment`),'legacy DELETE UI');
  await until(async()=> (await evaluate('document.body.innerText')).includes('mixed scoped reply'),'orphan remains visible after parent deletion');
  await evaluate(`(()=>{const p=[...document.querySelector('aside').querySelectorAll('p')].find(p=>p.textContent==='mixed scoped reply');p.parentElement.querySelectorAll('button')[2].click()})()`);
  await until(()=>!externalRecords.has(at(commentCollection,'mixed-scoped')),'owned orphan DELETE UI');
  await until(async()=> (await evaluate('document.body.innerText')).includes('mixed legacy deep reply'),'deeper orphan remains visible');
  await save('browser-orphan-cleanup.json',await evaluate('({text:document.body.innerText})'));
  check('browser parent deletion preserves orphan reply display and permits owned reply deletion');
  await graphMenu(legacyId);await clickText('Unclaim task');
  await until(()=>!externalRecords.has(`at://${did}/org.impactindexer.review.comment/legacy-claim`),'legacy unclaim UI');
  check('browser old projectless unique comment delete, old like unlike and old claim unclaim preserve ownership');
  await searchFor('SHARED');
  await until(async()=>await evaluate(`Boolean([...document.querySelectorAll('button')].find(b=>b.textContent.includes('Imported root cody-ops')))`),'root search restore');
  await evaluate(`(()=>{const b=[...document.querySelectorAll('button')].find(b=>b.textContent.includes('Imported root cody-ops'));b.click()})()`);
  await save('browser-legacy-records.json',{requests:receipts.browserRequests.filter(r=>r.url.startsWith(nextUrl+'/api/records')),records:receipts.externalRecords});



  await cdp('Page.navigate',{url:nextUrl+'/?panel=activity'});
  await until(async()=> (await evaluate('document.body.innerText')).includes('6 issues'),'activity browser data');
  const activityLink={from:{id:roots['cody-ops']},to:{id:ids['cody-ops-extra']},kind:'blocks'};
  const observedDependency={project:'cody-ops',sourceKey:'cody-ops\0'+ids['cody-ops-extra'],targetKey:'cody-ops\0'+roots['cody-ops'],publicTarget:'SHARED',label:'removed dependency on SHARED'};
  await evaluate('void(window.__i8DependencyObservation=('+observeDependencyRemoval.toString()+')('+JSON.stringify(observedDependency)+'))');
  async function assertSingleRemoval(stage,requireStale=false){
    const observation=await evaluate('window.__i8DependencyObservation.read()');
    await save('dependency-removal-sequence.json',{stage,...observation});
    assert.deepEqual(observation.errors,[]);
    const populated=observation.samples.filter(sample=>sample.activityPresent&&sample.events.length);
    assert.ok(populated.length>0,'actual removal event observed');
    const ids=new Set(populated.flatMap(sample=>sample.events.map(event=>event.id)));
    assert.equal(ids.size,1,'one actual unlink must keep one event identity');
    assert.ok(populated.every(sample=>sample.events.length===1),'no repeated removal events');
    assert.ok(populated.every(sample=>sample.desktopRemovalLabels===1),'one desktop removal label');
    const animationTimes=new Set(observation.samples.flatMap(sample=>sample.renderedLinks.map(link=>link.removeTime)).filter(time=>time!==null));
    assert.equal(animationTimes.size,1,'exit animation starts once');
    const [removeTime]=animationTimes;
    const expired=observation.samples.filter(sample=>sample.graphPresent&&sample.observedAt>removeTime+1100);
    assert.ok(expired.length>=10,'observe frames after animation expiry');
    assert.ok(expired.every(sample=>sample.renderedLinks.length===0),'no phantom or restarted removed edge');
    const generations=new Set(populated.filter(sample=>sample.freshness?.state==='fresh').map(sample=>sample.freshness.generated_at));
    assert.ok(generations.size>=3,'several unchanged fresh generations observed');
    if(requireStale){
      const stale=populated.filter(sample=>sample.freshness?.state==='stale');
      assert.ok(stale.length>0,'stale snapshots observed with the same removal');
      assert.ok(populated.some(sample=>sample.observedAt>stale[0].observedAt&&sample.freshness?.state==='fresh'),'fresh recovery retains the same removal');
    }
    const unlinks=receipts.calls.filter(call=>call.method==='bead.unlink');
    assert.equal(unlinks.length,1,'exactly one real Plug unlink');
    return {samples:observation.samples.length,removalEventIds:[...ids],animationTimes:[...animationTimes],freshGenerations:generations.size,unlinks:unlinks.length};
  }

  await rpc('cody-ops','bead.link',activityLink);
  await until(async()=> (await evaluate('document.body.innerText')).includes('added a dependency on SHARED'),'public dependency add label');
  const linkedProjection=await http('/api/beads');
  await rpc('cody-ops','bead.unlink',activityLink);
  await until(async()=> (await evaluate('document.body.innerText')).includes('removed dependency on SHARED'),'public dependency remove label');
  await pause(3400);await assertSingleRemoval('unchanged-fresh-frames');
  await type('input[placeholder="Search activity..."]','SHARED');
  const activityDOM=await evaluate('document.body.innerText');assert.ok(activityDOM.includes('added a dependency on SHARED'));assert.ok(activityDOM.includes('removed dependency on SHARED'));assert.ok(!activityDOM.includes('\0'));
  await save('browser-dependency-labels.json',{text:activityDOM,publicTarget:'SHARED',canonicalTarget:roots['cody-ops']});
  // An additional exact-source pure function control removes the target from
  // the *new* frame, proving its old verified public identity is retained.
  // It does not claim a native delete operation or replace browser transport.
  const activityContext=vm.createContext({console});
  const transpile=file=>ts.default.transpileModule(file,{compilerOptions:{target:99,module:99}}).outputText;
  const diffModule=new vm.SourceTextModule(transpile(await readFile(join(ui,'lib/diff-beads.ts'),'utf8')),{context:activityContext});await diffModule.link(()=>{throw Error('unexpected diff import')});await diffModule.evaluate();
  const activityModule=new vm.SourceTextModule(transpile(await readFile(join(ui,'lib/activity.ts'),'utf8')),{context:activityContext});await activityModule.link(name=>{assert.equal(name,'./diff-beads');return diffModule});await activityModule.evaluate();
  const priorUI={...linkedProjection,graphData:{nodes:linkedProjection.graphData.nodes.map(n=>({...n,id:n.graphKey})),links:linkedProjection.graphData.links.map(l=>({...l,source:l.sourceKey,target:l.targetKey}))}};
  const absentTarget={...priorUI,graphData:{nodes:priorUI.graphData.nodes.filter(n=>n.canonicalId!==roots['cody-ops']),links:[]}};
  const removedEvents=activityModule.namespace.diffToActivityEvents(diffModule.namespace.diffBeadsData(priorUI,absentTarget),absentTarget.graphData.nodes,priorUI.graphData.nodes);
  assert.ok(removedEvents.some(e=>e.type==='link-removed'&&e.meta.targetLegacyId==='SHARED'));
  assert.ok(removedEvents.filter(e=>e.type==='link-removed').every(e=>!e.detail.includes('\0')));
  await save('removed-target-activity-control.json',{removedEvents,boundary:'exact-source helper with prior actual projection and deliberately absent new-frame target, no native delete claimed'});
  check('browser actual link/unlink renders and searches public target IDs; removed-target helper retains verified prior ID');
  await searchFor('SHARED');
  await until(async()=>await evaluate(`Boolean([...document.querySelectorAll('button')].find(b=>b.textContent.includes('Imported root cody-ops')))`),'post-activity root search');
  await evaluate(`(()=>{const b=[...document.querySelectorAll('button')].find(b=>b.textContent.includes('Imported root cody-ops'));b.click()})()`);
  const ref={id:roots['cody-ops']};
  async function close(reason){const current=await rpc('cody-ops','bead.get',{id:ref});return rpc('cody-ops','tx.apply',{changeSet:{expect:[{ref,revision:current.bead.revision}],ops:[{kind:'close',target:ref,reason},{kind:'setMeta',target:ref,key:'i8.host.close_reason',value:{schema:'plug-host-close-reason/1',reason}}]}});}
  await close('original');await until(async()=> (await http('/api/beads')).graphData.nodes.find(n=>n.project==='cody-ops'&&n.id==='SHARED').closeReason==='original','first typed close');
  const current=await rpc('cody-ops','bead.get',{id:ref});
  await rpc('cody-ops','tx.apply',{changeSet:{expect:[{ref,revision:current.bead.revision}],ops:[{kind:'patch',target:ref,fields:{description:'after fixture field'}},{kind:'setMeta',target:ref,key:'i8.host.close_reason',value:{schema:'plug-host-close-reason/1',reason:'amended'}}]}});
  await until(async()=>{const text=await evaluate('document.body.innerText');return text.includes('after fixture field')&&text.includes('amended')},'browser field-only/amended reason');
  await save('browser-amended.json',await evaluate('({text:document.body.innerText,status:document.querySelector("[data-testid=plug-freshness]")?.textContent})'));
  await save('browser-amended.png',Buffer.from((await cdp('Page.captureScreenshot',{format:'png'})).data,'base64'));
  check('browser publishes field-only/amended reason without lifecycle animation');

  // Capture real HTTP framing, complete fresh/stale payloads and heartbeat.
  const captureAbort=new AbortController();const feed=await fetch(nextUrl+'/api/beads/stream',{signal:captureAbort.signal});
  const reader=feed.body.getReader();let text='';
  const reading=(async()=>{try{for(;;){const {done,value}=await reader.read();if(done)break;text+=new TextDecoder().decode(value)}}catch{}})();
  await until(()=>text.includes('"state":"fresh"'),'fresh stream');fault='outage';
  await until(()=>text.includes('"state":"stale"'),'stale stream');await until(async()=> (await evaluate('document.querySelector("[data-testid=plug-freshness]")?.textContent || ""')).includes('Stale'),'browser stale status');
  await save('browser-stale.json',await evaluate('({text:document.body.innerText,status:document.querySelector("[data-testid=plug-freshness]")?.textContent})'));
  fault='none';await until(async()=> (await evaluate('document.querySelector("[data-testid=plug-freshness]")?.textContent || ""')).includes('snapshot fresh'),'browser freshness recovery');
  await until(()=>text.includes(': heartbeat\n\n'),'heartbeat',35000);captureAbort.abort();await reading;await save('sse-framing.txt',text);
  const frames=text.split('\n\n').filter(s=>s.startsWith('data: ')).map(s=>JSON.parse(s.slice(6)));assert.ok(frames.every(s=>s.graphData.nodes.length===6));
  check('actual SSE fresh/stale/heartbeat and browser freshness recovery',{frames:frames.length});
  await rpc('cody-ops','bead.reopen',{id:ref});const reopened=await http('/api/beads');assert.equal(reopened.graphData.nodes.find(n=>n.project==='cody-ops'&&n.id==='SHARED').closeReason,undefined);await save('reopened.json',reopened);check('actual reopen omits typed historical reason');

  fault='incarnation';await until(async()=> (await evaluate('document.body.innerText')).includes('incarnation'),'browser authority error');await save('browser-error.json',await evaluate('({text:document.body.innerText})'));fault='none';
  await until(async()=> (await evaluate('document.body.innerText')).includes('6 issues'),'browser parsed-error reconnect',45000);check('browser parsed-error bounded reconnect restores both projects');
  await stop(next);await until(async()=> (await evaluate('document.body.innerText')).includes('disconnected'),'browser network error');next=startNext();
  await until(async()=> (await evaluate('document.body.innerText')).includes('6 issues'),'browser network reconnect',45000);check('browser network reconnect restores both projects');
  const canceledRef={id:ids['cody-ops-extra']};
  const canceledBefore=await rpc('cody-ops','bead.get',{id:canceledRef});
  await rpc('cody-ops','tx.apply',{changeSet:{expect:[{ref:canceledRef,revision:canceledBefore.bead.revision}],ops:[{kind:'setMeta',target:canceledRef,key:'i8.host.close_reason',value:{schema:'plug-host-close-reason/1',reason:'canceled fixture reason'}}]}});
  await rpc('cody-ops','bead.cancel',{id:canceledRef});const canceled=await http('/api/beads');const canceledNode=canceled.graphData.nodes.find(n=>n.canonicalId===canceledRef.id);assert.equal(canceledNode.status,'abandoned');assert.equal(canceledNode.statusCategory,'canceled');assert.equal(canceledNode.closeReason,'canceled fixture reason');await save('canceled-reason.json',canceledNode);check('actual canceled terminal category retains typed reason');
  const removalEvidence=await assertSingleRemoval('fresh-stale-recovery-and-reconnect',true);
  await save('dependency-removal-sequence.json',await evaluate('window.__i8DependencyObservation.stop()'));
  check('one actual unlink yields one removal event and one expiring animation across unchanged fresh/stale/recovery snapshots',removalEvidence);
  await save('browser-complete-before-disconnect.json',await evaluate('({text:document.body.innerText,html:document.body.outerHTML,status:document.querySelector("[data-testid=plug-freshness]")?.textContent})'));
  await save('browser-complete-before-disconnect.png',Buffer.from((await cdp('Page.captureScreenshot',{format:'png'})).data,'base64'));
  await save('candidate-source.json',JSON.parse(await readFile(join(fixtureRoot,'source-binding.json'),'utf8')));
  // Navigation may retain the prior document/connection. Record it without
  // relabeling it an HTTP disconnect; the assertions below close real clients.
  await cdp('Page.navigate',{url:'about:blank'});await pause(1700);const navigationCount=rpcCount;await pause(2200);
  const teardown={navigation:{before:navigationCount,after:rpcCount,delta:rpcCount-navigationCount,boundary:'navigation only; no socket-close inference'}};
  closingBrowser=true;ws.close();ws=null;evaluate=null;cdp=null;
  await stop(chrome);await pause(1700);const closedBrowserCount=rpcCount;await pause(2200);
  teardown.browserProcessClose={before:closedBrowserCount,after:rpcCount,delta:rpcCount-closedBrowserCount};await save('actual-disconnect-cleanup.json',teardown);
  assert.equal(rpcCount,closedBrowserCount);check('actual browser process disconnect stops polling',{afterDisconnectRPCs:rpcCount-closedBrowserCount});

  let directText='',directResponse,directSocket,intentionalClose=false;const directErrors=[];
  let directClosed;
  const directClose=new Promise(resolve=>{directClosed=resolve;});
  const directRequest=httpRequest(nextUrl+'/api/beads/stream',{agent:false},response=>{
    directResponse=response;directSocket=response.socket;response.setEncoding('utf8');response.on('data',chunk=>directText+=chunk);
    response.on('error',error=>{if(!intentionalClose)directErrors.push(error.message);});response.on('close',directClosed);
  });
  directRequest.on('error',error=>{if(!intentionalClose)directErrors.push(error.message);});directRequest.end();
  await until(()=>directText.includes('"state":"fresh"'),'direct HTTP SSE fresh before socket cancel');
  assert.equal(directResponse.statusCode,200);intentionalClose=true;directRequest.destroy();await directClose;assert.ok(directSocket.destroyed);
  await pause(1700);const closedSocketCount=rpcCount;await pause(2200);
  teardown.directSocketClose={destroyed:directSocket.destroyed,before:closedSocketCount,after:rpcCount,delta:rpcCount-closedSocketCount,errors:directErrors};
  await save('actual-disconnect-cleanup.json',teardown);await save('direct-canceled-sse.txt',directText);
  assert.deepEqual(directErrors,[]);assert.equal(rpcCount,closedSocketCount);check('actual HTTP SSE socket cancellation stops polling',{afterDisconnectRPCs:rpcCount-closedSocketCount});
  const unexpectedInterceptions=receipts.browserInterceptionFailures.filter(record=>{
    let parsed;try{parsed=JSON.parse(record.intercept);}catch{return true;}
    const canceled=record.duringBrowserClose||receipts.browserNetworkFailures.some(f=>f.requestId===record.networkId&&f.canceled===true);
    return !(parsed.code===-32602&&parsed.message==='Invalid InterceptionId.'&&canceled);
  });
  await save('browser-interception-outcomes.json',{all:receipts.browserInterceptionFailures,networkFailures:receipts.browserNetworkFailures,unexpected:unexpectedInterceptions});
  assert.deepEqual(unexpectedInterceptions,[]);assert.deepEqual(receipts.browserErrors,[]);
  check('complete actual import -> HTTP -> browser -> SSE fixture');
} catch(error) {
  receipts.failure={message:error.message,stack:error.stack};console.error(error);process.exitCode=1;
  if(evaluate)try{const observation=await evaluate('window.__i8DependencyObservation?.stop() || null');if(observation)await save('dependency-removal-sequence.json',observation);}catch{}
  if(evaluate)try{await save('browser-failure.json',await evaluate('({text:document.body.innerText,html:document.body.outerHTML,location:location.href})'));}catch{}
  if(cdp)try{await save('browser-failure.png',Buffer.from((await cdp('Page.captureScreenshot',{format:'png'})).data,'base64'));}catch{}
} finally {
  if(ws)ws.close();await save('next.log',nextLog);await save('e2e-results.json',receipts);
  for(const child of [...children])await stop(child);
  for(const server of servers)server.closeAllConnections();
  await Promise.all([...servers].map(server=>new Promise(resolve=>server.close(resolve))));
}
