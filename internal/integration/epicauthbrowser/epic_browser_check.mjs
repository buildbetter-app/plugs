import assert from "node:assert/strict";
import {spawn} from "node:child_process";
import {once} from "node:events";
import {mkdtemp,rm} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
let input=""; for await (const chunk of process.stdin) input+=chunk;
const fixture=JSON.parse(input); input="";
const profile=await mkdtemp(path.join(os.tmpdir(),"plug-auth-epic-"));
const chrome=spawn(fixture.chrome,["--headless","--no-sandbox","--disable-gpu","--disable-dev-shm-usage","--disable-background-networking","--disable-component-update","--disable-sync","--no-first-run","--no-default-browser-check","--remote-debugging-port=0","--user-data-dir="+profile,"about:blank"],{stdio:["ignore","ignore","pipe"]});
let stderr="",ws;
chrome.stderr.on("data",chunk=>{stderr+=chunk.toString();});
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(fn,what){const end=Date.now()+10000;while(Date.now()<end){if(await fn())return;await delay(25);}throw new Error("Timed out: "+what);}
try {
 await until(()=>/DevTools listening on ws:\/\/[^\s]+/.test(stderr),"Chrome startup");
 const devtools="http://"+new URL(stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)[1]).host;
 const target=await(await fetch(devtools+"/json/new?about:blank",{method:"PUT"})).json();
 ws=new WebSocket(target.webSocketDebuggerUrl); await once(ws,"open");
 let sequence=0; const pending=new Map(),exceptions=[];
 const send=(method,params={})=>new Promise((resolve,reject)=>{const id=++sequence;pending.set(id,{resolve,reject});ws.send(JSON.stringify({id,method,params}));});
 ws.addEventListener("message",({data})=>{const e=JSON.parse(data.toString());if(e.id){const p=pending.get(e.id);pending.delete(e.id);if(p)e.error?p.reject(new Error("Chrome command refused")):p.resolve(e.result);}else if(e.method==="Runtime.exceptionThrown")exceptions.push("uncaught browser exception");});
 await send("Page.enable");await send("Runtime.enable");await send("Network.enable");
 const evaluate=async expression=>{const r=await send("Runtime.evaluate",{expression,returnByValue:true,awaitPromise:true});assert(!r.exceptionDetails,"browser evaluation failed");return r.result.value;};
 const settled=()=>until(()=>evaluate("document.querySelector('#cancel')?.hidden && document.querySelector('#connection-state')?.textContent !== 'Loading'"),"completed UI read");
 await send("Page.navigate",{url:fixture.base+"/ui/?view=epic&epicId="+fixture.parent});
 await until(()=>evaluate("document.readyState==='complete' && !!globalThis.PlugEpicProgress"),"embedded page");await settled();
 assert(await evaluate("!!document.querySelector('#auth-token') && !!document.querySelector('#auth-form')"),"embedded UI must offer explicit bearer sign-in");
 await evaluate(`document.querySelector('#auth-token').value=${JSON.stringify(fixture.token)};document.querySelector('#auth-form').requestSubmit()`);
 await settled();
 assert.equal(await evaluate("document.querySelector('.epic-visible-total')?.textContent"),fixture.visible+" visible children");
 assert.equal(await evaluate("document.querySelector('#epic-remainder').textContent"),fixture.hidden+" more you cannot see");
 assert.equal(await evaluate("document.querySelector('#auth-token').value"),"");
 assert.equal(await evaluate("localStorage.length+sessionStorage.length"),0);
 assert.deepEqual(exceptions,[]);
 console.log("PASS authenticated Chrome sign-in -> real native view.run -> visible counts and exact hidden remainder");
} finally { if(ws)ws.close();const stopped=once(chrome,"exit").catch(()=>{});chrome.kill("SIGKILL");await stopped;await rm(profile,{recursive:true,force:true}); }
