package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type effectNative struct {
	t *testing.T
	root,dir,binary string
	env []string
	sequence int
	tables []string
	roles map[string]string
}

type effectNativeState map[string][]map[string]json.RawMessage

func (n *effectNative) command(label string, fail bool, args ...string) []byte {
	n.t.Helper()
	n.sequence++
	ctx,cancel := context.WithTimeout(context.Background(),2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx,n.binary,args...)
	cmd.Dir,cmd.Env = n.dir,append([]string(nil),n.env...)
	var out,stderr bytes.Buffer
	cmd.Stdout,cmd.Stderr = &out,&stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		code = -2
		var exit *exec.ExitError
		if errors.As(err,&exit) { code = exit.ExitCode() }
	}
	stem := filepath.Join(n.root,fmt.Sprintf("%05d-%s",n.sequence,label))
	for suffix,data := range map[string][]byte{"stdout":out.Bytes(),"stderr":stderr.Bytes()} {
		if e := os.WriteFile(stem+"."+suffix,data,0600); e != nil { n.t.Fatal(e) }
	}
	r := &nativeRecorder{t:n.t,root:n.root}
	r.writeJSON(stem+".request.json",map[string]any{"argv":cmd.Args,"directory":cmd.Dir,"environment":cmd.Env,"exit":code,"error":fmt.Sprint(err),"stdout_sha256":hashBytes(out.Bytes()),"stderr_sha256":hashBytes(stderr.Bytes())})
	if ctx.Err() != nil || code < 0 || (!fail && code != 0) || (fail && code != 1) { n.t.Fatalf("native %s exit=%d error=%v stdout=%s stderr=%s",label,code,err,out.Bytes(),stderr.Bytes()) }
	return out.Bytes()
}

func (n *effectNative) batch(label,script string) {
	n.t.Helper()
	file := filepath.Join(n.root,fmt.Sprintf("script-%05d-%s.txt",n.sequence,label))
	if err := os.WriteFile(file,[]byte(script),0600); err != nil { n.t.Fatal(err) }
	n.command(label,false,"batch","--file",file,"--json")
}

func (n *effectNative) graph(label string,nodes []map[string]any) {
	n.t.Helper()
	file := filepath.Join(n.root,label+".plan.json")
	(&nativeRecorder{t:n.t,root:n.root}).writeJSON(file,map[string]any{"nodes":nodes,"edges":[]any{}})
	n.command(label,false,"create","--graph",file,"--json")
}

func newEffectNative(t *testing.T,binary string) *effectNative {
	root := filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"),strings.ReplaceAll(t.Name(),"/","-"))
	if err := os.MkdirAll(root,0700); err != nil { t.Fatal(err) }
	workspace := t.TempDir()
	dir,home,tmp := filepath.Join(workspace,"project"),filepath.Join(workspace,"home"),filepath.Join(workspace,"tmp")
	for _,path := range []string{dir,home,tmp} { if err := os.Mkdir(path,0700); err != nil { t.Fatal(err) } }
	server := publicnative.StartDolt(t,root)
	n := &effectNative{t:t,root:root,dir:dir,binary:"/usr/bin/git",roles:map[string]string{},env:server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin","HOME="+home,"USER=b07-effects","TZ=UTC","LC_ALL=C","NO_COLOR=1","CI=true","GIT_CONFIG_NOSYSTEM=1","GIT_CONFIG_GLOBAL=/dev/null","GIT_AUTHOR_NAME=B07 Public Effects","GIT_AUTHOR_EMAIL=b07@example.invalid","GIT_COMMITTER_NAME=B07 Public Effects","GIT_COMMITTER_EMAIL=b07@example.invalid","TMPDIR="+tmp})}
	n.command("git-init",false,"-c","core.hooksPath=/dev/null","init","--quiet","--template=")
	n.binary = binary
	raw := n.command("version",false,"version","--json")
	var identity struct{Commit,Build string}
	if json.Unmarshal(raw,&identity) != nil || identity.Commit != pinnedSource.Commit || identity.Build != "b20-public-fixture-"+pinnedSource.Commit { t.Fatal("unchanged canonical native binary identity differs") }
	n.command("init",false,"init","--server","--external","--server-host","127.0.0.1","--server-port",strconv.Itoa(server.Port),"--database","b07_effects","--prefix","b07","--skip-hooks","--skip-agents","--non-interactive","--quiet")
	server.RequireDatabaseDir(t,"b07_effects")
	n.command("journal-enable",false,"config","set","events-journal","true","--json")
	n.command("journal-prune-disable",false,"config","set","events-journal-auto-prune","false","--json")
	raw = n.command("actual-user-tables",false,"--readonly","sql","SELECT table_name FROM information_schema.tables WHERE table_schema = 'b07_effects' AND table_type = 'BASE TABLE' ORDER BY table_name","--json")
	var tables []map[string]string
	if json.Unmarshal(raw,&tables) != nil { t.Fatal("actual native table catalog unavailable") }
	for _,row := range tables {
		table := row["table_name"]
		if table == "" { table = row["TABLE_NAME"] }
		if strings.HasPrefix(table,"dolt_") { continue }
		if table == "" || strings.Trim(table,"abcdefghijklmnopqrstuvwxyz0123456789_") != "" { t.Fatal("unsafe native table name") }
		n.tables = append(n.tables,table)
	}
	if len(n.tables) == 0 { t.Fatal("native table catalog empty") }
	(&nativeRecorder{t:t,root:root}).writeJSON(filepath.Join(root,"actual-tables.json"),n.tables)
	return n
}

func (n *effectNative) image(label string) effectNativeState {
	n.t.Helper()
	state := effectNativeState{}
	for _,table := range n.tables {
		raw := n.command(label+"-"+table,false,"--readonly","sql","SELECT * FROM `"+table+"`","--json")
		var rows []map[string]json.RawMessage
		if json.Unmarshal(raw,&rows) != nil { n.t.Fatal("invalid exact native SQL rows",table) }
		sort.Slice(rows,func(i,j int) bool { a,_:=json.Marshal(rows[i]); b,_:=json.Marshal(rows[j]); return string(a)<string(b) })
		state[table] = rows
	}
	(&nativeRecorder{t:n.t,root:n.root}).writeJSON(filepath.Join(n.root,fmt.Sprintf("image-%05d-%s.json",n.sequence,label)),state)
	return state
}

func nativeEffectText(t *testing.T,raw json.RawMessage) string {
	t.Helper()
	var value string
	if json.Unmarshal(raw,&value) != nil { t.Fatal("native exact text token unavailable",string(raw)) }
	return value
}

func (n *effectNative) edge(state effectNativeState,from,to string) map[string]json.RawMessage {
	n.t.Helper()
	var found map[string]json.RawMessage
	for _,rows := range state {
		for _,row := range rows {
			if string(row["type"]) == "" || string(row["issue_id"]) == "" || nativeEffectText(n.t,row["issue_id"]) != from { continue }
			for key,value := range row {
				if strings.HasPrefix(key,"depends_on_") && string(value) != "null" && nativeEffectText(n.t,value) == to {
					if found != nil { n.t.Fatal("duplicate native directed edge") }
					found = row
				}
			}
		}
	}
	return found
}

func (n *effectNative) requireReady(label string,want []string) []string {
	n.t.Helper()
	raw := n.command(label,false,"ready","--json","--limit","0","--include-ephemeral")
	var rows []struct{ID string}
	if json.Unmarshal(raw,&rows) != nil { n.t.Fatal("native public complete ready page invalid") }
	got := []string{}
	seen := map[string]bool{}
	for _,row := range rows {
		role,ok := n.roles[row.ID]
		if !ok || seen[row.ID] { n.t.Fatal("unknown/duplicate native ready identity",row.ID) }
		seen[row.ID] = true
		got = append(got,role)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got,want) { n.t.Fatalf("native %s ready=%v independently want=%v",label,got,want) }
	return got
}

func (n *effectNative) step(c effectCase,phase,source string,want []string,guarded bool) effectStep {
	n.t.Helper()
	ready := n.requireReady(phase+"-ready",want)
	if guarded {
		before := n.image(phase+"-guard-before")
		n.command(phase+"-guard",true,"close",source,"--json")
		if !reflect.DeepEqual(before,n.image(phase+"-guard-after")) { n.t.Fatal("native guarded close changed exact current state") }
	}
	return effectStep{c.Name,phase,ready,guarded}
}

func (n *effectNative) sequences() []effectStep {
	expected := []string{}
	steps := []effectStep{}
	for index,c := range effectCases() {
		completed := false
		n.t.Run(c.Name,func(t *testing.T) {
			old := n.t
			n.t = t
			defer func(){n.t=old}()
			label := fmt.Sprintf("case-%02d",index)
			source,target,sentinel := fmt.Sprintf("b07-%d",1000+index*3),fmt.Sprintf("b07-%d",1001+index*3),fmt.Sprintf("b07-%d",1002+index*3)
			nodes := []map[string]any{{"key":"source","id":source,"title":label+"-source"},{"key":"target","id":target,"title":label+"-target"}}
			if c.Route == "active-durable" || c.Route == "active-active" { nodes[0]["storage_class"]="ephemeral" }
			if c.Route == "durable-active" || c.Route == "active-active" { nodes[1]["storage_class"]="ephemeral" }
			if c.Route != "durable" {
				nodes=append(nodes,map[string]any{"key":"sentinel","id":sentinel,"title":label+"-sentinel","storage_class":"ephemeral"})
				n.roles[sentinel]=label+"-sentinel"
				expected=append(expected,label+"-sentinel")
			}
			n.graph(label+"-create",nodes)
			n.roles[source],n.roles[target]=label+"-source",label+"-target"
			expected=append(expected,label+"-source",label+"-target")
			sort.Strings(expected)
			baseline:=append([]string{},expected...)
			steps=append(steps,n.step(c,"created",source,baseline,false))
			add:="dep add "+source+" "+target+" "+strconv.Quote(c.Kind)+"\n"
			remove:="dep remove "+source+" "+target+"\n"
			n.batch(label+"-link",add)
			image:=n.image(label+"-linked")
			edge:=n.edge(image,source,target)
			if edge==nil || nativeEffectText(t,edge["type"])!=c.Kind { t.Fatal("native exact directed edge missing") }
			blocked:=c.Kind=="blocks"
			want:=baseline
			if blocked {want=effectWithout(want,label+"-source")}
			steps=append(steps,n.step(c,"linked",source,want,blocked))
			n.batch(label+"-unlink-open",remove)
			if n.edge(n.image(label+"-unlinked"),source,target)!=nil {t.Fatal("native exact unlink retained edge")}
			steps=append(steps,n.step(c,"unlinked-open",source,baseline,false))
			n.batch(label+"-relink-open",add)
			image=n.image(label+"-relinked")
			edge=n.edge(image,source,target)
			if edge==nil || nativeEffectText(t,edge["type"])!=c.Kind {t.Fatal("native relink changed edge kind")}
			steps=append(steps,n.step(c,"relinked-open",source,want,blocked))
			if c.Settlement=="unlink" {
				n.batch(label+"-unlink-final",remove)
				if n.edge(n.image(label+"-unlinked-final"),source,target)!=nil {t.Fatal("native final unlink retained edge")}
				steps=append(steps,n.step(c,"unlinked-final",source,baseline,false))
				n.command(label+"-close-source-unlinked",false,"close",source,"--json")
				expected=effectWithout(baseline,label+"-source")
				steps=append(steps,n.step(c,"source-closed-unlinked",source,expected,false))
				n.image(label+"-closed-source-unlinked")
				completed=true
				return
			}
			n.command(label+"-close-target",false,"close",target,"--json")
			want=effectWithout(baseline,label+"-target")
			steps=append(steps,n.step(c,"target-closed",source,want,false))
			if !reflect.DeepEqual(edge,n.edge(n.image(label+"-target-closed"),source,target)) {t.Fatal("native target close changed exact edge identity/metadata")}
			// Every bd read is a fresh process/SQL connection; retain the same
			// phase as Plug's explicit physical SQL-handle reopen observation.
			steps=append(steps,n.step(c,"store-readback",source,want,false))
			if c.Route=="durable" || c.Route=="active-durable" {
				n.command(label+"-reopen-target",false,"reopen",target,"--json")
				want=baseline
				if blocked {want=effectWithout(want,label+"-source")}
				steps=append(steps,n.step(c,"target-reopened",source,want,blocked))
				if !reflect.DeepEqual(edge,n.edge(n.image(label+"-target-reopened"),source,target)) {t.Fatal("native target reopen changed original edge")}
				n.command(label+"-reclose-target",false,"close",target,"--json")
				want=effectWithout(baseline,label+"-target")
				steps=append(steps,n.step(c,"target-reclosed",source,want,false))
			}
			n.command(label+"-close-source",false,"close",source,"--json")
			want=effectWithout(want,label+"-source")
			steps=append(steps,n.step(c,"source-closed",source,want,false))
			if !reflect.DeepEqual(edge,n.edge(n.image(label+"-source-closed"),source,target)) {t.Fatal("native source close changed original edge")}
			expected=want
			completed=true
		})
		if !completed {n.t.Fatal("native effect sequence incomplete",c.Name)}
	}
	(&nativeRecorder{t:n.t,root:n.root}).writeJSON(filepath.Join(n.root,"effect-sequences.json"),steps)
	return steps
}

func (n *effectNative) parentSequence(route string) []effectStep {
	child,parent,blocker := "b07-201","b07-202","b07-203"
	nodes := []map[string]any{{"key":"child","id":child,"title":"actual child"},{"key":"parent","id":parent,"title":"actual parent"},{"key":"blocker","id":blocker,"title":"actual blocker"}}
	baseline := []string{"blocker","child","parent"}
	if route=="active-durable" || route=="active-active" {nodes[0]["storage_class"]="ephemeral"}
	if route=="durable-active" || route=="active-active" {nodes[1]["storage_class"]="ephemeral"}
	if route!="durable" {
		nodes=append(nodes,map[string]any{"key":"sentinel","id":"b07-204","title":"actual sentinel","storage_class":"ephemeral"})
		n.roles["b07-204"]="sentinel"
		baseline=append(baseline,"sentinel")
	}
	n.roles[child],n.roles[parent],n.roles[blocker]="child","parent","blocker"
	n.graph("parent-endpoints",nodes)
	c:=effectCase{Name:"parent-child/"+route,Kind:"parent-child",Route:route}
	steps:=[]effectStep{n.step(c,"created",child,baseline,false)}
	n.batch("child-parent","dep add "+child+" "+parent+" parent-child\n")
	image:=n.image("child-parent-linked")
	edge:=n.edge(image,child,parent)
	if edge==nil || nativeEffectText(n.t,edge["type"])!="parent-child" {n.t.Fatal("native scheduling edge missing")}
	steps=append(steps,n.step(c,"open-parent",child,baseline,false))
	n.batch("parent-blocker","dep add "+parent+" "+blocker+" blocks\n")
	want:=effectWithout(baseline,"child","parent")
	steps=append(steps,n.step(c,"blocked-parent",child,want,false))
	n.image("parent-blocked")
	n.command("close-blocker",false,"close",blocker,"--json")
	want=effectWithout(baseline,"blocker")
	steps=append(steps,n.step(c,"parent-unblocked-by-close",child,want,false))
	n.command("reopen-blocker",false,"reopen",blocker,"--json")
	want=effectWithout(baseline,"child","parent")
	steps=append(steps,n.step(c,"parent-reblocked",child,want,false))
	n.batch("unlink-parent-blocker","dep remove "+parent+" "+blocker+"\n")
	steps=append(steps,n.step(c,"parent-unblocked-by-unlink",child,baseline,false))
	if route=="durable" || route=="active-durable" {
		n.command("defer-parent",false,"defer",parent,"--json")
		want=effectWithout(baseline,"child","parent")
		steps=append(steps,n.step(c,"deferred-parent",child,want,false))
	} else {
		n.batch("relink-parent-blocker","dep add "+parent+" "+blocker+" blocks\n")
		want=effectWithout(baseline,"child","parent")
		steps=append(steps,n.step(c,"parent-reblocked-by-relink",child,want,false))
	}
	n.command("close-child",false,"close",child,"--json")
	steps=append(steps,n.step(c,"child-closed-with-ineligible-parent",child,want,false))
	if !reflect.DeepEqual(edge,n.edge(n.image("child-closed"),child,parent)) {n.t.Fatal("native child close changed original scheduling edge")}
	(&nativeRecorder{t:n.t,root:n.root}).writeJSON(filepath.Join(n.root,"parent-effect-sequences.json"),steps)
	return steps
}

type effectNativeExternal struct {
	TargetCase,Kind,StoredArm string
	ReadyAfterLink []string
	GuardRefused bool
}

func (n *effectNative) external(targetCase,kind string) effectNativeExternal {
	const source="b07-301"
	const unrelated="b07-302"
	n.roles[source],n.roles[unrelated]="source","unrelated"
	n.graph("external-endpoints",[]map[string]any{{"key":"source","id":source,"title":"external source"},{"key":"unrelated","id":unrelated,"title":"unrelated exact"}})
	target:="external:unconfigured:ticket"
	if targetCase=="cross-prefix" {target="remote-1"}
	baseline:=[]string{"source","unrelated"}
	n.requireReady("external-created",baseline)
	before:=n.image("missing-local-before")
	n.command("missing-same-prefix-local",true,"dep","add",source,"b07-absent","--type","blocks","--json")
	if !reflect.DeepEqual(before,n.image("missing-local-after")) {n.t.Fatal("native missing-local refusal changed exact state")}
	add:="dep add "+source+" "+target+" "+strconv.Quote(kind)+"\n"
	remove:="dep remove "+source+" "+target+"\n"
	n.batch("external-link",add)
	image:=n.image("external-linked")
	edge:=n.edge(image,source,target)
	if edge==nil || nativeEffectText(n.t,edge["depends_on_external"])!=target || nativeEffectText(n.t,edge["type"])!=kind {n.t.Fatal("native exact external storage arm differs")}
	// The public decorator recognizes external: capabilities. It does not
	// infer that a bare differently-prefixed ID is a configured capability.
	guarded:=targetCase=="explicit-capability" && kind=="blocks"
	want:=baseline
	if guarded {want=[]string{"unrelated"}}
	c:=effectCase{Name:targetCase+"/"+kind,Kind:kind}
	step:=n.step(c,"external-linked",source,want,guarded)
	n.batch("external-unlink",remove)
	if n.edge(n.image("external-unlinked"),source,target)!=nil {n.t.Fatal("native external unlink retained edge")}
	n.requireReady("external-unlinked-ready",baseline)
	n.batch("external-relink",add)
	edge=n.edge(n.image("external-relinked"),source,target)
	if edge==nil || nativeEffectText(n.t,edge["depends_on_external"])!=target {n.t.Fatal("native external relink lost its arm")}
	n.step(c,"external-relinked",source,want,guarded)
	if guarded {n.batch("external-remove-for-close",remove)}
	n.command("external-source-close",false,"close",source,"--json")
	n.requireReady("external-closed-ready",[]string{"unrelated"})
	n.image("external-source-closed")
	result:=effectNativeExternal{targetCase,kind,"depends_on_external",step.Ready,guarded}
	(&nativeRecorder{t:n.t,root:n.root}).writeJSON(filepath.Join(n.root,"external-public-policy.json"),result)
	return result
}

func TestB07PinnedPublicContinuationCounterparts(t *testing.T) {
	root:=filepath.Join(os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"),"b07-effects-build")
	if err:=os.MkdirAll(root,0700);err!=nil {t.Fatal(err)}
	r:=&nativeRecorder{t:t,root:root}
	checkout:=os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	for _,name:=range []string{"PLUG_B07_REQUEST_SHA","GITHUB_RUN_ID","GITHUB_RUN_ATTEMPT","GITHUB_SHA","GITHUB_WORKFLOW_REF"} {
		if os.Getenv(name)=="" {t.Fatal("missing exact native run binding",name)}
	}
	actual:=strings.TrimSpace(string(r.buildCommand(checkout,"candidate-head","/usr/bin/git",[]string{"rev-parse","HEAD"})))
	if actual!=os.Getenv("PLUG_B07_REQUEST_SHA") {t.Fatal("candidate request identity differs")}
	r.buildCommand(checkout,"candidate-tree-parents","/usr/bin/git",[]string{"show","-s","--format=%H %T %P","HEAD"})
	raw,err:=os.ReadFile(filepath.Join(checkout,"spec/s1-b09-native/native_inputs.json"))
	if err!=nil || hashBytes(raw)!="a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2" {t.Fatal("unchanged pinned native protocol differs")}
	var protocol struct{ExporterLabel string `json:"exporter_label"`}
	if json.Unmarshal(raw,&protocol)!=nil || protocol.ExporterLabel=="" {t.Fatal("native canonical label unavailable")}
	r.target=protocol.ExporterLabel
	r.writeJSON(filepath.Join(root,"run-binding.json"),map[string]any{"candidate":actual,"run":os.Getenv("GITHUB_RUN_ID"),"attempt":os.Getenv("GITHUB_RUN_ATTEMPT"),"workflow_sha":os.Getenv("GITHUB_SHA"),"workflow_ref":os.Getenv("GITHUB_WORKFLOW_REF"),"native_source":pinnedSource,"scope":"new public continuation sequences; no old vocabulary or metadata HTTP unit"})
	binary:=r.buildCanonicalSource(pinnedSource)
	var nativeSteps []effectStep
	parentSteps:=map[string][]effectStep{}
	var externalSteps []effectNativeExternal
	for _,twin:=range []string{"a","b"} {
		t.Run("native_"+twin,func(t *testing.T) {
			if !t.Run("continuations",func(t *testing.T) {
				got:=newEffectNative(t,binary).sequences()
				if nativeSteps==nil {nativeSteps=got} else if !reflect.DeepEqual(nativeSteps,got) {t.Fatal("independent native continuation twins differ")}
			}) {t.Fatal("native continuation incomplete")}
			for _,route:=range []string{"durable","durable-active","active-durable","active-active"} {
				t.Run("parent/"+route,func(t *testing.T) {
					got:=newEffectNative(t,binary).parentSequence(route)
					if prior,ok:=parentSteps[route];!ok {parentSteps[route]=got} else if !reflect.DeepEqual(prior,got) {t.Fatal("independent native scheduling twins differ")}
				})
			}
			var external []effectNativeExternal
			for _,targetCase:=range []string{"explicit-capability","cross-prefix"} {
				for _,kind:=range []string{"blocks","parent-child","custom kind"} {
					t.Run("external/"+targetCase+"/"+kind,func(t *testing.T) {external=append(external,newEffectNative(t,binary).external(targetCase,kind))})
				}
			}
			if externalSteps==nil {externalSteps=external} else if !reflect.DeepEqual(externalSteps,external) {t.Fatal("independent native external-policy twins differ")}
		})
	}
	if len(nativeSteps)==0 || len(parentSteps)!=4 || len(externalSteps)!=6 {t.Fatal("native counterpart leaves missing")}
	for _,engine:=range []string{"memory","sqlite","postgres"} {
		t.Run("paired_"+engine,func(t *testing.T) {
			t.Run("continuations",func(t *testing.T) {if !reflect.DeepEqual(nativeSteps,effectPublic(t,engine)) {t.Fatal("declared public native/product continuation outcomes differ")}})
			for _,route:=range []string{"durable","durable-active","active-durable","active-active"} {
				t.Run("parent/"+route,func(t *testing.T) {if !reflect.DeepEqual(parentSteps[route],effectParentPublic(t,engine,route)) {t.Fatal("declared native/product scheduling outcomes differ")}})
			}
			t.Run("external",func(t *testing.T) {effectExternalPublic(t,engine)})
		})
	}
}
