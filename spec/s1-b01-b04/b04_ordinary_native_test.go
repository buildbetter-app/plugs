package s1b01b04

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
	"strconv"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/publicnative"
)

// Authored from the ordinary public native conformance contract, before
// execution. Native observations are checked against these expectations;
// neither a Plug result nor an old recording supplies an expected result.
type ordinaryB04NativeRow struct {
	ID string `json:"id"`
	Title string `json:"title"`
	Description string `json:"description"`
	Status string `json:"status"`
	Assignee string `json:"assignee"`
	Priority json.Number `json:"priority"`
	Labels []string `json:"labels"`
}

type ordinaryB04NativeObservation struct {
	Direct ordinaryB04NativeRow
	Active ordinaryB04NativeRow
	Next ordinaryB04NativeRow
	Empty []ordinaryB04NativeRow
	WIPRefused bool
	DirectVersionDelta int
	ActiveVersionDelta int
	NextVersionDelta int
	SameHolderVersionDelta int
}

func ordinaryNativeRows(t *testing.T, raw []byte) []ordinaryB04NativeRow {
	t.Helper()
	var rows []ordinaryB04NativeRow
	nativeDecode(t,raw,&rows)
	return rows
}

func ordinaryNativeVersions(t *testing.T,c *nativeCommand,label,id string) []json.RawMessage {
	t.Helper()
	var out struct { Recording bool `json:"recording"`; Versions []json.RawMessage `json:"versions"` }
	nativeDecode(t,c.run(label,"versions",id,"--json"),&out)
	if !out.Recording || out.Versions==nil {t.Fatal("native version recording must be actually enabled")}
	return out.Versions
}

func ordinaryNativeRefuse(t *testing.T,c *nativeCommand,label string,args ...string) {
	t.Helper()
	ctx,cancel:=context.WithTimeout(context.Background(),2*time.Minute);defer cancel()
	command:=exec.CommandContext(ctx,c.binary,args...)
	command.Dir,command.Env=c.workspace,c.env
	var stdout,stderr bytes.Buffer
	command.Stdout,command.Stderr=&stdout,&stderr
	started:=time.Now().UTC();err:=command.Run()
	var failed *exec.ExitError
	exit:=-1;if errors.As(err,&failed) {exit=failed.ExitCode()}
	nativeWrite(t,filepath.Join(c.output,label+".stdout"),stdout.Bytes())
	nativeWrite(t,filepath.Join(c.output,label+".stderr"),stderr.Bytes())
	nativeJSON(t,filepath.Join(c.output,label+".receipt.json"),map[string]any{"argv":append([]string{c.binary},args...),"cwd":c.workspace,"env":c.env,"startedUTC":started,"endedUTC":time.Now().UTC(),"exit":exit,"error":fmt.Sprint(err),"timedOut":ctx.Err()!=nil,"stdoutSHA256":sha256Hex(stdout.Bytes()),"stderrSHA256":sha256Hex(stderr.Bytes())})
	if err==nil || exit<=0 || ctx.Err()!=nil {t.Fatalf("native ordinary refusal: exit=%d error=%v",exit,err)}
}

func recordOrdinaryB04Native(t *testing.T,root string) ordinaryB04NativeObservation {
	t.Helper()
	var inputs nativeInputs
	nativeDecode(t,nativeInputsBytes,&inputs)
	binary:=nativePrepared(t,root,inputs)
	server:=publicnative.StartDolt(t,root)
	workspace:=t.TempDir();home:=filepath.Join(workspace,"home")
	if err:=os.Mkdir(home,0700);err!=nil {t.Fatal(err)}
	c:=&nativeCommand{t:t,binary:binary,workspace:workspace,output:root,env:server.ChildEnv([]string{"PATH="+filepath.Dir(server.DoltBinary)+":/usr/bin:/bin","HOME="+home,"USERPROFILE="+home,"XDG_CONFIG_HOME="+filepath.Join(home,".config"),"USER=ordinary-b04","TZ=UTC","LC_ALL=C","NO_COLOR=1","CI=true","GIT_CONFIG_NOSYSTEM=1","BEADS_ACTOR="+owner})}
	var version struct {Commit,Build string}
	nativeDecode(t,c.run("ordinary-version","version","--json"),&version)
	if version.Commit!=inputs.Source || version.Build!="public-"+inputs.Source {t.Fatal("native linked source/build mismatch")}
	c.run("ordinary-init","init","--backend","dolt","--server","--external","--server-host","127.0.0.1","--server-port",strconv.Itoa(server.Port),"--database","b04_ordinary","--prefix","b04","--quiet","--skip-hooks","--skip-agents","--non-interactive")
	server.RequireDatabaseDir(t,"b04_ordinary")
	c.run("ordinary-enable-history","config","set","versioned-history.enabled","true")
	c.run("ordinary-custom-statuses","config","set","status.custom","awaiting_review:active,qa_testing:wip")
	for _,row:=range []struct{id,title,status,priority string}{{"b04-direct","ordinary hydrated row","open","2"},{"b04-active","native category active","awaiting_review","3"},{"b04-wip","native category wip","qa_testing","1"}} {
		args:=[]string{"create","--id",row.id,"--title",row.title,"--type","task","--force","--priority",row.priority,"--status",row.status,"--json"}
		if row.id=="b04-direct" {args=append(args,"--description","preserve description","--labels","ordinary,public")}
		c.run("ordinary-create-"+row.id,args...)
	}
	beforeDirect:=ordinaryNativeVersions(t,c,"ordinary-direct-versions-before","b04-direct")
	direct:=ordinaryNativeRows(t,c.run("ordinary-direct-claim","update","b04-direct","--claim","--json"))
	readDirect:=ordinaryNativeRows(t,c.run("ordinary-direct-readback","show","b04-direct","--json"))
	wantDirect:=ordinaryB04NativeRow{ID:"b04-direct",Title:"ordinary hydrated row",Description:"preserve description",Status:"in_progress",Assignee:owner,Priority:json.Number("2"),Labels:[]string{"ordinary","public"}}
	if len(direct)!=1 || len(readDirect)!=1 || !reflect.DeepEqual(direct[0],wantDirect) || !reflect.DeepEqual(readDirect[0],wantDirect) {t.Fatalf("native direct independent expectation: result=%+v readback=%+v",direct,readDirect)}
	afterDirect:=ordinaryNativeVersions(t,c,"ordinary-direct-versions-after","b04-direct")
	beforeNoop:=c.run("ordinary-noop-before","show","b04-direct","--json")
	c.run("ordinary-same-holder","update","b04-direct","--claim","--json")
	afterNoop:=c.run("ordinary-noop-after","show","b04-direct","--json")
	noopVersions:=ordinaryNativeVersions(t,c,"ordinary-noop-versions","b04-direct")
	if !bytes.Equal(beforeNoop,afterNoop) || !reflect.DeepEqual(afterDirect,noopVersions) {t.Fatal("native same-holder claim changed exact row or history")}
	beforeWIP:=c.run("ordinary-wip-before","show","b04-wip","--json")
	wipVersions:=ordinaryNativeVersions(t,c,"ordinary-wip-versions-before","b04-wip")
	ordinaryNativeRefuse(t,c,"ordinary-wip-refusal","update","b04-wip","--claim","--json")
	if !bytes.Equal(beforeWIP,c.run("ordinary-wip-after","show","b04-wip","--json")) || !reflect.DeepEqual(wipVersions,ordinaryNativeVersions(t,c,"ordinary-wip-versions-after","b04-wip")) {t.Fatal("native wip refusal mutated row/history")}
	beforeActive:=ordinaryNativeVersions(t,c,"ordinary-active-versions-before","b04-active")
	active:=ordinaryNativeRows(t,c.run("ordinary-active-claim","update","b04-active","--claim","--json"))
	wantActive:=ordinaryB04NativeRow{ID:"b04-active",Title:"native category active",Status:"in_progress",Assignee:owner,Priority:json.Number("3")}
	if len(active)!=1 || !reflect.DeepEqual(active[0],wantActive) {t.Fatalf("native active independent expectation: %+v",active)}
	afterActive:=ordinaryNativeVersions(t,c,"ordinary-active-versions-after","b04-active")
	for _,row:=range []struct{id,title,priority string}{{"b04-next","hydrated next winner","2"},{"b04-closed","closed blocker","4"},{"b04-dependent","blocked dependent","4"}} {c.run("ordinary-graph-create-"+row.id,"create","--id",row.id,"--title",row.title,"--type","task","--force","--priority",row.priority,"--json")}
	c.run("ordinary-close-blocker","close","b04-closed","--json")
	c.run("ordinary-next-dependency","dep","add","b04-next","b04-closed","--type","blocks","--json")
	c.run("ordinary-next-dependent","dep","add","b04-dependent","b04-next","--type","blocks","--json")
	beforeNext:=ordinaryNativeVersions(t,c,"ordinary-next-versions-before","b04-next")
	nextRaw:=c.run("ordinary-next-claim","ready","--claim","--json","--limit","1")
	next:=ordinaryNativeRows(t,nextRaw)
	wantNext:=ordinaryB04NativeRow{ID:"b04-next",Title:"hydrated next winner",Status:"in_progress",Assignee:owner,Priority:json.Number("2")}
	if len(next)!=1 || !reflect.DeepEqual(next[0],wantNext) {t.Fatalf("native next independent expectation: %+v",next)}
	var hydrated []map[string]any
	nativeDecode(t,nextRaw,&hydrated)
	if fmt.Sprint(hydrated[0]["dependency_count"])!="1" || fmt.Sprint(hydrated[0]["dependent_count"])!="1" {t.Fatalf("native next lost seeded hydration cardinalities: %+v",hydrated)}
	nextRead:=ordinaryNativeRows(t,c.run("ordinary-next-readback","show","b04-next","--json"))
	if len(nextRead)!=1 || !reflect.DeepEqual(nextRead[0],wantNext) {t.Fatal("native claimNext differs from stored postimage")}
	afterNext:=ordinaryNativeVersions(t,c,"ordinary-next-versions-after","b04-next")
	empty:=ordinaryNativeRows(t,c.run("ordinary-empty-claim","ready","--claim","--json","--limit","1"))
	if len(empty)!=0 {t.Fatalf("native drained front claimed an ineligible row: %+v",empty)}
	if !reflect.DeepEqual(afterActive,ordinaryNativeVersions(t,c,"ordinary-empty-active-versions","b04-active")) || !reflect.DeepEqual(noopVersions,ordinaryNativeVersions(t,c,"ordinary-empty-direct-versions","b04-direct")) {t.Fatal("native empty claim minted a domain version")}
	obs:=ordinaryB04NativeObservation{Direct:direct[0],Active:active[0],Next:next[0],Empty:empty,WIPRefused:true,DirectVersionDelta:len(afterDirect)-len(beforeDirect),ActiveVersionDelta:len(afterActive)-len(beforeActive),NextVersionDelta:len(afterNext)-len(beforeNext),SameHolderVersionDelta:len(noopVersions)-len(afterDirect)}
	if obs.DirectVersionDelta!=1 || obs.ActiveVersionDelta!=1 || obs.NextVersionDelta!=1 || obs.SameHolderVersionDelta!=0 {t.Fatalf("native per-win version cardinality: %+v",obs)}
	nativeJSON(t,filepath.Join(root,"ordinary-native-observation.json"),obs)
	return obs
}

func TestB04OrdinaryActualNativeAndThreeStorePublicReplay(t *testing.T) {
	root,err:=os.MkdirTemp(nativeEnv(t,"TEST_UNDECLARED_OUTPUTS_DIR"),"b04-ordinary-native-")
	if err!=nil {t.Fatal(err)}
	obs:=recordOrdinaryB04Native(t,root)
	h:=newPostgresHarness(t)
	for _,backend:=range []string{"memory","sqlite","postgres"} {
		t.Run(backend,func(t *testing.T) {
			dsn:="";if backend=="postgres" {dsn=h.dsn(t)}
			o:=newOrdinaryB04(t,backend,dsn,[]string{"b04-direct","b04-active","b04-wip","b04-next","b04-closed","b04-dependent"})
			o.seed("native-public-create",createOp("direct",project,map[string]any{"title":"ordinary hydrated row","description":"preserve description","labels":[]string{"ordinary","public"},"priority":2},""),createOp("active",project,map[string]any{"title":"native category active","status":"awaiting_review","priority":3},""),createOp("wip",project,map[string]any{"title":"native category wip","status":"qa_testing","priority":1},""))
			o.reopen("native-before")
			o.win("native-direct","bead.claim","b04-direct","todo",map[string]any{"id":ref("b04-direct")})
			doc:=o.f.document("b04-direct")
			if doc.Fields["title"]!=obs.Direct.Title || doc.Fields["description"]!=obs.Direct.Description || doc.Fields["status"]!=obs.Direct.Status || doc.Lease.Agent!=obs.Direct.Assignee || fmt.Sprint(doc.Fields["priority"])!=obs.Direct.Priority.String() || !reflect.DeepEqual(doc.Labels,obs.Direct.Labels) {t.Fatal("actual native/public direct semantic comparison failed")}
			before:=o.state("native-noop-before")
			o.commit("native-fresh-key-noop","bead.claim",map[string]any{"id":ref("b04-direct")})
			o.equalState("native-noop-after",before)
			o.refusal("native-wip-refused","bead.claim",owner,"invalid_params",false,map[string]any{"id":ref("b04-wip")})
			o.win("native-active","bead.claim","b04-active","awaiting_review",map[string]any{"id":ref("b04-active")})
			doc=o.f.document("b04-active")
			if doc.Fields["title"]!=obs.Active.Title || doc.Fields["status"]!=obs.Active.Status || doc.Lease.Agent!=obs.Active.Assignee || fmt.Sprint(doc.Fields["priority"])!=obs.Active.Priority.String() {t.Fatal("actual native/public active semantic comparison failed")}
			o.seed("native-public-graph",createOp("next",project,map[string]any{"title":"hydrated next winner","priority":2},""),createOp("closed",project,map[string]any{"title":"closed blocker","priority":4},""),createOp("dependent",project,map[string]any{"title":"blocked dependent","priority":4},""))
			o.commit("native-public-close","bead.close",map[string]any{"id":ref("b04-closed")})
			o.commit("native-public-dependency","bead.link",map[string]any{"from":ref("b04-next"),"to":ref("b04-closed"),"kind":"blocks"})
			o.commit("native-public-dependent","bead.link",map[string]any{"from":ref("b04-dependent"),"to":ref("b04-next"),"kind":"blocks"})
			o.win("native-next","bead.claimNext","b04-next","todo",nil)
			doc=o.f.document("b04-next")
			if doc.Fields["title"]!=obs.Next.Title || doc.Fields["status"]!=obs.Next.Status || doc.Lease.Agent!=obs.Next.Assignee || fmt.Sprint(doc.Fields["priority"])!=obs.Next.Priority.String() || len(doc.Links)!=1 {t.Fatal("actual native/public next semantic comparison failed")}
			o.empty("native-drained",nil)
			o.reopen("native-after")
			o.retain("native-comparison",map[string]any{"native":obs,"public":o.state("native-final"),"qualification":"ordinary public claim semantics only; native and Plug envelopes/history accounting remain separate"})
			t.Logf("B04_ACTUAL_NATIVE_PUBLIC_PARITY backend=%s direct=true custom_active=true wip_refused=true fresh_key_same_holder_noop=true win_versions=1",backend)
		})
	}
}
