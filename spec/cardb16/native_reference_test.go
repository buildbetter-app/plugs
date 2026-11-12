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
    "sort"
    "strconv"
    "strings"
    "testing"
    "time"

    "github.com/buildbetter-app/plugs/internal/publicnative"
)

type b16Native struct { t *testing.T; r *nativeRecorder; binary,dir string; env []string }

func (n *b16Native) command(label string, failure bool, args ...string) []byte {
    n.t.Helper();ctx,cancel:=context.WithTimeout(context.Background(),2*time.Minute);defer cancel()
    cmd:=exec.CommandContext(ctx,n.binary,args...);cmd.Dir=n.dir;cmd.Env=append([]string{},n.env...)
    var out,errout bytes.Buffer;cmd.Stdout=&out;cmd.Stderr=&errout;started:=time.Now().UTC();err:=cmd.Run();ended:=time.Now().UTC();exit:=0
    if err!=nil {exit=-2;var e *exec.ExitError;if errors.As(err,&e){exit=e.ExitCode()}}
    for suffix,data:=range map[string][]byte{"stdout":out.Bytes(),"stderr":errout.Bytes()}{if err:=os.WriteFile(filepath.Join(n.r.root,label+"."+suffix),data,0600);err!=nil{n.t.Fatal(err)}}
    n.r.writeJSON(filepath.Join(n.r.root,label+".request.json"),map[string]any{"argv":cmd.Args,"directory":n.dir,"environment":n.env,"started":started,"ended":ended,"exit":exit,"error":fmt.Sprint(err),"stdout_sha256":hashBytes(out.Bytes()),"stderr_sha256":hashBytes(errout.Bytes())})
    if ctx.Err()!=nil || exit<0 || failure && exit==0 || !failure && exit!=0 {n.t.Fatalf("native %s exit=%d: %v %s",label,exit,err,errout.Bytes())};return out.Bytes()
}
func (n *b16Native) sql(label,query string) []map[string]any {
    n.t.Helper();var rows []map[string]any;b16Decode(n.t,n.command(label,false,"--json","--readonly","sql",query),&rows);if rows==nil{rows=[]map[string]any{}};return rows
}
func (n *b16Native) observedLabels(label,id string) []string {
    n.t.Helper();var rows []map[string]any;b16Decode(n.t,n.command(label+"-show",false,"--json","show",id),&rows)
    if len(rows)!=1 {n.t.Fatalf("show cardinality %d",len(rows))};got:=b16Strings(n.t,rows[0]["labels"])
    table:="labels";if strings.HasPrefix(id,"b16-w"){table="wisp_labels"}
    physical:=n.sql(label+"-physical","SELECT label FROM "+table+" WHERE issue_id = '"+id+"' ORDER BY label")
    stored:=[]string{};for _,row:=range physical{value,ok:=row["label"].(string);if !ok{n.t.Fatal("native label SQL scalar type")};stored=append(stored,value)};b16Equal(n.t,stored,got);return got
}
func (n *b16Native) labels(label,id string,want []string) []string {
    n.t.Helper();got:=n.observedLabels(label,id);b16Equal(n.t,got,want);return got
}
func (n *b16Native) create(id,title string,labels []string,ephemeral bool) {
    args:=[]string{"--json","create","--id",id,"--title",title,"--type","task"};if len(labels)>0{args=append(args,"--labels",strings.Join(labels,","))};if ephemeral{args=append(args,"--ephemeral")};n.command("create-"+id,false,args...)
}
func (n *b16Native) query(label string,want []string,args ...string) {
    n.t.Helper();base:=[]string{"--json","list","--all","--limit","0"};base=append(base,args...);var rows []map[string]any;b16Decode(n.t,n.command("query-"+label,false,base...),&rows)
    got:=[]string{};for _,row:=range rows{title,ok:=row["title"].(string);if !ok{n.t.Fatal("native query title")};got=append(got,title)};sort.Strings(got);expected:=append([]string{},want...);sort.Strings(expected);b16Equal(n.t,got,expected)
}
func (n *b16Native) rename(label,old,new string) b16Counts {
    n.t.Helper();var value map[string]any;b16Decode(n.t,n.command(label,false,"--json","label","rename",old,new),&value)
    b16Equal(n.t,value["status"],"renamed");b16Equal(n.t,value["old_label"],old);b16Equal(n.t,value["new_label"],new)
    observedRenamed,err:=value["renamed"].(json.Number).Int64();if err!=nil{n.t.Fatal(err)}
    observedMerged,err:=value["merged"].(json.Number).Int64();if err!=nil{n.t.Fatal(err)}
    return b16Counts{Renamed:int(observedRenamed),Merged:int(observedMerged)}
}
func (n *b16Native) snapshot(label string,ids []string) map[string][]string {
    n.t.Helper();out:=map[string][]string{};for _,id:=range ids{out[id]=n.observedLabels(label+"-"+id,id)};return out
}
func b16NativeObservedRename(t *testing.T,before,after map[string][]string,old,new string) b16Counts {
    t.Helper();counts:=b16Counts{}
    for id,prior:=range before {
        next,ok:=after[id];if !ok{t.Fatalf("native rename missing readback %q",id)}
        expected:=[]string{};hasOld:=false
        for _,label:=range prior{if label==old{hasOld=true;continue};if !b16Has(expected,label){expected=append(expected,label)}}
        if hasOld {if !b16Has(expected,new){expected=append(expected,new)};sort.Strings(expected);b16Equal(t,next,expected);counts.Renamed++;if b16Has(prior,new){counts.Merged++}
        } else {b16Equal(t,next,prior)}
    }
    return counts
}
func (n *b16Native) renameCommitted(label,old,new string,ids []string,expected b16Counts) b16Counts {
    n.t.Helper();before:=n.snapshot(label+"-before",ids);response:=n.rename(label,old,new);after:=n.snapshot(label+"-after",ids)
    observed:=b16NativeObservedRename(n.t,before,after,old,new);b16Equal(n.t,response,observed);b16Equal(n.t,observed,expected);return observed
}

func TestB16CanonicalNativePublicLabels(t *testing.T) {
    root:=os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR");checkout:=os.Getenv("PLUG_SOURCE_MODULE_ROOT");if !filepath.IsAbs(root)||!filepath.IsAbs(checkout){t.Fatal("retained output and candidate checkout required")}
    r:=&nativeRecorder{t:t,root:root};actual:=strings.TrimSpace(string(r.buildCommand(checkout,"b16-candidate","/usr/bin/git",[]string{"rev-parse","HEAD"})))
    if actual=="" || actual!=os.Getenv("PLUG_B16_REQUEST_SHA"){t.Fatal("actual candidate checkout differs from explicit requested source")}
    protocol,err:=os.ReadFile(filepath.Join(checkout,"spec/s1-b09-native/native_inputs.json"));if err!=nil || hashBytes(protocol)!="a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2"{t.Fatal("canonical native protocol changed",err)}
    var input struct{ExporterLabel string `json:"exporter_label"`};b16Decode(t,protocol,&input);r.target=input.ExporterLabel
    r.writeJSON(filepath.Join(root,"b16-request-binding.json"),map[string]any{"candidate":actual,"target":os.Getenv("TEST_TARGET"),"run":os.Getenv("GITHUB_RUN_ID"),"attempt":os.Getenv("GITHUB_RUN_ATTEMPT"),"workflow_sha":os.Getenv("GITHUB_SHA"),"workflow_ref":os.Getenv("GITHUB_WORKFLOW_REF"),"native_source":pinnedSource,"private":"HARD_DISABLED","official":"UNSPENT","scope":"PUBLIC_NATIVE_LABELS_ONLY"})
    binary:=r.buildCanonicalSource(pinnedSource)
    work:=t.TempDir();home,project,tmp:=filepath.Join(work,"home"),filepath.Join(work,"project"),filepath.Join(work,"tmp");for _,dir:=range []string{home,project,tmp}{if err:=os.Mkdir(dir,0700);err!=nil{t.Fatal(err)}}
    server:=publicnative.StartDolt(t,root)
    env:=server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin","HOME="+home,"USER=b16-public","TZ=UTC","LC_ALL=C","NO_COLOR=1","CI=true","GIT_CONFIG_NOSYSTEM=1","GIT_CONFIG_GLOBAL=/dev/null","GIT_AUTHOR_NAME=B16 Public","GIT_AUTHOR_EMAIL=b16@example.invalid","GIT_COMMITTER_NAME=B16 Public","GIT_COMMITTER_EMAIL=b16@example.invalid","TMPDIR="+tmp,"BEADS_ACTOR=b16-public","BEADS_NO_DAEMON=1"})
    n:=&b16Native{t:t,r:r,binary:binary,dir:project,env:env}
    git:=exec.Command("/usr/bin/git","-c","core.hooksPath=/dev/null","init","--quiet","--template=");git.Dir=project;git.Env=env;if out,err:=git.CombinedOutput();err!=nil{t.Fatalf("public git init: %v %s",err,out)}
    var version struct{Commit,Build string};b16Decode(t,n.command("version",false,"version","--json"),&version);b16Equal(t,version.Commit,pinnedSource.Commit);b16Equal(t,version.Build,"b20-public-fixture-"+pinnedSource.Commit)
    n.command("init",false,"init","--server","--external","--server-host","127.0.0.1","--server-port",strconv.Itoa(server.Port),"--database","b16_labels","--prefix","b16","--skip-hooks","--skip-agents","--non-interactive","--quiet");server.RequireDatabaseDir(t,"b16_labels")
    // Preserve the actual SQL schema and collation rather than assuming Go
    // ordering proves the native SQL comparison behavior.
    n.sql("native-label-schema","SHOW CREATE TABLE labels");n.sql("native-ephemeral-label-schema","SHOW CREATE TABLE wisp_labels");n.sql("native-label-collation","SHOW FULL COLUMNS FROM labels");n.sql("native-ephemeral-label-collation","SHOW FULL COLUMNS FROM wisp_labels")
    n.create("b16-state","state",[]string{"z","a","A","internal space","東京","é","a",""},false)
    initial:=n.labels("initial","b16-state",b16Initial)
    n.command("add",false,"--json","label","add","b16-state","added,added,");n.labels("added","b16-state",[]string{"A","a","added","internal space","z","é","東京"})
    n.command("remove",false,"--json","label","remove","b16-state","added");n.labels("removed","b16-state",b16Initial)
    for _,operation:=range []string{"add","remove"}{label:="a";if operation=="remove"{label="missing"};var results []map[string]any;b16Decode(t,n.command(operation+"-noop",false,"--json","label",operation,"b16-state",label),&results);if len(results)!=1{t.Fatal("native noop outcome cardinality")};b16Equal(t,results[0]["status"],"unchanged");n.labels(operation+"-noop-state","b16-state",b16Initial)}
    n.command("remove-wins",false,"--json","update","b16-state","--set-labels","replace,both,,replace","--add-label","added,both,added,","--remove-label","both")
    patched:=n.labels("patched","b16-state",b16FinalPatch)
    max:=strings.Repeat("界",255);n.create("b16-bound","bounded",[]string{max},false);n.labels("bound255","b16-bound",[]string{max});n.command("bound256-refusal",true,"--json","label","add","b16-bound",strings.Repeat("界",256));n.labels("bound-refusal-readback","b16-bound",[]string{max})
    n.create("b16-d1","rename",[]string{"Old","old","side"},false);n.create("b16-d2","merge",[]string{"old","new"},false);n.create("b16-d3","destination",[]string{"new"},false);n.create("b16-empty","empty",nil,false)
    n.query("old",[]string{"rename","merge"},"--label","old");n.query("case",[]string{"rename"},"--label","Old");n.query("all",[]string{"merge"},"--label","old,new");n.query("any",[]string{"rename","merge","destination"},"--label-any","old,new");n.query("exclude",[]string{"destination","empty"},"--exclude-label","old,added,"+max)
    n.command("self-trim-refusal",true,"--json","label","rename","old"," old ");n.command("rename256-refusal",true,"--json","label","rename","old",strings.Repeat("界",256));n.labels("rename-refusal-readback","b16-d1",[]string{"Old","old","side"})
    durableIDs:=[]string{"b16-d1","b16-d2","b16-d3","b16-empty"}
    durableCounts:=n.renameCommitted("durable-rename","old","new",durableIDs,b16Counts{Renamed:2,Merged:1})
    final:=map[string][]string{"rename":n.labels("renamed","b16-d1",[]string{"Old","new","side"}),"merge":n.labels("merged","b16-d2",[]string{"new"}),"destination":n.labels("destination","b16-d3",[]string{"new"}),"empty":n.labels("empty","b16-empty",[]string{})}
    outcome:=b16Outcome{Initial:initial,Patched:patched,Renamed:durableCounts.Renamed,Merged:durableCounts.Merged,Final:final};r.writeJSON(filepath.Join(root,"b16-native-durable-outcome.json"),outcome)
    n.renameCommitted("absent-rename","absent","new",durableIDs,b16Counts{Renamed:0,Merged:0})
    // Now exercise one actual native rename that sweeps both physical planes.
    n.command("readd-old-d1",false,"--json","label","add","b16-d1","old");n.command("readd-old-d2",false,"--json","label","add","b16-d2","old")
    n.create("b16-w1","active rename",[]string{"z","a","A","internal space","東京","é","a",""},true)
    n.labels("active-sorted","b16-w1",b16Initial)
    n.command("active-add",false,"--json","label","add","b16-w1","added,added,");n.labels("active-added","b16-w1",[]string{"A","a","added","internal space","z","é","東京"})
    n.command("active-remove",false,"--json","label","remove","b16-w1","added");n.labels("active-removed","b16-w1",b16Initial)
    for _,operation:=range []string{"add","remove"}{label:="a";if operation=="remove"{label="missing"};var results []map[string]any;b16Decode(t,n.command("active-"+operation+"-noop",false,"--json","label",operation,"b16-w1",label),&results);if len(results)!=1{t.Fatal("native active noop outcome cardinality")};b16Equal(t,results[0]["status"],"unchanged");n.labels("active-"+operation+"-noop-state","b16-w1",b16Initial)}
    n.command("active-remove-wins",false,"--json","update","b16-w1","--set-labels","replace,both,,replace","--add-label","added,both,added,","--remove-label","both");n.labels("active-patched","b16-w1",b16FinalPatch)
    n.command("active-empty",false,"--json","update","b16-w1","--set-labels","");n.labels("active-empty-state","b16-w1",[]string{})
    n.command("active-bound255",false,"--json","label","add","b16-w1",max);n.labels("active-bound255-state","b16-w1",[]string{max})
    n.command("active-bound256-refusal",true,"--json","label","add","b16-w1",strings.Repeat("界",256));n.labels("active-bound-refused-state","b16-w1",[]string{max})
    n.command("active-red-case",false,"--json","update","b16-w1","--set-labels","Red,red");n.labels("active-red-case-before","b16-w1",[]string{"Red","red"})
    n.query("active-red-case-filter",[]string{"active rename"},"--include-ephemeral","--label","red")
    n.renameCommitted("active-red-case-rename","red","blue",[]string{"b16-w1"},b16Counts{Renamed:1,Merged:0});n.labels("active-red-case-after","b16-w1",[]string{"Red","blue"})
    n.command("active-reset",false,"--json","update","b16-w1","--set-labels","Old,old,side")
    n.create("b16-w2","active merge",[]string{"old","new"},true)
    n.labels("ephemeral-initial-one","b16-w1",[]string{"Old","old","side"});n.labels("ephemeral-initial-two","b16-w2",[]string{"new","old"})
    n.query("both-planes",[]string{"rename","merge","active rename","active merge"},"--include-ephemeral","--label","old")
    n.create("b16-w3","active destination",[]string{"new"},true);n.create("b16-w4","active empty",nil,true)
    n.query("mixed-case",[]string{"rename","active rename"},"--include-ephemeral","--label","Old")
    n.query("mixed-all",[]string{"merge","rename","active merge"},"--include-ephemeral","--label","old,new")
    n.query("mixed-any",[]string{"rename","merge","destination","active rename","active merge","active destination"},"--include-ephemeral","--label-any","old,new")
    n.query("mixed-conjunction",[]string{"merge","active merge"},"--include-ephemeral","--label","old","--exclude-label","side")
    n.query("mixed-exclude",[]string{"destination","empty","active destination","active empty"},"--include-ephemeral","--exclude-label","old,side,added,"+max)
    n.query("mixed-durable-only",[]string{"rename","merge"},"--label","old")
    n.query("mixed-unknown",[]string{},"--include-ephemeral","--label-any","unknown")
    mixedIDs:=[]string{"b16-d1","b16-d2","b16-d3","b16-empty","b16-w1","b16-w2","b16-w3","b16-w4"}
    mixedCounts:=n.renameCommitted("two-plane-rename","old","new",mixedIDs,b16Counts{Renamed:4,Merged:3})
    mixed:=b16MixedOutcome{Counts:mixedCounts,Final:map[string][]string{
        "active rename":n.labels("ephemeral-renamed","b16-w1",[]string{"Old","new","side"}),
        "active merge":n.labels("ephemeral-merged","b16-w2",[]string{"new"}),
        "rename":n.labels("durable-two-plane-one","b16-d1",[]string{"Old","new","side"}),
        "merge":n.labels("durable-two-plane-two","b16-d2",[]string{"new"}),
    }}
    n.query("mixed-after-old",[]string{},"--include-ephemeral","--label","old")
    n.query("mixed-after-new",[]string{"rename","merge","destination","active rename","active merge","active destination"},"--include-ephemeral","--label","new")
    n.query("mixed-after-all",[]string{"rename","active rename"},"--include-ephemeral","--label","new,side")
    n.query("mixed-after-any",[]string{"rename","active rename"},"--include-ephemeral","--label-any","Old,side")
    n.query("mixed-after-conjunction",[]string{"merge","destination","active merge","active destination"},"--include-ephemeral","--label","new","--exclude-label","side")
    // The auxiliary state and bound rows also carry neither excluded label.
    n.query("mixed-after-exclude",[]string{"empty","active empty","state","bounded"},"--include-ephemeral","--exclude-label","new,side")
    n.query("mixed-after-case",[]string{"rename","active rename"},"--include-ephemeral","--label","Old")
    n.query("mixed-after-durable-only",[]string{"rename","merge","destination"},"--label","new")
    r.writeJSON(filepath.Join(root,"b16-native-mixed-outcome.json"),mixed)
    for _,engine:=range []string{"memory","sqlite","postgres"}{t.Run("paired-"+engine,func(t *testing.T){
        t.Run("durable",func(t *testing.T){got:=b16DurableVector(t,engine);b16Equal(t,got,outcome)})
        t.Run("active",func(t *testing.T){got:=b16ActiveVector(t,engine);b16Equal(t,got,mixed)})
    })}
}
