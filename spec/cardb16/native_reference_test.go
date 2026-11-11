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
func (n *b16Native) labels(label,id string,want []string) []string {
    n.t.Helper();var rows []map[string]any;b16Decode(n.t,n.command(label+"-show",false,"--json","show",id),&rows)
    if len(rows)!=1 {n.t.Fatalf("show cardinality %d",len(rows))};got:=b16Strings(n.t,rows[0]["labels"]);b16Equal(n.t,got,want)
    table:="labels";if strings.HasPrefix(id,"b16-w"){table="wisp_labels"}
    physical:=n.sql(label+"-physical","SELECT label FROM "+table+" WHERE issue_id = '"+id+"' ORDER BY label")
    stored:=[]string{};for _,row:=range physical{value,ok:=row["label"].(string);if !ok{n.t.Fatal("native label SQL scalar type")};stored=append(stored,value)};b16Equal(n.t,stored,want);return got
}
func (n *b16Native) create(id,title string,labels []string,wisp bool) {
    args:=[]string{"--json","create","--id",id,"--title",title,"--type","task"};if len(labels)>0{args=append(args,"--labels",strings.Join(labels,","))};if wisp{args=append(args,"--ephemeral")};n.command("create-"+id,false,args...)
}
func (n *b16Native) query(label string,want []string,args ...string) {
    n.t.Helper();base:=[]string{"--json","list","--all","--limit","0"};base=append(base,args...);var rows []map[string]any;b16Decode(n.t,n.command("query-"+label,false,base...),&rows)
    got:=[]string{};for _,row:=range rows{title,ok:=row["title"].(string);if !ok{n.t.Fatal("native query title")};got=append(got,title)};sort.Strings(got);expected:=append([]string{},want...);sort.Strings(expected);b16Equal(n.t,got,expected)
}
func (n *b16Native) rename(label,old,new string,renamed,merged int) {
    n.t.Helper();var value map[string]any;b16Decode(n.t,n.command(label,false,"--json","label","rename",old,new),&value)
    b16Equal(n.t,value["status"],"renamed");b16Equal(n.t,value["old_label"],old);b16Equal(n.t,value["new_label"],new);b16Equal(n.t,value["renamed"],json.Number(strconv.Itoa(renamed)));b16Equal(n.t,value["merged"],json.Number(strconv.Itoa(merged)))
}

func TestB16CanonicalNativePublicLabels(t *testing.T) {
    root:=os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR");checkout:=os.Getenv("PLUG_SOURCE_MODULE_ROOT");if !filepath.IsAbs(root)||!filepath.IsAbs(checkout){t.Fatal("retained output and candidate checkout required")}
    r:=&nativeRecorder{t:t,root:root};actual:=strings.TrimSpace(string(r.buildCommand(checkout,"b16-candidate","/usr/bin/git",[]string{"rev-parse","HEAD"})))
    if actual=="" || actual!=os.Getenv("GITHUB_SHA"){t.Fatal("actual candidate checkout differs from workflow source")}
    protocol,err:=os.ReadFile(filepath.Join(checkout,"spec/s1-b09-native/native_inputs.json"));if err!=nil || hashBytes(protocol)!="a7ddd9047eb94e54cdaa138b1adf756c5088a50bc01605ee2c6eafbfb9d0d9c2"{t.Fatal("canonical native protocol changed",err)}
    var input struct{ExporterLabel string `json:"exporter_label"`};b16Decode(t,protocol,&input);r.target=input.ExporterLabel
    r.writeJSON(filepath.Join(root,"b16-request-binding.json"),map[string]any{"candidate":actual,"target":os.Getenv("TEST_TARGET"),"run":os.Getenv("GITHUB_RUN_ID"),"attempt":os.Getenv("GITHUB_RUN_ATTEMPT"),"native_source":pinnedSource,"private":"HARD_DISABLED","official":"UNSPENT","scope":"PUBLIC_NATIVE_LABELS_ONLY"})
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
    n.sql("native-label-schema","SHOW CREATE TABLE labels");n.sql("native-wisp-label-schema","SHOW CREATE TABLE wisp_labels");n.sql("native-label-collation","SHOW FULL COLUMNS FROM labels");n.sql("native-wisp-label-collation","SHOW FULL COLUMNS FROM wisp_labels")
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
    n.rename("durable-rename","old","new",2,1)
    final:=map[string][]string{"rename":n.labels("renamed","b16-d1",[]string{"Old","new","side"}),"merge":n.labels("merged","b16-d2",[]string{"new"}),"destination":n.labels("destination","b16-d3",[]string{"new"}),"empty":n.labels("empty","b16-empty",[]string{})}
    outcome:=b16Outcome{Initial:initial,Patched:patched,Renamed:2,Merged:1,Final:final};r.writeJSON(filepath.Join(root,"b16-native-durable-outcome.json"),outcome)
    n.rename("absent-rename","absent","new",0,0)
    // Now exercise one actual native rename that sweeps both physical planes.
    n.command("readd-old-d1",false,"--json","label","add","b16-d1","old");n.command("readd-old-d2",false,"--json","label","add","b16-d2","old")
    n.create("b16-w1","active rename",[]string{"old","side"},true);n.create("b16-w2","active merge",[]string{"old","new"},true)
    n.labels("wisp-initial-one","b16-w1",[]string{"old","side"});n.labels("wisp-initial-two","b16-w2",[]string{"new","old"})
    n.query("both-planes",[]string{"rename","merge","active rename","active merge"},"--include-ephemeral","--label","old")
    n.rename("two-plane-rename","old","new",4,3)
    n.labels("wisp-renamed","b16-w1",[]string{"new","side"});n.labels("wisp-merged","b16-w2",[]string{"new"});n.labels("durable-two-plane-one","b16-d1",[]string{"Old","new","side"});n.labels("durable-two-plane-two","b16-d2",[]string{"new"})
    for _,engine:=range []string{"memory","sqlite","postgres"}{t.Run("paired-"+engine,func(t *testing.T){got:=b16DurableVector(t,engine);b16Equal(t,got,outcome)})}
}
