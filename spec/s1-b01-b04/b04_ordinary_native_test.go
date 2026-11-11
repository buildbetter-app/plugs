package s1b01b04

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

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
	DirectReadback ordinaryB04NativeRow
	Active ordinaryB04NativeRow
	Next ordinaryB04NativeRow
	Empty []ordinaryB04NativeRow
	WIPRefused bool
	DirectVersionDelta int
	ActiveVersionDelta int
	NextVersionDelta int
	SameHolderVersionDelta int
	DirectHistoryDelta int
	ActiveHistoryDelta int
	NextHistoryDelta int
	SameHolderHistoryDelta int
	Filters ordinaryB04NativeFilters
}

type ordinaryB04NativeFilters struct {
	ParentPriorityTypeWins []string
	LabelContainmentWins []string
	WalkPrefix int
	WalkWinner string
	Additive map[string][]string
	Listing map[string][]string
}

func ordinaryNativeIDs(t *testing.T,c *nativeCommand,label string,args ...string) []string {
	t.Helper();rows:=ordinaryNativeRows(t,c.run(label,args...));ids:=[]string{};seen:=map[string]bool{}
	for _,row:=range rows {if row.ID=="" || seen[row.ID] {t.Fatal("native empty/duplicate row")};seen[row.ID]=true;ids=append(ids,row.ID)}
	return ids
}

func recordOrdinaryNativeFilters(t *testing.T,c *nativeCommand,root string) ordinaryB04NativeFilters {
	t.Helper()
	for _,row:=range []struct{id,title,parent,kind,priority string}{
		{"b04-filter-root","ordinary root","","task","0"},
		{"b04-filter-root.1","ordinary selected child","b04-filter-root","task","2"},
		{"b04-filter-root.1.1","ordinary selected grandchild","b04-filter-root.1","task","2"},
		{"b04-filter-outside","ordinary outside","","task","2"},
		{"b04-filter-root.2","ordinary wrong priority","b04-filter-root","task","1"},
		{"b04-filter-root.3","ordinary wrong type","b04-filter-root","bug","2"},
	} {args:=[]string{"create","--title",row.title,"--type",row.kind,"--priority",row.priority,"--labels","parent-case","--json"};if row.parent!="" {args=append(args,"--parent",row.parent)}else {args=append(args,"--id",row.id,"--force")};var made ordinaryB04NativeRow;nativeDecode(t,c.run("ordinary-filter-create-"+row.id,args...),&made);if made.ID!=row.id {t.Fatalf("native public child allocator ID=%s want=%s",made.ID,row.id)}}
	selectors:=[]string{"--parent","b04-filter-root","--priority","2","--type","task"}
	args:=append([]string{"ready","--json","--limit","0"},selectors...)
	if got:=ordinaryNativeIDs(t,c,"ordinary-filter-front",args...);!reflect.DeepEqual(got,[]string{"b04-filter-root.1","b04-filter-root.1.1"}) {t.Fatalf("native exact transitive parent/priority/type front: %v",got)}
	untouched:=map[string][]byte{}
	for _,id:=range []string{"b04-filter-root","b04-filter-outside","b04-filter-root.2","b04-filter-root.3"} {untouched[id]=c.run("ordinary-filter-before-"+id,"show",id,"--json")}
	obs:=ordinaryB04NativeFilters{ParentPriorityTypeWins:[]string{},LabelContainmentWins:[]string{}}
	for _,id:=range []string{"b04-filter-root.1","b04-filter-root.1.1"} {
		before:=ordinaryNativeVersions(t,c,"ordinary-filter-versions-before-"+id,id)
		rows:=ordinaryNativeWin(t,c,"ordinary-filter-claim-"+id,append([]string{"ready","--claim","--json"},selectors...)...)
		if len(rows)!=1 || rows[0].ID!=id || rows[0].Status!="in_progress" || rows[0].Assignee!=owner || rows[0].Priority!=json.Number("2") {t.Fatalf("native filtered postimage: %+v",rows)}
		if read:=ordinaryNativeRows(t,c.run("ordinary-filter-readback-"+id,"show",id,"--json"));len(read)!=1 || !reflect.DeepEqual(rows[0],read[0]) {t.Fatal("native filtered winner/readback differs")}
		if len(ordinaryNativeVersions(t,c,"ordinary-filter-versions-after-"+id,id))-len(before)!=1 {t.Fatal("native filtered win must record one version")}
		obs.ParentPriorityTypeWins=append(obs.ParentPriorityTypeWins,id)
	}
	if got:=ordinaryNativeIDs(t,c,"ordinary-filter-drained",append([]string{"ready","--claim","--json"},selectors...)...);len(got)!=0 {t.Fatal("native filtered front did not drain")}
	for id,want:=range untouched {if !bytes.Equal(want,c.run("ordinary-filter-after-"+id,"show",id,"--json")) {t.Fatalf("native filtered claim changed outsider %s",id)}}
	// Native labels are a separately observed selector until a public mapping
	// is dispositioned. Their presence is never represented by filter.query.
	for _,row:=range []struct{id,labels,priority string}{{"b04-label-one","ordinary","0"},{"b04-label-winner","ordinary,public","2"},{"b04-label-outside","public","1"}} {c.run("ordinary-label-create-"+row.id,"create","--id",row.id,"--title",row.id,"--type","task","--force","--priority",row.priority,"--labels",row.labels,"--json")}
	for _,id:=range []string{"b04-label-one","b04-label-outside"} {untouched[id]=c.run("ordinary-label-before-"+id,"show",id,"--json")}
	labelVersions:=ordinaryNativeVersions(t,c,"ordinary-label-versions-before","b04-label-winner")
	labelRows:=ordinaryNativeWin(t,c,"ordinary-label-claim","ready","--claim","--json","--label","ordinary,public")
	if len(labelRows)!=1 || labelRows[0].ID!="b04-label-winner" || labelRows[0].Status!="in_progress" || labelRows[0].Assignee!=owner {t.Fatalf("native label containment winner: %+v",labelRows)}
	if len(ordinaryNativeVersions(t,c,"ordinary-label-versions-after","b04-label-winner"))-len(labelVersions)!=1 {t.Fatal("native label win must append one version")};if read:=ordinaryNativeRows(t,c.run("ordinary-label-winner-readback","show","b04-label-winner","--json"));len(read)!=1 || !reflect.DeepEqual(read[0],labelRows[0]) {t.Fatal("native label winner differs from stored row")}
	obs.LabelContainmentWins=append(obs.LabelContainmentWins,labelRows[0].ID)
	for _,id:=range []string{"b04-label-one","b04-label-outside"} {if !bytes.Equal(untouched[id],c.run("ordinary-label-after-"+id,"show",id,"--json")) {t.Fatal("native label filter changed outsider")}}
	const prefix=1001
	var lines bytes.Buffer
	wantIDs:=[]string{}
	for i:=0;i<=prefix;i++ {id:=fmt.Sprintf("b04-native-walk-%04d",i);wantIDs=append(wantIDs,id);holder:="other-agent";priority:=0;if i==prefix {holder="";priority=3};data,err:=json.Marshal(map[string]any{"id":id,"title":id,"issue_type":"task","status":"open","priority":priority,"assignee":holder,"labels":[]string{"walk-case"},"created_at":"2026-11-01T00:00:00Z","updated_at":"2026-11-01T00:00:00Z"});if err!=nil {t.Fatal(err)};lines.Write(data);lines.WriteByte('\n')}
	input:=filepath.Join(root,"ordinary-walk-input.jsonl");nativeWrite(t,input,lines.Bytes());c.run("ordinary-walk-public-import","import","--input",input,"--json")
	frontRaw:=c.run("ordinary-walk-front","ready","--json","--limit","0","--label","walk-case")
	front:=ordinaryNativeRows(t,frontRaw)
	if len(front)!=len(wantIDs) {t.Fatalf("native actual full front=%d want=%d",len(front),len(wantIDs))}
	var frontFull []map[string]any;nativeDecode(t,frontRaw,&frontFull)
	for i,row:=range front {if row.ID!=wantIDs[i] || row.Status!="open" || i<prefix && row.Assignee!="other-agent" || i==prefix && row.Assignee!="" {t.Fatalf("native actual ineligible prefix at%d: %+v",i,row)}}
	winner:=wantIDs[prefix];beforeWin:=ordinaryNativeVersions(t,c,"ordinary-walk-win-versions-before",winner)
	walk:=ordinaryNativeWin(t,c,"ordinary-walk-claim","ready","--claim","--json","--label","walk-case")
	if len(walk)!=1 || walk[0].ID!=winner || walk[0].Status!="in_progress" || walk[0].Assignee!=owner {t.Fatalf("native full front traversal: %+v",walk)}
	if len(ordinaryNativeVersions(t,c,"ordinary-walk-win-versions-after",winner))-len(beforeWin)!=1 {t.Fatal("native walk win version delta differs")}
	var afterFull []map[string]any;nativeDecode(t,c.run("ordinary-walk-prefix-after","ready","--json","--limit","0","--label","walk-case"),&afterFull)
	if !reflect.DeepEqual(afterFull,frontFull[:prefix]) {t.Fatal("native walk changed the complete ineligible prefix")}
	obs.WalkPrefix=prefix;obs.WalkWinner=winner
	obs.Additive,obs.Listing=recordOrdinaryNativeAdditive(t,c)
	return obs
}

func ordinaryNativeWin(t *testing.T,c *nativeCommand,label string,args ...string) []ordinaryB04NativeRow {
	t.Helper();before:=ordinaryNativeHistoryCount(t,c,label+"-branch-history-before")
	rows:=ordinaryNativeRows(t,c.run(label,args...))
	if len(rows)!=1 || ordinaryNativeHistoryCount(t,c,label+"-branch-history-after")-before!=1 {t.Fatalf("native durable win must add exactly one branch history entry: %+v",rows)}
	return rows
}

func recordOrdinaryNativeAdditive(t *testing.T,c *nativeCommand) (map[string][]string,map[string][]string) {
	t.Helper();observed:=map[string][]string{};listed:=map[string][]string{}
	for _,v:=range ordinaryB04SelectionCases() {
		root:=ordinarySelectionID(v.Name,"root")
		c.run(v.Name+"-root","create","--id",root,"--title",root,"--type","task","--force","--priority","4","--json")
		ids:=[]string{root}
		for _,row:=range v.Rows {id:=ordinarySelectionID(v.Name,row.Role);ids=append(ids,id);args:=[]string{"create","--title",id,"--type",row.Kind,"--parent",root,"--priority",strconv.Itoa(row.Priority),"--json"};if len(row.Labels)>0 {args=append(args,"--labels",strings.Join(row.Labels,","))};var made ordinaryB04NativeRow;nativeDecode(t,c.run(v.Name+"-create-"+row.Role,args...),&made);if made.ID!=id {t.Fatalf("native actual child ID=%s want=%s",made.ID,id)};if row.Owned {owned:=ordinaryNativeWin(t,c,v.Name+"-own-"+row.Role,"update",id,"--claim","--json");if owned[0].ID!=id || owned[0].Status!="in_progress" || owned[0].Assignee!=owner {t.Fatal("native unassigned control did not retain actual holder")}}}
		selectors:=append([]string{"--parent",root},v.Native...)
		want:=[]string{};for _,role:=range v.Ready {want=append(want,ordinarySelectionID(v.Name,role))}
		if got:=ordinaryNativeIDs(t,c,v.Name+"-front",append([]string{"ready","--json","--limit","0"},selectors...)...);!reflect.DeepEqual(got,want) {t.Fatalf("native selector %s membership=%v want=%v",v.Name,got,want)}
		if v.Name=="unassigned" {
			for i,flag:=range []string{"","--no-assignee=false","--no-assignee=true"} {args:=[]string{"list","--json","--limit","0","--parent",root};if flag!="" {args=append(args,flag)};expected:=ids[1:];if i==2 {expected=want};got:=ordinaryNativeIDs(t,c,fmt.Sprintf("%s-list-%d",v.Name,i),args...);if len(got)!=len(expected) {t.Fatalf("native unassigned case%d list=%v want=%v",i,got,expected)};for _,id:=range expected {found:=false;for _,value:=range got {found=found || value==id};if !found {t.Fatalf("native listing omitted %s",id)}}}
		}
		listingBefore:=ordinaryNativeDomain(t,c,v.Name+"-listing-before",ids);listingHistory:=ordinaryNativeHistoryCount(t,c,v.Name+"-listing-history-before")
		listIDs:=ordinaryNativeIDs(t,c,v.Name+"-selector-list",append([]string{"list","--json","--limit","0","--parent",root},v.ListNative...)...);sort.Strings(listIDs);listWant:=[]string{};for _,role:=range v.Listed {listWant=append(listWant,ordinarySelectionID(v.Name,role))};sort.Strings(listWant)
		if !reflect.DeepEqual(listIDs,listWant) {t.Fatalf("native listing selector %s=%v want=%v",v.Name,listIDs,listWant)}
		if !reflect.DeepEqual(listingBefore,ordinaryNativeDomain(t,c,v.Name+"-listing-after",ids)) || ordinaryNativeHistoryCount(t,c,v.Name+"-listing-history-after")!=listingHistory {t.Fatal("native listing changed rows/versions/history")}
		listed[v.Name]=listIDs
		observed[v.Name]=[]string{}
		for _,id:=range want {domainBefore:=ordinaryNativeDomain(t,c,v.Name+"-win-domain-before-"+id,ids);versions:=ordinaryNativeVersions(t,c,v.Name+"-before-"+id,id);rows:=ordinaryNativeWin(t,c,v.Name+"-claim-"+id,append([]string{"ready","--claim","--json"},selectors...)...);if rows[0].ID!=id || rows[0].Status!="in_progress" || rows[0].Assignee!=owner {t.Fatalf("native selector %s winner=%+v",v.Name,rows)};read:=ordinaryNativeRows(t,c.run(v.Name+"-get-"+id,"show",id,"--json"));if len(read)!=1 || !reflect.DeepEqual(read[0],rows[0]) || len(ordinaryNativeVersions(t,c,v.Name+"-after-"+id,id))-len(versions)!=1 {t.Fatal("native additive winner version/postimage mismatch")};domainAfter:=ordinaryNativeDomain(t,c,v.Name+"-win-domain-after-"+id,ids);for _,other:=range ids {if other!=id && (!bytes.Equal(domainBefore[other+"/row"],domainAfter[other+"/row"]) || !bytes.Equal(domainBefore[other+"/versions"],domainAfter[other+"/versions"])) {t.Fatalf("native selector win changed unrelated row/version %s",other)}};observed[v.Name]=append(observed[v.Name],id)}
		before:=ordinaryNativeDomain(t,c,v.Name+"-empty-before",ids);history:=ordinaryNativeHistoryCount(t,c,v.Name+"-empty-history-before")
		if got:=ordinaryNativeIDs(t,c,v.Name+"-empty",append([]string{"ready","--claim","--json"},selectors...)...);len(got)!=0 {t.Fatal("native filtered terminal front is not empty")}
		if !reflect.DeepEqual(before,ordinaryNativeDomain(t,c,v.Name+"-empty-after",ids)) || ordinaryNativeHistoryCount(t,c,v.Name+"-empty-history-after")!=history {t.Fatal("native filtered empty changed domain or branch history")}
	}
	return observed,listed
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

func ordinaryNativeHistoryCount(t *testing.T,c *nativeCommand,label string) int {
	t.Helper()
	var rows []struct{Count json.Number `json:"n"`}
	nativeDecode(t,c.run(label,"sql","SELECT COUNT(*) AS n FROM dolt_log","--json"),&rows)
	if len(rows)!=1 {t.Fatal("native branch-history count must return one actual row")}
	n,err:=strconv.Atoi(rows[0].Count.String());if err!=nil || n<0 {t.Fatalf("native exact branch-history count: %+v %v",rows,err)}
	return n
}

func ordinaryNativeDomain(t *testing.T,c *nativeCommand,label string,ids []string) map[string]json.RawMessage {
	t.Helper();image:=map[string]json.RawMessage{}
	for _,id:=range ids {image[id+"/row"]=c.run(label+"-row-"+id,"show",id,"--json");data,err:=json.Marshal(ordinaryNativeVersions(t,c,label+"-versions-"+id,id));if err!=nil {t.Fatal(err)};image[id+"/versions"]=data}
	return image
}

func recordOrdinaryB04Native(t *testing.T,root string) ordinaryB04NativeObservation {
	t.Helper()
	var inputs nativeInputs
	nativeDecode(t,nativeInputsBytes,&inputs)
	binary:=nativePrepared(t,root,inputs)
	server:=publicnative.StartDolt(t,root)
	defer func(){if err:=server.Stop();err!=nil {t.Error(err)}}()
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
	neverHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-never-populated-history-before")
	if rows:=ordinaryNativeRows(t,c.run("ordinary-never-populated","ready","--claim","--json"));len(rows)!=0 {t.Fatal("native never-populated front is not empty")}
	if ordinaryNativeHistoryCount(t,c,"ordinary-never-populated-history-after")!=neverHistory {t.Fatal("native empty initial front minted branch history")}
	for _,row:=range []struct{id,title,status,priority string}{{"b04-direct","ordinary hydrated row","open","2"},{"b04-active","native category active","awaiting_review","3"},{"b04-wip","native category wip","qa_testing","1"}} {
		args:=[]string{"create","--id",row.id,"--title",row.title,"--type","task","--force","--priority",row.priority,"--status",row.status,"--json"}
		if row.id=="b04-direct" {args=append(args,"--description","preserve description","--labels","ordinary,public")}
		c.run("ordinary-create-"+row.id,args...)
	}
	beforeDirect:=ordinaryNativeVersions(t,c,"ordinary-direct-versions-before","b04-direct")
	beforeDirectHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-direct-history-before")
	direct:=ordinaryNativeRows(t,c.run("ordinary-direct-claim","update","b04-direct","--claim","--json"))
	readDirect:=ordinaryNativeRows(t,c.run("ordinary-direct-readback","show","b04-direct","--json"))
	wantDirect:=ordinaryB04NativeRow{ID:"b04-direct",Title:"ordinary hydrated row",Description:"preserve description",Status:"in_progress",Assignee:owner,Priority:json.Number("2"),Labels:[]string{"ordinary","public"}}
	wantReadback:=wantDirect
	if len(direct)!=1 || len(readDirect)!=1 || !reflect.DeepEqual(direct[0],wantDirect) || !reflect.DeepEqual(readDirect[0],wantReadback) {t.Fatalf("native CLI direct row and hydrated readback independent expectations: result=%+v readback=%+v",direct,readDirect)}
	afterDirect:=ordinaryNativeVersions(t,c,"ordinary-direct-versions-after","b04-direct")
	afterDirectHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-direct-history-after")
	earlyDomainIDs:=[]string{"b04-direct","b04-active","b04-wip"}
	beforeNoopDomain:=ordinaryNativeDomain(t,c,"ordinary-noop-complete-before",earlyDomainIDs)
	beforeNoop:=c.run("ordinary-noop-before","show","b04-direct","--json")
	noop:=ordinaryNativeRows(t,c.run("ordinary-same-holder","update","b04-direct","--claim","--json"))
	if !reflect.DeepEqual(noop,direct) {t.Fatal("native same-holder returned a different CLI row")}
	afterNoop:=c.run("ordinary-noop-after","show","b04-direct","--json")
	noopVersions:=ordinaryNativeVersions(t,c,"ordinary-noop-versions","b04-direct")
	noopHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-noop-history-after")
	if !bytes.Equal(beforeNoop,afterNoop) || !reflect.DeepEqual(afterDirect,noopVersions) {t.Fatal("native same-holder claim changed exact row or history")}
	if !reflect.DeepEqual(beforeNoopDomain,ordinaryNativeDomain(t,c,"ordinary-noop-complete-after",earlyDomainIDs)) {t.Fatal("native no-op changed an unrelated row/version")}
	beforeWIPDomain:=ordinaryNativeDomain(t,c,"ordinary-wip-complete-before",earlyDomainIDs)
	beforeWIP:=c.run("ordinary-wip-before","show","b04-wip","--json")
	wipVersions:=ordinaryNativeVersions(t,c,"ordinary-wip-versions-before","b04-wip")
	wipHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-wip-history-before")
	// The sentinel is defined by the pinned public issueops/errors.go. An
	// operational/parse error cannot satisfy this ordinary status refusal.
	c.refuse("ordinary-wip-refusal","issue not claimable","update","b04-wip","--claim","--json")
	if !bytes.Equal(beforeWIP,c.run("ordinary-wip-after","show","b04-wip","--json")) || !reflect.DeepEqual(wipVersions,ordinaryNativeVersions(t,c,"ordinary-wip-versions-after","b04-wip")) {t.Fatal("native wip refusal mutated row/history")}
	if wipHistory!=ordinaryNativeHistoryCount(t,c,"ordinary-wip-history-after") {t.Fatal("native WIP refusal minted branch history")}
	if !reflect.DeepEqual(beforeWIPDomain,ordinaryNativeDomain(t,c,"ordinary-wip-complete-after",earlyDomainIDs)) {t.Fatal("native refusal changed an unrelated row/version")}
	beforeActive:=ordinaryNativeVersions(t,c,"ordinary-active-versions-before","b04-active")
	beforeActiveHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-active-history-before")
	active:=ordinaryNativeRows(t,c.run("ordinary-active-claim","update","b04-active","--claim","--json"))
	wantActive:=ordinaryB04NativeRow{ID:"b04-active",Title:"native category active",Status:"in_progress",Assignee:owner,Priority:json.Number("3")}
	if len(active)!=1 || !reflect.DeepEqual(active[0],wantActive) {t.Fatalf("native active independent expectation: %+v",active)}
	afterActive:=ordinaryNativeVersions(t,c,"ordinary-active-versions-after","b04-active")
	afterActiveHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-active-history-after")
	for _,row:=range []struct{id,title,priority string}{{"b04-next","hydrated next winner","2"},{"b04-closed","closed blocker","4"},{"b04-dependent","blocked dependent","4"}} {c.run("ordinary-graph-create-"+row.id,"create","--id",row.id,"--title",row.title,"--type","task","--force","--priority",row.priority,"--json")}
	c.run("ordinary-close-blocker","close","b04-closed","--json")
	c.run("ordinary-next-dependency","dep","add","b04-next","b04-closed","--type","blocks","--json")
	c.run("ordinary-next-dependent","dep","add","b04-dependent","b04-next","--type","blocks","--json")
	beforeNext:=ordinaryNativeVersions(t,c,"ordinary-next-versions-before","b04-next")
	beforeNextHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-next-history-before")
	nextRaw:=c.run("ordinary-next-claim","ready","--claim","--json")
	next:=ordinaryNativeRows(t,nextRaw)
	wantNext:=ordinaryB04NativeRow{ID:"b04-next",Title:"hydrated next winner",Status:"in_progress",Assignee:owner,Priority:json.Number("2")}
	if len(next)!=1 || !reflect.DeepEqual(next[0],wantNext) {t.Fatalf("native next independent expectation: %+v",next)}
	var hydrated []map[string]any
	nativeDecode(t,nextRaw,&hydrated)
	if fmt.Sprint(hydrated[0]["dependency_count"])!="1" || fmt.Sprint(hydrated[0]["dependent_count"])!="1" {t.Fatalf("native next lost seeded hydration cardinalities: %+v",hydrated)}
	nextRead:=ordinaryNativeRows(t,c.run("ordinary-next-readback","show","b04-next","--json"))
	if len(nextRead)!=1 || !reflect.DeepEqual(nextRead[0],wantNext) {t.Fatal("native claimNext differs from stored postimage")}
	afterNext:=ordinaryNativeVersions(t,c,"ordinary-next-versions-after","b04-next")
	afterNextHistory:=ordinaryNativeHistoryCount(t,c,"ordinary-next-history-after")
	domainIDs:=[]string{"b04-direct","b04-active","b04-wip","b04-next","b04-closed","b04-dependent"}
	beforeEmpty:=ordinaryNativeDomain(t,c,"ordinary-empty-before",domainIDs)
	empty:=ordinaryNativeRows(t,c.run("ordinary-empty-claim","ready","--claim","--json"))
	if len(empty)!=0 {t.Fatalf("native drained front claimed an ineligible row: %+v",empty)}
	if !reflect.DeepEqual(beforeEmpty,ordinaryNativeDomain(t,c,"ordinary-empty-after",domainIDs)) || ordinaryNativeHistoryCount(t,c,"ordinary-empty-history-after")!=afterNextHistory {t.Fatal("native empty claim changed complete rows, domain versions or branch history")}
	obs:=ordinaryB04NativeObservation{Direct:direct[0],DirectReadback:readDirect[0],Active:active[0],Next:next[0],Empty:empty,WIPRefused:true,DirectVersionDelta:len(afterDirect)-len(beforeDirect),ActiveVersionDelta:len(afterActive)-len(beforeActive),NextVersionDelta:len(afterNext)-len(beforeNext),SameHolderVersionDelta:len(noopVersions)-len(afterDirect),DirectHistoryDelta:afterDirectHistory-beforeDirectHistory,ActiveHistoryDelta:afterActiveHistory-beforeActiveHistory,NextHistoryDelta:afterNextHistory-beforeNextHistory,SameHolderHistoryDelta:noopHistory-afterDirectHistory}
	obs.Filters=recordOrdinaryNativeFilters(t,c,root)
	if obs.DirectVersionDelta!=1 || obs.ActiveVersionDelta!=1 || obs.NextVersionDelta!=1 || obs.SameHolderVersionDelta!=0 {t.Fatalf("native per-win version cardinality: %+v",obs)}
	if obs.DirectHistoryDelta!=1 || obs.ActiveHistoryDelta!=1 || obs.NextHistoryDelta!=1 || obs.SameHolderHistoryDelta!=0 {t.Fatalf("native independent branch-history cardinality: %+v",obs)}
	nativeJSON(t,filepath.Join(root,"ordinary-native-observation.json"),obs)
	return obs
}

func TestB04OrdinaryActualNativeAndThreeStorePublicReplay(t *testing.T) {
	root,err:=os.MkdirTemp(nativeEnv(t,"TEST_UNDECLARED_OUTPUTS_DIR"),"b04-ordinary-native-")
	if err!=nil {t.Fatal(err)}
	nativeJSON(t,filepath.Join(root,"projection-contract.json"),map[string]any{"fields":[]string{"fixed ID/title/description/status/assignee/priority/labels","ordered parent-priority-type winners","ordered label-containment winners","complete observed ineligible-prefix cardinality and exact winner","per-call durable version and independent branch-history deltas","ordered additive label/exclusion selectors"},"declaredNondeterminism":[]string{"native wall-clock timestamps and lease deadline","generated native version commit identities","owned temporary roots, ports and process IDs"},"limits":"raw streams and exact within-run row/history preservation are retained; no cross-system clock, epoch, revision-envelope or ephemeral-plane parity is asserted"})
	var obs ordinaryB04NativeObservation
	var golden []byte
	for i:=0;i<2;i++ {capture:=filepath.Join(root,fmt.Sprintf("capture-%d",i));if err:=os.Mkdir(capture,0700);err!=nil {t.Fatal(err)};current:=recordOrdinaryB04Native(t,capture);encoded,err:=json.Marshal(current);if err!=nil {t.Fatal(err)};if i==0 {obs=current;golden=encoded}else if !bytes.Equal(golden,encoded) {t.Fatal("two fresh canonical native semantic recordings differ")}}
	h:=newPostgresHarness(t)
	for _,backend:=range []string{"memory","sqlite","postgres"} {
		t.Run(backend,func(t *testing.T) {
			dsn:="";if backend=="postgres" {dsn=h.dsn(t)}
			o:=newOrdinaryB04(t,backend,dsn,[]string{"b04-direct","b04-active","b04-wip","b04-next","b04-closed","b04-dependent","b04-filter-root","b04-filter-outside"})
			o.empty("native-never-populated",nil)
			o.seed("native-public-create",ordinaryCreateOp("direct",project,map[string]any{"title":"ordinary hydrated row","description":"preserve description","labels":[]string{"ordinary","public"},"priority":2},""),ordinaryCreateOp("active",project,map[string]any{"title":"native category active","status":"awaiting_review","priority":3},""),ordinaryCreateOp("wip",project,map[string]any{"title":"native category wip","status":"qa_testing","priority":1},""))
			o.reopen("native-before")
			o.win("native-direct","bead.claim","b04-direct","todo",map[string]any{"id":ref("b04-direct")})
			doc:=o.f.document("b04-direct")
			if doc.Fields["title"]!=obs.Direct.Title || doc.Fields["description"]!=obs.Direct.Description || doc.Fields["status"]!=obs.Direct.Status || doc.Lease.Agent!=obs.Direct.Assignee || fmt.Sprint(doc.Fields["priority"])!=obs.Direct.Priority.String() || !reflect.DeepEqual(doc.Labels,obs.DirectReadback.Labels) {t.Fatal("actual native/public direct semantic comparison failed")}
			o.sameHolder("native-fresh-key-noop","b04-direct")
			o.refusal("native-wip-refused","bead.claim",owner,"invalid_params",false,map[string]any{"id":ref("b04-wip")})
			o.win("native-active","bead.claim","b04-active","awaiting_review",map[string]any{"id":ref("b04-active")})
			doc=o.f.document("b04-active")
			if doc.Fields["title"]!=obs.Active.Title || doc.Fields["status"]!=obs.Active.Status || doc.Lease.Agent!=obs.Active.Assignee || fmt.Sprint(doc.Fields["priority"])!=obs.Active.Priority.String() {t.Fatal("actual native/public active semantic comparison failed")}
			o.seed("native-public-graph",ordinaryCreateOp("next",project,map[string]any{"title":"hydrated next winner","priority":2},""),ordinaryCreateOp("closed",project,map[string]any{"title":"closed blocker","priority":4},""),ordinaryCreateOp("dependent",project,map[string]any{"title":"blocked dependent","priority":4},""))
			o.commit("native-public-close","bead.close",map[string]any{"id":ref("b04-closed")})
			o.commit("native-public-dependency","bead.link",map[string]any{"from":ref("b04-next"),"to":ref("b04-closed"),"kind":"blocks"})
			o.commit("native-public-dependent","bead.link",map[string]any{"from":ref("b04-dependent"),"to":ref("b04-next"),"kind":"blocks"})
			o.win("native-next","bead.claimNext","b04-next","todo",nil)
			doc=o.f.document("b04-next")
			if doc.Fields["title"]!=obs.Next.Title || doc.Fields["status"]!=obs.Next.Status || doc.Lease.Agent!=obs.Next.Assignee || fmt.Sprint(doc.Fields["priority"])!=obs.Next.Priority.String() || len(doc.Links)!=1 {t.Fatal("actual native/public next semantic comparison failed")}
			o.empty("native-drained",nil)
			bug:=ordinaryCreateOp("type",project,map[string]any{"title":"ordinary wrong type","priority":2},"b04-filter-root");bug.Create.Type="bug"
			o.seed("native-public-parent-fixture",ordinaryCreateOp("root",project,map[string]any{"title":"ordinary root","priority":0},""),ordinaryCreateOp("child",project,map[string]any{"title":"ordinary selected child","priority":2},"b04-filter-root"),ordinaryCreateOp("grandchild",project,map[string]any{"title":"ordinary selected grandchild","priority":2},"b04-filter-root.1"),ordinaryCreateOp("outside",project,map[string]any{"title":"ordinary outside","priority":2},""),ordinaryCreateOp("priority",project,map[string]any{"title":"ordinary wrong priority","priority":1},"b04-filter-root"),bug)
			for i,id:=range obs.Filters.ParentPriorityTypeWins {o.win(fmt.Sprintf("native-common-filter-%d",i),"bead.claimNext",id,"todo",map[string]any{"filter":map[string]any{"parent":"b04-filter-root","priority":2,"types":[]string{"issue"}}})}
			o.empty("native-common-filter-drained",map[string]any{"filter":map[string]any{"parent":"b04-filter-root","priority":2,"types":[]string{"issue"}}})
			o.reopen("native-after")
			o.retain("native-comparison",map[string]any{"native":obs,"public":o.state("native-final"),"qualification":"ordinary public claim semantics only; native and Plug envelopes/history accounting remain separate"})
			t.Logf("B04_ACTUAL_NATIVE_PUBLIC_COMMON backend=%s direct=true custom_active=true wip_refused=true fresh_key_same_holder_noop=true parent_priority_type=true win_versions=1",backend)
		})
		for _,v:=range ordinaryB04SelectionCases() {t.Run(backend+"/native-selection/"+v.Name,func(t *testing.T){
			dsn:="";if backend=="postgres" {dsn=h.dsn(t)};ids:=[]string{ordinarySelectionID(v.Name,"root")}
			o:=newOrdinaryB04(t,backend,dsn,ids);filter:=o.selectionFixture(v);o.reopen("native-selection-before");o.selectionListingControl(filter,obs.Filters.Listing[v.Name]);if got:=o.listingIDs("native-listing-comparison",filter);!reflect.DeepEqual(got,obs.Filters.Listing[v.Name]) {t.Fatalf("actual native/public snapshot listing membership differs: %v vs %v",got,obs.Filters.Listing[v.Name])}
			if got:=o.readyIDs("native-selection-front",filter);!reflect.DeepEqual(got,obs.Filters.Additive[v.Name]) {t.Fatalf("actual native/public selector membership differs: %v vs %v",got,obs.Filters.Additive[v.Name])}
			for i,id:=range obs.Filters.Additive[v.Name] {o.win(fmt.Sprintf("native-selection-win-%d",i),"bead.claimNext",id,"todo",map[string]any{"filter":filter})};o.empty("native-selection-empty",map[string]any{"filter":filter});o.reopen("native-selection-after")
		})}
	}
}
