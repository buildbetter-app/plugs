package cardb20

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/depid"
)

// These are literal names in the unchanged reference's public SQL protocol.
const (
	effectNativeActiveTable = "wisps"
	effectNativeActiveEdges = "wisp_dependencies"
	effectNativeActiveEvents = "wisp_events"
	effectNativeActiveTarget = "depends_on_wisp_id"
	effectNativeJournal = "bd_events_journal"
	effectNativeSequence = "bd_events_seq"
)

type effectNativeEndpoint struct {Title,Plane string}
type effectNativeMutation struct {Verb,From,To,Kind string;Started,Finished time.Time;Until *time.Time}
type effectNativeRows map[string]map[string]json.RawMessage

func nativeEffectToken(t *testing.T,value any) json.RawMessage {
	t.Helper();raw,err:=json.Marshal(value);if err!=nil {t.Fatal(err)};return raw
}

func nativeEffectIndex(t *testing.T,rows []map[string]json.RawMessage) effectNativeRows {
	t.Helper();result:=effectNativeRows{}
	for _,row:=range rows {
		key:=""
		for _,name:=range []string{"id","key","seq"} {if raw,ok:=row[name];ok {key=name+":"+string(raw);break}}
		if key=="" {key=string(nativeEffectToken(t,row))}
		if _,duplicate:=result[key];duplicate {t.Fatal("duplicate exact native table identity",key)}
		copy:=map[string]json.RawMessage{};for name,value:=range row {copy[name]=append(json.RawMessage(nil),value...)};result[key]=copy
	}
	return result
}

func nativeEffectInstant(t *testing.T,raw json.RawMessage,started,finished time.Time) {
	t.Helper();value:=nativeEffectText(t,raw)
	for _,layout:=range []string{time.RFC3339Nano,"2006-01-02 15:04:05.999999999","2006-01-02T15:04:05.999999999"} {
		if at,err:=time.Parse(layout,value);err==nil {if at.Before(started.Add(-time.Second)) || at.After(finished.Add(time.Second)) {t.Fatal("native generated instant outside declared request interval",value)};return}
	}
	t.Fatal("native generated instant is not a declared SQL timestamp",value)
}

// Independently encode the reference's Issue JSON shape from a fully checked
// physical endpoint. Unrequested relational/lease/await fields are absent in
// these fixtures; an extra journal field cannot borrow the SQL row's credit.
func nativeEffectSnapshot(t *testing.T,row map[string]json.RawMessage,includeBlocked bool) map[string]json.RawMessage {
	t.Helper();want:=map[string]json.RawMessage{}
	for _,name:=range []string{"id","title","priority"} {want[name]=row[name]}
	for _,name:=range []string{"description","design","acceptance_criteria","notes","spec_id","status","issue_type","assignee","owner","created_by","close_reason","closed_by_session","source_system","sender","storage_class","await_type","await_id","source_location","mol_type","work_type","event_kind","actor","target","payload"} {
		if raw:=row[name];len(raw)!=0 && string(raw)!="null" && nativeEffectText(t,raw)!="" {want[name]=raw}
	}
	for _,name:=range []string{"created_at","updated_at","started_at","closed_at","due_at","defer_until","compacted_at"} {
		raw:=row[name];if len(raw)==0 || string(raw)=="null" {continue}
		value:=nativeEffectText(t,raw);parsed:=false
		for _,layout:=range []string{time.RFC3339Nano,"2006-01-02 15:04:05.999999999","2006-01-02T15:04:05.999999999"} {
			if at,err:=time.Parse(layout,value);err==nil {want[name]=nativeEffectToken(t,at.UTC());parsed=true;break}
		}
		if !parsed {t.Fatal("native snapshot timestamp is invalid",name,value)}
	}
	for _,name:=range []string{"ephemeral","no_history","pinned","is_template","is_blocked"} {
		if name=="is_blocked" && !includeBlocked {continue}
		if raw:=string(row[name]);raw=="1" || raw=="true" {want[name]=json.RawMessage("true")} else if raw!="" && raw!="null" && raw!="0" && raw!="false" {t.Fatal("native boolean column has undeclared token",name,raw)}
	}
	for _,name:=range []string{"estimated_minutes","compaction_level","original_size","timeout"} {
		if raw:=row[name];len(raw)!=0 && string(raw)!="null" && string(raw)!="0" {want[name]=raw}
	}
	for _,name:=range []string{"external_ref","compacted_at_commit"} {if raw:=row[name];len(raw)!=0 && string(raw)!="null" {want[name]=raw}}
	// Pinned ScanIssueFrom omits the exact empty-object SQL metadata cell.
	// Unrequested fixture fields stay absent; any extra journal key still fails
	// the complete snapshot comparison below.
	if raw:=row["metadata"];len(raw)!=0 && string(raw)!="null" {value:=nativeEffectText(t,raw);if value!="" && value!="{}" {if !json.Valid([]byte(value)) {t.Fatal("native metadata token is invalid")};want["metadata"]=json.RawMessage(value)}}
	return want
}

func nativeEffectFreshToken(t *testing.T,old,next json.RawMessage,text bool) {
	t.Helper();value:=string(next);if text {value=nativeEffectText(t,next)}
	if number,err:=strconv.ParseInt(value,10,64);err!=nil || number==0 || string(old)==string(next) {t.Fatal("native opaque token did not change to a nonzero signed integer")}
}

func (n *effectNative) endpoint(state effectNativeState,id string) (string,map[string]json.RawMessage) {
	n.t.Helper();found:="";var result map[string]json.RawMessage
	for _,table:=range []string{"issues",effectNativeActiveTable} {for _,row:=range state[table] {if nativeEffectText(n.t,row["id"])==id {if result!=nil {n.t.Fatal("native endpoint appears in both planes",id)};found,result=table,row}}}
	if result==nil {n.t.Fatal("native endpoint disappeared",id)};return found,result
}

func (n *effectNative) verifySuccess(label string,after effectNativeState) {
	n.t.Helper()
	if len(n.previous)!=len(after) {n.t.Fatal("native success changed actual table inventory")}
	want:=map[string]effectNativeRows{}
	got:=map[string]effectNativeRows{}
	for table,rows:=range n.previous {actual,ok:=after[table];if !ok {n.t.Fatal("native table disappeared",table)};want[table],got[table]=nativeEffectIndex(n.t,rows),nativeEffectIndex(n.t,actual)}
	primary:=map[string]string{}
	flipped:=map[string]bool{}
	created:=map[string]bool{}
	for _,mutation:=range n.pending {
		primary[mutation.From]=mutation.Verb
		if mutation.Verb=="create" {
			created[mutation.From]=true
			table,row:=n.endpoint(after,mutation.From)
			definition:=n.endpoints[mutation.From]
			if table!=definition.Plane || nativeEffectText(n.t,row["title"])!=definition.Title || nativeEffectText(n.t,row["status"])!="open" || nativeEffectText(n.t,row["issue_type"])!="task" || string(row["priority"])!="2" {n.t.Fatal("native created endpoint differs from declared body/plane",mutation.From)}
			if table==effectNativeActiveTable {if nativeEffectText(n.t,row["storage_class"])!="ephemeral" {n.t.Fatal("native active class differs from requested plane")}} else if string(row["storage_class"])!="null" {n.t.Fatal("native default versioned class must retain its unset SQL token")}
			nativeEffectInstant(n.t,row["created_at"],mutation.Started,mutation.Finished)
			nativeEffectInstant(n.t,row["updated_at"],mutation.Started,mutation.Finished)
			key:="id:"+string(row["id"]);if _,exists:=want[table][key];exists {n.t.Fatal("native create overwrote original endpoint")};want[table][key]=nativeEffectIndex(n.t,[]map[string]json.RawMessage{row})[key]
			continue
		}
		table,row:=n.endpoint(after,mutation.From)
		key:="id:"+string(row["id"])
		previous:=want[table][key]
		if previous==nil {n.t.Fatal("native success lost original source identity")}
		if mutation.Verb=="dep_add" || mutation.Verb=="dep_remove" {n.expectedEdgeMutation(want,got,mutation);continue}
		status:=map[string]string{"close":"closed","reopen":"open","defer":"deferred"}[mutation.Verb]
		if status=="" {n.t.Fatal("undeclared native lifecycle mutation")}
		previous["status"]=nativeEffectToken(n.t,status)
		if mutation.Verb=="close" {nativeEffectInstant(n.t,row["closed_at"],mutation.Started,mutation.Finished);previous["closed_at"]=row["closed_at"];previous["close_reason"]=nativeEffectToken(n.t,"");previous["closed_by_session"]=nativeEffectToken(n.t,"")} else {previous["closed_at"]=json.RawMessage("null")}
		if mutation.Until!=nil {
			at,err:=time.Parse(time.RFC3339Nano,nativeEffectText(n.t,row["defer_until"]))
			if err!=nil || !at.Equal(*mutation.Until) {n.t.Fatal("native future defer differs from independently requested exact instant")}
			previous["defer_until"]=row["defer_until"]
		}
		nativeEffectInstant(n.t,row["updated_at"],mutation.Started,mutation.Finished);previous["updated_at"]=row["updated_at"]
		nativeEffectFreshToken(n.t,previous["row_lock"],row["row_lock"],false);previous["row_lock"]=row["row_lock"]
	}
	// The raw SQL flag is independent of the public ready policy. In these
	// declared acyclic fixtures only local blocks and inherited parent blocks
	// set it. Deferred-parent eligibility and external: policy do not set it.
	blockedIDs:=n.expectedNativeBlocked(want)
	for id,definition:=range n.endpoints {
		table,row:=n.endpoint(after,id);if table!=definition.Plane || nativeEffectText(n.t,row["title"])!=definition.Title {n.t.Fatal("native success changed retained body/plane",id)}
		key:="id:"+string(row["id"]);previous:=want[table][key]
		blocked:=blockedIDs[id]
		bit:=0;if blocked {bit=1}
		next:=nativeEffectToken(n.t,bit)
		if string(previous["is_blocked"])=="true" || string(previous["is_blocked"])=="false" {next=nativeEffectToken(n.t,blocked)}
		if !reflect.DeepEqual(previous["is_blocked"],next) && !created[id] {flipped[id]=true}
		previous["is_blocked"]=next
	}
	n.expectedNativeEvents(want,got,after,primary,flipped)
	for table,rows:=range want {if !reflect.DeepEqual(rows,got[table]) {n.t.Fatalf("native %s success changed %s outside declared exact operation/projected effect",label,table)}}
	n.previous=after;n.pending=nil
}

func (n *effectNative) expectedNativeBlocked(want map[string]effectNativeRows) map[string]bool {
	n.t.Helper()
	statuses:=map[string]string{}
	for id,definition:=range n.endpoints {
		row:=want[definition.Plane]["id:"+string(nativeEffectToken(n.t,id))]
		if row==nil {n.t.Fatal("declared native endpoint absent from expected state",id)}
		statuses[id]=nativeEffectText(n.t,row["status"])
	}
	blocked:=map[string]bool{}
	// The B07 fixtures have no cyclic ancestry or subtree-internal blockers.
	// Iterate the finite declared graph, without consulting actual flags or
	// either implementation's ready output.
	for pass:=0;pass<=len(statuses);pass++ {
		changed:=false
		for _,table:=range []string{"dependencies",effectNativeActiveEdges} {
			for _,edge:=range want[table] {
				from:=nativeEffectText(n.t,edge["issue_id"])
				if statuses[from]=="closed" || statuses[from]=="pinned" {continue}
				kind:=nativeEffectText(n.t,edge["type"])
				if kind!="blocks" && kind!="parent-child" {continue}
				to:=""
				for _,arm:=range []string{"depends_on_issue_id",effectNativeActiveTarget} {
					if raw:=edge[arm];len(raw)!=0 && string(raw)!="null" {to=nativeEffectText(n.t,raw)}
				}
				status,local:=statuses[to]
				if !local {continue}
				active:=status!="closed" && status!="pinned"
				if !blocked[from] && ((kind=="blocks" && active) || (kind=="parent-child" && blocked[to])) {blocked[from]=true;changed=true}
			}
		}
		if !changed {return blocked}
	}
	n.t.Fatal("declared native blocker graph failed to converge")
	return nil
}

func (n *effectNative) expectedEdgeMutation(want,got map[string]effectNativeRows,mutation effectNativeMutation) {
	n.t.Helper();table:="dependencies"
	if n.endpoints[mutation.From].Plane==effectNativeActiveTable {table=effectNativeActiveEdges}
	id:=depid.New(mutation.From,mutation.To);key:="id:"+string(nativeEffectToken(n.t,id))
	if mutation.Verb=="dep_remove" {
		old:=want[table][key];if old==nil || nativeEffectText(n.t,old["type"])!=mutation.Kind {n.t.Fatal("native unlink did not name its exact original edge")};delete(want[table],key)
	} else {
		if want[table][key]!=nil {n.t.Fatal("native new edge overwrote a retained directed identity")}
		actual:=got[table][key];if actual==nil {n.t.Fatal("native edge was not routed by its actual source plane")}
		nativeEffectInstant(n.t,actual["created_at"],mutation.Started,mutation.Finished)
		row:=map[string]json.RawMessage{"id":nativeEffectToken(n.t,id),"issue_id":nativeEffectToken(n.t,mutation.From),"depends_on_issue_id":json.RawMessage("null"),effectNativeActiveTarget:json.RawMessage("null"),"depends_on_external":json.RawMessage("null"),"type":nativeEffectToken(n.t,mutation.Kind),"created_at":actual["created_at"],"created_by":nativeEffectToken(n.t,"b07-effects"),"metadata":nativeEffectToken(n.t,"{}"),"thread_id":nativeEffectToken(n.t,"")}
		column:="depends_on_external"
		if target,local:=n.endpoints[mutation.To];local {column="depends_on_issue_id";if target.Plane==effectNativeActiveTable {column=effectNativeActiveTarget}}
		row[column]=nativeEffectToken(n.t,mutation.To);want[table][key]=row
	}
	if mutation.Kind=="parent-child" && mutation.Verb=="dep_add" {
		sum:=sha256.Sum256([]byte(mutation.To));shard:=uint16(sum[0])<<4|uint16(sum[1])>>4
		name:=fmt.Sprintf("dependency-coordination/v1/%s/%03x",table,shard)
		key:="key:"+string(nativeEffectToken(n.t,name));actual:=got["local_metadata"][key]
		if actual==nil {n.t.Fatal("native parent edit lost its declared coordination projection")}
		var old json.RawMessage;if previous:=want["local_metadata"][key];previous!=nil {old=previous["value"]}
		nativeEffectFreshToken(n.t,old,actual["value"],true)
		want["local_metadata"][key]=map[string]json.RawMessage{"key":nativeEffectToken(n.t,name),"value":actual["value"]}
	}
}

func (n *effectNative) expectedNativeEvents(want,got map[string]effectNativeRows,after effectNativeState,primary map[string]string,flipped map[string]bool) {
	n.t.Helper()
	for _,table:=range []string{"events",effectNativeActiveEvents} {
		counts:=map[string]int{}
		for key,row:=range got[table] {
			if old:=want[table][key];old!=nil {continue}
			id:=nativeEffectText(n.t,row["issue_id"]);verb,expected:=primary[id]
			plane:=n.endpoints[id].Plane;sourceTable:="events";if plane==effectNativeActiveTable {sourceTable=effectNativeActiveEvents}
			if !expected || sourceTable!=table || verb=="dep_add" || verb=="dep_remove" || nativeEffectText(n.t,row["actor"])!="b07-effects" {n.t.Fatal("native success appended an unrelated audit row")}
			typeName:=map[string]string{"create":"created","close":"closed","reopen":"reopened","defer":"status_changed"}[verb]
			if nativeEffectText(n.t,row["event_type"])!=typeName {n.t.Fatal("native audit does not record requested lifecycle operation",verb)}
			var declared effectNativeMutation;for _,mutation:=range n.pending {if mutation.From==id {declared=mutation}}
			nativeEffectInstant(n.t,row["created_at"],declared.Started,declared.Finished)
			if len(row)!=8 || string(row["comment"])!="null" {n.t.Fatal("native human audit has undeclared columns/comment")}
			if verb=="close" || verb=="reopen" {if nativeEffectText(n.t,row["old_value"])!="" || nativeEffectText(n.t,row["new_value"])!="" {n.t.Fatal("native lifecycle audit changed the exact empty reason")}}
			if verb=="defer" {
				_,oldRow:=n.endpoint(n.previous,id)
				var old map[string]json.RawMessage
				if json.Unmarshal([]byte(nativeEffectText(n.t,row["old_value"])),&old)!=nil || !reflect.DeepEqual(old,nativeEffectSnapshot(n.t,oldRow,false)) {n.t.Fatal("native defer audit lost its exact previous endpoint body")}
					patch:=map[string]json.RawMessage{"status":nativeEffectToken(n.t,"deferred")}
					if declared.Until!=nil {patch["defer_until"]=nativeEffectToken(n.t,declared.Until.UTC())}
					var actual map[string]json.RawMessage
					if json.Unmarshal([]byte(nativeEffectText(n.t,row["new_value"])),&actual)!=nil || !reflect.DeepEqual(actual,patch) {n.t.Fatal("native defer audit differs from the independently requested exact patch")}
			}
			counts[id]++;if counts[id]>1 {n.t.Fatal("native success duplicated its human audit event")};want[table][key]=row
		}
		for id,verb:=range primary {
			if verb=="create" || verb=="dep_add" || verb=="dep_remove" {continue}
			sourceTable:="events";if n.endpoints[id].Plane==effectNativeActiveTable {sourceTable=effectNativeActiveEvents}
			if sourceTable==table && counts[id]!=1 {n.t.Fatal("native lifecycle audit event missing",id)}
		}
	}
	additions:=0;counts:=map[string]int{};projected:=map[string]int{}
	for key,row:=range got[effectNativeJournal] {
		if want[effectNativeJournal][key]!=nil {continue}
		id,op:=nativeEffectText(n.t,row["issue_id"]),nativeEffectText(n.t,row["op"])
		verb,isPrimary:=primary[id];expectedOp:=verb;if verb=="reopen" || verb=="defer" {expectedOp="update"}
		actor:=nativeEffectText(n.t,row["actor"])
		if isPrimary && op==expectedOp && actor=="b07-effects" {counts[id]++} else if flipped[id] && op=="update" && actor=="" {projected[id]++;if projected[id]>1 {n.t.Fatal("native duplicated an explicit blocked-state projection")}} else {n.t.Fatal("native appended journal data outside the declared operation/projection identity",id,op,actor)}
		if string(row["comment_json"])!="null" {n.t.Fatal("dependency/lifecycle mutation invented a comment payload")}
		var snapshot map[string]json.RawMessage
		if json.Unmarshal([]byte(nativeEffectText(n.t,row["issue_json"])),&snapshot)!=nil {n.t.Fatal("native journal current snapshot unavailable")}
		definition:=n.endpoints[id]
		endpoint:=want[definition.Plane]["id:"+string(nativeEffectToken(n.t,id))]
		if !reflect.DeepEqual(snapshot,nativeEffectSnapshot(n.t,endpoint,true)) {n.t.Fatal("native journal differs from the complete declared endpoint body")}
		if len(row)!=8 {n.t.Fatal("native journal has undeclared columns")}
		var started,finished time.Time
		for _,mutation:=range n.pending {if started.IsZero() || mutation.Started.Before(started) {started=mutation.Started};if mutation.Finished.After(finished) {finished=mutation.Finished}}
		nativeEffectInstant(n.t,row["ts"],started,finished)
		if op=="dep_add" || op=="dep_remove" {
			var dependency struct{Kind,Target,Metadata string}
			if json.Unmarshal([]byte(nativeEffectText(n.t,row["dep_json"])),&dependency)!=nil {n.t.Fatal("native journal edge payload unavailable")}
			var declared effectNativeMutation;for _,mutation:=range n.pending {if mutation.From==id {declared=mutation}}
			if dependency.Kind!=declared.Kind || dependency.Target!=declared.To || dependency.Metadata!="{}" {n.t.Fatal("native journal edge differs from exact requested identity/metadata")}
			var fields map[string]json.RawMessage
			if json.Unmarshal([]byte(nativeEffectText(n.t,row["dep_json"])),&fields)!=nil || len(fields)!=3 {n.t.Fatal("native dependency journal payload has undeclared fields")}
		} else if string(row["dep_json"])!="null" {n.t.Fatal("native status mutation invented dependency payload")}
		want[effectNativeJournal][key]=row;additions++
	}
	for id:=range primary {if counts[id]!=1 {n.t.Fatal("native exact primary journal operation missing/duplicated",id)}}
	for id:=range flipped {if projected[id]!=1 {n.t.Fatal("native exact system blocker projection missing",id)}}
	if len(want[effectNativeSequence])!=1 {n.t.Fatal("native journal counter inventory differs")}
	for key,row:=range want[effectNativeSequence] {
		previous,err:=strconv.ParseInt(string(row["next_seq"]),10,64);if err!=nil {n.t.Fatal(err)}
		for offset:=1;offset<=additions;offset++ {if got[effectNativeJournal]["seq:"+strconv.FormatInt(previous+int64(offset),10)]==nil {n.t.Fatal("native success journal sequence has a gap")}}
		row["next_seq"]=nativeEffectToken(n.t,previous+int64(additions));want[effectNativeSequence][key]=row
	}
}
