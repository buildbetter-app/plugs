package cardb20

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	publicproof "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func effectVersion(t *testing.T, before, after proof.State, previous, current driver.Record, oldVersions, versions []driver.Version, method string, params map[string]any, commit model.Commit, started, finished time.Time) {
	t.Helper()
	requested,ok:=params["id"].(model.Ref)
	if method=="bead.link" || method=="bead.unlink" {requested,ok=params["from"].(model.Ref)}
	if !ok {t.Fatal("independent requested publication reference unavailable")}
	effectPublicationRef(t,current.Ref,requested)
	last := after.Changes[len(after.Changes)-1]
	if last.Time.Before(started) || last.Time.After(finished) { t.Fatal("changeset time is outside its exact request interval") }
	want := driver.Version{Ref:requested, Revision:after.Meta.Head, Base:len(oldVersions)%driver.VersionBaseEvery==0, CreatedAt:last.Time, Principal:publicproof.Anonymous}
	if want.Base { want.Body=append([]byte(nil),current.Body...) } else {
		var err error
		want.Patch,err=json.Marshal([]map[string]any{{"op":"replace","path":"","value":json.RawMessage(current.Body)}})
		if err!=nil {t.Fatal(err)}
	}
	version:=versions[len(oldVersions)]
	effectPublicationRef(t,version.Ref,requested)
	version.Ref=requested
	if !reflect.DeepEqual(want,version) { t.Fatalf("appended version differs from exact requested body/base/patch/revision/principal: got %+v want %+v",version,want) }
	result := commit.Results[0]
	effectPublicationRef(t,result.Ref,requested)
	if result.Revision!=current.Revision || !bytes.Equal(result.Record,current.Body) {t.Fatal("public commit does not bind the exact current/version body")}
	operation:=model.OpPatch
	paths:=[]string{"/fields/status"}
	keys:=[]string{"fields.status"}
	switch method {
	case "bead.link", "bead.unlink":
		operation=model.OpLink
		if method=="bead.unlink" {operation=model.OpUnlink}
		paths,keys=[]string{"/links"},[]string{"links"}
		if params["kind"]=="parent-child" && params["targetKind"]!="external" {operation=model.OpReparent;paths=append(paths,"/parent");keys=append(keys,"parent")}
	case "bead.close":
		// This ordinary host lowers its named lifecycle slot to one patch;
		// source-installed typed-close history remains a separate retained unit.
		operation=model.OpPatch
		paths=append(paths,"/closedAt");keys=append(keys,"closedAt")
	case "bead.reopen", "bead.defer":
		doc,err:=pipeline.DecodeDocument(previous.Body)
		if err!=nil {t.Fatal(err)}
		if doc.ClosedAt!=nil {paths=append(paths,"/closedAt");keys=append(keys,"closedAt")}
	default:t.Fatal("undeclared publication operation",method)
	}
	sort.Strings(paths)
	wantChange:=model.ChangeRecord{Ref:requested,Op:operation,FromRevision:previous.Revision,ChangedPaths:paths}
	if len(last.Changes)!=1 {t.Fatal("history has extra/missing requested operations")}
	gotChange:=last.Changes[0]
	// Inferred create refs and explicitly decoded public refs can carry
	// different private tags. Bind their complete public arm/ID/branch bytes.
	if !bytes.Equal(effectToken(t,gotChange.Ref),effectToken(t,requested)) {t.Fatal("history differs from the independently requested public reference")}
	gotChange.Ref=requested
	if !reflect.DeepEqual(gotChange,wantChange) {t.Fatalf("exact operation/history differs: got %+v want %+v",last.Changes,wantChange)}
	if len(result.SetBy)!=len(keys) {t.Fatal("result setBy has extra/missing field provenance")}
	for _,key:=range keys { if result.SetBy[key]=="" {t.Fatal("result missing exact field provenance",key)} }
	if value,ok:=result.SetBy["fields.status"];ok && value!="request" {t.Fatal("status provenance differs from the requested lifecycle slot")}
	for _,key:=range []string{"links","parent"} {
		if value,ok:=result.SetBy[key];ok && value!="request" {t.Fatal("relation provenance differs from public request",key)}
	}
	if value,ok:=result.SetBy["closedAt"];ok && value!="core" {t.Fatal("lifecycle provenance differs from core")}
}

// Reader rows materialize branch zero while public requests may omit that
// default. Check the complete public identity before detaching the private Go
// arm tag for comparisons of the remaining exact version/publication fields.
func effectPublicationRef(t *testing.T,actual,requested model.Ref) {
	t.Helper()
	if actual.Validate()!=nil || requested.Validate()!=nil || actual.IsID()!=requested.IsID() || actual.ID!=requested.ID || actual.Key!=requested.Key || actual.Branch.String()!=requested.Branch.String() {t.Fatalf("publication reference differs from the independently requested arm/ID/branch: got %s want %s",effectToken(t,actual),effectToken(t,requested))}
}

func effectRows(t *testing.T, rows []driver.DerivedRow) map[string]driver.DerivedRow {
	t.Helper()
	indexed:=map[string]driver.DerivedRow{}
	for _,row:=range rows {if _,duplicate:=indexed[row.Key];duplicate {t.Fatal("duplicate derived identity",row.Key)};indexed[row.Key]=row}
	return indexed
}

// These fixtures mutate no priority, creation instant or source import. Thus
// only the touched durable bead's ready projection is republished, and a
// local durable parent edit adds/removes its one declared depth-one closure.
// Every other table and every unrelated row remains byte-exact.
func effectDerived(t *testing.T, before, after proof.State, changed map[string]bool, method string, params map[string]any, started, finished time.Time) {
	t.Helper()
	if len(before.Derived)!=len(after.Derived) {t.Fatal("derived table inventory changed")}
	for table,rows:=range before.Derived {
		actual,exists:=after.Derived[table]
		if !exists {t.Fatal("derived table disappeared",table)}
		want,got:=effectRows(t,rows),effectRows(t,actual)
		if table==driver.DerivedSortKey {
			for id:=range changed {
				key:="ready/"+id
				old,ok:=want[key]
				next,present:=got[key]
				if !ok || !present {t.Fatal("touched durable ready projection absent",id)}
				_,previous,err:=pipeline.DecodeReadySortKey(old)
				if err!=nil {t.Fatal(err)}
				actualID,value,err:=pipeline.DecodeReadySortKey(next)
				if err!=nil || actualID!=id || value.RankedAt.Before(started) || value.RankedAt.After(finished) {t.Fatal("ready projection has wrong identity/clock",err)}
				previous.RankedAt,previous.Revision=value.RankedAt,after.Meta.Head
				encoded,err:=json.Marshal(previous)
				if err!=nil {t.Fatal(err)}
				want[key]=driver.DerivedRow{Key:key,Value:encoded}
			}
		}
		if table==driver.DerivedAncestorClosure && (method=="bead.link" || method=="bead.unlink") && params["kind"]=="parent-child" && params["targetKind"]!="external" {
			from,to:=params["from"].(model.Ref),params["to"].(model.Ref)
			if changed[from.ID] {
				key:=base64.RawURLEncoding.EncodeToString([]byte(from.ID))+"."+base64.RawURLEncoding.EncodeToString([]byte(to.ID))
				if method=="bead.link" {if _,exists:=want[key];exists {t.Fatal("new parent fixture already had its closure")};want[key]=driver.DerivedRow{Key:key,Value:[]byte("1")}} else {if _,exists:=want[key];!exists {t.Fatal("original parent closure absent")};delete(want,key)}
			}
		}
		if !reflect.DeepEqual(want,got) {t.Fatalf("derived %s differs outside declared exact effect",table)}
	}
}
