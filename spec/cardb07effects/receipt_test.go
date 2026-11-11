package cardb20

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	proof "github.com/buildbetter-app/plugs/internal/integration/b07positive"
	publicproof "github.com/buildbetter-app/plugs/internal/integration/b07positive/public"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Bind the one new opaque receipt to the independently constructed normalized
// public request, its actual public result, and the retained invocation item.
// No expected identity is taken from the receipt or its appended audit entry.
func effectProviderReceipt(t *testing.T, before, after proof.State, method, label string, params map[string]any, primary model.Ref, commit model.Commit, started, finished time.Time) {
	t.Helper()
	oldState, newState := before.Workflow(t), after.Workflow(t)
	oldItems := effectObject(t, oldState["items"])
	item := effectObject(t, oldItems[primary.ID])
	var run string
	if json.Unmarshal(item["runId"], &run) != nil || run == "" { t.Fatal("original invocation run identity unavailable") }
	var generation uint64
	if json.Unmarshal(item["generation"], &generation) != nil { t.Fatal("original item generation unavailable") }
	wantCommit := model.Commit{Revision:model.Natural64(0), Results:[]model.OpResult{{Ref:primary, Revision:model.Natural64(generation+1)}}, Warnings:[]model.Warning{}}
	if !reflect.DeepEqual(commit, wantCommit) { t.Fatal("provider result differs from requested endpoint and exact next generation") }
	var request any
	event := "workflow.relation"
	if method == "bead.close" {
		request = struct { ID model.Ref `json:"id"`; Reason string `json:"reason"` }{primary,""}
		event = "workflow.close"
	} else {
		var metadata *model.LinkMetadata
		if value,ok := params["metadata"].(*model.LinkMetadata); ok { metadata=value }
		request = struct {
			From model.Ref `json:"from"`
			To model.Ref `json:"to"`
			Kind string `json:"kind"`
			Metadata *model.LinkMetadata `json:"metadata,omitempty"`
			Unlink bool `json:"unlink"`
		}{params["from"].(model.Ref),params["to"].(model.Ref),params["kind"].(string),metadata,method=="bead.unlink"}
	}
	requestBytes := effectToken(t,request)
	principal,err := driver.PrincipalKey(publicproof.Anonymous)
	if err != nil { t.Fatal(err) }
	principalHash := proof.Hash(effectToken(t,publicproof.Anonymous))
	key := proof.Hash([]byte(label))[:32]
	scope := map[string]json.RawMessage{}
	for name,value := range map[string]string{"principal":principal,"principalHash":principalHash,"agent":publicproof.Agent,"project":"b07positive","plugin":"workflow","instance":"default","incarnation":before.Meta.Incarnation,"method":method,"key":key,"digest":proof.Hash(requestBytes),"runId":run,"itemId":primary.ID} { scope[name]=effectToken(t,value) }
	oldReceipts,newReceipts := effectObject(t,oldState["receipts"]),effectObject(t,newState["receipts"])
	mapKey := strings.Join([]string{principal,principalHash,publicproof.Agent,"b07positive","workflow","default",before.Meta.Incarnation,method,key},"\x00")
	if _,exists := oldReceipts[mapKey]; exists || len(newReceipts)!=len(oldReceipts)+1 { t.Fatal("new receipt does not have the exact public request scope") }
	receipt := effectObject(t,newReceipts[mapKey])
	var expires time.Time
	if json.Unmarshal(receipt["expiresAt"],&expires)!=nil || expires.Before(started.Add(driver.IdempotencyWindow)) || expires.After(finished.Add(driver.IdempotencyWindow)) { t.Fatal("receipt expiry is outside the operation's exact clock interval") }
	wantReceipt := map[string]json.RawMessage{}
	for name,value := range scope { wantReceipt[name]=value }
	wantReceipt["expiresAt"],wantReceipt["result"] = receipt["expiresAt"],effectToken(t,wantCommit)
	effectJSONSame(t,wantReceipt,receipt)
	var audit []json.RawMessage
	if json.Unmarshal(newState["audit"],&audit)!=nil || len(audit)==0 { t.Fatal("new audit unavailable") }
	appended := effectObject(t,audit[len(audit)-1])
	var at time.Time
	if json.Unmarshal(appended["at"],&at)!=nil || at.Before(started) || at.After(finished) { t.Fatal("audit time is outside the exact request interval") }
	wantAudit := map[string]json.RawMessage{}
	for name,value := range scope { wantAudit[name]=value }
	wantAudit["at"],wantAudit["event"] = appended["at"],effectToken(t,event)
	effectJSONSame(t,wantAudit,appended)
}
