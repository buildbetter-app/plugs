package s1b01b04

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type nativeRPCScope struct {
	Method, Key string
	Write       bool
}

// A driver no-op receipt has a real zero Changeset, which is not a valid
// product wire changeset. Preserve that exact state explicitly in test
// evidence; nonzero changesets retain their full ordinary wire encoding.
type nativeReceiptEvidence struct {
	RequestHash     string
	Commit          model.Commit
	Changeset       *model.Changeset
	ResponsePending bool
	ZeroChangeset   bool
}

func nativeReceiptState(receipt *driver.IdempotencyRecord) nativeReceiptEvidence {
	state := nativeReceiptEvidence{RequestHash: receipt.RequestHash, Commit: receipt.Commit, ResponsePending: receipt.ResponsePending}
	if reflect.DeepEqual(receipt.Changeset, model.Changeset{}) {
		state.ZeroChangeset = true
	} else {
		changeset := receipt.Changeset
		state.Changeset = &changeset
	}
	return state
}

type nativeReplay struct {
	f      *fixture
	output string
	refs   map[string]model.Ref
	nodes  map[string]nativeNode
	calls  []nativeRPCScope
}

// The native matrix is longer than the driver's ordinary ten-second storage
// lease. Keep the fixture's existing host authority alive with its normal
// heartbeat; do not change the lease budget or any application/SQL clock.
func (r *nativeReplay) renewLease() {
	r.f.t.Helper()
	if err := r.f.lease.Renew(r.f.ctx); err != nil {
		r.f.t.Fatalf("native replay lease renewal: %v", err)
	}
}

// nativeReplayManifest is the finite H03/H04 execution boundary. The
// existing recorder has broader hierarchy, external and category phases, but
// this child requires the finite deferred-parent, live-ancestor blocker and
// affected-subtree continuation names. Keeping the boundary explicit prevents
// a filtered native run from looking complete because it happened to execute
// PostgreSQL leaves while omitting the memory/SQLite roots.
type nativeReplayManifest struct {
	Schema               string            `json:"schema"`
	Backends             []string          `json:"backends"`
	Root                 string            `json:"root"`
	HierarchyPhases      []string          `json:"hierarchyPhases"`
	MembershipPhases     []string          `json:"membershipPhases"`
	RootProof            map[string]string `json:"rootProof"`
	ExternalNativePhases []string          `json:"externalNativePhases"`
	RawXMLNamedSet       string            `json:"rawXMLNamedSet"`
	FilterGuard          string            `json:"filterGuard"`
}

var nativeReplayBackends = []string{"memory", "sqlite", "postgres"}

func nativeRequiredReplayManifest() nativeReplayManifest {
	return nativeReplayManifest{
		Schema:   "plug.s1.b01.native-replay-manifest.v1",
		Backends: append([]string(nil), nativeReplayBackends...),
		Root:     "root",
		HierarchyPhases: []string{
			"resumed",
			"blocked-parent",
			"detached-from-blocked",
			"reparented-to-open",
			"reattached-to-blocked",
			"blocker-closed",
			"blocker-reopened",
			"restored",
		},
		MembershipPhases: []string{
			"status-resumed",
			"ancestor-blocked",
			"close-under-blocked-ancestor",
			"reopen-under-blocked-ancestor",
			"ancestor-unblocked",
		},
		RootProof: map[string]string{
			"memory":   "seed/link/before-after conservation; no SQL reopen claim",
			"sqlite":   "signed multirow cursor across physical close/reopen with new handle and greater lease epoch",
			"postgres": "signed multirow cursor across physical close/reopen with new handle and greater lease epoch",
		},
		ExternalNativePhases: []string{"external-refused"},
		RawXMLNamedSet:       "exact equality with the emitted paths set; representation differences remain visible",
		FilterGuard:          "every expected root and phase must execute; filtered omission fails closed",
	}
}

func nativeReplayManifestPaths(inputs nativeInputs) []string {
	manifest := nativeRequiredReplayManifest()
	paths := []string{}
	for _, vector := range inputs.Vectors {
		for _, run := range vector.Runs {
			for _, backend := range manifest.Backends {
				base := fmt.Sprintf("%s/%s/%s/replay/%s", nativeVectorTestName(vector), vector.ID, run.ID, backend)
				paths = append(paths, base+"/"+manifest.Root)
				for _, phase := range manifest.HierarchyPhases {
					paths = append(paths, base+"/"+phase)
				}
				for _, phase := range manifest.MembershipPhases {
					paths = append(paths, base+"/membership/"+phase)
				}
			}
		}
	}
	return paths
}

type nativeReplayCoverage struct {
	Backend    string
	Root       bool
	Hierarchy  map[string]bool
	Membership map[string]bool
}

func newNativeReplayCoverage(backend string) *nativeReplayCoverage {
	return &nativeReplayCoverage{
		Backend:    backend,
		Hierarchy:  map[string]bool{},
		Membership: map[string]bool{},
	}
}

func (c *nativeReplayCoverage) require(t *testing.T, vector, run string) {
	t.Helper()
	manifest := nativeRequiredReplayManifest()
	if !c.Root {
		t.Fatalf("native replay selector omitted %s/%s/%s/root", vector, run, c.Backend)
	}
	for _, phase := range manifest.HierarchyPhases {
		if !c.Hierarchy[phase] {
			t.Fatalf("native replay selector omitted %s/%s/%s/%s", vector, run, c.Backend, phase)
		}
	}
	for _, phase := range manifest.MembershipPhases {
		if !c.Membership[phase] {
			t.Fatalf("native replay selector omitted %s/%s/%s/membership/%s", vector, run, c.Backend, phase)
		}
	}
}

func (r *nativeReplay) call(label, method string, params map[string]any, write bool) map[string]any {
	r.f.t.Helper()
	r.renewLease()
	key := r.f.key(label, method, owner)
	requestParams := make(map[string]any, len(params)+1)
	for name, value := range params {
		requestParams[name] = value
	}
	requestParams["_meta"] = map[string]any{"project": project, "agent": owner, "idempotencyKey": key}
	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": len(r.calls) + 1, "method": method, "params": requestParams,
	})
	if err != nil {
		r.f.t.Fatal(err)
	}
	name := fmt.Sprintf("%03d-%s", len(r.calls)+1, label)
	nativeWrite(r.f.t, filepath.Join(r.output, name+".request.json"), request)
	started := time.Now().UTC()
	response, err := r.f.transport.RoundTrip(r.f.ctx, request)
	nativeWrite(r.f.t, filepath.Join(r.output, name+".response.json"), response)
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	nativeJSON(r.f.t, filepath.Join(r.output, name+".receipt.json"), map[string]any{
		"method": method, "requestSHA256": sha256Hex(request), "requestBytes": len(request),
		"responseSHA256": sha256Hex(response), "responseBytes": len(response),
		"startedUTC": started, "endedUTC": time.Now().UTC(), "transportError": errorText,
	})
	r.calls = append(r.calls, nativeRPCScope{Method: method, Key: key, Write: write})
	if err != nil {
		r.f.t.Fatalf("actual replay RPC %s: %v", method, err)
	}
	return result(r.f.t, response)
}

func (r *nativeReplay) write(label, method string, params map[string]any) model.Commit {
	r.f.t.Helper()
	value := r.call(label, method, params, true)
	data, err := json.Marshal(value)
	if err != nil {
		r.f.t.Fatal(err)
	}
	wantResults := 1
	if method == "tx.apply" {
		cs, ok := params["changeSet"].(model.ChangeSet)
		if !ok || len(cs.Ops) == 0 {
			r.f.t.Fatal("native replay transaction requires its exact ordered operation set")
		}
		wantResults = len(cs.Ops)
	}
	if method == "bead.defer" {
		// The nonempty reason lowers to a comment after the lifecycle patch;
		// Commit.Results contains one result for each of those operations.
		if reason, ok := params["reason"].(string); !ok || reason == "" {
			r.f.t.Fatal("native replay defer requires its captured nonempty reason")
		}
		wantResults = 2
	}
	var commit model.Commit
	if err := json.Unmarshal(data, &commit); err != nil || len(commit.Results) != wantResults {
		r.f.t.Fatalf("actual committed result %s: %v %s", method, err, data)
	}
	if method == "bead.defer" {
		parent, ok := params["id"].(model.Ref)
		if !ok || parent.ID == "" {
			r.f.t.Fatal("native replay defer lacks its actual parent ref")
		}
		for _, item := range commit.Results {
			if !reflect.DeepEqual(item.Ref, parent) || item.Revision != commit.Revision ||
				!reflect.DeepEqual(item, commit.Results[0]) {
				r.f.t.Fatalf("defer operation result changed parent, revision or final record: %+v", commit)
			}
		}
	}
	return commit
}

func (r *nativeReplay) sourceImages() map[string]string {
	r.f.t.Helper()
	r.renewLease()
	reader, err := r.f.store.Read(r.f.ctx, driver.View{Incarnation: r.f.metaSnapshot().Incarnation})
	if err != nil {
		r.f.t.Fatal(err)
	}
	defer reader.Close()
	images := map[string]string{}
	for role, ref := range r.refs {
		rows, err := reader.Get(r.f.ctx, []driver.Ref{ref})
		if err != nil || len(rows) != 1 || rows[0].Ref.ID != ref.ID {
			r.f.t.Fatalf("actual source image %s: %d rows: %v", role, len(rows), err)
		}
		// Record includes exact Body bytes, identity, kind and revision. This
		// catches an extra descendant version or byte rewrite at defer time.
		data, err := json.Marshal(rows[0])
		if err != nil {
			r.f.t.Fatal(err)
		}
		images[role] = sha256Hex(data)
	}
	return images
}

// Includes source images, complete short fixture history, every version of
// all four actual refs, and every known write receipt. Every observed read
// key is checked absent; read keys are not added to the state digest.
func (r *nativeReplay) snapshot() []byte {
	r.f.t.Helper()
	r.renewLease()
	meta := r.f.metaSnapshot()
	image := r.f.imageDigest()
	reader, err := r.f.store.Read(r.f.ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		r.f.t.Fatal(err)
	}
	defer reader.Close()
	versions := map[string][]driver.Version{}
	derived := map[driver.DerivedTable][]driver.DerivedRow{}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := reader.DerivedRows(r.f.ctx, table, "")
		if err != nil {
			r.f.t.Fatalf("complete derived state %s: %v", table, err)
		}
		derived[table] = rows
	}
	facts, err := reader.Observe(r.f.ctx, []driver.LockKey{pipeline.GraphValidityLockKey()})
	if err != nil || len(facts) != 1 {
		r.f.t.Fatalf("graph validity fact observation: %v", err)
	}
	for role, ref := range r.refs {
		page, err := reader.Versions(r.f.ctx, ref, driver.PageReq{Limit: 1000})
		if err != nil || page.NextCursor != nil {
			r.f.t.Fatalf("complete fixture version history %s: %v", role, err)
		}
		versions[role] = page.Items
	}
	receipts := map[string]nativeReceiptEvidence{}
	for _, call := range r.calls {
		receipt, err := reader.IdempotencyByKey(r.f.ctx, "anonymous:anonymous", owner, call.Method, call.Key)
		if err != nil || (call.Write && (receipt == nil || receipt.ResponsePending)) || (!call.Write && receipt != nil) {
			r.f.t.Fatalf("receipt scope %s/write=%v: %v, %+v", call.Method, call.Write, err, receipt)
		}
		if call.Write {
			receipts[call.Method+"/"+call.Key] = nativeReceiptState(receipt)
		}
	}
	data, err := json.Marshal(map[string]any{
		"meta": meta, "sourceAndChangesetSHA256": image, "versions": versions, "writeReceipts": receipts,
		"allNineDerivedTables": derived, "graphValidityFact": facts,
	})
	if err != nil {
		r.f.t.Fatal(err)
	}
	return data
}

func (r *nativeReplay) ids(page map[string]any) []string {
	r.f.t.Helper()
	items, ok := page["items"].([]any)
	if !ok {
		r.f.t.Fatal("actual replay ready items are not an array")
	}
	ids := []string{}
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			r.f.t.Fatal("actual replay ready item is not an object")
		}
		id, ok := item["id"].(string)
		if !ok || id == "" {
			r.f.t.Fatal("actual replay ready item lacks its identity")
		}
		ids = append(ids, id)
	}
	return ids
}

func (r *nativeReplay) drain(label string, first map[string]any, filters ...map[string]any) []string {
	r.f.t.Helper()
	page := first
	asOf, err := json.Marshal(page["asOf"])
	if err != nil || bytes.Equal(asOf, []byte("null")) {
		r.f.t.Fatal("actual page is missing its snapshot binding")
	}
	ids := []string{}
	seenIDs, seenCursors := map[string]bool{}, map[string]bool{}
	for count := 0; count < 6; count++ {
		binding, err := json.Marshal(page["asOf"])
		if err != nil || !bytes.Equal(binding, asOf) {
			r.f.t.Fatal("signed continuation changed its pinned snapshot")
		}
		current := r.ids(page)
		if len(current) > 1 {
			r.f.t.Fatal("actual one-row page exceeded its limit")
		}
		for _, id := range current {
			if seenIDs[id] {
				r.f.t.Fatalf("actual page duplicated %s", id)
			}
			seenIDs[id], ids = true, append(ids, id)
		}
		cursor, _ := page["nextCursor"].(string)
		if cursor == "" {
			return ids
		}
		if seenCursors[cursor] || len(current) != 1 {
			r.f.t.Fatal("actual cursor loop or empty nonterminal page")
		}
		seenCursors[cursor] = true
		params := map[string]any{"limit": 1, "cursor": cursor}
		if len(filters) != 0 && filters[0] != nil {
			params["filter"] = filters[0]
		}
		page = r.call(fmt.Sprintf("%s-page-%d", label, count+1), "bead.ready", params, false)
	}
	r.f.t.Fatal("actual ready pagination did not terminate within the four-node corpus")
	return nil
}

func (r *nativeReplay) wantIDs(roles []string) []string {
	ids := []string{}
	for _, role := range roles {
		ids = append(ids, r.refs[role].ID)
	}
	return ids
}

func (r *nativeReplay) observe(label string, deferred bool) {
	r.f.t.Helper()
	before, epoch := r.snapshot(), r.f.lease.Epoch()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-before.json"), before)
	for _, role := range nativeRoles {
		value := r.call(label+"-get-"+role, "bead.get", map[string]any{"id": r.refs[role]}, false)
		bead, ok := value["bead"].(map[string]any)
		if !ok || bead["id"] != r.refs[role].ID {
			r.f.t.Fatalf("public readback lost its actual ref for %s", role)
		}
		wantStatus := "todo"
		if deferred && role == "parent" {
			wantStatus = "backlog"
		}
		if bead["status"] != wantStatus {
			r.f.t.Fatalf("public %s status=%v want=%s", role, bead["status"], wantStatus)
		}
		fields, ok := bead["fields"].(map[string]any)
		priority, priorityOK := fields["priority"].(json.Number)
		if !ok || fields["title"] != r.nodes[role].Title || !priorityOK || priority.String() != strconv.Itoa(r.nodes[role].Priority) {
			r.f.t.Fatalf("public %s title/priority changed: %+v", role, fields)
		}
		parent, _ := bead["parent"].(string)
		wantParent := ""
		if role == "child" {
			wantParent = r.refs["parent"].ID
		} else if role == "grandchild" {
			wantParent = r.refs["child"].ID
		}
		if parent != wantParent {
			r.f.t.Fatalf("public %s parent=%s want=%s", role, parent, wantParent)
		}
	}
	roles := nativeRoles
	if deferred {
		roles = []string{"free"}
	}
	want := r.wantIDs(roles)
	full := r.call(label+"-full", "bead.ready", map[string]any{"limit": 1000}, false)
	first := r.call(label+"-one", "bead.ready", map[string]any{"limit": 1}, false)
	fullBinding, bindingErr := json.Marshal(full["asOf"])
	firstBinding, firstBindingErr := json.Marshal(first["asOf"])
	var binding struct {
		Revision    model.Revision
		Incarnation string
	}
	if bindingErr != nil || firstBindingErr != nil || !bytes.Equal(fullBinding, firstBinding) {
		r.f.t.Fatal("full and one-row ready reads disagree on their snapshot")
	}
	nativeDecode(r.f.t, fullBinding, &binding)
	meta := r.f.metaSnapshot()
	if binding.Revision.String() != meta.Head.String() || binding.Incarnation != meta.Incarnation {
		r.f.t.Fatal("actual ready snapshot differs from source head/incarnation")
	}
	if cursor, _ := full["nextCursor"].(string); cursor != "" {
		r.f.t.Fatal("four-row full control unexpectedly has a continuation")
	}
	if !reflect.DeepEqual(r.ids(full), want) || !reflect.DeepEqual(r.drain(label, first), want) {
		r.f.t.Fatalf("actual %s membership/order differs from %v", label, want)
	}
	after := r.snapshot()
	nativeWrite(r.f.t, filepath.Join(r.output, label+"-state-after.json"), after)
	if !bytes.Equal(before, after) || r.f.lease.Epoch() != epoch {
		r.f.t.Fatal("public reads changed source/head/incarnation/history/known receipts/authority")
	}
	select {
	case <-r.f.lease.Lost():
		r.f.t.Fatal("read sequence lost its active authority")
	default:
	}
}

func (r *nativeReplay) reopen(label string) {
	r.f.t.Helper()
	r.renewLease()
	before, oldStore, oldLease := r.snapshot(), r.f.store, r.f.lease
	if err := oldLease.Release(r.f.ctx); err != nil {
		r.f.t.Fatal(err)
	}
	if err := oldStore.Close(); err != nil {
		r.f.t.Fatal(err)
	}
	select {
	case <-oldLease.Lost():
	default:
		r.f.t.Fatal("released original authority did not close")
	}
	r.f.store = openStore(r.f.t, r.f.backend, r.f.path, r.f.dsn, r.f.clock)
	lease, err := r.f.store.Lease(context.Background(), "s1-b01-native-reopened", driver.LeaseTTL)
	if err != nil {
		r.f.t.Fatal(err)
	}
	r.f.lease = lease
	r.f.install() // same declared cursor signing key and actual incarnation
	after := r.snapshot()
	if r.f.store == oldStore || lease.Epoch() <= oldLease.Epoch() || !bytes.Equal(before, after) {
		r.f.t.Fatal("actual SQL reopen failed handle/epoch/source/history/receipt conservation")
	}
	nativeJSON(r.f.t, filepath.Join(r.output, label+"-actual-sql-reopen.json"), map[string]any{
		"engine": r.f.backend, "oldHandle": fmt.Sprintf("%p", oldStore), "newHandle": fmt.Sprintf("%p", r.f.store),
		"oldEpoch": oldLease.Epoch(), "newEpoch": lease.Epoch(), "stateSHA256": sha256Hex(after), "meta": r.f.metaSnapshot(),
	})
	r.f.t.Logf("B01_NATIVE_ACTUAL_SQL_REOPEN case=%s stage=%s engine=%s old=%p new=%p state=%s", r.f.t.Name(), label, r.f.backend, oldStore, r.f.store, sha256Hex(after))
}

func replayNativeVector(t *testing.T, capture nativeCapture, backend, dsn, decision string, coverage *nativeReplayCoverage) {
	t.Helper()
	output := filepath.Join(capture.Output, "replay-"+backend)
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	generated := []string{}
	for _, role := range nativeRoles {
		generated = append(generated, "plug-b01-"+capture.Vector.Mode+"-"+role)
	}
	f := newFixture(t, backend, dsn, service.NewOntology(nil), generated)
	r := &nativeReplay{f: f, output: output, refs: map[string]model.Ref{}, nodes: capture.Before.Nodes}
	manifest := nativeRequiredReplayManifest()
	for _, phase := range manifest.ExternalNativePhases {
		if _, ok := capture.Transitions[phase]; !ok {
			t.Fatalf("native external phase %s is missing; E01/H05-H08 remains separate from H03/H04", phase)
		}
	}
	nativeJSON(t, filepath.Join(output, "external-refused-boundary.json"), map[string]any{
		"label":                "E01/H05-H08",
		"phase":                "external-refused",
		"includedInH03H04":     false,
		"nativeCapturePresent": true,
	})
	if coverage == nil {
		coverage = newNativeReplayCoverage(backend)
	}
	rootRan := false
	var observedAfter []string
	if !t.Run("root", func(rootTest *testing.T) {
		rootRan = true
		parentTest := r.f.t
		r.f.t = rootTest
		defer func() { r.f.t = parentTest }()
		bindings := map[string]any{}
		for _, role := range nativeRoles {
			node := capture.Before.Nodes[role]
			commit := r.write("seed-"+role, "bead.create", map[string]any{
				"type": "issue", "fields": map[string]any{"title": node.Title, "priority": node.Priority},
			})
			actual := commit.Results[0].Ref
			if actual.ID == "" || actual.ID == node.ID || !actual.Branch.IsZero() {
				rootTest.Fatal("replay must bind actual returned refs separately from source IDs")
			}
			r.refs[role] = actual
			bindings[role] = map[string]any{"sourceID": node.ID, "actualPlugRef": actual}
		}
		nativeJSON(rootTest, filepath.Join(output, "source-to-plug-refs.json"), bindings)
		_, observedAfter = replayNativeRoot(rootTest, r, capture, backend, decision)
		coverage.Root = true
	}) || !rootRan {
		t.Fatal("native replay root was filtered or failed; H03/H04 requires a named root for every backend")
	}
	decision = decisionForNativeVector(capture.Vector, decision)
	t.Logf("B01_NATIVE_REPLAY vector=%s run=%s engine=%s referenceAfter=%s plugAfter=free decision=%s", capture.Vector.ID, capture.Run.ID, backend, strings.Join(observedAfter, ","), decision)
	r.replayHierarchyContinuations(capture, coverage)
	r.replayMembershipContinuations(capture, coverage)
	r.replayParentCompatibility(capture)
	coverage.require(t, capture.Vector.ID, capture.Run.ID)
	nativeJSON(t, filepath.Join(output, "h03-h04-coverage.json"), map[string]any{
		"manifest": manifest, "backend": backend,
		"vector": capture.Vector.ID, "run": capture.Run.ID,
		"root": coverage.Root, "hierarchy": coverage.Hierarchy, "membership": coverage.Membership,
	})
}

func decisionForNativeVector(vector nativeVector, decision string) string {
	if vector.Mode == "timed" {
		return "273e90f1-19bf-4ddb-84a3-1a49a957a547"
	}
	return decision
}

func replayNativeRoot(t *testing.T, r *nativeReplay, capture nativeCapture, backend, decision string) ([]string, []string) {
	t.Helper()
	r.write("link-child", "bead.link", map[string]any{"from": r.refs["child"], "to": r.refs["parent"], "kind": "parent-child"})
	r.write("link-grandchild", "bead.link", map[string]any{"from": r.refs["grandchild"], "to": r.refs["child"], "kind": "parent-child"})
	r.observe("before", false)
	if backend != "memory" {
		first := r.call("before-reopen-first", "bead.ready", map[string]any{"limit": 1}, false)
		cursor, _ := first["nextCursor"].(string)
		if cursor == "" {
			t.Fatal("four-row control did not produce an actual signed cursor")
		}
		r.reopen("before")
		if got := r.drain("across-actual-reopen", first); !reflect.DeepEqual(got, r.wantIDs(nativeRoles)) {
			t.Fatalf("signed cursor after actual SQL reopen=%v", got)
		}
		r.observe("before-reopened", false)
	}
	unchanged := r.sourceImages()
	r.write("defer-parent", "bead.defer", map[string]any{"id": r.refs["parent"], "reason": "distinct " + capture.Vector.Mode + " hierarchy vector"})
	afterImages := r.sourceImages()
	for _, role := range []string{"child", "grandchild", "free"} {
		if afterImages[role] != unchanged[role] {
			t.Fatalf("defer changed descendant/free source bytes or revision for %s", role)
		}
	}
	r.observe("after", true)
	if backend != "memory" {
		r.reopen("after")
		r.observe("after-reopened", true)
	}
	observedBefore, err := nativeRolesForIDs(capture.Vector, capture.Before.Ready)
	if err != nil {
		t.Fatal(err)
	}
	observedAfter, err := nativeRolesForIDs(capture.Vector, capture.After.Ready)
	if err != nil {
		t.Fatal(err)
	}
	var expectedNativeAfter []string
	classification := ""
	switch capture.Vector.Mode {
	case "timed":
		expectedNativeAfter = []string{"grandchild", "free"}
		decision = "273e90f1-19bf-4ddb-84a3-1a49a957a547"
		classification = "adopted timed transitive-ancestor reference membership difference"
	case "indefinite":
		expectedNativeAfter = []string{"child", "grandchild", "free"}
		classification = "adopted indefinite reference membership deviation"
	default:
		t.Fatalf("unknown actual native vector mode %q", capture.Vector.Mode)
	}
	if decision == "" {
		t.Fatal("native comparison is missing its recorded disposition")
	}
	nativeJSON(t, filepath.Join(r.output, "comparison.json"), map[string]any{
		"actualNativeBeforeRoles": observedBefore, "actualNativeAfterRoles": observedAfter,
		"assertedPlugBeforeRoles": nativeRoles, "assertedPlugAfterRoles": []string{"free"},
		"classification": classification, "decision": decision, "nativeAfter": capture.After,
		"membershipParity": reflect.DeepEqual(observedAfter, []string{"free"}), "expectedNativeAfterRoles": expectedNativeAfter,
		"clockScope": "Plug fixture clock controls Plug only; native Go/SQL clocks remain uncontrolled",
		"publicPath": "inprocess RPC registry -> core service -> real Writer and selected driver",
	})
	if !reflect.DeepEqual(observedBefore, nativeRoles) {
		t.Fatalf("actual open-parent reference control differs: %v", observedBefore)
	}
	if !reflect.DeepEqual(observedAfter, expectedNativeAfter) {
		t.Fatalf("actual %s reference differs from its declared membership: %v want=%v; originals retained", capture.Vector.Mode, observedAfter, expectedNativeAfter)
	}
	return observedBefore, observedAfter
}
