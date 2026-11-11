// Package b07positive contains disposable public-route acceptance support.
package b07positive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const Namespace = "plugin/workflow/default/durable"

func Hash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func Output(t *testing.T) string {
	t.Helper()
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if root == "" {
		root = t.TempDir()
	}
	root = filepath.Join(root, "b07positive", strings.ReplaceAll(t.Name(), "/", "-"))
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	return root
}

func Save(t *testing.T, root, name string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func Write(t *testing.T, root, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}

type Scope struct{ Method, Key string }

type Receipt struct {
	RequestHash string
	Commit      model.Commit
	Changeset   model.Changeset
	Pending     bool
}

type State struct {
	Meta        driver.Meta
	Rows        []driver.Record
	Versions    map[string][]driver.Version
	Changes     []model.Changeset
	Derived     map[driver.DerivedTable][]driver.DerivedRow
	Checkpoints map[string]driver.Checkpoint
	Receipts    map[string]Receipt
}

func Capture(t *testing.T, store driver.Store, by model.PrincipalRef, agent string, scopes map[string]Scope) State {
	t.Helper()
	ctx := context.Background()
	meta, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capability, ok := store.(driver.CheckpointInventoryStore)
	if !ok {
		t.Fatal("complete checkpoint inventory unavailable")
	}
	r, err := capability.ReadCheckpointInventory(ctx, driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	s := State{Meta: meta, Versions: map[string][]driver.Version{}, Derived: map[driver.DerivedTable][]driver.DerivedRow{}, Checkpoints: map[string]driver.Checkpoint{}, Receipts: map[string]Receipt{}}
	var cursor *driver.Cursor
	for {
		page, err := r.Query(ctx, driver.Query{Limit: 128, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		s.Rows = append(s.Rows, page.Items...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
			t.Fatal("record pagination stalled")
		}
		cursor = page.NextCursor
	}
	sort.Slice(s.Rows, func(i, j int) bool { return s.Rows[i].Ref.ID < s.Rows[j].Ref.ID })
	for _, row := range s.Rows {
		cursor = nil
		for {
			page, err := r.Versions(ctx, row.Ref, driver.PageReq{Limit: 128, Cursor: cursor})
			if err != nil {
				t.Fatal(err)
			}
			s.Versions[row.Ref.ID] = append(s.Versions[row.Ref.ID], page.Items...)
			if page.NextCursor == nil {
				break
			}
			if len(page.Items) == 0 || reflect.DeepEqual(cursor, page.NextCursor) {
				t.Fatal("version pagination stalled")
			}
			cursor = page.NextCursor
		}
	}
	cursor = nil
	if !meta.HistoryFloor.IsZero() {
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: meta.HistoryFloor}
	}
	for {
		sets, err := r.Changesets(ctx, cursor, 128)
		if err != nil {
			t.Fatal(err)
		}
		s.Changes = append(s.Changes, sets...)
		if len(sets) < 128 {
			break
		}
		cursor = &driver.Cursor{Incarnation: meta.Incarnation, Revision: sets[len(sets)-1].Revision, Index: model.Natural64(0)}
	}
	for _, table := range []driver.DerivedTable{driver.DerivedReadyState, driver.DerivedAncestorClosure, driver.DerivedCutClosure, driver.DerivedAttribute, driver.DerivedSortKey, driver.DerivedReferenceIndex, driver.DerivedGrant, driver.DerivedOntology, driver.DerivedPlugins} {
		rows, err := r.DerivedRows(ctx, table, "")
		if err != nil {
			t.Fatal(err)
		}
		s.Derived[table] = rows
	}
	principal, err := driver.PrincipalKey(by)
	if err != nil {
		t.Fatal(err)
	}
	if by.Kind != "anonymous" {
		agent = ""
	}
	for label, scope := range scopes {
		receipt, err := r.IdempotencyByKey(ctx, principal, agent, scope.Method, scope.Key)
		if err != nil {
			t.Fatal(err)
		}
		if receipt != nil {
			s.Receipts[label] = Receipt{receipt.RequestHash, receipt.Commit, receipt.Changeset, receipt.ResponsePending}
		}
	}
	keys, err := r.CheckpointKeys()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		checkpoint, err := r.SourcePlaneCheckpoint(key)
		if err != nil {
			t.Fatal(err)
		}
		s.Checkpoints[key] = checkpoint
	}
	return s
}

func Same(t *testing.T, want, got State) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatal("complete core/checkpoint/receipt image changed")
	}
}

func (s State) Workflow(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	var namespace struct {
		Values map[string]struct {
			Value json.RawMessage `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal(s.Checkpoints[Namespace].Payload, &namespace); err != nil {
		t.Fatal(err)
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(namespace.Values["workflow/state"].Value, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (s State) Row(t *testing.T, id string) driver.Record {
	t.Helper()
	for _, row := range s.Rows {
		if row.Ref.ID == id {
			return row
		}
	}
	t.Fatal(fmt.Sprintf("record %q absent", id))
	return driver.Record{}
}
