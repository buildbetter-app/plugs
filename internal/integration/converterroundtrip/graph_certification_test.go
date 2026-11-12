package converterroundtrip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type graphPreflightTx struct{ driver.WriteTx }

func (*graphPreflightTx) PutGraphRecord(context.Context, driver.Record, *model.Ref, []driver.SourceLink) error {
	panic("preflight must never stage records")
}

func TestImportGraphPreflightValidatesEveryExactDocument(t *testing.T) {
	record := func(id string, change func(*pipeline.Document)) driver.Record {
		doc := pipeline.Document{ID: id, Type: "task", Project: "p", Fields: map[string]any{"title": "isolated"}}
		if change != nil {
			change(&doc)
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return driver.Record{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Body: body}
	}
	tx := &graphPreflightTx{}
	for _, test := range []struct {
		name    string
		records []driver.Record
		valid   bool
	}{
		{"isolated", []driver.Record{record("p-a", nil)}, true},
		{"dangling-parent", []driver.Record{record("p-a", func(d *pipeline.Document) { r := model.RefByID("p-missing", model.Natural64(0)); d.Parent = &r })}, false},
		{"dangling-link", []driver.Record{record("p-a", func(d *pipeline.Document) {
			d.Links = []pipeline.Link{{Kind: "related", To: model.RefByID("p-missing", model.Natural64(0))}}
		})}, false},
		{"invalid-isolated-link-kind", []driver.Record{record("p-a", func(d *pipeline.Document) {
			d.Links = []pipeline.Link{{Kind: "", To: model.RefByID("p-a", model.Natural64(0))}}
		})}, false},
		{"complete-parent-and-link", []driver.Record{record("p-a", nil), record("p-b", func(d *pipeline.Document) {
			r := model.RefByID("p-a", model.Natural64(0))
			d.Parent = &r
			d.Links = []pipeline.Link{{Kind: "related", To: r}}
		})}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ok, err := prepareImportGraph(context.Background(), tx, test.records)
			if ok != test.valid || (err == nil) != test.valid {
				t.Fatal(ok, err)
			}
		})
	}
	bad := record("p-a", nil)
	bad.Ref.ID = "p-other"
	if _, err := prepareImportGraph(context.Background(), tx, []driver.Record{bad}); err == nil {
		t.Fatal("body/ref mismatch certified")
	}
	// Capability absence preserves the legacy path even for images that the new
	// semantic validator would refuse; no optional certification failure is added.
	if ok, err := prepareImportGraph(context.Background(), struct{ driver.WriteTx }{}, []driver.Record{bad}); ok || err != nil {
		t.Fatal("missing capability changed legacy import", ok, err)
	}
}

type graphLegacyStore struct{ driver.Store }

func (s graphLegacyStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return struct {
		driver.WriteTx
		driver.OfflineRestoreTx
	}{tx, tx.(driver.OfflineRestoreTx)}, nil
}

func assertImportGraphUncertified(t *testing.T, r driver.Reader) {
	t.Helper()
	ctx := context.Background()
	states, err := r.Observe(ctx, []driver.LockKey{pipeline.GraphValidityLockKey()})
	if err != nil || len(states) != 1 || states[0].Present {
		t.Fatal("uncertified import/abort retained semantic fact", states, err)
	}
	if reader, ok := r.(driver.HierarchyChildrenReader); ok {
		if _, _, err := reader.HierarchyChildren(ctx, nil, 0); !errors.Is(err, driver.ErrUnsupported) {
			t.Fatal("uncertified parent coverage", err)
		}
	}
	if reader, ok := r.(driver.InverseSourceLinksReader); ok {
		if _, _, err := reader.InverseSourceLinks(ctx, nil, nil, nil, 0); !errors.Is(err, driver.ErrUnsupported) {
			t.Fatal("uncertified link coverage", err)
		}
	}
}

func TestPortableImportCertifiesOrRetainsUnsupportedWriter(t *testing.T) {
	data, check := supportedBytes(t, nil), supportedVerification(t)
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, typed := range []bool{true, false} {
			name := engine + "/typed"
			if !typed {
				name = engine + "/legacy"
			}
			t.Run(name, func(t *testing.T) {
				store, lease, _ := openIdentityTarget(t, engine)
				target := store
				if !typed {
					target = graphLegacyStore{store}
				}
				report, err := RestoreWithLease(context.Background(), bytes.NewReader(data), target, lease, check)
				if err != nil {
					t.Fatal(err)
				}
				if report.GraphCertification == nil || (report.GraphCertification.State == "certified") != typed {
					t.Fatal("missing/incorrect capability report", report.GraphCertification)
				}
				r, err := store.Read(context.Background(), driver.View{Incarnation: report.Restored.Incarnation})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				if !typed {
					assertImportGraphUncertified(t, r)
					return
				}
				recovered := recoveredImportGraphReport(context.Background(), r, report.Restored.Head)
				if recovered == nil || recovered.State != "certified" || recovered.From != report.Restored.HistoryFloor {
					t.Fatal("snapshot certificate/floor/coverage missing", recovered, report.Restored)
				}
			})
		}
	}
}
