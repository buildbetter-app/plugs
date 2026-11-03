package sourceactivation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// LegacyUpgrade simulates the old ontology-first publisher's absent markers,
// then uses only ordinary untyped writes to create genuinely unknown coverage.
// The separate OntologyFirst regression never calls the maintenance builder.
func LegacyUpgrade(t *testing.T, store driver.Store, lease driver.ActiveLease, oldPublisher func()) {
	t.Helper()
	ctx := context.Background()
	ref := func(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref("ontology/global"), Kind: "ontology", Op: model.OpCreate, Body: []byte(`{"version":1}`)}}); err != nil {
		t.Fatal(err)
	}
	oldPublisher()
	parent := ref("pl-upgrade-0000")
	changes := make([]conformance.FixtureChange, 300)
	for i := range changes {
		doc := pipeline.Document{ID: fmt.Sprintf("pl-upgrade-%04d", i), Type: "issue", Project: "local", Fields: map[string]any{"status": "todo"}}
		if i > 0 {
			doc.Parent = &parent
			doc.Links = []pipeline.Link{{Kind: "blocks", To: parent}, {Kind: "custom", To: parent}}
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		body = append(append([]byte(" \n"), body...), []byte(" \t")...)
		changes[i] = conformance.FixtureChange{Ref: ref(doc.ID), Kind: "bead", Op: model.OpCreate, Body: body}
	}
	for first := 0; first < len(changes); first += 100 {
		if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, changes[first:first+100]); err != nil {
			t.Fatal(err)
		}
	}
	before, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	read := func(at *model.Revision) driver.Reader {
		r, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation, At: at})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	unsupported := func(r driver.Reader) {
		t.Helper()
		if rows, more, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("unknown parent coverage became absence: rows=%d more=%t err=%v", len(rows), more, err)
		}
		if rows, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("unknown link coverage became absence: rows=%d more=%t err=%v", len(rows), more, err)
		}
	}
	pinned := read(nil)
	unsupported(pinned)
	decode := func(record driver.Record) (driver.GraphSourceMetadata, error) {
		doc, err := pipeline.DecodeDocument(record.Body)
		if err != nil {
			return driver.GraphSourceMetadata{}, err
		}
		metadata := driver.GraphSourceMetadata{Parent: doc.Parent}
		for _, link := range doc.Links {
			metadata.Links = append(metadata.Links, driver.SourceLink{Kind: link.Kind, To: link.To})
		}
		return metadata, nil
	}
	builder := store.(driver.GraphSourceIndexBuilder)
	calls := 0
	injected := errors.New("injected after first bounded source page")
	if err := builder.BuildGraphSourceIndexes(ctx, lease, func(record driver.Record) (driver.GraphSourceMetadata, error) {
		calls++
		if calls == 129 {
			return driver.GraphSourceMetadata{}, injected
		}
		return decode(record)
	}); !errors.Is(err, injected) || calls != 129 {
		t.Fatalf("partial build did not fail atomically: calls=%d err=%v", calls, err)
	}
	unsupported(read(nil))
	cancelCtx, cancel := context.WithCancel(ctx)
	calls = 0
	err = builder.BuildGraphSourceIndexes(cancelCtx, lease, func(record driver.Record) (driver.GraphSourceMetadata, error) {
		calls++
		if calls == 129 {
			cancel()
		}
		return decode(record)
	})
	cancel()
	if !errors.Is(err, context.Canceled) || calls < 129 {
		t.Fatalf("canceled partial build did not roll back: calls=%d err=%v", calls, err)
	}
	unsupported(read(nil))
	if err := pipeline.BuildSourceIndexes(ctx, store, lease); err != nil {
		t.Fatal(err)
	}
	after, err := store.Meta(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("maintenance changed public metadata: before=%+v after=%+v err=%v", before, after, err)
	}
	unsupported(pinned)
	one := model.Natural64(1)
	unsupported(read(&one)) // Current-only proof must not invent old coverage.
	current := read(nil)
	for name, lookup := range map[string]func() ([]driver.Record, bool, error){
		"parent": func() ([]driver.Record, bool, error) {
			return current.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, []model.Ref{parent}, 1000)
		},
		"links": func() ([]driver.Record, bool, error) {
			return current.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, []model.Ref{parent}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 1000)
		},
	} {
		rows, more, err := lookup()
		if err != nil || more || len(rows) != 299 {
			t.Fatalf("%s backfill incomplete: rows=%d more=%t err=%v", name, len(rows), more, err)
		}
		for i, row := range rows {
			want := changes[i+1]
			if row.Ref != want.Ref || !bytes.Equal(row.Body, want.Body) || row.Revision.String() != fmt.Sprint(2+(i+1)/100) {
				t.Fatalf("%s backfill changed exact source image/revision at %d: %+v", name, i, row)
			}
		}
	}
	versions, err := current.Versions(ctx, changes[1].Ref, driver.PageReq{})
	if err != nil || len(versions.Items) != 1 {
		t.Fatalf("maintenance wrote public versions: %+v err=%v", versions, err)
	}
	// Ready coverage is a no-op, not an excuse to rescan every source on startup.
	if err := builder.BuildGraphSourceIndexes(ctx, lease, func(driver.Record) (driver.GraphSourceMetadata, error) {
		t.Error("ready startup decoded a source")
		return driver.GraphSourceMetadata{}, injected
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref("ontology/global"), Kind: "ontology", Op: model.OpPatch, Body: []byte(`{"version":2}`)}}); err != nil {
		t.Fatal(err)
	}
	// The backfill's exact older source revisions must remain available after a
	// source-free publication advances the ready range.
	historical := read(&before.Head)
	rows, more, err := historical.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, []model.Ref{parent}, 1)
	if err != nil || !more || len(rows) != 1 || rows[0].Revision.String() != "2" || !bytes.Equal(rows[0].Body, changes[1].Body) {
		t.Fatalf("post-upgrade history changed: rows=%+v more=%t err=%v", rows, more, err)
	}
	t.Log("300 legacy sources upgraded atomically; failure after image 128 published nothing; original revisions/bytes/history floor retained; earlier and pinned unknown coverage refused")
}

// RenewalBoundary pauses the core decoder after each full SQL page. Application
// time crosses the original ten-second expiry without shortening the real TTL or
// sleeping through it. Renew runs on a separate goroutine, just as at startup.
// A lost second renewal must roll back the build, never publish partial coverage.
func RenewalBoundary(t *testing.T, store driver.Store, lease driver.ActiveLease, advance func(time.Duration), lose bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parent := model.RefByID("pl-renew-0000", model.Natural64(0))
	changes := make([]conformance.FixtureChange, 300)
	for i := range changes {
		doc := pipeline.Document{ID: fmt.Sprintf("pl-renew-%04d", i), Type: "issue", Project: "local", Fields: map[string]any{"status": "todo"}}
		if i > 0 {
			doc.Parent = &parent
			doc.Links = []pipeline.Link{{Kind: "blocks", To: parent}}
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		// Deliberately untyped legacy writes: no fixture backfill or activation.
		changes[i] = conformance.FixtureChange{Ref: model.RefByID(doc.ID, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body}
	}
	for first := 0; first < len(changes); first += 100 {
		if _, err := conformance.Seed(ctx, store, lease, driver.ChangesetHeader{}, changes[first:first+100]); err != nil {
			t.Fatal(err)
		}
	}
	before, err := store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	probe := func() driver.Reader {
		r, err := store.Read(ctx, driver.View{Incarnation: before.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	unsupported := func(r driver.Reader) {
		t.Helper()
		if rows, more, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, []model.Ref{parent}, 1000); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("partial parent coverage escaped: rows=%d more=%t err=%v", len(rows), more, err)
		}
		if rows, more, err := r.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, []model.Ref{parent}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 1000); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("partial link coverage escaped: rows=%d more=%t err=%v", len(rows), more, err)
		}
	}
	pinned := probe()
	unsupported(pinned)
	paused := make(chan chan struct{})
	done := make(chan error, 1)
	go func() {
		calls := 0
		done <- store.(driver.GraphSourceIndexBuilder).BuildGraphSourceIndexes(ctx, lease, func(record driver.Record) (driver.GraphSourceMetadata, error) {
			calls++
			if calls == 129 || calls == 257 {
				resume := make(chan struct{})
				select {
				case paused <- resume:
				case <-ctx.Done():
					return driver.GraphSourceMetadata{}, ctx.Err()
				}
				select {
				case <-resume:
				case <-ctx.Done():
					return driver.GraphSourceMetadata{}, ctx.Err()
				}
			}
			doc, err := pipeline.DecodeDocument(record.Body)
			metadata := driver.GraphSourceMetadata{Parent: doc.Parent}
			for _, link := range doc.Links {
				metadata.Links = append(metadata.Links, driver.SourceLink{Kind: link.Kind, To: link.To})
			}
			return metadata, err
		})
	}()
	for step := 0; step < 2; step++ {
		var resume chan struct{}
		select {
		case resume = <-paused:
		case err := <-done:
			t.Fatalf("build stopped before renewal %d: %v", step+1, err)
		case <-time.After(5 * time.Second):
			t.Fatal("build never reached renewal boundary")
		}
		delta := driver.LeaseTTL - time.Second
		if lose && step == 1 {
			delta = driver.LeaseTTL // Exactly at the renewed expiry, not before it.
		}
		advance(delta)
		renewCtx, stop := context.WithTimeout(ctx, 2*time.Second)
		renewed := make(chan error, 1)
		go func() { renewed <- lease.Renew(renewCtx) }()
		select {
		case err = <-renewed:
		case <-renewCtx.Done():
			err = renewCtx.Err()
		}
		stop()
		var fenced driver.ErrFenced
		wantLoss := lose && step == 1
		if !wantLoss && err != nil || wantLoss && !errors.As(err, &fenced) {
			cancel()
			close(resume)
			<-done // Release locks before reporting the old implementation's deadlock.
			t.Fatalf("renewal %d could not progress during decoding: wantLoss=%t err=%v", step+1, wantLoss, err)
		}
		unsupported(probe()) // Readers opened between committed pages still fail closed.
		close(resume)
	}
	err = <-done
	var fenced driver.ErrFenced
	if !lose && err != nil || lose && !errors.As(err, &fenced) {
		t.Fatalf("build final lease fence: lose=%t err=%v", lose, err)
	}
	after, metaErr := store.Meta(ctx)
	if metaErr != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("backfill changed public metadata: before=%+v after=%+v err=%v", before, after, metaErr)
	}
	unsupported(pinned)
	current := probe()
	if lose {
		unsupported(current)
		if err := lease.Renew(ctx); !errors.As(err, &fenced) {
			t.Fatalf("failed renewal resurrected the lease: %v", err)
		}
		t.Log("renewal genuinely expired after one page-boundary heartbeat; partial build rolled back and both readers remained unsupported")
		return
	}
	for name, lookup := range map[string]func() ([]driver.Record, bool, error){
		"parent": func() ([]driver.Record, bool, error) {
			return current.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, []model.Ref{parent}, 1000)
		},
		"links": func() ([]driver.Record, bool, error) {
			return current.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, []model.Ref{parent}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 1000)
		},
	} {
		rows, more, err := lookup()
		if err != nil || more || len(rows) != 299 {
			t.Fatalf("renewed %s build incomplete: rows=%d more=%t err=%v", name, len(rows), more, err)
		}
		for i, row := range rows {
			if row.Ref != changes[i+1].Ref || !bytes.Equal(row.Body, changes[i+1].Body) || row.Revision.String() != fmt.Sprint(1+(i+1)/100) {
				t.Fatalf("renewed %s build changed exact source %d: %+v", name, i, row)
			}
		}
	}
	t.Log("two page-boundary renewals succeeded while decoding across 18 seconds of application time; all 300 exact sources activated, pinned unknown readers still refused")
}
