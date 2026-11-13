package postgres

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// These observations cover materialized current and reconstructed historical
// bodies, including discarded current rows. SQL executor work is unmeasured.
type readerPayloadWork struct {
	gets, payloads, bytes atomic.Int64
}

func (w *readerPayloadWork) ObserveReadWork(kind driver.ReadWorkKind, amount int64) {
	switch kind {
	case driver.ReadGetCalls:
		w.gets.Add(amount)
	case driver.ReadPayloadRecords:
		w.payloads.Add(amount)
	case driver.ReadPayloadBytes:
		w.bytes.Add(amount)
	}
}

type readerQueryCaptureKey struct{}
type readerQueryEventKey struct{}

type readerQueryEvent struct {
	sql       string
	args      []any
	completed bool
	err       error
}

type readerQueryCapture struct {
	mu     sync.Mutex
	events []*readerQueryEvent
}

type readerQueryTracer struct{}

func (readerQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	capture, _ := ctx.Value(readerQueryCaptureKey{}).(*readerQueryCapture)
	if capture == nil {
		return ctx
	}
	sql := strings.Join(strings.Fields(data.SQL), " ")
	if (!strings.HasPrefix(sql, "SELECT ") && !strings.HasPrefix(sql, "WITH ")) ||
		(!strings.Contains(sql, "FROM plug_postgres.records") && !strings.Contains(sql, "FROM plug_postgres.versions")) {
		return ctx
	}
	event := &readerQueryEvent{sql: sql, args: append([]any(nil), data.Args...)}
	capture.mu.Lock()
	capture.events = append(capture.events, event)
	capture.mu.Unlock()
	return context.WithValue(ctx, readerQueryEventKey{}, event)
}

func (readerQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	event, _ := ctx.Value(readerQueryEventKey{}).(*readerQueryEvent)
	if event == nil {
		return
	}
	capture, _ := ctx.Value(readerQueryCaptureKey{}).(*readerQueryCapture)
	if capture == nil {
		return
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	event.err = data.Err
	event.completed = true
}

func openReaderQueryTest(t *testing.T) (*Store, driver.ActiveLease) {
	t.Helper()
	config, err := pgx.ParseConfig(freshDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	config.Tracer = readerQueryTracer{}
	dsn := stdlib.RegisterConnConfig(config)
	t.Cleanup(func() { stdlib.UnregisterConnConfig(dsn) })
	s := openTest(t, dsn)
	return s, testLease(t, s)
}

// This is a public-reader equivalence control: each scalar read establishes
// the expected historical image, while the traced batch read must return the
// same image and preserve the caller's order and duplicates.
func TestBatchedHistoricalRecordReadsPreserveScalarImagesAndRoundTrips(t *testing.T) {
	ctx := context.Background()
	s, lease := openReaderQueryTest(t)
	const count = 64
	refs := make([]driver.Ref, count)
	keys := make([]driver.LockKey, count)
	for i := range refs {
		refs[i] = testRef(fmt.Sprintf("versions-batch-%03d", i))
		keys[i] = testLock(refs[i])
	}
	firstTx, firstHeader := beginTest(t, s, lease, keys...)
	for _, ref := range refs {
		if err := firstTx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"revision":1}`)}); err != nil {
			t.Fatal(err)
		}
		firstHeader.Changes = append(firstHeader.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate})
	}
	first, err := firstTx.Commit(ctx, firstHeader)
	if err != nil {
		t.Fatal(err)
	}

	secondTx, secondHeader := beginTest(t, s, lease, keys...)
	for _, ref := range refs {
		if err := secondTx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"revision":2}`)}); err != nil {
			t.Fatal(err)
		}
		secondHeader.Changes = append(secondHeader.Changes, model.ChangeRecord{Ref: ref, Op: model.OpPatch, FromRevision: first.Revision})
	}
	second, err := secondTx.Commit(ctx, secondHeader)
	if err != nil {
		t.Fatal(err)
	}
	thirdTx, thirdHeader := beginTest(t, s, lease, keys...)
	for _, ref := range refs {
		if err := thirdTx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"revision":3}`)}); err != nil {
			t.Fatal(err)
		}
		thirdHeader.Changes = append(thirdHeader.Changes, model.ChangeRecord{Ref: ref, Op: model.OpPatch, FromRevision: second.Revision})
	}
	if _, err := thirdTx.Commit(ctx, thirdHeader); err != nil {
		t.Fatal(err)
	}

	// Call the scalar recordAt implementation directly to establish the
	// independent reference image. Calling scalar.Get would exercise the new
	// recordsAt path and would not be an independent equivalence oracle.
	scalar := testRead(t, s, &second.Revision)
	scalarReader, ok := scalar.(*reader)
	if !ok {
		t.Fatalf("scalar reader type=%T", scalar)
	}
	var scalarWork readerPayloadWork
	scalarCtx := driver.WithReadWorkObserver(ctx, &scalarWork)
	want := make(map[string]driver.Record, len(refs))
	for _, ref := range refs {
		key, err := recordKey(ref)
		if err != nil {
			t.Fatal(err)
		}
		value, exists, err := scalarReader.recordAt(scalarCtx, key)
		if err != nil || !exists || value.deleted {
			t.Fatalf("scalar historical read %s: exists=%v deleted=%v err=%v", ref.ID, exists, value.deleted, err)
		}
		want[ref.ID] = value.record
	}
	if err := scalar.Close(); err != nil {
		t.Fatal(err)
	}

	request := make([]driver.Ref, 0, count+count/7+1)
	for i := len(refs) - 1; i >= 0; i-- {
		request = append(request, refs[i])
		if i%7 == 0 {
			request = append(request, refs[i])
		}
	}
	batched := testRead(t, s, &second.Revision)
	capture := &readerQueryCapture{}
	var batchWork readerPayloadWork
	readCtx := context.WithValue(driver.WithReadWorkObserver(ctx, &batchWork), readerQueryCaptureKey{}, capture)
	got, err := batched.Get(readCtx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(request) {
		t.Fatalf("batched rows=%d want=%d", len(got), len(request))
	}
	for i, row := range got {
		wantRow := want[request[i].ID]
		if !reflect.DeepEqual(row, wantRow) {
			t.Fatalf("batched row %d=%+v want scalar=%+v", i, row, wantRow)
		}
	}
	// The batch path replaces scalar recordAt reconstruction, but must retain
	// its live-body observations once per distinct key, including duplicates
	// in the caller's output. Nil instrumentation must preserve the image.
	plain, err := batched.Get(driver.WithReadWorkObserver(ctx, nil), request)
	if err != nil || !reflect.DeepEqual(plain, got) {
		t.Fatalf("nil observer changed historical batch: rows=%d err=%v", len(plain), err)
	}
	wantPayloads := int64(2 * count) // one discarded current and one historical body per key
	wantBytes := int64(count * (len(`{"revision":3}`) + len(`{"revision":2}`)))
	if scalarWork.payloads.Load() != wantPayloads || scalarWork.bytes.Load() != wantBytes ||
		batchWork.gets.Load() != 1 || batchWork.payloads.Load() != wantPayloads || batchWork.bytes.Load() != wantBytes {
		t.Fatalf("historical payload observations: scalar=%d/%d batch_gets=%d batch=%d/%d want=%d/%d",
			scalarWork.payloads.Load(), scalarWork.bytes.Load(), batchWork.gets.Load(), batchWork.payloads.Load(), batchWork.bytes.Load(), wantPayloads, wantBytes)
	}
	current := testRead(t, s, nil)
	var currentWork readerPayloadWork
	currentRows, currentErr := current.Get(driver.WithReadWorkObserver(ctx, &currentWork), request)
	if currentErr != nil || len(currentRows) != len(request) || currentWork.gets.Load() != 1 ||
		currentWork.payloads.Load() != count || currentWork.bytes.Load() != int64(count*len(`{"revision":3}`)) {
		t.Fatalf("current payload observations: rows=%d gets=%d payloads=%d bytes=%d err=%v",
			len(currentRows), currentWork.gets.Load(), currentWork.payloads.Load(), currentWork.bytes.Load(), currentErr)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("materialized-body observations: current=%d historical_work=%d/%d nil_observer_parity=true discarded_current_accounted=true sql_executor_work=UNMEASURED",
		currentWork.payloads.Load(), batchWork.payloads.Load(), batchWork.bytes.Load())
	if err := batched.Close(); err != nil {
		t.Fatal(err)
	}

	capture.mu.Lock()
	events := append([]*readerQueryEvent(nil), capture.events...)
	capture.mu.Unlock()
	versions, records := 0, 0
	for _, event := range events {
		if !event.completed || event.err != nil {
			t.Fatalf("historical query did not complete: %+v", event)
		}
		switch {
		case strings.Contains(event.sql, "FROM plug_postgres.versions"):
			versions++
			if !strings.Contains(event.sql, "unnest($1::bytea[])") || strings.Contains(event.sql, "WHERE key=$1") {
				t.Fatalf("historical read used a scalar versions query: %s", event.sql)
			}
		case strings.Contains(event.sql, "FROM plug_postgres.records"):
			records++
		}
	}
	if len(events) != 2 || records != 1 || versions != 1 {
		t.Fatalf("historical PG round trips=%d records=%d versions=%d; want 2=1+1", len(events), records, versions)
	}
	t.Logf("historical scalar-equivalence PASS: rows=%d record_selects=%d versions_selects=%d round_trips=%d", len(got), records, versions, len(events))
}

func TestBatchedHistoricalRecordReadsHonorDecodedRevisionBound(t *testing.T) {
	ctx := context.Background()
	s, lease := openReaderQueryTest(t)
	ref := testRef("versions-batch-revision-bound")
	key := testLock(ref)
	firstTx, firstHeader := beginTest(t, s, lease, key)
	if err := firstTx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"revision":1}`)}); err != nil {
		t.Fatal(err)
	}
	firstHeader.Changes = append(firstHeader.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate})
	first, err := firstTx.Commit(ctx, firstHeader)
	if err != nil {
		t.Fatal(err)
	}
	secondTx, secondHeader := beginTest(t, s, lease, key)
	if err := secondTx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"revision":2}`)}); err != nil {
		t.Fatal(err)
	}
	secondHeader.Changes = append(secondHeader.Changes, model.ChangeRecord{Ref: ref, Op: model.OpPatch, FromRevision: first.Revision})
	if _, err := secondTx.Commit(ctx, secondHeader); err != nil {
		t.Fatal(err)
	}
	keyID, err := recordKey(ref)
	if err != nil {
		t.Fatal(err)
	}
	malformed, err := encode(driver.Version{Ref: ref, Revision: model.Natural64(0), Base: true, Body: []byte(`{"revision":1}`), Principal: model.PrincipalRef{Kind: "system", ID: "postgres-test"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.versions SET value=$1 WHERE key=$2 AND revision=$3`, malformed, []byte(keyID), first.Revision.String()); err != nil {
		t.Fatal(err)
	}

	scalar := testRead(t, s, &first.Revision)
	scalarReader, ok := scalar.(*reader)
	if !ok {
		t.Fatalf("scalar reader type=%T", scalar)
	}
	_, _, scalarErr := scalarReader.recordAt(ctx, keyID)
	if err := scalar.Close(); err != nil {
		t.Fatal(err)
	}
	if scalarErr == nil || !strings.Contains(scalarErr.Error(), "historical version has no base") {
		t.Fatalf("scalar malformed bound error=%v; want no-base", scalarErr)
	}

	batched := testRead(t, s, &first.Revision)
	_, batchErr := batched.Get(ctx, []driver.Ref{ref})
	if err := batched.Close(); err != nil {
		t.Fatal(err)
	}
	if batchErr == nil || batchErr.Error() != scalarErr.Error() {
		t.Fatalf("batched malformed bound error=%v scalar=%v", batchErr, scalarErr)
	}
	t.Logf("decoded revision-bound parity PASS: error=%q", batchErr)
}

func TestScalarHistoricalReadWorkCountsEveryRepeatedFetch(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	first := seedTest(t, s, lease, "repeat-history", `{"value":1}`)
	seedTest(t, s, lease, "repeat-history", `{"value":222}`)
	r := testRead(t, s, &first.Revision).(*reader)
	key, err := recordKey(testRef("repeat-history"))
	if err != nil {
		t.Fatal(err)
	}
	plain, present, err := r.recordAt(ctx, key)
	if err != nil || !present || string(plain.record.Body) != `{"value":1}` {
		t.Fatalf("scalar historical control: %+v %v", plain, err)
	}
	var work readerPayloadWork
	for repeat := int64(1); repeat <= 2; repeat++ {
		got, present, err := r.recordAt(driver.WithReadWorkObserver(ctx, &work), key)
		if err != nil || !present || !reflect.DeepEqual(plain, got) {
			t.Fatalf("scalar historical observer changed result: %+v %v", got, err)
		}
		wantBytes := repeat * int64(len(`{"value":222}`)+len(`{"value":1}`))
		if work.payloads.Load() != repeat*2 || work.bytes.Load() != wantBytes {
			t.Fatalf("scalar repeated fetch omitted real bodies: repeat=%d payloads=%d bytes=%d want=%d/%d", repeat, work.payloads.Load(), work.bytes.Load(), repeat*2, wantBytes)
		}
		t.Logf("POSTGRES_SCALAR_HISTORICAL_WORK repeat=%d current_bodies=%d historical_bodies=%d payloads=%d bytes=%d", repeat, repeat, repeat, work.payloads.Load(), work.bytes.Load())
	}
}
