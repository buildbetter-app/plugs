package postgres

import (
	"context"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// The retained scalar reader uses the decoded Version.Revision as the replay
// bound, even when that value is ahead of the SQL revision that selected the
// latest row. Keep this malformed-store behavior equivalent while collapsing
// the historical fallback to one query.
func TestBatchedHistoricalRecordReadsPreserveDecodedRevisionAheadOfSQLLatest(t *testing.T) {
	ctx := context.Background()
	s, lease := openReaderQueryTest(t)
	ref := testRef("versions-batch-decoded-revision-ahead")
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
	second, err := secondTx.Commit(ctx, secondHeader)
	if err != nil {
		t.Fatal(err)
	}

	keyID, err := recordKey(ref)
	if err != nil {
		t.Fatal(err)
	}
	// Select the SQL revision-1 row at the first snapshot, but make its
	// decoded revision point at the later SQL revision-2 row. This is the
	// malformed witness that distinguishes recordAt's replay bound from the
	// SQL selector bound.
	ahead, err := encode(driver.Version{
		Ref:       ref,
		Revision:  second.Revision,
		Base:      true,
		Body:      []byte(`{"revision":1}`),
		Principal: model.PrincipalRef{Kind: "system", ID: "postgres-test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.versions SET value=$1 WHERE key=$2 AND revision=$3`, ahead, []byte(keyID), first.Revision.String()); err != nil {
		t.Fatal(err)
	}

	scalar := testRead(t, s, &first.Revision)
	scalarReader, ok := scalar.(*reader)
	if !ok {
		t.Fatalf("scalar reader type=%T", scalar)
	}
	want, exists, err := scalarReader.recordAt(ctx, keyID)
	if err != nil || !exists || want.deleted {
		t.Fatalf("scalar decoded-ahead read: exists=%v deleted=%v err=%v", exists, want.deleted, err)
	}
	if want.record.Revision != second.Revision {
		t.Fatalf("scalar revision=%s want decoded future revision=%s", want.record.Revision, second.Revision)
	}
	if string(want.record.Body) != `{"revision":2}` {
		t.Fatalf("scalar body=%s want replayed future body", want.record.Body)
	}
	if err := scalar.Close(); err != nil {
		t.Fatal(err)
	}

	batched := testRead(t, s, &first.Revision)
	got, err := batched.Get(ctx, []driver.Ref{ref})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], want.record) {
		t.Fatalf("batched decoded-ahead image=%+v want scalar=%+v", got, want.record)
	}
	if err := batched.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("decoded-ahead scalar parity PASS: revision=%s body=%s", got[0].Revision, got[0].Body)
}

// The records query has no ordering contract. The retained scalar reader
// reports the first requested key's replay/decode error, so the batched miss
// list must be rebuilt in caller order rather than inheriting physical row
// order from PostgreSQL.
func TestBatchedHistoricalRecordReadsPreserveCallerErrorOrder(t *testing.T) {
	ctx := context.Background()
	s, lease := openReaderQueryTest(t)
	refA, refB := testRef("versions-batch-error-order-a"), testRef("versions-batch-error-order-b")
	keyA, err := recordKey(refA)
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := recordKey(refB)
	if err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{testLock(refA), testLock(refB)}
	firstTx, firstHeader := beginTest(t, s, lease, keys...)
	for _, ref := range []driver.Ref{refA, refB} {
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
	for _, ref := range []driver.Ref{refA, refB} {
		if err := secondTx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"revision":2}`)}); err != nil {
			t.Fatal(err)
		}
		secondHeader.Changes = append(secondHeader.Changes, model.ChangeRecord{Ref: ref, Op: model.OpPatch, FromRevision: first.Revision})
	}
	if _, err := secondTx.Commit(ctx, secondHeader); err != nil {
		t.Fatal(err)
	}

	// Key A fails while decoding the selected latest version. Key B has a
	// valid JSON version whose decoded revision is zero, producing the scalar
	// no-base error. Request B first so the error identity is observable.
	badJSON := []byte(`{"malformed"`)
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.versions SET value=$1 WHERE key=$2 AND revision=$3`, badJSON, []byte(keyA), first.Revision.String()); err != nil {
		t.Fatal(err)
	}
	bounds, err := encode(driver.Version{
		Ref:       refB,
		Revision:  model.Natural64(0),
		Base:      true,
		Body:      []byte(`{"revision":1}`),
		Principal: model.PrincipalRef{Kind: "system", ID: "postgres-test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.versions SET value=$1 WHERE key=$2 AND revision=$3`, bounds, []byte(keyB), first.Revision.String()); err != nil {
		t.Fatal(err)
	}

	scalar := testRead(t, s, &first.Revision).(*reader)
	_, _, bErr := scalar.recordAt(ctx, keyB)
	_, _, aErr := scalar.recordAt(ctx, keyA)
	if bErr == nil || aErr == nil {
		t.Fatalf("scalar controls did not produce independent errors: B=%v A=%v", bErr, aErr)
	}
	if bErr.Error() == aErr.Error() {
		t.Fatalf("scalar controls need distinct errors: B=%v A=%v", bErr, aErr)
	}
	if err := scalar.Close(); err != nil {
		t.Fatal(err)
	}

	batched := testRead(t, s, &first.Revision)
	_, batchErr := batched.Get(ctx, []driver.Ref{refB, refA})
	if err := batched.Close(); err != nil {
		t.Fatal(err)
	}
	if batchErr == nil || batchErr.Error() != bErr.Error() {
		t.Fatalf("batched error=%v; want first caller error=%v (second=%v)", batchErr, bErr, aErr)
	}
	t.Logf("caller error-order parity PASS: first=%q second=%q", batchErr, aErr)
}

// Eligible current rows retain the scalar counter/decode error precedence, and
// newer rows stay on the historical path instead of being parsed as current.
func TestBatchedCurrentRecordReadsPreserveScalarErrorPrecedence(t *testing.T) {
	ctx := context.Background()
	s, lease := openReaderQueryTest(t)
	ref := testRef("versions-batch-current-error-precedence")
	key, err := recordKey(ref)
	if err != nil {
		t.Fatal(err)
	}
	tx, header := beginTest(t, s, lease, testLock(ref))
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "test", Body: []byte(`{"revision":1}`)}); err != nil {
		t.Fatal(err)
	}
	header.Changes = append(header.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate})
	if _, err := tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE plug_postgres.records SET revision=$1,ref=$2 WHERE key=$3`, "-1", []byte(`{"malformed"`), []byte(key)); err != nil {
		t.Fatal(err)
	}

	scalar := testRead(t, s, nil).(*reader)
	_, _, scalarErr := scalar.recordAt(ctx, key)
	if scalarErr == nil {
		t.Fatal("scalar malformed current row unexpectedly succeeded")
	}
	if err := scalar.Close(); err != nil {
		t.Fatal(err)
	}

	batched := testRead(t, s, nil)
	_, batchErr := batched.Get(ctx, []driver.Ref{ref})
	if err := batched.Close(); err != nil {
		t.Fatal(err)
	}
	if batchErr == nil || batchErr.Error() != scalarErr.Error() {
		t.Fatalf("batched current error=%v; want scalar error=%v", batchErr, scalarErr)
	}
	t.Logf("current decode/counter precedence PASS: error=%q", batchErr)
}
