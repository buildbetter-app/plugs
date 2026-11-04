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
	s, lease := openCommitQueryTest(t)
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
