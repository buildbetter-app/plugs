package postsql_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/pluginapi"
)

func assertNoReceipt(t *testing.T, f *fixture, m model.WriteMeta) {
	t.Helper()
	r := f.reader(t)
	defer r.Close()
	principal, _ := driver.PrincipalKey(writerPrincipal)
	entry, err := r.IdempotencyByKey(f.ctx, principal, "", m.Method, m.IdempotencyKey)
	if err != nil || entry != nil {
		t.Fatalf("refused write published a response: entry=%+v err=%v", entry, err)
	}
}

func TestGraphInvariantAndEventLogRefusalsAreAtomicOnSQL(t *testing.T) {
	engines(t, func(t *testing.T, e engine) {
		f := newFixture(t, e)
		var posts atomic.Int32
		host := nativeHost(t, post{"atomic", func(context.Context, pluginapi.CommittedChange, pluginapi.SnapshotReader) error {
			posts.Add(1)
			return nil
		}})
		w := f.writer(host, nil)
		created := apply(t, f, w, create("left", "right"), meta(601))
		a, b := created.Commit.Results[0].Ref, created.Commit.Results[1].Ref
		link := func(from, to model.Ref) model.Op {
			return model.Op{Kind: model.OpLink, Link: &model.LinkOp{From: from, To: to, LinkKind: "blocks"}}
		}
		apply(t, f, w, model.ChangeSet{Ops: []model.Op{link(a, b)}}, meta(602))
		before := f.graph(t, a, b)
		failed := patch(a, "must-rollback")
		failed.Ops = append(failed.Ops, link(b, a))
		if _, err := w.Apply(f.ctx, writerPrincipal, failed, meta(603)); code(err) != "ontology_guard_failed" {
			t.Fatalf("blocking cycle did not reject the complete batch: %v", err)
		}
		same(t, before, f.graph(t, a, b))
		assertNoReceipt(t, f, meta(603))
		if posts.Load() != 2 {
			t.Fatal("invariant refusal ran a post subscriber")
		}

		// Refuse at the actual changeset INSERT, after graph versions have
		// been staged in this transaction. Both drivers must roll everything
		// back, including the head, predicates, response and event plan.
		install := `CREATE TRIGGER i4_reject_log BEFORE INSERT ON changesets BEGIN
SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM versions WHERE rev=NEW.rev) THEN RAISE(ABORT,'i4_version_not_staged') END;
SELECT RAISE(ABORT,'i4_reject_log'); END;`
		drop := "DROP TRIGGER i4_reject_log;"
		if e.name == "postgres" {
			install = `CREATE FUNCTION plug_postgres.i4_reject_log() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM plug_postgres.versions WHERE revision=NEW.revision) THEN RAISE EXCEPTION 'i4_version_not_staged'; END IF;
RAISE EXCEPTION 'i4_reject_log'; RETURN NEW; END $$;
CREATE TRIGGER i4_reject_log BEFORE INSERT ON plug_postgres.changesets FOR EACH ROW EXECUTE FUNCTION plug_postgres.i4_reject_log();`
			drop = "DROP TRIGGER i4_reject_log ON plug_postgres.changesets; DROP FUNCTION plug_postgres.i4_reject_log();"
		}
		e.sql(t, install)
		_, err := w.Apply(f.ctx, writerPrincipal, patch(a, "log-must-commit-with-write"), meta(604))
		if err == nil || !strings.Contains(err.Error(), "i4_reject_log") {
			t.Fatalf("did not reach late SQL publication refusal: %v", err)
		}
		same(t, before, f.graph(t, a, b))
		assertNoReceipt(t, f, meta(604))
		if posts.Load() != 2 {
			t.Fatal("rolled-back event log ran a post subscriber")
		}
		e.sql(t, drop)
		f.reopen(t)
		same(t, before, f.graph(t, a, b))
		assertNoReceipt(t, f, meta(603))
		assertNoReceipt(t, f, meta(604))
		retried := apply(t, f, f.writer(host, nil), patch(a, "log-must-commit-with-write"), meta(604))
		entry := f.receipt(t, meta(604))
		if retried.Replayed || len(entry.Changeset.Events) != 1 || len(entry.Changeset.Changes) != 1 || posts.Load() != 3 {
			t.Fatalf("repaired publication did not commit one write/event/post: out=%+v entry=%+v posts=%d", retried, entry, posts.Load())
		}
		same(t, entry.Commit, retried.Commit)
		t.Log("real SQL invariant and late-log refusal leave no partial graph/version/log/head/receipt; reopen preserves rollback and same-key retry commits one event with the write")
	})
}
