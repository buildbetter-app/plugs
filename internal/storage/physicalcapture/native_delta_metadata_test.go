package physicalcapture_test

import (
	"reflect"
	"testing"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
)

func TestNativeDeltaPhysicalStoredKeyAndParentOrder(t *testing.T) {
	ctx, conn := workingFixture(t)
	workingExec(t, ctx, conn, "CREATE TABLE ordered_keys (a BIGINT, b BIGINT, payload TEXT, PRIMARY KEY (b,a))")
	workingExec(t, ctx, conn, "INSERT INTO ordered_keys VALUES (1,2,'common')")
	workingCommit(t, ctx, conn)
	workingExec(t, ctx, conn, "CALL DOLT_BRANCH('public_delta_side')")
	workingExec(t, ctx, conn, "INSERT INTO ordered_keys VALUES (3,4,'first parent')")
	first := workingCommit(t, ctx, conn)
	workingExec(t, ctx, conn, "CALL DOLT_CHECKOUT('public_delta_side')")
	workingExec(t, ctx, conn, "INSERT INTO ordered_keys VALUES (5,6,'second parent')")
	second := workingCommit(t, ctx, conn)
	workingExec(t, ctx, conn, "CALL DOLT_CHECKOUT('main')")
	workingExec(t, ctx, conn, "CALL DOLT_MERGE('public_delta_side','--no-ff','-m','public native delta merge')")
	head, err := versioncontrolops.CurrentCommit(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	before := workingRead(t, ctx, conn, head)
	immutable, err := physicalcapture.ReadCommit(ctx, conn, head)
	if err != nil {
		t.Fatal(err)
	}
	for _, image := range []physicalcapture.WorkingImage{before, {Tables: immutable.Tables}} {
		table := workingTable(t, image, "ordered_keys")
		if len(table.Columns) != 3 || table.Columns[0].Name != "a" || table.Columns[1].Name != "b" || table.Columns[0].PrimaryKeyOrdinal == nil || *table.Columns[0].PrimaryKeyOrdinal != 1 || table.Columns[1].PrimaryKeyOrdinal == nil || *table.Columns[1].PrimaryKeyOrdinal != 0 || table.Columns[2].PrimaryKeyOrdinal != nil {
			t.Fatal("physical PK order was replaced with display-column order")
		}
	}
	history, err := physicalcapture.ReadHistory(ctx, conn, head)
	if err != nil || !history.ReadOnlyScope {
		t.Fatalf("history observation failed: %v", err)
	}
	found := false
	for _, commit := range history.Commits {
		if commit.ID == head {
			found = true
			if !reflect.DeepEqual(commit.OrderedParents, []string{first, second}) {
				t.Fatal("merge first-parent/full stored order changed")
			}
		}
	}
	if !found || workingRead(t, ctx, conn, head).WorkingRootHash != before.WorkingRootHash {
		t.Fatal("detached metadata read lost HEAD or changed source")
	}
}
