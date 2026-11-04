package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestReadyCommittedAuthorityWith94SaturatedWriters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	ref := testRef("ready-authority-source")
	body := []byte(`{"id":"ready-authority-source","type":"issue","project":"local","fields":{"title":"committed","status":"todo"}}`)
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body}}); err != nil {
		t.Fatal(err)
	}
	m, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(m.StoreID), Incarnation: m.Incarnation, InstallationID: "fixed", Authority: testRef("authority")}
	app, err := authzrpc.New(ctx, authzrpc.Config{Store: s, Lease: lease,
		Provider:  authnapikey.New(authnapikey.Config{AuthorizationInstalled: func() bool { return true }}),
		Facts:     authzrpc.StaticFacts{Facts: authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{{Ref: testRef("local"), Name: "local", InstallationID: "fixed", Active: true}}}},
		Namespace: ns, DefaultProject: "local"})
	if err != nil {
		t.Fatal(err)
	}
	writers := make([]driver.WriteTx, 0, 94)
	defer func() {
		for _, tx := range writers {
			_ = tx.Close()
		}
	}()
	for i := 0; i < 94; i++ {
		tx, err := s.Write(ctx, lease)
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
		writers = append(writers, tx)
	}
	if stats := s.db.Stats(); stats.MaxOpenConnections != 96 || stats.InUse != 96 {
		t.Fatalf("saturated topology changed: %+v", stats)
	}
	tx := writers[0]
	if _, err := tx.(driver.FreshReadyAuthorityReader).FreshReadyAuthority(ctx); err == nil {
		t.Fatal("pre-Head authority was accepted")
	}
	if _, err := tx.Lock(ctx, driver.LockSet{{Key: testLock(ref), Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	committed, err := tx.(driver.FreshReadyAuthorityReader).FreshReadyAuthority(ctx)
	if err != nil {
		t.Fatal(err)
	}
	by := model.PrincipalRef{Kind: "user", ID: "user:reader"}
	expected, err := app.Checker.ReadAuthorityBinding(ctx, by, committed)
	if err != nil {
		t.Fatal(err)
	}
	// A staged image must remain visible to the Writer, while its borrowed
	// committed authority retains the actual post-Head source and binding.
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"id":"ready-authority-source","type":"issue","project":"moved","fields":{"title":"staged","status":"todo"}}`)}); err != nil {
		t.Fatal(err)
	}
	rows, err := committed.Get(ctx, []model.Ref{ref})
	if err != nil || len(rows) != 1 || string(rows[0].Body) != string(body) {
		t.Fatalf("committed source includes staged overlay: %+v %v", rows, err)
	}
	staged, err := tx.Get(ctx, []model.Ref{ref})
	if err != nil || len(staged) != 1 || string(staged[0].Body) == string(body) {
		t.Fatalf("writer lost its staged image: %+v %v", staged, err)
	}
	final, err := app.Checker.ReadAuthorityBinding(ctx, by, committed)
	if err != nil || final != expected {
		t.Fatalf("same-connection authority fence: binding=%s err=%v", final, err)
	}
	if stats := s.db.Stats(); stats.WaitCount != 0 {
		t.Fatalf("authority fence requested another pool connection: %+v", stats)
	}
	t.Logf("READY_TERMINAL_SATURATION writers=94 pool=96 reserved=2 head=true final_authority=true staged_overlay=false wait_count=0 image=%s", fmt.Sprintf("%s", rows[0].Revision.String()))
}
