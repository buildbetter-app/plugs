package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/testclock"
)

// Renew must be the first expiry-sensitive operation: SaveCheckpoint or Write
// would otherwise close Lost in checkLease and hide Renew's SQL no-row branch.
// Only the existing disposable PostgreSQL SQL-clock seam advances server time;
// the independent application clock does not decide storage lease expiration.
func TestB05PostgresExpiredSQLRenewPreservesReclaimReplayAcrossReopen(t *testing.T) {
	for _, point := range []struct {
		name  string
		delta time.Duration
	}{
		{"exactly", driver.LeaseTTL},
		{"after", driver.LeaseTTL + time.Microsecond},
	} {
		t.Run(point.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sqlClock := testclock.New(t, time.Date(2037, 11, 3, 4, 29, 59, 123456000, time.UTC))
			dsn := sqlClock.BindPostgres(t, freshDSN(t))
			open := func() *Store { return openTest(t, dsn) }
			owner, observer := open(), open()
			// The existing counter observes real function evaluations, including
			// rolled-back statements; it does not replace the clock or SQL result.
			clockCalls := installLeaseClockCounter(t, owner)
			appClock := testclock.New(t, time.Date(2041, 6, 1, 12, 0, 0, 0, time.UTC))
			svc, lease := b05Service(t, owner, appClock)
			local := lease.(*activeLease)
			by := model.PrincipalRef{Kind: "anonymous", ID: "anonymous"}
			core := model.PrincipalRef{Kind: "system", ID: "core"}
			created, err := svc.Create(ctx, by, service.CreateRequest{
				Type: "issue", Fields: map[string]any{"title": "SQL-expired Renew first"},
			}, b05Meta("expired-renew/create"))
			if err != nil || len(created.Results) != 1 {
				t.Fatalf("create=%+v err=%v", created, err)
			}
			ref := created.Results[0].Ref
			if _, err := svc.Claim(ctx, by, service.ClaimRequest{ID: ref}, b05Meta("expired-renew/claim")); err != nil {
				t.Fatal(err)
			}
			appClock.Advance(t, 2*time.Second)
			heartbeatMeta := b05Meta("expired-renew/heartbeat")
			heartbeat, err := svc.HeartbeatWithOutcome(ctx, by, service.HeartbeatRequest{ID: ref}, heartbeatMeta)
			if err != nil || heartbeat.Replayed || !heartbeat.LeaseExpiresAt.Equal(appClock.Now().Add(5*time.Minute)) {
				t.Fatalf("seed heartbeat=%+v err=%v", heartbeat, err)
			}
			_, claimed := b05Document(t, observer, ref)
			if claimed.Lease == nil || claimed.Lease.Epoch.String() != model.Natural64(lease.Epoch()).String() {
				t.Fatalf("independently read persisted claim=%+v", claimed.Lease)
			}
			overlay, err := observer.LoadCheckpoint(ctx, "plug/lease/"+b05Digest("plug/lease/"+ref.ID))
			if err != nil || overlay.Generation == 0 || len(overlay.Payload) == 0 {
				t.Fatalf("independently read persisted heartbeat checkpoint=%+v err=%v", overlay, err)
			}
			initial := b05StorageLease(t, observer)
			if initial.Epoch != lease.Epoch() || initial.Owner == "" || initial.Holder != "b05-clock-host" ||
				!initial.Expires.Equal(sqlClock.Now().Add(driver.LeaseTTL)) {
				t.Fatalf("independently read SQL-generated server lease=%+v", initial)
			}
			before := b05PersistedState(t, observer)
			local.mu.Lock()
			deadline := local.timerDeadline
			local.mu.Unlock()
			applicationAt := appClock.Now()

			sqlClock.Advance(t, point.delta)
			serverAt := testclock.PostgresNow(t, dsn)
			if !serverAt.Equal(initial.Expires.Add(point.delta-driver.LeaseTTL)) || !appClock.Now().Equal(applicationAt) {
				t.Fatalf("SQL boundary=%s persisted expiry=%s application=%s", serverAt, initial.Expires, appClock.Now())
			}
			select {
			case <-lease.Lost():
				t.Fatal("lease was already lost before the first expired SQL Renew")
			default:
			}
			if !time.Now().Before(deadline) || ctx.Err() != nil {
				t.Fatal("local deadline or context expired before the SQL predicate could decide")
			}
			callsBefore := clockCalls()
			err = lease.Renew(ctx)
			var fenced driver.ErrFenced
			// Only the sql.ErrNoRows arm in activeLease.Renew omits ObservedEpoch.
			// All local-loss/deadline arms return ObservedEpoch == lease.Epoch().
			if !errors.As(err, &fenced) || fenced.ExpectedEpoch != lease.Epoch() || fenced.ObservedEpoch != 0 {
				t.Fatalf("first expired SQL Renew=%v fenced=%+v; want the SQL no-row arm", err, fenced)
			}
			if calls := clockCalls() - callsBefore; calls != 1 {
				t.Fatalf("expired Renew SQL clock calls=%d; want one WHERE evaluation and no SET evaluation", calls)
			}
			select {
			case <-lease.Lost():
			default:
				t.Fatal("expired SQL Renew did not close Lost")
			}
			local.mu.Lock()
			unchangedDeadline := local.timerDeadline.Equal(deadline)
			local.mu.Unlock()
			if !unchangedDeadline || b05StorageLease(t, observer) != initial || b05PersistedState(t, observer) != before {
				t.Fatal("expired SQL Renew rearmed the local lease or changed durable lease/data/history/receipt/checkpoint bytes")
			}
			t.Logf("Renew-first boundary=%s SQL=%s persisted-expiry=%s clock-calls=1 SQL-no-row ExpectedEpoch=%d ObservedEpoch=0; no durable changes", point.name, serverAt, initial.Expires, lease.Epoch())

			callsBefore = clockCalls()
			if err := lease.Renew(ctx); !errors.As(err, &fenced) ||
				fenced.ExpectedEpoch != lease.Epoch() || fenced.ObservedEpoch != lease.Epoch() {
				t.Fatalf("repeat Renew must remain locally fenced=%v payload=%+v", err, fenced)
			}
			if clockCalls() != callsBefore {
				t.Fatal("terminally lost lease reached renewal SQL again")
			}
			if _, err := owner.SaveCheckpoint(ctx, lease, "b05/expired-sql-renew", 0, []byte("must not publish")); !errors.As(err, &fenced) {
				t.Fatalf("lost-owner checkpoint=%v", err)
			}
			wantLostService := func(err error) {
				t.Helper()
				var typed *model.Error
				// Service.admit uses the service failure constructor's Data.Hint.
				if !errors.As(err, &typed) || typed.Data.Code != "busy" ||
					typed.Data.Hint != "active storage lease was lost" || !typed.Data.Retryable {
					t.Fatalf("lost-server service admission=%v", err)
				}
			}
			_, err = svc.Get(ctx, by, service.GetRequest{ID: ref})
			wantLostService(err)
			_, err = svc.UpdateWithOutcome(ctx, by, service.UpdateRequest{ID: ref, Fields: map[string]any{"title": "must not publish"}}, b05Meta("expired-renew/refused-update"))
			wantLostService(err)
			_, err = svc.HeartbeatWithOutcome(ctx, by, service.HeartbeatRequest{ID: ref}, b05Meta("expired-renew/refused-heartbeat"))
			wantLostService(err)
			if clockCalls() != callsBefore || b05StorageLease(t, observer) != initial || b05PersistedState(t, observer) != before {
				t.Fatal("lost-owner repeats/admission/checkpoint changed persisted state or consulted the SQL clock")
			}
			if err := observer.Close(); err != nil {
				t.Fatal(err)
			}
			observer = open()
			if b05StorageLease(t, observer) != initial || b05PersistedState(t, observer) != before {
				t.Fatal("independent pool reopen changed expired lease or logical state before takeover")
			}

			nextService, next := b05Service(t, observer, appClock)
			taken := b05StorageLease(t, observer)
			if next.Epoch() != lease.Epoch()+1 || taken.Epoch != next.Epoch() ||
				taken.Owner == initial.Owner || taken.Owner == "" ||
				!taken.Expires.Equal(sqlClock.Now().Add(driver.LeaseTTL)) || b05PersistedState(t, observer) != before {
				t.Fatalf("permitted SQL-expired takeover=%+v", taken)
			}
			if err := lease.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			if err := lease.Renew(ctx); !errors.As(err, &fenced) ||
				fenced.ExpectedEpoch != lease.Epoch() || fenced.ObservedEpoch != lease.Epoch() {
				t.Fatalf("old lease resurrected after successor takeover and old-store close=%v", err)
			}
			if b05StorageLease(t, observer) != taken || b05PersistedState(t, observer) != before {
				t.Fatal("old-owner release/close erased successor or changed logical state")
			}
			replayedHeartbeat, err := nextService.HeartbeatWithOutcome(ctx, by, service.HeartbeatRequest{ID: ref}, heartbeatMeta)
			if err != nil || !replayedHeartbeat.Replayed || !replayedHeartbeat.LeaseExpiresAt.Equal(heartbeat.LeaseExpiresAt) ||
				b05StorageLease(t, observer) != taken || b05PersistedState(t, observer) != before {
				t.Fatalf("successor old-epoch heartbeat replay=%+v err=%v", replayedHeartbeat, err)
			}
			request := service.ReleaseRequest{ID: ref, Reason: "recovered after server SQL Renew refusal"}
			recoveryMeta := b05Meta("expired-renew/recovery")
			_, err = nextService.ReleaseWithOutcome(ctx, core, request, recoveryMeta)
			var protected *model.Error
			if !errors.As(err, &protected) || protected.Data.Code != "lease_expired" ||
				protected.Message != "bead claim is inside the reclaim grace window" ||
				b05StorageLease(t, observer) != taken || b05PersistedState(t, observer) != before {
				t.Fatalf("server takeover must not bypass application claim grace=%v", err)
			}

			// Application time advances only AFTER the SQL branch is proved, to
			// permit the separate domain recovery under the new live server lease.
			appClock.Advance(t, heartbeat.LeaseExpiresAt.Add(10*time.Minute+time.Nanosecond).Sub(appClock.Now()))
			out, err := nextService.ReleaseWithOutcome(ctx, core, request, recoveryMeta)
			if err != nil || out.Replayed {
				t.Fatalf("permitted recovery=%+v err=%v", out, err)
			}
			record, recovered := b05Document(t, observer, ref)
			if recovered.Lease != nil || recovered.Fields["status"] != "todo" ||
				len(recovered.Comments) != 1 || recovered.Comments[0].Body != request.Reason ||
				!reflect.DeepEqual(recovered.Comments[0].Principal, core) || !recovered.Comments[0].Time.Equal(appClock.Now()) ||
				record.Revision.String() != out.Commit.Revision.String() {
				t.Fatalf("actual recovered document=%+v", recovered)
			}
			reader := testRead(t, observer, nil)
			entries, logErr := reader.Changesets(ctx, nil, 100)
			versions, versionErr := reader.Versions(ctx, ref, driver.PageReq{Limit: 100})
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if logErr != nil || versionErr != nil || len(entries) != 3 || len(versions.Items) != 3 || versions.NextCursor != nil {
				t.Fatalf("recovery history changes=%d versions=%d logErr=%v versionErr=%v", len(entries), len(versions.Items), logErr, versionErr)
			}
			entry := entries[2]
			if entry.Revision.String() != out.Commit.Revision.String() || !entry.Time.Equal(appClock.Now()) ||
				!reflect.DeepEqual(entry.Principal, core) || len(entry.Events) != 1 ||
				entry.Events[0].Type != model.EventBeadLeaseReclaimed || entry.Events[0].Provenance.Method != "bead.release" ||
				entry.Events[0].Provenance.Cause.Root != recoveryMeta.IdempotencyKey {
				t.Fatalf("actual attributed recovery changeset=%+v", entry)
			}
			recoveredState := b05PersistedState(t, observer)
			if b05StorageLease(t, observer) != taken {
				t.Fatal("domain recovery changed the active server lease")
			}
			if err := observer.Close(); err != nil {
				t.Fatal(err)
			}
			// Both old pools are now physically closed. Read persisted bytes before
			// acquiring another real lease, then replay through a new public service.
			reopened := open()
			released := b05StorageLease(t, reopened)
			if released.Epoch != next.Epoch() || released.Owner != "" || released.Holder != "" ||
				!released.Expires.Equal(time.Unix(0, 0).UTC()) || b05PersistedState(t, reopened) != recoveredState {
				t.Fatalf("full reopen lost epoch or recovery/receipt bytes: %+v", released)
			}
			reopenedService, reopenedLease := b05Service(t, reopened, appClock)
			if reopenedLease.Epoch() != next.Epoch()+1 {
				t.Fatal("physical reopen reused the server epoch")
			}
			replay, err := reopenedService.ReleaseWithOutcome(ctx, core, request, recoveryMeta)
			if err != nil || !replay.Replayed || !reflect.DeepEqual(replay.Commit, out.Commit) || b05PersistedState(t, reopened) != recoveredState {
				t.Fatalf("reopened recovery replay=%+v err=%v", replay, err)
			}
			readback := open()
			reopenedRow := b05StorageLease(t, readback)
			if reopenedRow.Epoch != reopenedLease.Epoch() || reopenedRow.Owner == "" ||
				!reopenedRow.Expires.Equal(sqlClock.Now().Add(driver.LeaseTTL)) || b05PersistedState(t, readback) != recoveredState {
				t.Fatal("independent reopened pool differs in current SQL-generated lease or logical state after receipt replay")
			}
			t.Log("real PostgreSQL expired SQL Renew was the deciding first fence; Lost is terminal; lease/data/history/receipt/checkpoint bytes survived refusal; successor recovery and exact replay survived physical reopen")
		})
	}
}
