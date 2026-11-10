package converterroundtrip

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

type observedRestoreLease struct {
	driver.ActiveLease
	renewals atomic.Int64
	active   atomic.Int64
	releases atomic.Int64
	fail     bool
}

func (l *observedRestoreLease) Renew(ctx context.Context) error {
	l.renewals.Add(1)
	l.active.Add(1)
	defer l.active.Add(-1)
	if l.fail {
		return driver.ErrFenced
	}
	return l.ActiveLease.Renew(ctx)
}

func (l *observedRestoreLease) Release(ctx context.Context) error {
	l.releases.Add(1)
	return l.ActiveLease.Release(ctx)
}

type delayedRestorePublication struct {
	driver.Store
	lease   *observedRestoreLease
	fail    bool
	wait    time.Duration
	elapsed time.Duration
	commits int
}

func (s *delayedRestorePublication) Lease(ctx context.Context, owner string, ttl time.Duration) (driver.ActiveLease, error) {
	lease, err := s.Store.Lease(ctx, owner, ttl)
	if err != nil {
		return nil, err
	}
	s.lease = &observedRestoreLease{ActiveLease: lease, fail: s.fail}
	return s.lease, nil
}

func (s *delayedRestorePublication) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	// Preserve the actual driver's bound lease identity; only observe renewal.
	if observed, ok := lease.(*observedRestoreLease); ok {
		lease = observed.ActiveLease
	}
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &delayedRestorePublicationTx{
		WriteTx: tx, RestoreImportTx: tx.(driver.RestoreImportTx),
		AllocatorContinuation: tx.(driver.AllocatorContinuation), source: s,
	}, nil
}

type delayedRestorePublicationTx struct {
	driver.WriteTx
	driver.RestoreImportTx
	driver.AllocatorContinuation
	source *delayedRestorePublication
}

func (tx *delayedRestorePublicationTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	// All import staging and the Head lock have completed. Delay the real
	// atomic publication, without changing the driver's clock, TTL or fence.
	tx.source.commits++
	start := time.Now()
	timer := time.NewTimer(tx.source.wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		tx.source.elapsed = time.Since(start)
		return model.Changeset{}, ctx.Err()
	case <-timer.C:
		tx.source.elapsed = time.Since(start)
		return tx.WriteTx.Commit(ctx, header)
	}
}

func openRestoreLeaseControlStore(t *testing.T, engine string) driver.Store {
	t.Helper()
	ctx := context.Background()
	var store driver.Store
	var err error
	switch engine {
	case "memory":
		store = memory.New()
	case "sqlite":
		store, err = (sqlite.Driver{}).Open(ctx, driver.Config{DSN: filepath.Join(t.TempDir(), "restore-lease.db")})
	case "postgres":
		store, err = postgres.Open(ctx, driver.Config{DSN: identityPostgresDSN(t)})
	default:
		t.Fatal("unknown restore lease control driver", engine)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error("close restore lease control store", err)
		}
	})
	return store
}

func restoreLeasePublicationControl(t *testing.T, engine, mode string) {
	t.Helper()
	ctx := context.Background()
	data := admittedFixtureBytes(t)
	profile, archive := admittedImportProfile(t, data)
	store := openRestoreLeaseControlStore(t, engine)
	before := savedEmptyJointTarget(t, store, archive, profile)
	delayed := &delayedRestorePublication{Store: store, wait: driver.LeaseTTL + driver.LeaseTTL/3, fail: mode == "renewal-loss"}
	var report Report
	var err error
	if mode == "withheld" {
		lease, leaseErr := delayed.Lease(ctx, "restore-withheld-renewal-control", driver.LeaseTTL)
		if leaseErr != nil {
			t.Fatal(leaseErr)
		}
		report, err = RestoreImportWithLease(ctx, bytes.NewReader(data), delayed, lease, profile)
		if releaseErr := lease.Release(ctx); releaseErr != nil {
			t.Fatal("release caller-owned control lease", releaseErr)
		}
	} else {
		report, err = RestoreImport(ctx, bytes.NewReader(data), delayed, profile)
	}
	if delayed.lease == nil || delayed.commits != 1 || delayed.lease.active.Load() != 0 || delayed.lease.releases.Load() != 1 {
		t.Fatal("lease control did not reach exactly one publication and drain/release its owner", delayed.commits)
	}
	switch mode {
	case "renewed":
		if err != nil || delayed.elapsed <= driver.LeaseTTL || delayed.lease.renewals.Load() < 2 {
			t.Fatal("owned import did not renew through publication beyond one actual TTL", err, delayed.elapsed, delayed.lease.renewals.Load())
		}
		recovered, recoverErr := RecoverImport(ctx, store, profile, Verification{ArchiveSHA256: profile.ArchiveDigest, DerivedSHA256: profile.DerivedDigest})
		if recoverErr != nil || report.Restored.Head.IsZero() || !reflect.DeepEqual(recovered.Restored, report.Restored) {
			t.Fatal("long renewed publication did not retain its complete recoverable receipt", recoverErr)
		}
	case "withheld":
		if !errors.Is(err, driver.ErrFenced) || delayed.elapsed <= driver.LeaseTTL || delayed.lease.renewals.Load() != 0 {
			t.Fatal("unrenewed caller-owned lease did not fence after one actual TTL", err, delayed.elapsed, delayed.lease.renewals.Load())
		}
		if report.Receipt != nil || !reflect.DeepEqual(before, savedEmptyJointTarget(t, store, archive, profile)) {
			t.Fatal("fenced import published core/history/derived/blobs/receipt state")
		}
	case "renewal-loss":
		if !errors.Is(err, context.Canceled) || delayed.elapsed >= delayed.wait || delayed.lease.renewals.Load() != 1 {
			t.Fatal("renewal rejection did not cancel publication and stop the loop", err, delayed.elapsed, delayed.lease.renewals.Load())
		}
		if report.Receipt != nil || !reflect.DeepEqual(before, savedEmptyJointTarget(t, store, archive, profile)) {
			t.Fatal("cancelled renewal-loss import published core/history/derived/blobs/receipt state")
		}
	default:
		t.Fatal("unknown restore lease control mode", mode)
	}
	if out := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); out != "" {
		evidence := map[string]any{"engine": engine, "mode": mode, "lease_ttl_ns": int64(driver.LeaseTTL),
			"publication_delay_ns": int64(delayed.elapsed), "renewal_attempts": delayed.lease.renewals.Load(),
			"active_renewals_after_return": delayed.lease.active.Load(), "release_calls": delayed.lease.releases.Load(),
			"publication_attempts": delayed.commits, "source_scope": "declared-synthetic-public-control"}
		if err := os.WriteFile(filepath.Join(out, "restore-lease-"+mode+"-"+engine+".json"), encode(t, evidence), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRestoreImportOwnedLeaseRenewsPastTTLAcrossThreeDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { restoreLeasePublicationControl(t, engine, "renewed") })
	}
}

func TestRestoreImportCallerLeaseWithoutRenewalFencesAcrossThreeDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { restoreLeasePublicationControl(t, engine, "withheld") })
	}
}

func TestRestoreImportRenewalLossCancelsAndDrainsAcrossThreeDrivers(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { restoreLeasePublicationControl(t, engine, "renewal-loss") })
	}
}
