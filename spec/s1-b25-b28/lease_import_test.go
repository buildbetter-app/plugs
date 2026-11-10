package s1b25b28_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Bound only the real contender's acquisition, after archive validation has
// finished. A slow decoder must not masquerade as evidence of lease contention.
type b27ContendedImportStore struct {
	configUnionObservedStore
	leaseCalls int
	leaseError error
	contextErr error
}

func (s *b27ContendedImportStore) Lease(ctx context.Context, holder string, ttl time.Duration) (driver.ActiveLease, error) {
	s.leaseCalls++
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	lease, err := s.Store.Lease(bounded, holder, ttl)
	s.leaseError, s.contextErr = err, bounded.Err()
	return lease, err
}

func assertB27IncumbentLease(t *testing.T, s *stack, epoch uint64) {
	t.Helper()
	select {
	case <-s.lease.Lost():
		t.Fatal("archive import displaced the incumbent local lease")
	default:
	}
	health, err := s.store.Health(s.ctx)
	if err != nil || health.LeaseEpoch != epoch || s.lease.Epoch() != epoch {
		t.Fatalf("archive import changed the live lease epoch: health=%+v lease=%d want=%d err=%v", health, s.lease.Epoch(), epoch, err)
	}
	if err := s.lease.Renew(s.ctx); err != nil {
		t.Fatalf("incumbent local lease no longer renews: %v", err)
	}
}

func TestB27ImportConservesLiveLocalLeaseAcrossDrivers(t *testing.T) {
	// Authored synthetic snapshot only. Native capture, full source history and
	// complete B27 qualification require their own fresh evidence.
	data := configUnionArchive(t, func(in *configUnionInput) {
		addUnionArchive(t, in, "leases", dumpformat.Row{
			"issue_id": "demo-7", "holder": "authored-source-holder",
			"granted_at": "2026-11-01T00:00:00Z", "lease_expires_at": "2099-01-01T00:00:00Z",
			"heartbeat_at": nil, "granted_node": "public synthetic fixture",
		})
	})
	profile := configUnionProfile(t, data)
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sourceLeases := 0
	for _, record := range archive.Records("beads-archive") {
		if record.Source.Table == "leases" {
			sourceLeases++
		}
	}
	if sourceLeases != 1 {
		t.Fatalf("lease conservation control has %d source lease snapshots, want one", sourceLeases)
	}
	for _, kind := range []string{"memory", "sqlite", "postgres"} {
		t.Run(kind, func(t *testing.T) {
			s := openStack(t, kind, nil)
			epoch := s.lease.Epoch()
			before := relationState(t, s)
			target := &b27ContendedImportStore{configUnionObservedStore: configUnionObservedStore{Store: s.store}}
			report, err := converterroundtrip.RestoreImport(s.ctx, bytes.NewReader(data), target, profile)
			if target.leaseCalls != 1 || !errors.Is(target.leaseError, context.DeadlineExceeded) || !errors.Is(target.contextErr, context.DeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("valid import did not contend on the real live lease: calls=%d acquisition=%v context=%v import=%v", target.leaseCalls, target.leaseError, target.contextErr, err)
			}
			if target.writes != 0 || report.Archive == nil || !report.Restored.Head.IsZero() || report.Recovered {
				t.Fatalf("contended import crossed the before-write boundary: writes=%d report=%+v", target.writes, report)
			}
			if !reflect.DeepEqual(before, relationState(t, s)) {
				t.Fatal("contended import changed records, versions, changesets, metadata or derived rows")
			}
			assertUnionRefusalUnpublished(t, s, profile)
			assertB27IncumbentLease(t, s, epoch)

			// The exact same admitted input succeeds under the original lease:
			// contention, rather than invalid input, caused the previous refusal.
			report, err = converterroundtrip.RestoreImportWithLease(s.ctx, bytes.NewReader(data), target, s.lease, profile)
			if err != nil || target.leaseCalls != 1 || target.writes != 1 || report.Archive == nil || report.Restored.Head.String() != "1" || report.Recovered {
				t.Fatalf("same-lease import did not commit the unchanged archive exactly once: writes=%d acquisitions=%d report=%+v err=%v", target.writes, target.leaseCalls, report, err)
			}
			assertB27IncumbentLease(t, s, epoch)
			assertStoredConfigUnion(t, s, data, profile)
			committed := configUnionSnapshotState(t, s, data, profile)
			if kind != "memory" {
				s.reopen()
				assertStoredConfigUnion(t, s, data, profile)
				if !reflect.DeepEqual(committed, configUnionSnapshotState(t, s, data, profile)) {
					t.Fatal("same-lease import changed its durable snapshot or receipt after SQL reopen")
				}
			}
			t.Logf("B27 authored snapshot: source_leases=%d contender=deadline writes_before=0 incumbent_epoch=%d same_lease_import=committed SQL_reopen=%t", sourceLeases, epoch, kind != "memory")
		})
	}
}
