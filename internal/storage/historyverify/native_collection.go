package historyverify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/nativeprojection"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeCollectionAdmission fixes host intent before any source is admitted.
// Only the provider's logical byte commitment and the derived/history digests
// are calculated outputs. Source mappings are checked against the compact
// authenticated current image by the existing provider/receiving planner.
type NativeCollectionAdmission struct {
	Profile  driver.RestoreImportProfile
	Target   driver.SourcePlaneTargetV1
	Provider driver.SourcePlaneProviderV1
}

// NativeCaptureSetOpener transfers exclusive ownership of an actual landed
// reader result. The issuer supplies the common budget and receiving options.
// A callback cannot construct an Archive or confer checkpoint authority.
type NativeCaptureSetOpener func(context.Context, dumpformat.NativeCaptureSetOptions) (*dumpformat.Archive, error)

// NativeCollection is a private two-stage issuer. Append spools each verified
// source separately and drops its complete preparation before the next read.
// Bind then issues ONE actual Plan and ONE transcript for the final request.
// It is not a concatenation of independently sealed checkpoint proofs.
type NativeCollection struct {
	planner                              *nativeprojection.NativeCapturePlanner
	stream                               *NativeArchiveStream
	binding                              dumpformat.NativeJoinedBinding
	admission                            NativeCollectionAdmission
	fixedHash                            string
	wantDerived                          string
	dir                                  string
	next                                 int
	finished, bound, failed, closed      bool
	commits, tables, decoded, boundaries uint64
	complete                             bool
	head                                 map[string]checkpointRecord
	ids                                  map[string]uint8
	collisions                           int
	releaseAdmission, releaseProfile     func()
}

func nativeCollectionAdmissionHash(owner *dumpformat.Archive, admission NativeCollectionAdmission) (string, error) {
	p := admission.Profile
	p.SourcePlanes = nil
	p.DerivedDigest = strings.Repeat("0", 64)
	p.HistoryDigest = ""
	work, release := owner.NativeDeltaAccountingScope()
	defer release()
	p, releaseProfile, err := nativeprojection.DetachedProfile(work, p)
	if err != nil {
		return "", err
	}
	defer func() {
		admission.Profile, p = driver.RestoreImportProfile{}, driver.RestoreImportProfile{}
		releaseProfile()
	}()
	admission.Profile = p
	admission.Provider.LogicalImageSHA256 = ""
	admission.Provider.LogicalImageSize = 0
	if err := work.ChargeNativeDeltaJSON("collection_admission_encoding", admission); err != nil {
		return "", err
	}
	if err := owner.ChargeNativeDeltaBytes("collection_fixed_admission_hash", 64); err != nil {
		return "", err
	}
	raw, err := json.Marshal(admission)
	if err != nil {
		return "", err
	}
	return nativeSHA(raw), nil
}

func NewNativeCollection(ctx context.Context, binding dumpformat.NativeJoinedBinding, admission NativeCollectionAdmission, when time.Time) (_ *NativeCollection, err error) {
	planner, err := nativeprojection.NewNativeCapturePlanner(ctx, binding, when)
	if err != nil {
		return nil, err
	}
	c := &NativeCollection{planner: planner, complete: true, head: map[string]checkpointRecord{}, ids: map[string]uint8{}}
	defer func() {
		if err != nil {
			err = errors.Join(err, c.Close())
		}
	}()
	owner := planner.Owner()
	p := admission.Profile
	c.wantDerived = p.DerivedDigest
	if p.SourcePlanes != nil || p.HistoryDigest != "" || p.HistoryMode != driver.RestoreHistoryModeFull || p.HistoryTime != when.UTC().Format(time.RFC3339Nano) || p.ArchiveDigest != owner.SHA256() || string(p.SourceStoreID) != owner.Manifest().StoreID || !admission.Target.RequireEmpty || admission.Target.Head != 0 {
		return nil, ErrCheckpoint{"collection fixed admission differs"}
	}
	if p.DerivedDigest == "" {
		p.DerivedDigest = strings.Repeat("0", 64)
	}
	p, c.releaseAdmission, err = nativeprojection.DetachedProfile(owner, p)
	if err != nil {
		return nil, err
	}
	admission.Profile = p
	if err = detachNativeCollectionIntent(owner, &admission); err != nil {
		return nil, err
	}
	c.admission = admission
	c.fixedHash, err = nativeCollectionAdmissionHash(owner, admission)
	if err != nil {
		return nil, err
	}
	if err = owner.ChargeNativeDeltaJSON("collection_issuer_binding", binding); err != nil {
		return nil, err
	}
	c.binding, err = detachNativeCollectionBinding(owner, binding)
	if err != nil {
		return nil, err
	}
	c.dir, err = os.MkdirTemp("", "plug-native-collection-*")
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(c.dir, "history.frames"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	c.stream = &NativeArchiveStream{archive: owner, file: f, path: f.Name(), when: when, files: map[string]dumpformat.File{}, nativeCurrent: map[string]checkpointRecord{}, revisions: map[string]model.Revision{}, originalDir: c.dir}
	c.stream.cleanup = func() error {
		c.head, c.ids, c.binding, c.admission = nil, nil, dumpformat.NativeJoinedBinding{}, NativeCollectionAdmission{}
		c.releaseProfiles()
		if c.planner != nil {
			c.planner.Close()
			c.planner = nil
		}
		return os.RemoveAll(c.dir)
	}
	return c, nil
}

// Target/provider strings may come from a temporary decoded plan. Detach only
// those retained fields before that decoder's owner ends, without copying the
// already detached profile or retaining the full source plan.
func detachNativeCollectionIntent(owner *dumpformat.Archive, admission *NativeCollectionAdmission) error {
	intent := struct {
		Target   driver.SourcePlaneTargetV1
		Provider driver.SourcePlaneProviderV1
	}{admission.Target, admission.Provider}
	if err := owner.ChargeNativeDeltaJSON("collection_admission_intent", intent); err != nil {
		return err
	}
	work, release := owner.NativeDeltaAccountingScope()
	defer release()
	if err := work.ChargeNativeDeltaJSON("collection_admission_intent_encoding", intent); err != nil {
		return err
	}
	raw, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &intent); err != nil {
		return err
	}
	admission.Target, admission.Provider = intent.Target, intent.Provider
	return nil
}

// The caller reserves the retained detached binding separately. Encoding is
// simultaneous scratch, admitted before Marshal and dropped before release.
func detachNativeCollectionBinding(owner *dumpformat.Archive, binding dumpformat.NativeJoinedBinding) (dumpformat.NativeJoinedBinding, error) {
	work, release := owner.NativeDeltaAccountingScope()
	defer release()
	if err := work.ChargeNativeDeltaJSON("collection_issuer_encoding", binding); err != nil {
		return dumpformat.NativeJoinedBinding{}, err
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		return dumpformat.NativeJoinedBinding{}, err
	}
	var detached dumpformat.NativeJoinedBinding
	err = json.Unmarshal(raw, &detached)
	raw = nil
	return detached, err
}

func (c *NativeCollection) Owner() *dumpformat.Archive {
	if c == nil || c.closed || c.planner == nil {
		return nil
	}
	return c.planner.Owner()
}

// Append performs one original traversal when the opener uses the supplied
// PrepareReceiving option. All subsequent callbacks use the authenticated
// preparation; construction adds no hidden input traversal.
func (c *NativeCollection) Append(ctx context.Context, open NativeCaptureSetOpener) (err error) {
	if c == nil || c.closed || c.failed || c.finished || c.bound || c.next >= 2 || open == nil {
		return ErrCheckpoint{"collection source admission is closed"}
	}
	defer func() {
		if err != nil {
			c.failed = true
		}
	}()
	if ctx == nil {
		return ErrCheckpoint{"missing collection verification context"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = c.Owner().WithNativeDeltaAccounting(ctx)
	source, err := open(ctx, dumpformat.NativeCaptureSetOptions{PrepareReceiving: true})
	if err != nil {
		if source != nil {
			source.DiscardNativeCaptureSet()
		}
		return err
	}
	if source == nil {
		return ErrCheckpoint{"collection opener returned no source"}
	}
	// appendSource returns before discard: every source view, raw callback,
	// graph and preparation alias has ceased. Only charged detached current
	// candidates and compact indexes have transferred to the session owner.
	err = c.appendSource(ctx, source)
	source.DiscardNativeCaptureSet()
	source = nil
	if err != nil {
		return err
	}
	c.next++
	return ctx.Err()
}

func (c *NativeCollection) appendSource(ctx context.Context, source *dumpformat.Archive) error {
	if err := c.planner.Append(ctx, source); err != nil {
		return err
	}
	owner, release := source.NativeDeltaAccountingScope()
	defer release()
	s := &NativeArchiveStream{archive: owner, file: c.stream.file, when: c.stream.when, files: map[string]dumpformat.File{}, nativeCurrent: map[string]checkpointRecord{}, revisions: map[string]model.Revision{}}
	b, err := newNativeFrames(ctx, s)
	if err != nil {
		return err
	}
	if math.MaxUint64-c.commits < s.header.Head {
		return ErrCheckpoint{"collection sequence/count overflow"}
	}
	b.segment, b.offset = true, c.commits
	prepared, err := source.PrepareNativeDelta(ctx)
	if err != nil {
		return err
	}
	err = prepared.Visit(ctx, dumpformat.NativeDeltaVisitor{
		Commit: func(commit dumpformat.NativeDeltaRowCommit) error {
			for _, change := range commit.Changes {
				for _, record := range []*driver.Record{change.Before, change.After} {
					if record == nil {
						continue
					}
					work, done := owner.NativeDeltaAccountingScope()
					_, row, e := work.DecodeNativeRowRecord(*record)
					if e == nil && row != nil && (row.Table == dumpformat.DurableEntityTable || row.Table == dumpformat.EphemeralEntityTable) {
						e = c.addID(row.Fields["id"])
					}
					done()
					if e != nil {
						return e
					}
				}
			}
			return b.commit(commit)
		},
		TableState: func(state dumpformat.NativeDeltaTableState) error {
			if !state.Complete && state.Kind == "current" && (state.Schema.Name == dumpformat.DurableEntityTable || state.Schema.Name == dumpformat.EphemeralEntityTable) {
				for _, row := range state.Normalized.Rows {
					if err := c.addID(row["id"]); err != nil {
						return err
					}
				}
			}
			return b.checkpoint(state)
		},
		File: func(name string, raw []byte) error {
			if err := b.captureFile(name, raw); err != nil {
				return err
			}
			return c.spoolOriginal(name, raw)
		},
		PreparedFile: func(file dumpformat.NativeCaptureFile, input io.Reader) error {
			if err := b.captureFileCommitment(file.Name, dumpformat.File{Size: int64(file.Bytes), SHA256: file.SHA256}); err != nil {
				return err
			}
			return c.spoolOriginalStream(file, input)
		},
	})
	if err != nil {
		return err
	}
	if err = b.finish(); err != nil {
		return err
	}
	if math.MaxUint64-c.commits < b.commits || math.MaxUint64-c.tables < b.tables || math.MaxUint64-c.decoded < b.decoded {
		return ErrCheckpoint{"collection sequence/count overflow"}
	}
	if err = c.mergeCheckpoints(c.head, b.head); err != nil {
		return err
	}
	if err = c.mergeCheckpoints(c.stream.nativeCurrent, s.nativeCurrent); err != nil {
		return err
	}
	for key, revision := range s.revisions {
		if _, duplicate := c.stream.revisions[key]; duplicate {
			return ErrCheckpoint{"collection revision identity overlaps"}
		}
		if err = c.Owner().ChargeNativeDeltaBytes("collection_revision_index", uint64(len(key))+128); err != nil {
			return err
		}
		c.stream.revisions[strings.Clone(key)] = revision
	}
	c.commits += b.commits
	c.tables += b.tables
	c.decoded += b.decoded
	c.boundaries += s.header.NativeBoundaries
	c.complete = c.complete && s.header.Complete
	c.stream.peakFrame = max(c.stream.peakFrame, s.peakFrame)
	c.stream.peakChunk = max(c.stream.peakChunk, s.peakChunk)
	return ctx.Err()
}

func (c *NativeCollection) mergeCheckpoints(to, from map[string]checkpointRecord) error {
	for key, row := range from {
		if _, duplicate := to[key]; duplicate {
			return ErrCheckpoint{"collection checkpoint identity overlaps"}
		}
		if err := c.Owner().ChargeNativeDeltaJSON("collection_checkpoint_index", struct {
			Key string
			Row checkpointRecord
		}{key, row}); err != nil {
			return err
		}
		if err := c.Owner().ChargeNativeDeltaBytes("collection_checkpoint_slots", uint64(len(row.ref.ID)+len(row.kind)+len(row.hash))+384); err != nil {
			return err
		}
		row.ref.ID, row.kind, row.hash = strings.Clone(row.ref.ID), strings.Clone(row.kind), strings.Clone(row.hash)
		to[strings.Clone(key)] = row
	}
	return nil
}

func (c *NativeCollection) addID(value any) error {
	id, ok := value.(string)
	if !ok || id == "" {
		return ErrCheckpoint{"invalid source entity ID in collision census"}
	}
	bit := uint8(1 << c.next)
	old := c.ids[id]
	if old == 0 {
		if err := c.Owner().ChargeNativeDeltaBytes("collection_historical_id_census", uint64(len(id))+128); err != nil {
			return err
		}
		id = strings.Clone(id)
	}
	if old != 3 && old|bit == 3 {
		c.collisions++
	}
	c.ids[id] = old | bit
	return nil
}

// NativeIDCollisionError never carries an ID, project name or stored value.
type NativeIDCollisionError struct {
	Count int `json:"count"`
}

func (e *NativeIDCollisionError) Error() string {
	return fmt.Sprintf("cross_project_id_collision: count=%d", e.Count)
}
func (e *NativeIDCollisionError) Unwrap() error { return driver.ErrUnsupported }

func (c *NativeCollection) FinishCurrent() (*dumpformat.Archive, error) {
	if c == nil || c.closed || c.failed || c.finished || c.next != 2 {
		return nil, ErrCheckpoint{"collection preparation is incomplete"}
	}
	if c.collisions != 0 {
		c.failed = true
		return nil, &NativeIDCollisionError{Count: c.collisions}
	}
	a, err := c.planner.FinishCurrent()
	if err != nil {
		c.failed = true
		return nil, err
	}
	c.finished = true
	return a, nil
}

func (c *NativeCollection) Close() error {
	if c == nil || c.closed {
		return nil
	}
	c.closed = true
	if c.stream != nil {
		err := c.stream.Close()
		c.stream = nil
		return err
	}
	c.head, c.ids, c.binding, c.admission = nil, nil, dumpformat.NativeJoinedBinding{}, NativeCollectionAdmission{}
	c.releaseProfiles()
	if c.planner != nil {
		c.planner.Close()
		c.planner = nil
	}
	if c.dir != "" {
		return os.RemoveAll(c.dir)
	}
	return nil
}

// These copies belong to the long-lived planner owner. Callers drop their
// returned profile aliases before Close; release precedes that owner's close.
func (c *NativeCollection) releaseProfiles() {
	if c.releaseProfile != nil {
		c.releaseProfile()
		c.releaseProfile = nil
	}
	if c.releaseAdmission != nil {
		c.releaseAdmission()
		c.releaseAdmission = nil
	}
}
