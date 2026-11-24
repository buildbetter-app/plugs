package historyverify

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type nativeStreamMapping struct {
	SourceStoreID  string         `json:"sourceStoreId,omitempty"`
	SourceSequence uint64         `json:"sourceSequence,omitempty"`
	Project        string         `json:"project,omitempty"`
	Sequence       uint64         `json:"sequence"`
	Revision       model.Revision `json:"revision"`
	EventIDs       []string       `json:"eventIds"`
	CommitIDs      []string       `json:"commitIds"`
	ParentIDs      []string       `json:"parentIds"`
}
type nativeSourceFrame struct {
	Revision model.Revision                   `json:"revision"`
	Frame    driver.OfflineHistoryStreamFrame `json:"frame"`
}
type nativeStreamEvidence struct {
	Version          string                         `json:"streamVersion"`
	Source           string                         `json:"source"`
	Mode             driver.RestoreHistoryMode      `json:"mode"`
	Complete         bool                           `json:"complete"`
	First            uint64                         `json:"first"`
	Head             uint64                         `json:"head"`
	Floor            model.Revision                 `json:"floor"`
	Archive          string                         `json:"archiveSha256"`
	Derived          string                         `json:"derivedSha256"`
	History          string                         `json:"historySha256"`
	Gaps             []driver.SourceHistoryGap      `json:"gaps"`
	NativeBoundaries []driver.NativeHistoryBoundary `json:"nativeBoundaries"`
	Mapping          []nativeStreamMapping          `json:"mapping"`
	SourceFrames     []nativeSourceFrame            `json:"sourceFrames"`
}

// ValidateEvidence compares persisted stream evidence to the verifier's real
// archive-to-transcript correspondence. Legacy aggregate evidence is a distinct
// format. In particular, intermediate IDs, ordered parents and every mapping
// must agree, even when a different transcript has the same final HEAD.
func (s *NativeArchiveStream) ValidateEvidence(ctx context.Context, profile driver.RestoreImportProfile, raw []byte) error {
	if s == nil || s.closed || profile.HistoryDigest != s.digest {
		return ErrCheckpoint{"recovery transcript binding differs"}
	}
	hash, err := nativePreparationHash(s.archive, profile)
	if err != nil {
		return err
	}
	if hash != s.profileHash {
		return ErrCheckpoint{"recovery request differs"}
	}
	owner, release := s.archive.NativeDeltaAccountingScope()
	defer release()
	if err = owner.ChargeNativeDeltaBytes("verifier_recovery_evidence", uint64(len(raw))*8+1024); err != nil {
		return err
	}
	if driver.NativeReverseRead(ctx) {
		return s.validateReverseEvidence(ctx, profile, raw)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var actual nativeStreamEvidence
	if err = decoder.Decode(&actual); err != nil {
		return ErrCheckpoint{"invalid stream recovery evidence"}
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return ErrCheckpoint{"stream recovery evidence has trailing data"}
	}
	expected := nativeStreamEvidence{Version: driver.OfflineHistoryStreamVersion, Source: s.header.Source, Mode: profile.HistoryMode, Complete: s.header.Complete, First: s.header.First, Head: s.header.Head, Floor: model.Natural64(0), Archive: profile.ArchiveDigest, Derived: profile.DerivedDigest, History: profile.HistoryDigest, Gaps: []driver.SourceHistoryGap{}, NativeBoundaries: []driver.NativeHistoryBoundary{}, Mapping: []nativeStreamMapping{}}
	index := 0
	revision := model.Natural64(0)
	err = s.walk(ctx, func(frame driver.OfflineHistoryStreamFrame) error {
		if frame.Commit != nil {
			revision = model.Natural64(frame.Commit.Sequence)
		}
		if frame.Kind != "chunk" && frame.Kind != "end" {
			if index >= len(actual.SourceFrames) || actual.SourceFrames[index].Revision != revision || !reflect.DeepEqual(actual.SourceFrames[index].Frame, frame) {
				return ErrCheckpoint{"stream physical/source-frame evidence differs from authenticated transcript"}
			}
			index++
		}
		if frame.Boundary != nil {
			expected.NativeBoundaries = append(expected.NativeBoundaries, *frame.Boundary)
		}
		if c := frame.Commit; c != nil {
			if err := owner.ChargeNativeDeltaBytes("verifier_recovery_mapping", 512); err != nil {
				return err
			}
			expected.Mapping = append(expected.Mapping, nativeStreamMapping{SourceStoreID: c.SourceStoreID, SourceSequence: c.SourceSequence, Project: c.Project, Sequence: c.Sequence, Revision: model.Natural64(c.Sequence), EventIDs: []string{}, CommitIDs: []string{}, ParentIDs: []string{}})
		}
		if id := frame.ID; id != nil {
			if len(expected.Mapping) == 0 {
				return ErrCheckpoint{"provenance without recovery mapping"}
			}
			if err := owner.ChargeNativeDeltaBytes("verifier_recovery_ids", uint64(len(id.Value))+32); err != nil {
				return err
			}
			m := &expected.Mapping[len(expected.Mapping)-1]
			switch id.Kind {
			case "commit":
				m.CommitIDs = append(m.CommitIDs, id.Value)
			case "parent":
				m.ParentIDs = append(m.ParentIDs, id.Value)
			case "event":
				m.EventIDs = append(m.EventIDs, id.Value)
			default:
				return ErrCheckpoint{"unexpected recovery provenance kind"}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if index != len(actual.SourceFrames) {
		return ErrCheckpoint{"stream source-frame evidence is incomplete"}
	}
	actual.SourceFrames = nil
	if !reflect.DeepEqual(actual, expected) {
		return ErrCheckpoint{"stream recovery evidence differs from authenticated archive"}
	}
	return nil
}

// validateReverseEvidence compares the two long arrays one entry at a time.
// Each expected entry is generated by the same authenticated source spool
// used by the ordinary validator.
func (s *NativeArchiveStream) validateReverseEvidence(ctx context.Context, profile driver.RestoreImportProfile, raw []byte) error {
	fields, err := driver.ReadNativeHistoryMarkerFields(raw)
	if err != nil {
		return err
	}
	mapping, err := openEvidenceLedger(fields["mapping"])
	if err != nil {
		return err
	}
	frames, err := openEvidenceLedger(fields["sourceFrames"])
	if err != nil {
		return err
	}
	delete(fields, "mapping")
	delete(fields, "sourceFrames")
	metadata, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	var actual nativeStreamEvidence
	header := json.NewDecoder(bytes.NewReader(metadata))
	header.DisallowUnknownFields()
	if err := header.Decode(&actual); err != nil {
		return ErrCheckpoint{"invalid stream recovery evidence"}
	}
	expected := nativeStreamEvidence{
		Version: driver.OfflineHistoryStreamVersion, Source: s.header.Source,
		Mode: profile.HistoryMode, Complete: s.header.Complete, First: s.header.First, Head: s.header.Head,
		Floor: model.Natural64(0), Archive: profile.ArchiveDigest, Derived: profile.DerivedDigest, History: profile.HistoryDigest,
		Gaps: []driver.SourceHistoryGap{}, NativeBoundaries: []driver.NativeHistoryBoundary{},
	}
	var previous *nativeStreamMapping
	flushMapping := func() error {
		if previous == nil {
			return nil
		}
		var row nativeStreamMapping
		if err := mapping.Decode(&row); err != nil || !reflect.DeepEqual(row, *previous) {
			return ErrCheckpoint{"stream source mapping differs from authenticated transcript"}
		}
		return nil
	}
	revision := model.Natural64(0)
	err = s.walk(ctx, func(frame driver.OfflineHistoryStreamFrame) error {
		if commit := frame.Commit; commit != nil {
			if err := flushMapping(); err != nil {
				return err
			}
			revision = model.Natural64(commit.Sequence)
			previous = &nativeStreamMapping{
				SourceStoreID: commit.SourceStoreID, SourceSequence: commit.SourceSequence, Project: commit.Project,
				Sequence: commit.Sequence, Revision: revision, EventIDs: []string{}, CommitIDs: []string{}, ParentIDs: []string{},
			}
		}
		if frame.Kind != "chunk" && frame.Kind != "end" {
			var row nativeSourceFrame
			if err := frames.Decode(&row); err != nil || row.Revision != revision || !reflect.DeepEqual(row.Frame, frame) {
				return ErrCheckpoint{"stream physical/source-frame evidence differs from authenticated transcript"}
			}
		}
		if frame.Boundary != nil {
			expected.NativeBoundaries = append(expected.NativeBoundaries, *frame.Boundary)
		}
		if id := frame.ID; id != nil {
			if previous == nil {
				return ErrCheckpoint{"provenance without recovery mapping"}
			}
			switch id.Kind {
			case "commit":
				previous.CommitIDs = append(previous.CommitIDs, id.Value)
			case "parent":
				previous.ParentIDs = append(previous.ParentIDs, id.Value)
			case "event":
				previous.EventIDs = append(previous.EventIDs, id.Value)
			default:
				return ErrCheckpoint{"unexpected recovery provenance kind"}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flushMapping(); err != nil {
		return err
	}
	for _, ledger := range []*json.Decoder{mapping, frames} {
		if token, err := ledger.Token(); err != nil || token != json.Delim(']') {
			return ErrCheckpoint{"stream history ledger differs from authenticated transcript"}
		}
	}
	if !reflect.DeepEqual(actual, expected) {
		return ErrCheckpoint{"stream recovery evidence differs from authenticated archive"}
	}
	return nil
}

func openEvidenceLedger(raw []byte) (*json.Decoder, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
		return nil, ErrCheckpoint{"invalid stream recovery evidence ledger"}
	}
	return decoder, nil
}
