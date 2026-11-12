package driver

import (
	"bytes"
	"encoding/json"
	"fmt"
)

const NativeDeltaHistoryPolicyFormat = "beads-native-delta-history-policy/1"
const NativeDeltaHistoryEvidenceFormat = "beads-native-delta-history-evidence/1"

// NativeDeltaHistoryPolicy is an explicit operator selection, not source
// verification. FirstSequence is an authenticated capture ordinal, never a
// target revision. Full imports omit this field to preserve their binding.
type NativeDeltaHistoryPolicy struct {
	Format        string `json:"format"`
	FirstSequence uint64 `json:"firstSequence,omitempty"`
}

func (p *NativeDeltaHistoryPolicy) UnmarshalJSON(data []byte) error {
	if err := sourcePlaneJSONObject(data, map[string]bool{"format": true, "firstSequence": true}, false); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, raw := range fields {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("native delta policy fields cannot be null")
		}
	}
	type wire NativeDeltaHistoryPolicy
	var value wire
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&value); err != nil {
		return err
	}
	*p = NativeDeltaHistoryPolicy(value)
	return nil
}

func (p NativeDeltaHistoryPolicy) Validate(mode RestoreHistoryMode) error {
	if p.Format != NativeDeltaHistoryPolicyFormat || mode != RestoreHistoryModePartial && mode != RestoreHistoryModeSnapshot || mode == RestoreHistoryModePartial && p.FirstSequence == 0 || mode == RestoreHistoryModeSnapshot && p.FirstSequence != 0 {
		return ErrRestoreHistoryRefused{Mode: mode, Reason: RestoreHistoryReasonUnmappable}
	}
	return nil
}

// NativeDeltaHistoryEvidence describes only authenticated images. A baseline
// is an import floor, not a claimed source commit mapping. The unavailable
// prefix commitment retains CP11's unknown extent without inventing ordinals.
type NativeDeltaHistoryEvidence struct {
	Format                  string             `json:"format"`
	Mode                    RestoreHistoryMode `json:"mode"`
	ArchiveSHA256           string             `json:"archiveSha256"`
	SourceStoreID           string             `json:"sourceStoreId"`
	Project                 string             `json:"project"`
	FirstSequence           uint64             `json:"firstSequence"`
	FirstCommitID           string             `json:"firstCommitId,omitempty"`
	FirstRootSHA256         string             `json:"firstRootSha256,omitempty"`
	HeadSequence            uint64             `json:"headSequence"`
	HeadCommitID            string             `json:"headCommitId"`
	HeadRootSHA256          string             `json:"headRootSha256"`
	CurrentRootSHA256       string             `json:"currentRootSha256"`
	BaselineSequence        uint64             `json:"baselineSequence,omitempty"`
	BaselineCommitID        string             `json:"baselineCommitId,omitempty"`
	BaselineRootSHA256      string             `json:"baselineRootSha256,omitempty"`
	OmittedPrefix           *SourceHistoryGap  `json:"omittedPrefix,omitempty"`
	UnavailablePrefixSHA256 string             `json:"unavailablePrefixSha256,omitempty"`
}

func (e NativeDeltaHistoryEvidence) Validate(p RestoreImportProfile) error {
	if p.NativeDeltaHistory == nil || p.NativeDeltaHistory.Validate(p.HistoryMode) != nil || e.Format != NativeDeltaHistoryEvidenceFormat || e.Mode != p.HistoryMode || e.ArchiveSHA256 != p.ArchiveDigest || e.SourceStoreID != string(p.SourceStoreID) || e.Project == "" || e.HeadSequence == 0 || e.HeadCommitID == "" || !importDigest(e.HeadRootSHA256) || !importDigest(e.CurrentRootSHA256) || e.UnavailablePrefixSHA256 != "" && !importDigest(e.UnavailablePrefixSHA256) {
		return fmt.Errorf("invalid native delta history provenance")
	}
	if e.Mode == RestoreHistoryModeSnapshot {
		if e.FirstSequence != 0 || e.FirstCommitID != "" || e.FirstRootSHA256 != "" || e.BaselineSequence != 0 || e.BaselineCommitID != "" || e.BaselineRootSHA256 != "" || e.OmittedPrefix != nil {
			return fmt.Errorf("snapshot provenance cannot claim source history")
		}
		return nil
	}
	if e.FirstSequence != p.NativeDeltaHistory.FirstSequence || e.FirstSequence > e.HeadSequence || e.FirstCommitID == "" || !importDigest(e.FirstRootSHA256) {
		return fmt.Errorf("partial provenance differs from selected source floor")
	}
	if e.FirstSequence == 1 {
		if e.BaselineSequence != 0 || e.BaselineCommitID != "" || e.BaselineRootSHA256 != "" || e.OmittedPrefix != nil {
			return fmt.Errorf("first source image has no invented predecessor")
		}
	} else if e.BaselineSequence != e.FirstSequence-1 || e.BaselineCommitID == "" || !importDigest(e.BaselineRootSHA256) || e.OmittedPrefix == nil || *e.OmittedPrefix != (SourceHistoryGap{First: 1, Last: e.FirstSequence - 1, Reason: "operator-selected-floor"}) {
		return fmt.Errorf("partial provenance requires its verified baseline and exact omitted prefix")
	}
	return nil
}
