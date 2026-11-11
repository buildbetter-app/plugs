package dumpformat

// NativeDeltaHistoryPoint is scalar authenticated provenance. It borrows only
// immutable strings and never clones the full proof's table maps or row images.
type NativeDeltaHistoryPoint struct {
	Sequence   uint64
	CommitID   string
	RootSHA256 string
}

func (a *Archive) NativeDeltaHistoryPoint(sequence uint64) (NativeDeltaHistoryPoint, bool) {
	if !a.HasNativeDelta() || sequence == 0 || sequence > uint64(len(a.delta.proof.Commits)) {
		return NativeDeltaHistoryPoint{}, false
	}
	commit := a.delta.proof.Commits[sequence-1]
	if commit.Sequence != sequence {
		return NativeDeltaHistoryPoint{}, false
	}
	return NativeDeltaHistoryPoint{Sequence: sequence, CommitID: commit.CommitID, RootSHA256: commit.RootSHA256}, true
}

func (a *Archive) NativeDeltaCheckpointRoots() (head, current string, ok bool) {
	if !a.HasNativeDelta() {
		return "", "", false
	}
	return a.delta.proof.HeadRootSHA256, a.delta.proof.CurrentRootSHA256, true
}
