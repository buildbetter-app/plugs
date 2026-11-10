package converterroundtrip

import (
	"bytes"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

// Original producer outputs from e25cf419c9bc516fa3dd91394ee93a846255d7ba,
// run 42786069732. This container changes no member bytes. Its public source
// remains public/public_commit_history; no private project is substituted.
func nativeBoundaryFixtureMembers(t *testing.T) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/public-history-boundary-e25cf419.tar")
	if err != nil {
		t.Fatal(err)
	}
	if hashBytes(raw) != "46677d8d6021ee35611e81238fd037d33739e4e44eb5308430b90d5f0ba5f93a" {
		t.Fatal("original boundary/multiversion fixture container changed")
	}
	members := tarMembers(t, raw)
	want := map[string]string{
		"public-shallow-boundary-v66.tar":                    "0afc3756eaf4094ffa6bb76441a3b48e0d142b1614885f6996c0b86880f6382d",
		"public-shallow-boundary-v66-physical-evidence.json": "de781286afd5d5fdc11e91bc406063ff3be133fd03aa944c53699c15994f2b20",
		"public-shallow-boundary-v66-facts.json":             "934a27be3d256e7e60dc52d9fc76437d1d2e056f786d268fc924ec3d9b0e2180",
		"public-shallow-boundary-v66-stderr.jsonl":           "cce9fd12e48de6ec0ff116581f43ef5530826586df893cb6150b3e131cff6e83",
		"public-shallow-boundary-v66-stdout.json":            "f8b445574233f69aab29d6d2895d50c022cd0f3a0247eb869c2a20221faf5999",
		"public-multiversion-v66.tar":                        "d27127f59239c50d83e6ab162339f210a950b483118ceda66f49094cd63802f3",
		"public-multiversion-v66-physical-evidence.json":     "2a0fd8e1f2f2fbb669db94f30acbc4d7d4973005de89da2dd6191439f92a40d5",
		"public-multiversion-v66-facts.json":                 "597aabc0f039a532a73d62b4311ed16da87ea8472c7c0b64c2c604883f2d3c39",
		"public-multiversion-unsupported-diagnostic.jsonl":   "f150db3897b0c1ab461f725591896e8527212ded056abef3af4d4f4aefb0ca55",
	}
	if len(members) != len(want) {
		t.Fatal("original boundary/multiversion member census changed")
	}
	for name, digest := range want {
		if hashBytes(members[name]) != digest {
			t.Fatal("original producer output changed", name)
		}
	}
	return members
}

func actualNativeBoundaryArchive(t *testing.T) (*dumpformat.Archive, []byte) {
	t.Helper()
	members := nativeBoundaryFixtureMembers(t)
	raw := members["public-shallow-boundary-v66.tar"]
	archive, err := dumpformat.Read(bytes.NewReader(raw))
	if err != nil {
		t.Fatal("original genuine shallow archive", err)
	}
	var facts struct {
		Fixture    string `json:"fixture"`
		FullSource struct {
			Head    string
			Clean   bool
			Commits []dumpformat.SourceCommit
		} `json:"full_source"`
		Observed struct {
			Head          string
			ReadOnlyScope bool
			Commits       []dumpformat.SourceCommit
			Boundaries    []struct {
				CommitID string
				Reason   string
				SQLCodes []uint16
			}
		} `json:"observed_shallow"`
		BeforeWorking struct {
			Database        string
			HeadCommitID    string
			WorkingRootHash string
			ReadOnlyScope   bool
			Complete        bool
		} `json:"before_working"`
		CloneDepth      uint64                            `json:"clone_depth"`
		VerifiedImages  uint64                            `json:"verified_images"`
		Coverage        *dumpformat.NativeHistoryCoverage `json:"coverage"`
		Checkpoints     map[string]string                 `json:"migration_checkpoints"`
		CloneUnchanged  bool                              `json:"clone_unchanged_after_reopen"`
		SourceUnchanged bool                              `json:"source_unchanged_after_reopen"`
	}
	decode(t, members["public-shallow-boundary-v66-facts.json"], &facts)
	scopes := archive.NativeSourceHistories()
	if len(scopes) != 1 {
		t.Fatal("original shallow source census changed")
	}
	scope := scopes[0]
	coverage := scope.HistoryCoverage
	if archive.Manifest().MainVersion != 66 || archive.Manifest().IgnoredVersion != 26 || !reflect.DeepEqual(scope.Projects, []string{"public"}) || scope.Complete || len(scope.Gaps) != 0 || len(scope.Commits) != 13 || len(scope.Images) != 13 || coverage == nil || coverage.VerifiedCommitCount != 13 || len(coverage.Boundaries) != 1 || coverage.EventsAttachment.Rows != 2 || !reflect.DeepEqual(facts.Coverage, coverage) {
		t.Fatal("genuine boundary completeness/count/coverage or original public scope changed")
	}
	boundary := coverage.Boundaries[0]
	if boundary.Reason != "ghost_parent" || boundary.CommitOrdinalFromHead != 14 || boundary.CommitSchemaVersion != nil || len(boundary.SQLDoltCodes) != 0 || len(boundary.ChildCommitIDs) != 1 {
		t.Fatal("genuine ghost frontier gained schema, codes or different coordinates")
	}
	if facts.Fixture != "pinned-dolt-file-push-shallow-clone" || facts.CloneDepth != 13 || facts.VerifiedImages != 13 || !facts.CloneUnchanged || !facts.SourceUnchanged || !facts.FullSource.Clean || facts.FullSource.Head != scope.Head || len(facts.FullSource.Commits) != 14 || !facts.Observed.ReadOnlyScope || facts.Observed.Head != scope.Head || len(facts.Observed.Commits) != 13 || len(facts.Observed.Boundaries) != 1 || facts.BeforeWorking.Database != "public_commit_history" || facts.BeforeWorking.HeadCommitID != scope.Head || facts.BeforeWorking.WorkingRootHash == "" || !facts.BeforeWorking.ReadOnlyScope || !facts.BeforeWorking.Complete {
		t.Fatal("actual donor/clone/working-root facts or reopen conservation differ")
	}
	observedBoundary := facts.Observed.Boundaries[0]
	if observedBoundary.CommitID != boundary.CommitID || observedBoundary.Reason != boundary.Reason || len(observedBoundary.SQLCodes) != 0 {
		t.Fatal("archived frontier differs from the physically observed ghost")
	}
	full, observed := map[string][]string{}, map[string][]string{}
	for _, commit := range facts.FullSource.Commits {
		if _, duplicate := full[commit.ID]; duplicate {
			t.Fatal("duplicate full-source commit")
		}
		full[commit.ID] = commit.Parents
	}
	for _, commit := range facts.Observed.Commits {
		if _, duplicate := observed[commit.ID]; duplicate {
			t.Fatal("duplicate physical shallow commit")
		}
		observed[commit.ID] = commit.Parents
	}
	genesis, present := full[boundary.CommitID]
	if !present || len(genesis) != 0 {
		t.Fatal("actual unavailable frontier is not the authenticated donor genesis")
	}
	delete(full, boundary.CommitID)
	if !reflect.DeepEqual(full, observed) {
		t.Fatal("shallow input omitted more than the actual donor genesis")
	}
	for _, commit := range scope.Commits {
		parents, present := observed[commit.ID]
		if !present || !reflect.DeepEqual(parents, commit.Parents) || commit.ID == boundary.CommitID {
			t.Fatal("retained commit or parent edge differs from actual shallow graph")
		}
		delete(observed, commit.ID)
	}
	if len(observed) != 0 {
		t.Fatal("actual retained shallow commit missing from archive")
	}
	versions := assertNativeHistoryPhysicalInput(t, archive, members["public-shallow-boundary-v66-physical-evidence.json"], false)
	checkpoints := map[string]uint64{}
	tables, rows, absences, currentRows := 0, 0, 0, 0
	for _, image := range scope.Images {
		if image.SchemaProfile == nil || image.SchemaProfile.MainVersion == nil {
			t.Fatal("genuine retained image lost its own readable schema")
		}
		checkpoints[image.CommitID] = *image.SchemaProfile.MainVersion
		tables += len(image.Tables)
		absences += len(image.SchemaProfile.Absences)
		for _, table := range image.Tables {
			rows += len(table.Rows)
		}
	}
	for _, table := range scope.Current {
		currentRows += len(table.Rows)
	}
	if len(facts.Checkpoints) != 6 || tables != 266 || rows != 929 || absences != 354 || len(scope.Current) != 30 || currentRows != 109 {
		t.Fatal("genuine shallow image/current/profile census changed")
	}
	for _, version := range []uint64{39, 40, 41, 61, 62, 66} {
		id, present := facts.Checkpoints[strconv.FormatUint(version, 10)]
		if !present || checkpoints[id] != version {
			t.Fatal("genuine shallow checkpoint lacks its own schema", version)
		}
	}
	populated := func(version uint64, family, column string) bool {
		for _, image := range versions[version] {
			value := image[family]
			if len(value.Rows) == 0 {
				continue
			}
			for _, name := range value.Columns {
				if name == column {
					return true
				}
			}
		}
		return false
	}
	if !populated(39, "dependencies", "depends_on_id") || !populated(41, "dependencies", "depends_on_issue_id") || !populated(61, "events", "event_type") || len(scope.Current["events"].Rows) != 2 {
		t.Fatal("populated dependency reshape or separate immutable/current events lost")
	}
	for _, image := range versions[62] {
		if _, present := image["events"]; present {
			t.Fatal("post0062 immutable image gained current events")
		}
	}
	attachment, present := archive.Member(coverage.EventsAttachment.Member)
	var portable dumpformat.SourceHistory
	decode(t, attachment, &portable)
	if !present || hashBytes(attachment) != coverage.EventsAttachment.SHA256 || len(portable.Events) != 2 || portable.Range.Complete || portable.Range.Status != "unverified" || portable.Range.Reason != "native-history-boundary" {
		t.Fatal("actual events attachment lost rows, bytes or incomplete classification")
	}
	return archive, raw
}

func actualE25MultiVersionArchive(t *testing.T) (*dumpformat.Archive, []byte) {
	t.Helper()
	members := nativeBoundaryFixtureMembers(t)
	archive, raw := checkedNativeMultiVersionArchive(t, members["public-multiversion-v66.tar"], members["public-multiversion-v66-facts.json"])
	assertNativeCommitPhysicalInput(t, archive, members["public-multiversion-v66-physical-evidence.json"])
	return archive, raw
}

func TestActualNativeBoundaryAndCompleteOriginalPhysicalInput(t *testing.T) {
	t.Run("genuine-shallow", func(t *testing.T) { actualNativeBoundaryArchive(t) })
	t.Run("complete-multiversion-same-original-run", func(t *testing.T) { actualE25MultiVersionArchive(t) })
}

func TestActualNativeBoundaryReceivingAcrossThreeDrivers(t *testing.T) {
	_, input := actualNativeBoundaryArchive(t)
	exerciseNativeHistoryReceivingLibrary(t, input, "shallow-boundary", false)
}

func TestNativeReceivingExecutableActualBoundary(t *testing.T) {
	_, input := actualNativeBoundaryArchive(t)
	exerciseNativeHistoryReceivingExecutable(t, input, "shallow-boundary", false)
}

func TestActualE25NativeMultiVersionReceivingAcrossThreeDrivers(t *testing.T) {
	_, input := actualE25MultiVersionArchive(t)
	exerciseNativeHistoryReceivingLibrary(t, input, "e25-multiversion", true)
}

func TestNativeReceivingExecutableActualE25MultiVersion(t *testing.T) {
	_, input := actualE25MultiVersionArchive(t)
	exerciseNativeHistoryReceivingExecutable(t, input, "e25-multiversion", true)
}
