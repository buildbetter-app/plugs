package s1b09b12

import (
	"bytes"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"testing"
)

// The complete accepted public packet is retained literally, separately from
// the historical V1 twins. Its candidate is the reference producer, not the
// Plug candidate running these tests. No reference executable is run here.
//
//go:embed reference-v2/cb40-41563023339.packet.json
var acceptedPacketBytes []byte

const acceptedPacketSHA256 = "33cd8fdcb66b5b93f4a36329fc0598cb9c5d012fc350804809da5eb5a38d4067"

type acceptedRun struct {
	Card, WorkspaceID, SourceCommit, SourceTree, BinarySHA256, RuntimeBinarySHA256 string
	ShutdownVerified                                                               bool
	Commands                                                                       []referenceCommand
	Streams                                                                        []referenceStream
	Projection                                                                     map[string]any
	ProjectionSHA256                                                               string
}

type acceptedPair struct {
	Card          string
	First, Repeat acceptedRun
	Equality      referenceEquality
}

type acceptedPacket struct {
	Schema, Status string
	SelectedCards  []string
	Candidate      struct{ Commit, Tree string }
	Profile        struct{ ID string }
	Unexecuted     map[string]struct{ Status string }
	Recordings     []acceptedPair
}

var acceptedLabels = map[string][]string{
	"B09": {"init", "hash-first", "hash-second", "hash-collision-refusal", "hash-list", "cleanup-stop"},
	"B10": {"init", "create-b10-parent", "create-b10-parent.8", "child-9", "child-10", "delete-10", "child-11", "close-11", "child-12", "delete-live-children", "child-13", "children", "cleanup-stop"},
	"B11": {"init", "create-b11-source", "create-b11-target", "add-blocks", "readd-blocks", "show-before-retype", "retype-refusal", "show-after-retype", "remove-blocks", "add-relates", "show-after-readd", "remove-relates", "show-final", "cleanup-stop"},
}

func acceptedPacketFromBytes(data []byte) (acceptedPacket, error) {
	var p acceptedPacket
	if len(data) != 1514945 || referenceBytesDigest(data) != acceptedPacketSHA256 {
		return p, fmt.Errorf("accepted original packet byte identity differs")
	}
	if err := json.Unmarshal(data, &p); err != nil {
		return p, err
	}
	if p.Schema != "plug.s1-b09-b12.proxied-public-reference-packet.v2" || p.Status != "PASS" ||
		p.Candidate.Commit != "cb40cab68d65307a9bac17bfd43754eb0210d5c0" || p.Candidate.Tree != "09fe250b7ff627ade066d687b29b1410d8f1bdbf" ||
		p.Profile.ID != "s1-b09-b11-public9d6-proxied-dolt220-v2" ||
		!reflect.DeepEqual(p.SelectedCards, []string{"B09", "B10", "B11"}) || len(p.Recordings) != 3 ||
		len(p.Unexecuted) != 1 || p.Unexecuted["B12"].Status != "not-executed; full-card-unqualified" {
		return p, fmt.Errorf("accepted producer/profile/card-set boundary differs")
	}
	seen := map[string]bool{}
	for i, pair := range p.Recordings {
		if pair.Card != p.SelectedCards[i] {
			return p, fmt.Errorf("recording card order differs")
		}
		for _, run := range []acceptedRun{pair.First, pair.Repeat} {
			if run.Card != pair.Card || run.WorkspaceID == "" || seen[run.WorkspaceID] || !run.ShutdownVerified ||
				run.SourceCommit != referenceSourceCommit || run.SourceTree != referenceSourceTree ||
				run.BinarySHA256 != "3c6bbb888431751adce98dfebc8cbeeb5869300698e8b069a4d7438fabfb3af4" ||
				run.RuntimeBinarySHA256 != "72352f1d882472a7a8d6124e9c4dc12f80e2c2359ea95f1424c032e9250e35e6" {
				return p, fmt.Errorf("%s source/runtime/workspace binding differs", pair.Card)
			}
			seen[run.WorkspaceID] = true
		}
	}
	return p, nil
}

// sourceProjection is intentionally independent of Python producer/extractor
// projection fields. It derives the common semantic result from exact decoded
// stdout bytes and actual exit codes. The fixed packet hash binds all remaining
// source/profile/configuration data to the separately audited original receipt.
func sourceProjection(run acceptedRun) (map[string]any, error) {
	labels := acceptedLabels[run.Card]
	if len(labels) == 0 || len(run.Commands) != len(labels) || len(run.Streams) != len(labels) {
		return nil, fmt.Errorf("%s command/stream set is incomplete", run.Card)
	}
	outputs := map[string]json.RawMessage{}
	exits := map[string]int{}
	for i, label := range labels {
		cmd, stream := run.Commands[i], run.Streams[i]
		if cmd.Label != label || stream.Label != label || len(cmd.Argv) < 2 || cmd.Argv[0] != "reference-engine" {
			return nil, fmt.Errorf("%s command/stream order differs", run.Card)
		}
		stdout, err := base64.StdEncoding.Strict().DecodeString(stream.StdoutBase64)
		if err != nil {
			return nil, err
		}
		stderr, err := base64.StdEncoding.Strict().DecodeString(stream.StderrBase64)
		if err != nil {
			return nil, err
		}
		if referenceBytesDigest(stdout) != stream.StdoutSHA256 || referenceBytesDigest(stderr) != stream.StderrSHA256 {
			return nil, fmt.Errorf("%s %s stream hash differs", run.Card, label)
		}
		wantExit := 0
		if label == "hash-collision-refusal" || label == "retype-refusal" {
			wantExit = 1
		}
		if cmd.ExitCode != wantExit {
			return nil, fmt.Errorf("%s %s exit=%d", run.Card, label, cmd.ExitCode)
		}
		exits[label] = cmd.ExitCode
		if label != "init" {
			if !json.Valid(stdout) {
				return nil, fmt.Errorf("%s %s stdout is not JSON", run.Card, label)
			}
			outputs[label] = stdout
		}
	}
	var stop struct{ Stopped, Verified bool }
	if err := json.Unmarshal(outputs["cleanup-stop"], &stop); err != nil || !stop.Stopped || !stop.Verified {
		return nil, fmt.Errorf("%s public shutdown is not verified", run.Card)
	}
	switch run.Card {
	case "B09":
		var first, second sourceIssue
		var rows []sourceIssue
		if err := json.Unmarshal(outputs["hash-first"], &first); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(outputs["hash-second"], &second); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(outputs["hash-list"], &rows); err != nil {
			return nil, err
		}
		pattern := regexp.MustCompile(`^b09-[a-z0-9]{3,8}$`)
		return map[string]any{
			"createdIDsDistinct": first.ID != second.ID,
			"createdIDPattern":   pattern.MatchString(first.ID) && pattern.MatchString(second.ID),
			"sameSemanticFields": first.Title == second.Title && first.Description == second.Description && first.Type == second.Type && first.Status == second.Status,
			"collisionRefused":   exits["hash-collision-refusal"] == 1,
			"rowsAfterCollision": float64(len(rows)),
		}, nil
	case "B10":
		ids := make([]string, 0, 6)
		for _, label := range []string{"create-b10-parent.8", "child-9", "child-10", "child-11", "child-12", "child-13"} {
			var row sourceIssue
			if err := json.Unmarshal(outputs[label], &row); err != nil {
				return nil, err
			}
			ids = append(ids, row.ID)
		}
		var children []sourceIssue
		if err := json.Unmarshal(outputs["children"], &children); err != nil {
			return nil, err
		}
		return childProjection(ids, sourceIDs(children)), nil
	case "B11":
		rows := map[string][]sourceIssue{}
		for _, label := range []string{"show-before-retype", "show-after-retype", "show-after-readd", "show-final"} {
			var value []sourceIssue
			if err := json.Unmarshal(outputs[label], &value); err != nil || len(value) != 1 || value[0].ID != "b11-source" {
				return nil, fmt.Errorf("%s must contain the original source endpoint", label)
			}
			rows[label] = value
		}
		before, after, readded, final := rows["show-before-retype"][0], rows["show-after-retype"][0], rows["show-after-readd"][0], rows["show-final"][0]
		return map[string]any{
			"sameTypeReaddAccepted":           exits["readd-blocks"] == 0,
			"typedRetypeRefused":              exits["retype-refusal"] == 1,
			"retypeNoWrite":                   sourceHasEdge(before, "blocks") && sourceHasEdge(after, "blocks") && bytes.Equal(outputs["show-before-retype"], outputs["show-after-retype"]),
			"unlinkThenDifferentTypeAccepted": sourceHasEdge(readded, "relates-to"),
			"unlinkConservesEndpoints":        len(final.Dependencies) == 0 && final.DependencyCount == 0 && final.Title == before.Title,
		}, nil
	}
	return nil, fmt.Errorf("unsupported card")
}

type sourceIssue struct {
	ID, Title, Description, Status string
	Type                           string `json:"issue_type"`
	DependencyCount                int    `json:"dependency_count"`
	Dependencies                   []struct {
		ID   string
		Kind string `json:"dependency_type"`
	}
}

func sourceHasEdge(row sourceIssue, kind string) bool {
	return len(row.Dependencies) == 1 && row.Dependencies[0].ID == "b11-target" && row.Dependencies[0].Kind == kind
}

func sourceIDs(rows []sourceIssue) []string {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	return ids
}

// childProjection only defines the common comparison schema. Source IDs above
// come from raw streams; Plug IDs below come from fresh persisted records.
func childProjection(ids, children []string) map[string]any {
	sort.Strings(children)
	values := make([]any, len(ids))
	for i, id := range ids {
		values[i] = id
	}
	return map[string]any{
		"explicitHighWater":          len(ids) == 6 && ids[0] == "b10-parent.8",
		"mintedAfterImported":        len(ids) == 6 && ids[1] == "b10-parent.9",
		"sequentialContinuation":     len(ids) == 6 && reflect.DeepEqual(ids[2:], []string{"b10-parent.10", "b10-parent.11", "b10-parent.12", "b10-parent.13"}),
		"noReuseAfterDeleteAndClose": len(ids) == 6 && ids[5] == "b10-parent.13",
		"childRowsAfterCleanup":      reflect.DeepEqual(children, []string{"b10-parent.13"}),
		"observedIDs":                values,
	}
}

func acceptedPairs(t *testing.T) []acceptedPair {
	t.Helper()
	packet, err := acceptedPacketFromBytes(acceptedPacketBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range packet.Recordings {
		left, err := sourceProjection(pair.First)
		if err != nil {
			t.Fatal(err)
		}
		right, err := sourceProjection(pair.Repeat)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(left, right) || !reflect.DeepEqual(left, pair.First.Projection) || !reflect.DeepEqual(right, pair.Repeat.Projection) ||
			referenceDigest(left) != pair.First.ProjectionSHA256 || referenceDigest(right) != pair.Repeat.ProjectionSHA256 {
			t.Fatalf("%s independently derived source projection differs: left=%v right=%v", pair.Card, left, right)
		}
		rawEqual := true
		for i, a := range pair.First.Streams {
			b := pair.Repeat.Streams[i]
			if a.StdoutBase64 != b.StdoutBase64 || a.StderrBase64 != b.StderrBase64 {
				rawEqual = false
			}
		}
		if rawEqual || pair.Equality.RawStreamsEqual || !pair.Equality.NormalizedTranscriptsEqual || !pair.Equality.ProjectionsEqual {
			t.Fatalf("%s historical raw inequality was lost", pair.Card)
		}
	}
	return packet.Recordings
}

func TestAcceptedPacketIndependentProjectionAndFailureBoundaries(t *testing.T) {
	pairs := acceptedPairs(t)
	altered := append([]byte(nil), acceptedPacketBytes...)
	altered[len(altered)/2] ^= 1
	if _, err := acceptedPacketFromBytes(altered); err == nil {
		t.Fatal("altered packet accepted")
	}
	if _, err := acceptedPacketFromBytes(acceptedPacketBytes[:len(acceptedPacketBytes)-1]); err == nil {
		t.Fatal("truncated packet accepted")
	}
	for _, pair := range pairs {
		run := pair.First
		run.Streams = append([]referenceStream(nil), run.Streams...)
		run.Streams[1].StdoutBase64 = base64.StdEncoding.EncodeToString([]byte(`{}`))
		if _, err := sourceProjection(run); err == nil {
			t.Fatalf("%s corrupt raw stream accepted", pair.Card)
		}
		run = pair.First
		run.Commands = append([]referenceCommand(nil), run.Commands...)
		run.Commands[1].ExitCode = 1
		if _, err := sourceProjection(run); err == nil {
			t.Fatalf("%s failed source create accepted", pair.Card)
		}
		run = pair.First
		run.Streams = run.Streams[:len(run.Streams)-1]
		if _, err := sourceProjection(run); err == nil {
			t.Fatalf("%s incomplete shutdown accepted", pair.Card)
		}
		run = pair.First
		run.Projection = map[string]any{"fabricated": true}
		derived, err := sourceProjection(run)
		if err != nil || reflect.DeepEqual(derived, run.Projection) {
			t.Fatalf("%s trusted projection summary", pair.Card)
		}
	}
}
