package s1b01b04

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type nativeNode struct {
	ID, Title, Status string
	Priority          int
	Parent            string
	Until             string
	UntilState        string
	ExternalHierarchy []string
}

type nativeExplained struct {
	ID, Reason string
}

type nativeBlocked struct {
	ID, Title, Status string
	Priority          int
	BlockedBy         []struct {
		ID, Title, Status string
		Priority          int
	} `json:"blocked_by"`
	BlockedByCount int `json:"blocked_by_count"`
}

type nativePhase struct {
	Nodes        map[string]nativeNode
	Ready        []string
	First        []string
	ExplainReady []nativeExplained
	Blocked      []nativeBlocked
	TotalReady   int
	TotalBlocked int
}

func recordNativePhase(t *testing.T, command *nativeCommand, vector nativeVector, phase string) nativePhase {
	t.Helper()
	projection := nativePhase{Nodes: make(map[string]nativeNode), Ready: []string{}, First: []string{}, ExplainReady: []nativeExplained{}}
	for priority, role := range nativeRoles {
		var issues []struct {
			ID, Title, Status string
			Priority          int
			Parent            *string
			Until             json.RawMessage `json:"defer_until"`
			Dependencies      []struct {
				ID   string
				Kind string `json:"dependency_type"`
			}
		}
		data := command.run("show-"+phase+"-"+role, "show", vector.SourceIDs[role], "--json")
		nativeWrite(t, filepath.Join(command.output, phase+"-show-"+role+".json"), data)
		nativeDecode(t, data, &issues)
		if len(issues) != 1 {
			t.Fatalf("%s public show returned %d rows", role, len(issues))
		}
		issue := issues[0]
		if issue.ID != vector.SourceIDs[role] || issue.Title != "B01 "+vector.Mode+" "+role || issue.Priority != priority {
			t.Fatalf("public show identity/title/priority differs for %s: %+v", role, issue)
		}
		wantParent := ""
		if role == "child" {
			wantParent = vector.SourceIDs["parent"]
			if phase == "detached-from-blocked" {
				wantParent = ""
			} else if phase == "reparented-to-open" {
				wantParent = vector.SourceIDs["free"]
			}
		} else if role == "grandchild" {
			wantParent = vector.SourceIDs["child"]
		}
		parent := ""
		if issue.Parent != nil {
			parent = *issue.Parent
		}
		var hierarchy, externalHierarchy []string
		for _, dependency := range issue.Dependencies {
			if dependency.Kind == "parent-child" {
				if strings.HasPrefix(dependency.ID, "external:") {
					externalHierarchy = append(externalHierarchy, dependency.ID)
				} else {
					hierarchy = append(hierarchy, dependency.ID)
				}
			}
		}
		if parent != wantParent || (wantParent == "" && len(hierarchy) != 0) ||
			(wantParent != "" && !reflect.DeepEqual(hierarchy, []string{wantParent})) {
			t.Fatalf("public parent/dependency disagree for %s: parent=%s edges=%v want=%s", role, parent, hierarchy, wantParent)
		}
		wantStatus := "open"
		if phase == "after" && role == "parent" {
			wantStatus = "deferred"
		}
		if phase == "blocker-closed" && role == "free" {
			wantStatus = "closed"
		}
		if issue.Status != wantStatus {
			t.Fatalf("%s %s status=%s want=%s", phase, role, issue.Status, wantStatus)
		}
		if len(externalHierarchy) != 0 {
			t.Fatalf("%s %s native refusal control persisted external hierarchy edges=%v", phase, role, externalHierarchy)
		}
		node := nativeNode{ID: issue.ID, Title: issue.Title, Status: issue.Status, Priority: issue.Priority, Parent: parent, UntilState: "absent", ExternalHierarchy: externalHierarchy}
		if len(issue.Until) != 0 {
			node.UntilState = "null"
			if string(issue.Until) != "null" {
				nativeDecode(t, issue.Until, &node.Until)
				node.UntilState = "present"
			}
		}
		if phase == "after" && role == "parent" && vector.Until != nil {
			until, err := time.Parse(time.RFC3339Nano, node.Until)
			requested, requestedErr := time.Parse("2006-01-02", *vector.Until)
			if err != nil || requestedErr != nil || !until.After(time.Now().UTC()) || !until.Equal(requested) {
				t.Fatalf("actual persisted timed until=%q is not the requested future date: %v", node.Until, err)
			}
		} else if node.Until != "" || node.UntilState == "present" {
			t.Fatalf("%s %s unexpectedly has an actual timer: %q", phase, role, node.Until)
		}
		projection.Nodes[role] = node
	}
	var ready []struct{ ID string }
	nativeDecode(t, command.run("ready-"+phase, "ready", "--json", "--sort", "hybrid", "--limit", "0"), &ready)
	seen := map[string]bool{}
	known := map[string]bool{}
	for _, id := range vector.SourceIDs {
		known[id] = true
	}
	for _, item := range ready {
		if !known[item.ID] || seen[item.ID] {
			t.Fatalf("ready contains a foreign or duplicate actual identity %s", item.ID)
		}
		seen[item.ID] = true
		projection.Ready = append(projection.Ready, item.ID)
	}
	var first []struct{ ID string }
	nativeDecode(t, command.run("ready-one-"+phase, "ready", "--json", "--sort", "hybrid", "--limit", "1"), &first)
	for _, item := range first {
		projection.First = append(projection.First, item.ID)
	}
	wantFirst := projection.Ready
	if len(wantFirst) > 1 {
		wantFirst = wantFirst[:1]
	}
	if !reflect.DeepEqual(projection.First, wantFirst) {
		t.Fatalf("actual native %s applied the page before readiness filters: first=%v full=%v", phase, projection.First, projection.Ready)
	}
	var explanation struct {
		Ready   []nativeExplained
		Blocked []nativeBlocked
		Summary struct {
			Ready   *int `json:"total_ready"`
			Blocked *int `json:"total_blocked"`
		}
	}
	nativeDecode(t, command.run("explain-"+phase, "ready", "--json", "--sort", "hybrid", "--explain", "--limit", "0"), &explanation)
	if explanation.Summary.Ready == nil || explanation.Summary.Blocked == nil ||
		*explanation.Summary.Ready != len(explanation.Ready) || *explanation.Summary.Blocked != len(explanation.Blocked) {
		t.Fatal("actual explain cardinalities are missing or disagree with their arrays")
	}
	var explainedIDs []string
	for _, item := range explanation.Ready {
		if item.Reason == "" {
			t.Fatal("actual explain ready item omits its reason")
		}
		explainedIDs = append(explainedIDs, item.ID)
	}
	blockedIDs := map[string]bool{}
	for _, item := range explanation.Blocked {
		if !known[item.ID] || blockedIDs[item.ID] || seen[item.ID] || item.BlockedByCount != len(item.BlockedBy) {
			t.Fatalf("actual blocked explanation has unknown/duplicate/contradictory IDs or counts: %+v", item)
		}
		blockedIDs[item.ID] = true
	}
	if !reflect.DeepEqual(explainedIDs, projection.Ready) {
		t.Fatalf("actual ready order and actual explain order differ: %v / %v", projection.Ready, explainedIDs)
	}
	projection.ExplainReady, projection.Blocked = explanation.Ready, explanation.Blocked
	projection.TotalReady, projection.TotalBlocked = *explanation.Summary.Ready, *explanation.Summary.Blocked
	// Before is a control, while after membership remains an actual source
	// observation. The replay comparison records the explicit adopted deviation.
	if phase == "before" {
		var all []string
		for _, role := range nativeRoles {
			all = append(all, vector.SourceIDs[role])
		}
		if !reflect.DeepEqual(projection.Ready, all) {
			t.Fatalf("open-parent native control membership/order=%v want=%v", projection.Ready, all)
		}
	} else if phase == "after" && seen[vector.SourceIDs["parent"]] {
		t.Fatal("deferred parent is unexpectedly present in the actual ready result")
	} else if phase != "after" {
		var inputs nativeInputs
		nativeDecode(t, nativeInputsBytes, &inputs)
		roles, ok := inputs.HierarchyPhases[phase]
		if !ok {
			t.Fatalf("unrecognized native hierarchy phase %s", phase)
		}
		want := make([]string, 0, len(roles))
		for _, role := range roles {
			want = append(want, vector.SourceIDs[role])
		}
		if !reflect.DeepEqual(projection.Ready, want) {
			t.Fatalf("actual native %s membership/order=%v want=%v; originals retained", phase, projection.Ready, want)
		}
	}
	nativeJSON(t, filepath.Join(command.output, phase+"-projection.json"), projection)
	return projection
}

func nativeRolesForIDs(vector nativeVector, ids []string) ([]string, error) {
	roles := make(map[string]string)
	for role, id := range vector.SourceIDs {
		roles[id] = role
	}
	result := []string{}
	for _, id := range ids {
		role, ok := roles[id]
		if !ok {
			return nil, fmt.Errorf("unmapped observed source identity %s", id)
		}
		result = append(result, role)
	}
	return result, nil
}

func nativeSafeSuffix(id string) bool {
	suffix, ok := strings.CutPrefix(id, "b01-")
	if !ok || suffix == "" {
		return false
	}
	for _, char := range suffix {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}
