package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
)

func TestFreshRecipeProfileBindsBothSelectionsToLandedSource(t *testing.T) {
	data, err := os.ReadFile("testdata/fresh-native/recipe-profile.json")
	if err != nil || hash(data) != freshRecipeProfileSHA {
		t.Fatal("exact native recipe profile missing")
	}
	var profile map[string]string
	if err := json.Unmarshal(data, &profile); err != nil {
		t.Fatal(err)
	}
	for _, selection := range []string{"complete", "partial"} {
		target, err := freshRecipeTarget(data, profile["source_commit"], selection)
		if err != nil || target != profile[selection+"_target"] {
			t.Fatalf("landed recipe selection: %s %v", target, err)
		}
	}
	if _, err := freshRecipeTarget(data, profile["source_commit"], "adjacent"); err == nil {
		t.Fatal("unknown recipe selected")
	}
	if _, err := freshRecipeTarget(data, "0000000000000000000000000000000000000000", "complete"); err == nil {
		t.Fatal("wrong producer head selected")
	}
	for _, field := range []string{"schema", "source_commit", "complete_target", "partial_target"} {
		t.Run("missing-"+field, func(t *testing.T) {
			copy := map[string]string{}
			for k, v := range profile {
				copy[k] = v
			}
			delete(copy, field)
			changed, _ := json.Marshal(copy)
			if _, err := freshRecipeTarget(changed, profile["source_commit"], "complete"); err == nil {
				t.Fatal("incomplete immutable profile admitted")
			}
		})
	}
	profile["extra"] = "unreviewed"
	changed, _ := json.Marshal(profile)
	if _, err := freshRecipeTarget(changed, profile["source_commit"], "complete"); err == nil {
		t.Fatal("undeclared recipe profile field admitted")
	}
}

func TestFreshNativeCensusNamesAllTwelveIndependentCases(t *testing.T) {
	cases := freshNativeCases()
	seen := map[string]bool{}
	for _, item := range cases {
		key := item.Engine + "/" + item.Name
		if seen[key] || item.Recipe == "" || item.Target == "" || item.Marker == "" {
			t.Fatal("duplicate or incomplete fresh recording identity")
		}
		seen[key] = true
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, name := range []string{"complete", "complete-snapshot", "partial", "partial-snapshot"} {
			if !seen[engine+"/"+name] {
				t.Fatalf("missing %s/%s", engine, name)
			}
		}
	}
	if len(cases) != 12 {
		t.Fatal("fresh case count changed")
	}
}

func TestFreshRecipeArchiveRequiresOneOriginalMarker(t *testing.T) {
	marker := "PUBLIC_COMPLETE_SOURCE_ARCHIVE_BASE64="
	line := "    export_test.go:1: " + marker + base64.StdEncoding.EncodeToString([]byte("actual raw archive bytes"))
	got, err := freshArchiveMarker([]byte(line), marker)
	if err != nil || string(got) != "actual raw archive bytes" {
		t.Fatalf("unchanged original marker: %s %v", got, err)
	}
	for _, data := range []string{"", line + "\n" + line, marker + "not-base64", marker, marker + "eA"} {
		if _, err := freshArchiveMarker([]byte(data), marker); err == nil {
			t.Fatal("missing/duplicate/invalid original accepted")
		}
	}
}

func TestFreshRecipeXMLRequiresOneSelectedPassingRecipe(t *testing.T) {
	name := "TestPlugDumpCompleteRootFromPublicDoltActions"
	good := `<testsuite><testcase name="` + name + `"></testcase></testsuite>`
	if err := freshSingleRecipeXML([]byte(good), name); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{
		`<testsuite/>`, `<testsuite><testcase name="other"/></testsuite>`,
		`<testsuite><testcase name="` + name + `"><skipped/></testcase></testsuite>`,
		`<testsuite><testcase name="` + name + `"><failure/></testcase></testsuite>`,
		`<testsuite><testcase name="` + name + `"><error/></testcase></testsuite>`,
		`<testsuite><testcase name="` + name + `"/><testcase name="` + name + `"/></testsuite>`, `<bad`,
	} {
		if err := freshSingleRecipeXML([]byte(data), name); err == nil {
			t.Fatal("skipped/failed/extra/missing original recipe accepted")
		}
	}
}
