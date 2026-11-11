package main

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
)

type freshCase struct{ Engine, Name, Mode, Recipe, Target, Marker string }

func freshNativeCases() []freshCase {
	var cases []freshCase
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, recipe := range []struct{ name, mode, test, target, marker string }{
			{"complete", "full", "TestPlugDumpCompleteRootFromPublicDoltActions", "//cmd/bd:bd_test", "PUBLIC_COMPLETE_SOURCE_ARCHIVE_BASE64="},
			{"complete-snapshot", "snapshot", "TestPlugDumpCompleteRootFromPublicDoltActions", "//cmd/bd:bd_test", "PUBLIC_COMPLETE_SOURCE_ARCHIVE_BASE64="},
			{"partial", "partial", "TestSQLSourceHistoryReadsPinnedDoltCommitImages", "//internal/plugdump:plugdump_test", "PUBLIC_SOURCE_ARCHIVE_BASE64="},
			{"partial-snapshot", "snapshot", "TestSQLSourceHistoryReadsPinnedDoltCommitImages", "//internal/plugdump:plugdump_test", "PUBLIC_SOURCE_ARCHIVE_BASE64="},
		} {
			cases = append(cases, freshCase{engine, recipe.name, recipe.mode, recipe.test, recipe.target, recipe.marker})
		}
	}
	return cases
}

func freshArchiveMarker(log []byte, marker string) ([]byte, error) {
	var value string
	count := 0
	for _, line := range strings.Split(string(log), "\n") {
		if _, suffix, ok := strings.Cut(line, marker); ok {
			count++
			value = strings.TrimSpace(suffix)
		}
	}
	if count != 1 {
		return nil, fmt.Errorf("recipe export marker count=%d", count)
	}
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(data) == 0 || len(data) > dumpformat.MaxArchiveBytes || base64.StdEncoding.EncodeToString(data) != value {
		return nil, fmt.Errorf("recipe export marker invalid")
	}
	return data, nil
}

func freshSingleRecipeXML(data []byte, name string) error {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	cases := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "failure", "error", "skipped":
			return fmt.Errorf("recipe XML contains %s", start.Name.Local)
		case "testcase":
			cases++
			found := false
			for _, attr := range start.Attr {
				if attr.Name.Local == "name" && attr.Value == name {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("unselected native recipe testcase")
			}
		}
	}
	if cases != 1 {
		return fmt.Errorf("recipe XML cases=%d", cases)
	}
	return nil
}
