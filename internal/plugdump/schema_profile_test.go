package plugdump

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func schema66Fixture(t *testing.T) (nativeHistoryFixture, Inventory) {
	t.Helper()
	source, inventory := nativeHistoryFixtureForTest(t)
	images := []map[string]Table{source.fixtureSource}
	for _, image := range source.images {
		images = append(images, image)
	}
	for _, image := range images {
		for _, name := range []string{"issue_versions", "store_epoch", "epoch_minted_addresses"} {
			delete(image, name)
		}
		for _, name := range []string{"issues", "wisps"} {
			table, present := image[name]
			if !present {
				continue
			}
			columns := []string{}
			for _, column := range table.Columns {
				if column != "current_revision" && column != "participation_generation" {
					columns = append(columns, column)
				}
			}
			table.Columns = columns
			for _, row := range table.Rows {
				delete(row, "current_revision")
				delete(row, "participation_generation")
			}
			image[name] = table
		}
		for name, limit := range map[string]uint64{"schema_migrations": 66, "ignored_schema_migrations": 26} {
			table, present := image[name]
			if !present {
				continue
			}
			rows := []Row{}
			for _, row := range table.Rows {
				version, err := natural(row["version"])
				if err != nil {
					t.Fatal(err)
				}
				if version <= limit {
					rows = append(rows, row)
				}
			}
			table.Rows = rows
			image[name] = table
		}
	}
	return source, inventory
}

func TestNativeSchema66SyntheticCaptureConservesLiteralRowsAndHistory(t *testing.T) {
	source, inventory := schema66Fixture(t)
	// A genuinely older physical schema is still retained at its own commit.
	source.images[source.log.Commits[0].ID]["old_only_public"] = Table{Columns: []string{"cell"}, Rows: []Row{{"cell": json.Number("9007199254740993")}}}
	beforeCurrent, beforeHistory := mustJSON(t, source.fixtureSource), mustJSON(t, source.images)
	data := buildFixture(t, source, inventory)
	manifest, native := nativeCapture(t, data)
	if manifest.MainVersion != 66 || manifest.IgnoredVersion != 26 || native.SchemaProfile == nil || len(native.SchemaProfile.Absences) != 7 || !native.Complete || len(native.Images) != len(source.log.Commits) || len(native.Gaps) != 0 || len(native.Current) != 30 {
		t.Fatal("own-version profile or genuine native history was not conserved")
	}
	for _, absent := range native.SchemaProfile.Absences {
		if absent.Reason != AbsentBySourceSchema {
			t.Fatal("schema absence was not typed")
		}
		if absent.Column == "" {
			if _, present := native.Current[absent.Family]; present {
				t.Fatal("absent table was synthesized")
			}
		} else {
			for _, row := range native.Current[absent.Family].Rows {
				if _, present := row[absent.Column]; present {
					t.Fatal("default or NULL was added to raw source fields")
				}
			}
		}
	}
	if !bytes.Equal(beforeCurrent, mustJSON(t, source.fixtureSource)) || !bytes.Equal(beforeHistory, mustJSON(t, source.images)) {
		t.Fatal("capture mutated its input")
	}
	if output := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); output != "" {
		if err := os.WriteFile(filepath.Join(output, "public-v66-synthetic.tar"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNativeSchemaProfileRefusesForgedAbsencesAndCursors(t *testing.T) {
	for _, mode := range []string{"missing-profile", "missing-absence", "invented-absence", "reordered-absence", "wrong-introduced", "wrong-reason", "wrong-ignored", "wrong-manifest", "synthetic-table", "synthetic-null-column", "missing-required-family", "missing-required-column", "altered-prefix-hash", "missing-prefix-row", "unsupported-main"} {
		t.Run(mode, func(t *testing.T) {
			source, inventory := schema66Fixture(t)
			if mode == "missing-required-family" || mode == "missing-required-column" || mode == "altered-prefix-hash" || mode == "missing-prefix-row" || mode == "unsupported-main" {
				switch mode {
				case "missing-required-family":
					delete(source.fixtureSource, "comments")
				case "missing-required-column":
					table := source.fixtureSource["issues"]
					table.Columns = table.Columns[1:]
					source.fixtureSource["issues"] = table
				case "altered-prefix-hash":
					source.fixtureSource["schema_migrations"].Rows[0]["content_hash"] = "forged"
				case "missing-prefix-row":
					table := source.fixtureSource["schema_migrations"]
					table.Rows = table.Rows[1:]
					source.fixtureSource["schema_migrations"] = table
				case "unsupported-main":
					table := source.fixtureSource["schema_migrations"]
					table.Rows = table.Rows[:65]
					source.fixtureSource["schema_migrations"] = table
				}
				if _, err := Build(context.Background(), source, inventory); err == nil {
					t.Fatal("incomplete or unsupported source schema accepted")
				}
				return
			}
			data := buildFixture(t, source, inventory)
			manifest, native := nativeCapture(t, data)
			switch mode {
			case "missing-profile":
				native.SchemaProfile = nil
			case "missing-absence":
				native.SchemaProfile.Absences = native.SchemaProfile.Absences[1:]
			case "invented-absence":
				native.SchemaProfile.Absences = append(native.SchemaProfile.Absences, NativeSchemaAbsence{Family: "comments", IntroducedIn: 72, Reason: AbsentBySourceSchema})
			case "reordered-absence":
				native.SchemaProfile.Absences[0], native.SchemaProfile.Absences[1] = native.SchemaProfile.Absences[1], native.SchemaProfile.Absences[0]
			case "wrong-introduced":
				native.SchemaProfile.Absences[0].IntroducedIn = 67
			case "wrong-reason":
				native.SchemaProfile.Absences[0].Reason = "gap"
			case "wrong-ignored":
				native.SchemaProfile.IgnoredVersion = 28
			case "wrong-manifest":
				manifest.MainVersion, manifest.IgnoredVersion = 72, 28
			case "synthetic-table":
				native.Current["issue_versions"] = NativeTable{Columns: columnsFor("issue_versions"), Rows: []Row{}}
			case "synthetic-null-column":
				table := native.Current["issues"]
				table.Columns = append(table.Columns, "participation_generation")
				for _, row := range table.Rows {
					row["participation_generation"] = nil
				}
				native.Current["issues"] = table
			}
			members := archiveMembers(t, data)
			members[NativeHistoryMember], members["manifest.json"] = mustJSON(t, native), mustJSON(t, manifest)
			if _, err := Validate(bytes.NewReader(reframeSourcePlaneArchive(t, members))); err == nil {
				t.Fatal("forged source schema profile accepted")
			}
		})
	}
}

func TestNativeSchema72LegacyWireRemainsLiteral(t *testing.T) {
	source, inventory := nativeHistoryFixtureForTest(t)
	data := buildFixture(t, source, inventory)
	manifest, native := nativeCapture(t, data)
	if manifest.MainVersion != 72 || manifest.IgnoredVersion != 28 || native.SchemaProfile != nil || bytes.Contains(archiveMembers(t, data)[NativeHistoryMember], []byte(`"schema_profile"`)) || len(native.Current) != 33 {
		t.Fatal("legacy v72 wire or current census changed")
	}
}
