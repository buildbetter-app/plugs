package census

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var censusFixture = flag.String("census-fixture", "", "Bazel runfile path of the reviewed census fixture")

func loadReviewed(t *testing.T) Census {
	t.Helper()
	if *censusFixture == "" && os.Getenv("TEST_SRCDIR") == "" {
		c, _, err := Load("testdata/census.json")
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	if *censusFixture == "" || os.Getenv("TEST_SRCDIR") == "" {
		t.Fatal("Bazel did not supply the census fixture runfile")
	}
	c, _, err := Load(filepath.Join(os.Getenv("TEST_SRCDIR"), filepath.FromSlash(*censusFixture)))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCanonicalCensus(t *testing.T) {
	c := loadReviewed(t)
	if err := Validate(c); err != nil {
		t.Fatal(err)
	}
	digest, err := FixtureDigest(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(digest) != 64 {
		t.Fatalf("fixture digest length = %d", len(digest))
	}
	if digest != ExpectedFixtureDigest {
		t.Fatalf("fixture digest = %s, want %s", digest, ExpectedFixtureDigest)
	}
}

func TestCensusRatchetRejectsRemovedKnownSource(t *testing.T) {
	c := loadReviewed(t)
	c.Schema.Main = c.Schema.Main[1:]
	if err := Validate(c); err == nil {
		t.Fatal("removed known schema source was accepted")
	}
}

func TestCensusRatchetRejectsUnknownSchemaInput(t *testing.T) {
	c := loadReviewed(t)
	c.Schema.Main[0].Path = "internal/storage/schema/migrations/0073_unreviewed.up.sql"
	if err := Validate(c); err == nil {
		t.Fatal("unknown schema input was accepted")
	}
}

func TestCensusRatchetRejectsUnknownConfigurationInput(t *testing.T) {
	c := loadReviewed(t)
	c.ConfigSources[0].Path = ".beads/unreviewed.yaml"
	if err := Validate(c); err == nil {
		t.Fatal("unknown configuration input was accepted")
	}
}

func TestCensusRatchetRejectsUnknownStoreTable(t *testing.T) {
	c := loadReviewed(t)
	c.StoreTables[0].Name = "unreviewed_table"
	if err := Validate(c); err == nil {
		t.Fatal("unknown store table was accepted")
	}
}

func TestCensusRatchetRejectsUnknownStoreColumn(t *testing.T) {
	c := loadReviewed(t)
	c.StoreTables[0].Columns[0].Name = "unreviewed_column"
	if err := Validate(c); err == nil {
		t.Fatal("unknown store column was accepted")
	}
}

func TestCensusRatchetRejectsDeletedStoreDisposition(t *testing.T) {
	c := loadReviewed(t)
	c.StoreTables[0].Disposition = ""
	if err := Validate(c); err == nil {
		t.Fatal("store table with deleted disposition was accepted")
	}
}

func TestCensusRatchetRejectsDeletedDisposition(t *testing.T) {
	c := loadReviewed(t)
	c.TestDispositions[0].Disposition = ""
	if err := Validate(c); err == nil {
		t.Fatal("test with deleted disposition was accepted")
	}
}
