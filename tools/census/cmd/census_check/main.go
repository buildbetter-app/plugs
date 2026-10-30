package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbetter-app/plugs/tools/census"
)

func main() {
	fixture := flag.String("census-fixture", "", "path to the reviewed census fixture")
	flag.Parse()
	fixturePath := *fixture
	if fixturePath == "" {
		fmt.Fprintln(os.Stderr, "census_check: provide -census-fixture")
		os.Exit(2)
	}
	if root := os.Getenv("TEST_SRCDIR"); root != "" {
		fixturePath = filepath.Join(root, filepath.FromSlash(fixturePath))
	} else if root := os.Getenv("RUNFILES_DIR"); root != "" {
		fixturePath = filepath.Join(root, filepath.FromSlash(fixturePath))
	}
	c, _, err := census.Load(fixturePath)
	if err == nil {
		err = census.Validate(c)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "census_check:", err)
		os.Exit(1)
	}
	digest, err := census.FixtureDigest(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "census_check:", err)
		os.Exit(1)
	}
	fmt.Printf("census: main=%d ignored=%d tables=%d columns=%d storeSources=%d configs=%d tests=%d digest=%s\n", c.Counts.MainMigrations, c.Counts.IgnoredMigrations, c.Counts.StoreTables, c.Counts.StoreColumns, c.Counts.StoreSources, c.Counts.ConfigSources, c.Counts.TestDeclarations, digest)
}
