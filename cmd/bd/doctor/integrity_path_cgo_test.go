//go:build cgo

package doctor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/internal/beads"
	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/testutil"
)

func TestCheckRepoFingerprint_UsesTargetRepoOutsideCWD(t *testing.T) {
	// This fixture requires the suite's disposable server, including its
	// explicit skip policy and the required-server lane's fail-closed policy.
	testutil.RequireDoltContainer(t)

	outerRepo := t.TempDir()
	targetRepo := t.TempDir()

	setupGitRepoInDir(t, outerRepo)
	setupGitRepoInDir(t, targetRepo)

	targetRepoID, err := beads.ComputeRepoIDForPath(targetRepo)
	if err != nil {
		t.Fatalf("ComputeRepoIDForPath(targetRepo) failed: %v", err)
	}

	beadsDir := filepath.Join(targetRepo, ".beads")
	database := "doctest_fingerprint_" + targetRepoID[:8]
	port := doctorTestServerPort()
	cfg := &configfile.Config{
		Database:       "dolt",
		Backend:        configfile.BackendDolt,
		DoltMode:       configfile.DoltModeServer,
		DoltDatabase:   database,
		DoltServerHost: "127.0.0.1",
		DoltServerPort: port,
	}
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("failed to save config: %v", err)
	}

	ctx := context.Background()
	// This is the fixture's initialization; the fingerprint read below must
	// still refuse its separately named absent database without creating it.
	store, err := dolt.New(ctx, &dolt.Config{
		Path:            filepath.Join(beadsDir, "dolt"),
		Database:        database,
		ServerHost:      "127.0.0.1",
		ServerPort:      port,
		CreateIfMissing: true,
	})
	if err != nil {
		t.Fatalf("open disposable fingerprint database: %v", err)
	}
	defer func() { _ = store.Close() }()
	t.Cleanup(func() { dropDoctorTestDatabase(database, port) })
	if _, err := os.Stat(filepath.Join(beadsDir, "dolt")); !os.IsNotExist(err) {
		t.Fatalf("server-mode control unexpectedly has a local database: %v", err)
	}

	if err := store.SetMetadata(ctx, "repo_id", targetRepoID); err != nil {
		t.Fatalf("failed to set repo_id metadata: %v", err)
	}

	runInDir(t, outerRepo, func() {
		check := CheckRepoFingerprint(targetRepo)

		if check.Status != StatusOK {
			t.Fatalf("Status = %q, want %q (message=%q detail=%q)", check.Status, StatusOK, check.Message, check.Detail)
		}
		if check.Message != "Verified ("+targetRepoID[:8]+")" {
			t.Fatalf("Message = %q, want %q", check.Message, "Verified ("+targetRepoID[:8]+")")
		}

		// A server-mode workspace with a genuinely absent database must still
		// refuse verification, without creating that database during the read.
		cfg.DoltDatabase = database + "_absent"
		if err := cfg.Save(beadsDir); err != nil {
			t.Fatal(err)
		}
		check = CheckRepoFingerprint(targetRepo)
		if check.Status != StatusWarning || check.Message != "Unable to open database" {
			t.Fatalf("absent database: %+v", check)
		}
		var count int
		if err := store.UnderlyingDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", cfg.DoltDatabase).Scan(&count); err != nil || count != 0 {
			t.Fatalf("fingerprint check created absent database: count=%d err=%v", count, err)
		}
	})
}
