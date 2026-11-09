// Disposable backup-restore plumbing. Native engine and Beads commands are unchanged.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/versioncontrolops"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	backup := flag.String("backup-dir", "", "owned disposable copy of a native Dolt backup")
	scratch := flag.String("data-dir", "", "existing empty owner-only scratch data directory")
	database := flag.String("database", "", "new target database name")
	disposable := flag.Bool("disposable", false, "declare both paths disposable and owned")
	timeout := flag.Duration("timeout", 20*time.Minute, "restore deadline")
	flag.Parse()
	if flag.NArg() != 0 || !*disposable || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,127}$`).MatchString(*database) {
		return fmt.Errorf("explicit disposable source, scratch directory and new database required")
	}
	for _, path := range []string{*backup, *scratch} {
		resolved, err := filepath.EvalSymlinks(path)
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || err != nil || resolved != path {
			return fmt.Errorf("canonical absolute paths without symlinks required")
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return fmt.Errorf("existing owner-only directories required")
		}
	}
	for _, pair := range [][2]string{{*backup, *scratch}, {*scratch, *backup}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err != nil || rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("backup and scratch directories must be disjoint")
		}
	}
	entries, err := os.ReadDir(*scratch)
	if err != nil || len(entries) != 0 {
		return fmt.Errorf("scratch data directory must be empty")
	}
	manifest, err := os.Lstat(filepath.Join(*backup, "manifest"))
	if err != nil || !manifest.Mode().IsRegular() || manifest.Size() == 0 {
		return fmt.Errorf("native backup manifest required")
	}
	url, err := versioncontrolops.DirToFileURL(*backup)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	// Restore into a new database without --force. No current database is
	// selected; all filesystem changes stay in the empty scratch root.
	if err := restore(ctx, *scratch, url, *database); err != nil {
		return err
	}
	if err := verify(ctx, *scratch, *database); err != nil {
		return fmt.Errorf("restored database verification: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"format": "beads-native-backup-restore/1", "restored": true,
		"immutable_head_verified": true, "working_set_completeness": "not-inferred",
	})
}

func restore(ctx context.Context, dataDir, url, database string) (resultErr error) {
	db, closeDriver, err := physicalcapture.Open(dataDir, "")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, closeDriver()) }()
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, conn.Close()) }()
	return versioncontrolops.BackupRestore(ctx, conn, url, database, false)
}

func verify(ctx context.Context, dataDir, database string) (resultErr error) {
	db, closeDriver, err := physicalcapture.Open(dataDir, database)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, closeDriver()) }()
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, conn.Close()) }()
	head, err := versioncontrolops.CurrentCommit(ctx, conn)
	if err != nil {
		return err
	}
	image, err := physicalcapture.ReadCommit(ctx, conn, head)
	if err != nil {
		return err
	}
	if image.CommitID != head || !image.ReadOnlyScope {
		return fmt.Errorf("restored immutable head identity or read-only scope differs")
	}
	return nil
}
