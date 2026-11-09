// native-physical-restore is an explicit disposable-target snapshot restore
// companion beside the native importer. It does not change bd import semantics.
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
	"time"

	"github.com/steveyegge/beads/internal/storage/physicalcapture"
	"github.com/steveyegge/beads/internal/storage/physicalrestore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (resultErr error) {
	dataDir := flag.String("data-dir", "", "existing owner-only disposable target data directory")
	database := flag.String("database", "", "exact existing target database")
	input := flag.String("manifest", "", "exact typed physical restore manifest")
	output := flag.String("readback-output", "", "exclusive actual physical-root readback output")
	disposable := flag.Bool("disposable-target", false, "declare the target quiescent and disposable")
	timeout := flag.Duration("timeout", 5*time.Minute, "restore deadline")
	flag.Parse()
	if flag.NArg() != 0 || !*disposable || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`).MatchString(*database) {
		return errors.New("explicit disposable target and database required")
	}
	for _, path := range []string{*dataDir, *input, filepath.Dir(*output)} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("canonical absolute target/input/output paths required")
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || resolved != path {
			return errors.New("target/input/output paths must exist without symlinks")
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm()&0077 != 0 {
			return errors.New("owner-only target/input/output paths required")
		}
	}
	if filepath.Base(*output) == "." || !filepath.IsAbs(*output) || filepath.Clean(*output) != *output {
		return errors.New("canonical readback output path required")
	}
	if _, err := os.Lstat(*output); !os.IsNotExist(err) {
		return errors.New("readback output already exists")
	}
	file, err := os.Open(*input)
	if err != nil {
		return err
	}
	manifest, readErr := physicalrestore.Read(file)
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	db, closeDriver, err := physicalcapture.Open(*dataDir, *database)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close(), closeDriver()) }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := physicalrestore.Restore(ctx, conn, manifest); err != nil {
		return err
	}
	// This is a NEW snapshot commit. It is never labeled source-history replay.
	if _, err := conn.ExecContext(ctx, "CALL DOLT_ADD('.')"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "CALL DOLT_COMMIT('-Am','physical restore snapshot; no source history replay')"); err != nil {
		return err
	}
	var rootID string
	if err := conn.QueryRowContext(ctx, "SELECT DOLT_HASHOF('HEAD')").Scan(&rootID); err != nil {
		return err
	}
	image, err := physicalcapture.ReadCommit(ctx, conn, rootID)
	if err != nil {
		return err
	}
	wanted := map[string]bool{}
	for _, table := range manifest.Tables {
		wanted[table.Name] = true
	}
	selected := physicalcapture.Image{CommitID: image.CommitID, ReadOnlyScope: image.ReadOnlyScope}
	for _, table := range image.Tables {
		if wanted[table.Name] {
			selected.Tables = append(selected.Tables, table)
		}
	}
	actual, err := physicalrestore.FromImage(selected)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(actual, "", "  ")
	if err != nil {
		return err
	}
	// Release all native resources BEFORE publishing readback bytes.
	if err := conn.Close(); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	if err := closeDriver(); err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Dir(*output))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	out, err := root.OpenFile(filepath.Base(*output), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := out.Write(data); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"format": physicalrestore.Format, "target_root": rootID, "actual_readback": *output, "history": "explicit snapshot; source history not replayed", "scope": "public/disposable target; no private-store or cutover qualification"})
}
