package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/physicalcapture"
	"github.com/buildbetter-app/plugs/internal/storage/physicalrestore"
)

func runUndo(preserved, target, database, input, readback, loss, receiptOutput string, disposable bool, timeout time.Duration) (resultErr error) {
	// Once storage accepted the switch-back commit, every later failure (close,
	// reopen, verification, source census or output) is explicitly indeterminate.
	// Preserve all joined causes; never describe that state as atomic refusal.
	accepted := false
	defer func() {
		if accepted && resultErr != nil {
			resultErr = &physicalrestore.Error{Kind: "post-commit-indeterminate", Row: -1, Cause: resultErr}
		}
	}()

	if flag.NArg() != 0 || !disposable || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`).MatchString(database) {
		return errors.New("explicit disposable preserved-copy undo and exact database required")
	}
	for _, path := range []string{preserved, input, filepath.Dir(target), filepath.Dir(readback), filepath.Dir(loss), filepath.Dir(receiptOutput)} {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || resolved != path {
			return errors.New("canonical owner-only undo paths without symlinks required")
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm()&0077 != 0 {
			return errors.New("owner-only undo paths required")
		}
	}
	for _, path := range []string{target, readback, loss, receiptOutput} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("canonical new undo output paths required")
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return errors.New("undo target/readback/loss output must not exist")
		}
	}
	if readback == loss || readback == target || loss == target || receiptOutput == target || receiptOutput == readback || receiptOutput == loss {
		return errors.New("distinct undo outputs required")
	}
	// No output may modify the preserved original, even after its byte census.
	// Parents were resolved above, so this check covers canonical descendants
	// without relying on a prefix which could also match a sibling directory.
	for _, output := range []string{target, readback, loss, receiptOutput} {
		if output == preserved || strings.HasPrefix(output, preserved+string(os.PathSeparator)) || strings.HasPrefix(preserved, output+string(os.PathSeparator)) {
			return &physicalrestore.Error{Kind: "preserved-source-overlap", Row: -1, Cause: errors.New("every undo target and output must be disjoint from the preserved source")}
		}
	}
	if info, err := os.Stat(filepath.Join(preserved, database, ".dolt")); err != nil || !info.IsDir() {
		return errors.New("existing preserved native database required; undo performs no initialization")
	}
	inputRoot, err := os.OpenRoot(filepath.Dir(input))
	if err != nil {
		return err
	}
	file, err := inputRoot.Open(filepath.Base(input))
	if err != nil {
		return errors.Join(err, inputRoot.Close())
	}
	request, readErr := physicalrestore.ReadUndo(file)
	if err := errors.Join(readErr, file.Close(), inputRoot.Close()); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	copied, err := physicalrestore.CopyPreservedStore(ctx, preserved, target)
	if err != nil {
		return err
	}
	db, closeDriver, err := physicalcapture.Open(target, database)
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return errors.Join(err, db.Close(), closeDriver())
	}
	receipt, operationErr := physicalrestore.Undo(ctx, conn, request)
	accepted = operationErr == nil
	closeErr := errors.Join(conn.Close(), db.Close(), closeDriver())
	if err := errors.Join(operationErr, closeErr); err != nil {
		return err
	}
	// A real native store close/reopen precedes all published success/loss bytes.
	db, closeDriver, err = physicalcapture.Open(target, database)
	if err != nil {
		return err
	}
	conn, err = db.Conn(ctx)
	if err != nil {
		return errors.Join(err, db.Close(), closeDriver())
	}
	actual, verifyErr := physicalrestore.VerifyUndo(ctx, conn, request, receipt)
	closeErr = errors.Join(conn.Close(), db.Close(), closeDriver())
	if err := errors.Join(verifyErr, closeErr); err != nil {
		return &physicalrestore.Error{Kind: "post-commit-indeterminate", Row: -1, Cause: err}
	}
	unchanged, err := physicalrestore.InventoryPreservedStore(ctx, preserved)
	if err != nil {
		return err
	}
	if unchanged.SHA256 != copied.SHA256 {
		return errors.New("preserved source byte inventory changed")
	}
	receipt.Reopened = true
	readbackBytes, err := json.MarshalIndent(actual, "", "  ")
	if err != nil {
		return err
	}
	receiptBytes, err := json.MarshalIndent(struct {
		Undo physicalrestore.UndoReceipt `json:"undo"`
		Copy physicalrestore.CopyReceipt `json:"preserved_copy"`
	}{receipt, copied}, "", "  ")
	if err != nil {
		return err
	}
	reportBytes, err := physicalrestore.UndoReportBytes(request)
	if err != nil {
		return err
	}
	for _, out := range []struct {
		path string
		data []byte
	}{{readback, readbackBytes}, {loss, reportBytes}, {receiptOutput, receiptBytes}} {
		if err := publishUndoOutput(out.path, out.data); err != nil {
			return &physicalrestore.Error{Kind: "receipt-publication-after-commit", Row: -1, Cause: err}
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"format": physicalrestore.UndoFormat, "preserved_original_commit": receipt.OriginalCommit, "switch_back_commit": receipt.SwitchBackCommit, "new_commits": receipt.CurrentCommits - receipt.OriginalCommits, "actual_readback": readback, "loss_report": loss, "undo_receipt": receiptOutput, "reopened": true, "preserved_source_unchanged": true, "scope": "public/disposable preserved-store copy; no private/live action"})
}
func publishUndoOutput(path string, data []byte) (resultErr error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	file, err := root.OpenFile(filepath.Base(path), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	return errors.Join(writeErr, syncErr, file.Close())
}
