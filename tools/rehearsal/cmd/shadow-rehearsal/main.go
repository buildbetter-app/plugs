package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/buildbetter-app/plugs/tools/rehearsal"
)

func main() { os.Exit(run()) }

func run() int {
	local := flag.Bool("local", false, "run three owned synthetic preparations; never qualifies R1")
	kind := flag.String("driver", "sqlite", "memory or sqlite")
	sha := flag.String("source-sha", "", "full candidate SHA")
	tree := flag.String("source-tree", "", "full candidate tree")
	gate := flag.String("gate-evidence", "", "sanitized trusted evidence JSON under /tmp/plug-swarm/work")
	flag.Parse()
	if flag.NArg() != 0 || (*local && *gate != "") {
		fmt.Fprintln(os.Stderr, "choose one preparation or evidence mode")
		return 2
	}
	if *gate != "" {
		evidence, err := rehearsal.LoadGateEvidence(*gate, *sha, *tree)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		now := time.Now().UTC()
		waits := rehearsal.EvaluateGate(now, evidence)
		if json.NewEncoder(os.Stdout).Encode(map[string]any{"evaluated_at": now, "waits": waits, "authority": "evidence checks only; no switch/deploy/merge authorization"}) != nil {
			return 1
		}
		if len(waits) > 0 {
			return 3
		}
		return 0
	}
	if !*local {
		fmt.Println("PLAN: --local --driver memory|sqlite --source-sha FULL_SHA --source-tree FULL_TREE runs three disposable authenticated HTTP/MCP preparations and one owned edition rollback. Same-session runs do not satisfy three consecutive nights, the shadow fortnight, production rollback or staging qualification.")
		return 0
	}
	if err := prepareDisposableRunfiles(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	report, err := rehearsal.RunDisposable(ctx, *kind, *sha, *tree)
	if err != nil {
		fmt.Fprintln(os.Stderr, "disposable preparation failed")
		return 1
	}
	if json.NewEncoder(os.Stdout).Encode(report) != nil {
		return 1
	}
	return 0
}

// Bazel declares the two real static edition outputs and source bytes as this
// executable's data. Set only missing runfile variables; the rehearsal package
// still verifies exact checkout identity, runfile bytes and binary paths.
func prepareDisposableRunfiles() error {
	if os.Getenv("TEST_SRCDIR") == "" {
		dir := os.Getenv("RUNFILES_DIR")
		if dir == "" {
			executable, err := os.Executable()
			if err != nil {
				return fmt.Errorf("Bazel runfiles required")
			}
			dir = executable + ".runfiles"
		}
		if !filepath.IsAbs(dir) {
			return fmt.Errorf("absolute Bazel runfiles required")
		}
		if err := os.Setenv("TEST_SRCDIR", dir); err != nil {
			return err
		}
	}
	if os.Getenv("TEST_WORKSPACE") == "" {
		if err := os.Setenv("TEST_WORKSPACE", "plugs"); err != nil {
			return err
		}
	}
	if os.Getenv("PLUG_SOURCE_MODULE_ROOT") == "" {
		root := os.Getenv("BUILD_WORKSPACE_DIRECTORY")
		if !filepath.IsAbs(root) {
			return fmt.Errorf("exact Bazel source checkout required")
		}
		if err := os.Setenv("PLUG_SOURCE_MODULE_ROOT", root); err != nil {
			return err
		}
	}
	for _, pair := range [][2]string{
		{"R1_OLD_RUNTIME_BINARY", "tools/rehearsal/cmd/rollback-runtime/old-runtime_/old-runtime"},
		{"R1_CANDIDATE_RUNTIME_BINARY", "tools/rehearsal/cmd/rollback-runtime/candidate-runtime_/candidate-runtime"},
	} {
		if value := os.Getenv(pair[0]); value != "" && value != pair[1] {
			return fmt.Errorf("conflicting Bazel edition output")
		} else if value == "" {
			if err := os.Setenv(pair[0], pair[1]); err != nil {
				return err
			}
		}
	}
	return nil
}
