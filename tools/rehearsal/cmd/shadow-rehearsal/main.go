package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
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
		fmt.Println("PLAN: --local --driver memory|sqlite --source-sha FULL_SHA --source-tree FULL_TREE runs three disposable authenticated HTTP/MCP preparations. Same-session runs do not satisfy three consecutive nights, the shadow fortnight, rollback or staging qualification.")
		return 0
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
