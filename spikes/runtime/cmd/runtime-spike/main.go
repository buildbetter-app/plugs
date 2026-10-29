package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	runtimeprobe "github.com/buildbetter-app/plugs/spikes/runtime"
)

func main() {
	engine := flag.String("engine", "memory", "engine: memory or postgres")
	dsn := flag.String("postgres-dsn", "", "disposable PostgreSQL DSN (prefer E5_POSTGRES_DSN)")
	records := flag.Int("records", 200, "ready records")
	claimers := flag.Int("claimers", 200, "concurrent lazy claimers")
	mutators := flag.Int("mutators", 200, "concurrent mutators")
	deadline := flag.Duration("deadline", 90*time.Second, "overall probe deadline")
	runID := flag.String("run-id", "", "optional run namespace; empty generates a fresh ID")
	output := flag.String("output", "", "optional JSON output path")
	flag.Parse()

	config := runtimeprobe.ProbeConfig{Records: *records, Claimers: *claimers, Mutators: *mutators, Deadline: *deadline, RunID: *runID}
	ctx := context.Background()
	var (
		report runtimeprobe.ProbeReport
		err    error
	)
	switch *engine {
	case "memory":
		report, err = runtimeprobe.RunMemory(ctx, config)
	case "postgres":
		if *dsn == "" {
			*dsn = os.Getenv("E5_POSTGRES_DSN")
		}
		report, err = runtimeprobe.RunPostgres(ctx, *dsn, config)
	default:
		fmt.Fprintf(os.Stderr, "unknown engine %q\n", *engine)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *output != "" {
		if err := os.WriteFile(*output, append(encoded, '\n'), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Println(string(encoded))
}
