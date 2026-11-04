package cp7latency_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Binding uses the real SQL reader and installed chain first. Only the used
// SelectionAuthority reader is faulted, retaining Select instead of forcing
// the Query fallback. The underlying selector executes before the injected
// transport return, so its actual work and discarded result remain visible.
type compiledObservationReader struct {
	driver.Reader
	selector driver.SelectionReader
	fault    string
	executed atomic.Bool
	expired  atomic.Bool
}

func (r *compiledObservationReader) Select(ctx context.Context, plan driver.SelectionPlan) (driver.SelectionResult, error) {
	result, err := r.selector.Select(ctx, plan)
	if err != nil {
		return result, err
	}
	r.executed.Store(true)
	<-ctx.Done()
	r.expired.Store(errors.Is(ctx.Err(), context.DeadlineExceeded))
	// Return the real nonempty selection alongside the failure. No payload,
	// group, cursor or partial count may escape the product error boundary.
	return result, boundaryError(r.fault)
}

func TestCompiledSelectionObservationBoundary(t *testing.T) {
	const childEnv = "PLUG_CP7_COMPILED_OBSERVATION_BOUNDARY"
	if name := os.Getenv(childEnv); name != "" {
		f := newQualificationFixture(t, "sqlite", 11)
		var fault *compiledObservationReader
		f.views = qualificationViews(t, f, nil, func(cfg *viewops.Config) {
			bind := cfg.Selection.Bind
			cfg.Selection.Bind = func(ctx context.Context, p model.PrincipalRef, inline string) (*viewops.SelectionAuthority, error) {
				a, err := bind(ctx, p, inline)
				if err != nil {
					return a, err
				}
				selector, ok := a.Reader.(driver.SelectionReader)
				if !ok || a.Boundary == nil {
					t.Fatal("real compiled selector unavailable")
				}
				fault = &compiledObservationReader{Reader: a.Reader, selector: selector, fault: name}
				a.Reader = fault
				return a, nil
			}
		})
		before, err := f.store.Meta(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			after, err := f.store.Meta(context.Background())
			work := f.store.counts.work()
			if err != nil || before.Head.String() != after.Head.String() || fault == nil || !fault.executed.Load() || !fault.expired.Load() || work.Selections != 1 || work.SelectionSQL != 1 || work.SelectionControls != 1 || f.store.counts.value().Queries != 0 {
				t.Fatalf("compiled boundary lacked actual execution/deadline/no-commit proof: %v work=%+v", err, work)
			}
			t.Logf("CP7_COMPILED_BOUNDARY fault=%s expired=true selections=1 result_sql=1 control_rows=1 query_calls=0 head_unchanged=true", name)
		}()
		s := f.observe(t, "sqlite", 1, "Q6GroupedCount", "compiled_boundary", 0)
		if s.TargetPass || s.Status == "success" {
			t.Fatal("compiled failure qualified latency")
		}
		return
	}
	for _, name := range []string{"invalid_params", "internal_error", "untyped", "mixed_deadline", "deadline", "mapped_deadline", "cap", "write_pre"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "-test.run=^TestCompiledSelectionObservationBoundary$", "-test.v")
			cmd.Env = append(os.Environ(), childEnv+"="+name)
			out, err := cmd.CombinedOutput()
			t.Logf("compiled observation child %s:\n%s", name, out)
			if ctx.Err() != nil || !strings.Contains(string(out), "expired=true selections=1 result_sql=1 control_rows=1 query_calls=0 head_unchanged=true") {
				t.Fatal("compiled child execution boundary unproven")
			}
			unexpected := name == "invalid_params" || name == "internal_error" || name == "untyped" || name == "mixed_deadline"
			if unexpected {
				if err == nil || !strings.Contains(string(out), "unexpected product error retained above") || !strings.Contains(string(out), `"status":"correctness_error"`) {
					t.Fatal("compiled observation accepted an unexpected product failure")
				}
			} else if err != nil || !strings.Contains(string(out), `"target_pass":false`) {
				t.Fatalf("compiled permitted refusal lost its performance RED: %v", err)
			}
		})
	}
}
