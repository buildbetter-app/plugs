package authz

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// Keep the complete v6 workload. A slow case reports an error only after all
// cases and both engines have run, so its failure cannot hide the other data.
func TestLatency100K(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	var all []BenchCase
	for _, engine := range []Dialect{DialectSQLite, DialectPostgres} {
		t.Run(string(engine), func(t *testing.T) {
			var s *Store
			var err error
			if engine == DialectSQLite {
				s, err = OpenSQLite(filepath.Join(t.TempDir(), "authz.db"))
			} else {
				s, err = OpenPostgres(ctx, disposablePostgres(t))
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			var version string
			versionSQL := "SELECT sqlite_version()"
			if engine == DialectPostgres {
				versionSQL = "SELECT version()"
			}
			if err := s.DB.QueryRowContext(ctx, versionSQL).Scan(&version); err != nil {
				t.Fatal(err)
			}
			t.Logf("engine=%s version=%s beads=100000 depth=10 warmups=3 measured=20 page_target_ms=50 count_target_ms=250", engine, version)
			if err := s.Reset(ctx); err != nil {
				t.Fatal(err)
			}
			if err := s.Seed(ctx, 1); err != nil {
				t.Fatal(err)
			}
			var count, depth int
			if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*),MAX(depth) FROM bead").Scan(&count, &depth); err != nil {
				t.Fatal(err)
			}
			if count != 100000 || depth != 10 {
				t.Fatalf("actual corpus count/depth=%d/%d want 100000/10", count, depth)
			}
			for _, fraction := range []float64{1, .01, .001} {
				if err := s.SetVisibleFraction(ctx, fraction); err != nil {
					t.Fatal(err)
				}
				checkOracle(t, ctx, s, fraction)
				cases, err := s.Benchmark(ctx, fraction, 20)
				if err != nil {
					t.Fatal(err)
				}
				if len(cases) != 6 {
					t.Fatalf("cases=%d want 6", len(cases))
				}
				for _, c := range cases {
					all = append(all, c)
					raw, err := json.Marshal(c)
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("AUTHZ_CASE %s", raw)
					want := int(100000*fraction + .5)
					if c.Stats.Samples != 20 || c.VisibleRows != want || c.QualifiedRows != want {
						t.Errorf("invalid measured matrix/population: %+v", c)
					}
					if c.Query == Q5Claim {
						v := c.Claim
						if v == nil || !v.LockAcquired || !v.RecheckPassed || !v.Committed || v.RowsAffected != 1 || v.WarmupSamples != 3 || v.MeasuredSamples != 20 || v.ResetSamples != 23 || v.FreshCandidateCount != 20 || v.CommittedSampleCount != 20 || v.RecheckSampleCount != 20 || len(v.CandidateIDs) != 20 {
							t.Errorf("invalid full-Q5 accounting: %+v", v)
						}
					}
					target := 50.0
					if c.Query == Q6Count {
						target = 250
					}
					if c.Stats.P95 >= target {
						t.Errorf("LATENCY_FAIL engine=%s plan=%s query=%s fraction=%g p95_ms=%.3f target_ms=<%.0f", c.Engine, c.Plan, c.Query, c.Fraction, c.Stats.P95, target)
					}
				}
			}
			if err := s.SetVisibleFraction(ctx, 1); err != nil {
				t.Fatal(err)
			}
			if err := s.RefuseNormalClosureWrite(ctx, 2); err == nil || !strings.Contains(err.Error(), "closure_too_large") {
				t.Fatalf("normal closure refusal=%v", err)
			}
			for _, restricted := range []bool{false, true} {
				closure, err := s.RewriteClosureMigration(ctx, 2, 50011, restricted)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("AUTHZ_CLOSURE engine=%s operation=%s rows=%d duration_ms=%.3f", engine, closure.Operation, closure.Rows, closure.DurationMS)
				if closure.Rows != 50001 || !closure.MigrationClass {
					t.Errorf("invalid closure evidence: %+v", closure)
				}
			}
		})
	}
	if len(all) != 36 {
		t.Errorf("complete matrix cases=%d want 36", len(all))
	}
}

// Expected output comes from a separately enumerated authoritative fixture,
// never from the SQL grant projection or either candidate query's result.
func checkOracle(t *testing.T, ctx context.Context, s *Store, fraction float64) {
	t.Helper()
	wantN := int(100000*fraction + .5)
	ids := make([]int, 0, wantN)
	counts := make(map[int]int)
	for id := 1; id <= 100000; id++ {
		visible := fraction == 1 || (id >= 50003 && id < 50003+wantN)
		if visible {
			ids = append(ids, id)
			counts[1+id%100]++
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i]%3 != ids[j]%3 {
			return ids[i]%3 < ids[j]%3
		}
		return ids[i] < ids[j]
	})
	for _, plan := range []Plan{PlanA, PlanB} {
		rows, err := s.queryPage(ctx, plan, "principal:sample", 50)
		if err != nil {
			t.Fatal(err)
		}
		var got []int
		for rows.Next() {
			var id, bucket, rank, created int
			if err := rows.Scan(&id, &bucket, &rank, &created); err != nil {
				t.Fatal(err)
			}
			if bucket != id%3 || rank != id || created != id {
				t.Fatalf("unexpected stored ordering key for %d", id)
			}
			got = append(got, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, ids[:50]) {
			t.Fatalf("%s %s fraction=%g page IDs differ from independent oracle", s.Dialect, plan, fraction)
		}
		groups, err := s.queryCount(ctx, plan, "principal:sample")
		if err != nil {
			t.Fatal(err)
		}
		actual := make(map[int]int)
		for groups.Next() {
			var group, n int
			if err := groups.Scan(&group, &n); err != nil {
				t.Fatal(err)
			}
			if _, exists := actual[group]; exists {
				t.Fatalf("duplicate group %d", group)
			}
			actual[group] = n
		}
		if err := groups.Err(); err != nil {
			t.Fatal(err)
		}
		if err := groups.Close(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, counts) || len(actual) != 100 {
			t.Fatalf("%s %s fraction=%g counts differ from independent oracle", s.Dialect, plan, fraction)
		}
		evidence, err := s.claimNextEvidence(ctx, plan, "principal:sample")
		if err != nil {
			t.Fatal(err)
		}
		if evidence.CandidateID != ids[0] || !evidence.Committed || evidence.RowsAffected != 1 {
			t.Fatalf("claim differs from independent oracle: %+v", evidence)
		}
		var holder string
		if err := s.DB.QueryRowContext(ctx, s.bind("SELECT claimed_by FROM bead WHERE id=?"), ids[0]).Scan(&holder); err != nil || holder != "principal:sample" {
			t.Fatalf("committed claim readback holder=%q err=%v", holder, err)
		}
		if err := s.resetClaim(ctx, ids[0]); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("ORACLE_PASS engine=%s fraction=%g population=%d ordered_page=50 groups=100 committed_claim=%d", s.Dialect, fraction, wantN, ids[0])
}

func disposablePostgres(t *testing.T) string {
	t.Helper()
	const image = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"
	name := "plug-swarm-authz-latency-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	docker := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
		for _, value := range os.Environ() {
			key, _, _ := strings.Cut(value, "=")
			switch key {
			case "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH":
			default:
				cmd.Env = append(cmd.Env, value)
			}
		}
		return cmd.CombinedOutput()
	}
	if out, err := docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=authz", "-p", "127.0.0.1::5432", image); err != nil {
		t.Fatalf("disposable Postgres start: %v %s", err, out)
	}
	t.Cleanup(func() {
		if out, err := docker("rm", "-f", "-v", name); err != nil {
			t.Errorf("disposable Postgres cleanup: %v %s", err, out)
		}
	})
	out, err := docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatalf("disposable Postgres port: %v %s", err, out)
	}
	port := strings.TrimPrefix(strings.TrimSpace(string(out)), "127.0.0.1:")
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		t.Fatalf("invalid disposable loopback port: %q", port)
	}
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "authz"); err == nil {
			t.Logf("disposable PostgreSQL image=%s loopback_only=true cleanup=owned", image)
			return fmt.Sprintf("postgres://postgres@127.0.0.1:%s/authz?sslmode=disable", port)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("disposable Postgres readiness timed out")
	return ""
}
