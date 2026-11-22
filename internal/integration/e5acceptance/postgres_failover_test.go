package e5acceptance_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authzrpc"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
)

const (
	failoverDatabase            = "plug_e5"
	failoverReplicationUser     = "e5_repl"
	failoverReplicationPassword = "e5-repl"
)

type failoverContainers struct {
	network string
	primary string
	standby string

	primaryDSN string
	standbyDSN string
}

func TestPublicHTTPClaimNextTwoHundredAgentsPostgresFailover(t *testing.T) {
	runPostgresFailoverClaims(t, 1)
}

func TestPublicHTTPClaimNextTwoHundredAgentsPostgresLateFailover(t *testing.T) {
	runPostgresFailoverClaims(t, 100)
}

func runPostgresFailoverClaims(t *testing.T, killAfter int) {
	t.Helper()
	if os.Getenv("PLUG_E5_FAILOVER") != "1" {
		t.Skip("Lane A requires PLUG_E5_FAILOVER=1; the ordinary E5 target remains fixture-only")
	}
	const sessions = 200
	demand(t, killAfter > 0 && killAfter < sessions, "failover threshold must leave work before and after the kill")

	started := time.Now()
	containers := startFailoverContainers(t)

	// newLocalFixture owns the real WASI build, HTTP composition and primary
	// lease. The environment is set only after TestMain, so this test owns both
	// databases rather than the single-container default harness.
	e5PostgresDSN = containers.primaryDSN
	previousStorage := os.Getenv("PLUG_E5_STORAGE")
	if err := os.Setenv("PLUG_E5_STORAGE", "postgres"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Setenv("PLUG_E5_STORAGE", previousStorage) })
	primary := newLocalFixture(t)
	primary.allowClosedStoreFailure = true
	seedBulkReady(t, primary, 200)

	if err := containers.prepareStandby(t); err != nil {
		t.Fatal(err)
	}

	client := &failoverClient{current: primary, primary: primary, primaryAcked: map[string]publicWinnerEvidence{}, killAfter: killAfter}
	var failoverOnce sync.Once
	switchDone := make(chan struct{})
	client.switchDone = switchDone
	type takeoverResult struct {
		fixture            *localFixture
		killed             time.Time
		acknowledgedAtKill int
		err                error
	}
	takeover := make(chan takeoverResult, 1)

	// The acknowledged-claim threshold starts the outage with calls in flight.
	// Stop dispatching new calls to the dead endpoint during the explicit
	// topology switch; retain and account for any overlapping failed calls.
	trigger := func() {
		failoverOnce.Do(func() {
			defer close(switchDone)
			acknowledged := len(client.primaryAcknowledged())
			if acknowledged < killAfter {
				takeover <- takeoverResult{err: fmt.Errorf("primary acknowledgments before kill=%d want at least %d", acknowledged, killAfter)}
				return
			}
			killed := time.Now()
			if err := containers.killPrimary(); err != nil {
				takeover <- takeoverResult{err: err}
				return
			}
			replacement, err := containers.promoteAndOpen(t, primary)
			if err == nil {
				client.swap(replacement)
			}
			takeover <- takeoverResult{fixture: replacement, killed: killed, acknowledgedAtKill: acknowledged, err: err}
		})
	}
	client.onPrimaryCommit = trigger

	tokens := make([]string, sessions)
	agents := make([]model.PrincipalRef, sessions)
	principalKeys := make(map[string]struct{}, sessions)
	tokenSet := make(map[string]struct{}, sessions)
	workerKeys := make(map[string]struct{}, sessions)
	for i := 0; i < sessions; i++ {
		agents[i] = bulkAgent(i)
		principalKey, err := driver.PrincipalKey(agents[i])
		demand(t, err == nil, "failover principal key failed")
		if _, exists := principalKeys[principalKey]; exists {
			t.Fatalf("duplicate failover principal at worker %d", i)
		}
		principalKeys[principalKey] = struct{}{}
		key := fmt.Sprintf("%032x", 2000+i)
		workerKeys[key] = struct{}{}
		issued, err := primary.provider.Issue(agents[i], time.Now().Add(time.Minute))
		demand(t, err == nil, "failover authenticated agent issue failed")
		tokens[i] = issued.Token
		tokenSet[tokens[i]] = struct{}{}
	}
	demand(t, len(principalKeys) == sessions && len(tokenSet) == sessions && len(workerKeys) == sessions, "failover identity denominator is not distinct")

	type workerResult struct {
		final          publicClaimReply
		attempts       int
		conflicts      int
		busy           int
		rateLimited    int
		outageRetries  int
		outageCodes    map[string]int
		finalRetryable bool
		index          int
		key            string
		agent          model.PrincipalRef
	}
	start := make(chan struct{})
	results := make(chan workerResult, sessions)
	httpSem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		key := fmt.Sprintf("%032x", 2000+i)
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			<-start
			var last publicClaimReply
			outcome := workerResult{index: i, key: key, agent: agents[i], outageCodes: map[string]int{}}
			for attempt := 1; attempt <= publicRetryBudget; attempt++ {
				outcome.attempts++
				httpSem <- struct{}{}
				var interrupted bool
				last, interrupted = client.claim(key, tokens[i], agents[i])
				<-httpSem
				if interrupted && attempt < publicRetryBudget {
					// Only a failed call to the old composition overlapping
					// the deliberate kill gets this topology retry. Preserve
					// its class separately; never relabel it as public busy.
					outcome.outageRetries++
					outcome.outageCodes[last.errorCode]++
					continue
				}
				switch last.outcome() {
				case publicWriteConflict:
					outcome.conflicts++
				case publicBusy:
					outcome.busy++
				case publicRateLimited:
					outcome.rateLimited++
				default:
					outcome.final = last
					results <- outcome
					return
				}
				if attempt == publicRetryBudget {
					outcome.finalRetryable = true
					break
				}
				switch last.outcome() {
				case publicRateLimited:
					time.Sleep(time.Duration(3+i%7) * time.Millisecond)
				default:
					time.Sleep(time.Duration(1+i%4) * time.Millisecond)
				}
			}
			outcome.final = last
			results <- outcome
		}(i, key)
	}
	close(start)
	wg.Wait()
	close(results)

	var result takeoverResult
	select {
	case result = <-takeover:
	case <-time.After(time.Minute):
		t.Fatalf("primary workload never reached %d acknowledged claims to trigger failover", killAfter)
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.fixture == nil {
		t.Fatal("failover did not produce a standby fixture")
	}
	if result.killed.IsZero() {
		t.Fatal("failover kill timestamp was not recorded")
	}
	t.Logf("postgres failover trigger: kill_after=%d primary_acknowledged_at_kill=%d", killAfter, result.acknowledgedAtKill)
	demand(t, result.acknowledgedAtKill >= killAfter && result.acknowledgedAtKill < sessions, "primary was not killed at the requested mid-workload acknowledgment threshold")
	firstNewPrimaryCommit := client.firstNewPrimaryCommit()
	metrics := publicWorkloadSummary{Workers: sessions, Launched: sessions, Winners: map[string]int{}}
	expectedWinners := seededWinnerIDs(sessions)
	standby := result.fixture
	reader := standby.reader(t)
	winnerEvidence := map[string]publicWinnerEvidence{}
	errorCodes := map[string]int{}
	errorHints := map[string]int{}
	httpStatuses := map[string]int{}
	outageCodes := map[string]int{}
	outageRetries := 0
	acknowledgedMissing := 0
	primaryAcked := client.primaryAcknowledged()
	for winnerID, evidence := range primaryAcked {
		if err := validatePersistedClaim(reader, winnerID, evidence.principal, &evidence.result, true, model.Natural64(2)); err != nil {
			acknowledgedMissing++
		}
	}
	t.Logf("postgres failover retention: primary_acknowledged=%d acknowledged_missing_after_promotion=%d", len(primaryAcked), acknowledgedMissing)
	for outcome := range results {
		metrics.Received++
		httpStatuses[strconv.Itoa(outcome.final.status)]++
		metrics.Attempts += outcome.attempts
		metrics.Conflicts += outcome.conflicts
		metrics.Busy += outcome.busy
		metrics.RateLimited += outcome.rateLimited
		outageRetries += outcome.outageRetries
		for code, count := range outcome.outageCodes {
			outageCodes[code] += count
		}
		expectedKey := fmt.Sprintf("%032x", 2000+outcome.index)
		demand(t, outcome.key == expectedKey && outcome.final.key == expectedKey, fmt.Sprintf("failover worker %d response was correlated to the wrong key", outcome.index))
		demand(t, equalJSONValues(outcome.agent, agents[outcome.index]), fmt.Sprintf("failover worker %d principal correlation changed", outcome.index))
		switch outcome.final.outcome() {
		case publicSuccess:
			if outcome.finalRetryable {
				t.Fatalf("worker %d success was marked retry-exhausted", outcome.index)
			}
			if err := validatePublicClaimSuccess(outcome.final, outcome.agent, expectedKey, expectedWinners, reader, model.Natural64(2)); err != nil {
				t.Fatalf("failover worker %d success validation failed: %v", outcome.index, err)
			}
			metrics.Successes++
			if metrics.Winners[outcome.final.winnerID] != 0 {
				t.Fatalf("duplicate winner %s after promotion", outcome.final.winnerID)
			}
			metrics.Winners[outcome.final.winnerID]++
			winnerEvidence[outcome.final.winnerID] = publicWinnerEvidence{principal: outcome.agent, result: outcome.final.commit.Results[0]}
		case publicNull:
			metrics.Nulls++
		case publicWriteConflict, publicBusy, publicRateLimited:
			metrics.TerminalRetryable++
			errorCodes[outcome.final.errorCode]++
			errorHints[sanitizePublicHint(outcome.final.errorHint)]++
		default:
			metrics.Errors++
			code := outcome.final.errorCode
			if code == "" {
				if outcome.final.protocolError != "" {
					code = "protocol_error"
				} else {
					code = "other_error"
				}
			}
			errorCodes[code]++
			errorHints[sanitizePublicHint(outcome.final.errorHint)]++
		}
	}
	// Retain sanitized outcome evidence even when an unchanged assertion below
	// fails; never log response bodies, tokens, or raw protocol errors.
	promotedHead := "<unavailable>"
	if observed, err := standby.store.Meta(context.Background()); err == nil {
		promotedHead = observed.Head.String()
	}
	t.Logf("postgres failover claimNext partial/final: launched=%d received=%d A=%d C=%d B=%d L=%d S=%d N=%d E=%d X=%d O=%d primary_attempts=%d promoted_attempts=%d primary_acknowledged=%d new_primary_commit_observed=%t promoted_head=%s http_statuses=%s error_codes=%s error_hints=%s outage_error_codes=%s", metrics.Launched, metrics.Received, metrics.Attempts, metrics.Conflicts, metrics.Busy, metrics.RateLimited, metrics.Successes, metrics.Nulls, metrics.Errors, metrics.TerminalRetryable, outageRetries, client.primaryAttempts.Load(), client.promotedAttempts.Load(), len(client.primaryAcknowledged()), !firstNewPrimaryCommit.IsZero(), promotedHead, formatPublicCounters(httpStatuses), formatPublicCounters(errorCodes), formatPublicCounters(errorHints), formatPublicCounters(outageCodes))
	// O counts real failed HTTP attempts during the controlled outage. The
	// original RPC and final-outcome accounting still applies to A-O, with
	// the full equation checked explicitly so no attempt disappears.
	demand(t, metrics.Attempts == metrics.Conflicts+metrics.Busy+metrics.RateLimited+metrics.Successes+metrics.Nulls+metrics.Errors+outageRetries, "failover attempt accounting mismatch including outage retries")
	rpcMetrics := metrics
	rpcMetrics.Attempts -= outageRetries
	if err := rpcMetrics.validateAccounting(false); err != nil {
		t.Fatal(err)
	}
	demand(t, client.primaryAttempts.Load()+client.promotedAttempts.Load() == int64(metrics.Attempts), "failover route attempt accounting mismatch")
	demand(t, len(client.primaryAcknowledged()) > 0, "primary acknowledged no claims before kill")
	demand(t, client.promotedAttempts.Load() > 0, "no claims reached the promoted composition")
	demand(t, metrics.Successes == sessions, fmt.Sprintf("exactly one successful outcome per agent required, successes=%d", metrics.Successes))
	demand(t, validateWinnerSet(metrics.Winners, expectedWinners, sessions) == nil, "promoted winner set is not exactly the seeded distinct set")
	for winnerID, evidence := range winnerEvidence {
		if err := validatePersistedClaim(reader, winnerID, evidence.principal, &evidence.result, true, model.Natural64(2)); err != nil {
			t.Fatalf("promoted persisted winner %s failed validation: %v", winnerID, err)
		}
	}
	meta, err := standby.store.Meta(context.Background())
	demand(t, err == nil, "promoted metadata unavailable")
	demand(t, meta.Head.String() == strconv.Itoa(sessions+2), fmt.Sprintf("promoted head=%s want=%d", meta.Head, sessions+2))
	demand(t, acknowledgedMissing == 0, fmt.Sprintf("acknowledged commits missing after promotion=%d", acknowledgedMissing))
	select {
	case <-primary.lease.Lost():
	case <-time.After(driver.LeaseTTL):
		t.Fatal("old writer did not observe lease loss")
	}
	demand(t, standby.lease.Epoch() > primary.lease.Epoch(), "promotion did not advance the writer lease epoch")
	staleTx, staleErr := standby.store.Write(context.Background(), primary.lease)
	if staleTx != nil {
		_ = staleTx.Close()
	}
	var fenced driver.ErrFenced
	demand(t, errors.As(staleErr, &fenced), "promoted store accepted the stale writer lease")
	staleReply := publicClaimNextToken(primary, fmt.Sprintf("%032x", 9999), tokens[0], closedStart)
	demand(t, staleReply.outcome() == publicBusy && staleReply.errorHint == "active storage lease was lost", "old composition did not fence requests after lease loss")
	afterFence, err := standby.store.Meta(context.Background())
	demand(t, err == nil && afterFence.Head == meta.Head, "stale writer probe changed the promoted head")
	demand(t, !firstNewPrimaryCommit.IsZero(), "no successful commit observed after promotion")
	newPrimarySeconds := firstNewPrimaryCommit.Sub(result.killed).Seconds()
	demand(t, newPrimarySeconds >= 0, fmt.Sprintf("new-primary commit preceded kill: %.3fs", newPrimarySeconds))
	t.Logf("postgres failover claimNext: launched=%d received=%d A=%d C=%d B=%d L=%d S=%d N=%d E=%d X=%d distinct_winners=%d acknowledged_missing_after_promotion=%d kill_to_first_new_primary_commit_seconds=%.3f; old writer fenced by lease epoch; pinned image=%s elapsed=%.3fs", metrics.Launched, metrics.Received, metrics.Attempts, metrics.Conflicts, metrics.Busy, metrics.RateLimited, metrics.Successes, metrics.Nulls, metrics.Errors, metrics.TerminalRetryable, len(metrics.Winners), acknowledgedMissing, newPrimarySeconds, e5PostgresImage, time.Since(started).Seconds())
}

type failoverClient struct {
	mu               sync.RWMutex
	current          *localFixture
	primary          *localFixture
	firstNewCommit   atomic.Int64
	primaryAcked     map[string]publicWinnerEvidence
	primaryAttempts  atomic.Int64
	promotedAttempts atomic.Int64
	killAfter        int
	switching        bool
	switchDone       <-chan struct{}
	onPrimaryCommit  func()
}

func (c *failoverClient) swap(next *localFixture) {
	c.mu.Lock()
	c.current = next
	c.mu.Unlock()
}

func (c *failoverClient) firstNewPrimaryCommit() time.Time {
	ns := c.firstNewCommit.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (c *failoverClient) primaryAcknowledged() map[string]publicWinnerEvidence {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copy := make(map[string]publicWinnerEvidence, len(c.primaryAcked))
	for winner, evidence := range c.primaryAcked {
		copy[winner] = evidence
	}
	return copy
}

func (c *failoverClient) claim(key, token string, agent model.PrincipalRef) (publicClaimReply, bool) {
	c.mu.RLock()
	if c.switching {
		c.mu.RUnlock()
		<-c.switchDone
		c.mu.RLock()
	}
	f := c.current
	primary := c.primary
	c.mu.RUnlock()
	if f == primary {
		c.primaryAttempts.Add(1)
	} else {
		c.promotedAttempts.Add(1)
	}
	reply := publicClaimNextToken(f, key, token, closedStart)
	if reply.outcome() == publicSuccess {
		startSwitch := false
		c.mu.Lock()
		if f == primary {
			if c.primaryAcked == nil {
				c.primaryAcked = map[string]publicWinnerEvidence{}
			}
			c.primaryAcked[reply.winnerID] = publicWinnerEvidence{principal: agent, result: reply.commit.Results[0]}
			if !c.switching && len(c.primaryAcked) >= c.killAfter {
				c.switching = true
				startSwitch = true
			}
		} else {
			c.firstNewCommit.CompareAndSwap(0, time.Now().UnixNano())
		}
		c.mu.Unlock()
		if startSwitch {
			go c.onPrimaryCommit()
		}
	}
	c.mu.RLock()
	overlap := f == primary && c.switching
	c.mu.RUnlock()
	interrupted := false
	if overlap && reply.outcome() == publicOther && reply.protocolError == "" {
		switch reply.errorCode {
		case "authz_unavailable", "internal_error", "subscriber_unavailable":
			interrupted = true
		}
	}
	return reply, interrupted
}

var closedStart = func() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

func startFailoverContainers(t *testing.T) *failoverContainers {
	t.Helper()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	c := &failoverContainers{
		network: "plug-swarm-e5-failover-" + suffix,
		primary: "plug-swarm-e5-failover-primary-" + suffix,
		standby: "plug-swarm-e5-failover-standby-" + suffix,
	}
	if output, err := e5Docker("network", "create", c.network); err != nil {
		t.Fatalf("failover network create failed: %v %s", err, output)
	}
	t.Cleanup(func() {
		_, _ = e5Docker("rm", "-f", "-v", c.primary, c.standby)
		_, _ = e5Docker("network", "rm", c.network)
	})
	if output, err := e5Docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", c.primary, "--hostname", "primary", "--network", c.network, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB="+failoverDatabase, "-p", "127.0.0.1::5432", e5PostgresImage, "postgres", "-c", "wal_level=replica", "-c", "max_wal_senders=10", "-c", "max_replication_slots=2", "-c", "hot_standby=on", "-c", "listen_addresses=*"); err != nil {
		t.Fatalf("failover primary start failed: %v %s", err, output)
	}
	if output, err := e5Docker("run", "-d", "--rm", "--label", "plug.test.run="+os.Getenv("PLUG_TEST_CONTAINER_LABEL"), "--name", c.standby, "--hostname", "standby", "--network", c.network, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB="+failoverDatabase, "-p", "127.0.0.1::5432", e5PostgresImage, "tail", "-f", "/dev/null"); err != nil {
		t.Fatalf("failover standby shell start failed: %v %s", err, output)
	}
	primaryPort := containerPort(t, c.primary)
	standbyPort := containerPort(t, c.standby)
	c.primaryDSN = "postgres://postgres@127.0.0.1:" + primaryPort + "/" + failoverDatabase + "?sslmode=disable"
	c.standbyDSN = "postgres://postgres@127.0.0.1:" + standbyPort + "/" + failoverDatabase + "?sslmode=disable"
	waitPostgres(t, c.primary, failoverDatabase)
	if output, err := e5Docker("exec", c.primary, "psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", "CREATE ROLE "+failoverReplicationUser+" WITH REPLICATION LOGIN PASSWORD '"+failoverReplicationPassword+"'"); err != nil {
		t.Fatalf("replication role creation failed: %v %s", err, output)
	}
	if output, err := e5Docker("exec", c.primary, "sh", "-c", "printf 'host replication e5_repl 0.0.0.0/0 trust\\n' >> /var/lib/postgresql/data/pg_hba.conf"); err != nil {
		t.Fatalf("replication pg_hba update failed: %v %s", err, output)
	}
	if output, err := e5Docker("exec", "-u", "postgres", c.primary, "pg_ctl", "-D", "/var/lib/postgresql/data", "reload"); err != nil {
		t.Fatalf("replication pg_hba reload failed: %v %s", err, output)
	}
	return c
}

func (c *failoverContainers) prepareStandby(t *testing.T) error {
	t.Helper()
	// The idle standby bypasses the image's postgres initialization. Prepare an
	// empty, postgres-owned directory; pg_basebackup creates PG_VERSION below.
	if output, err := e5Docker("exec", c.standby, "install", "-d", "-m", "0700", "-o", "postgres", "-g", "postgres", "/var/lib/postgresql/data"); err != nil {
		return fmt.Errorf("standby data directory preparation: %v %s", err, output)
	}
	if output, err := e5Docker("exec", c.standby, "sh", "-c", "rm -rf /var/lib/postgresql/data/*"); err != nil {
		return fmt.Errorf("standby data reset: %v %s", err, output)
	}
	if output, err := e5Docker("exec", "-u", "postgres", "-e", "PGPASSWORD="+failoverReplicationPassword, c.standby, "pg_basebackup", "-h", "primary", "-U", failoverReplicationUser, "-D", "/var/lib/postgresql/data", "-Fp", "-Xs", "-P", "-R"); err != nil {
		return fmt.Errorf("standby basebackup: %v %s", err, output)
	}
	if output, err := e5Docker("exec", "-u", "postgres", "-d", c.standby, "postgres", "-D", "/var/lib/postgresql/data"); err != nil {
		return fmt.Errorf("standby postgres start: %v %s", err, output)
	}
	waitPostgres(t, c.standby, failoverDatabase)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, err := e5Docker("exec", c.standby, "psql", "-U", "postgres", "-d", failoverDatabase, "-Atqc", "SELECT pg_is_in_recovery() AND EXISTS (SELECT 1 FROM plug_postgres.meta) AND EXISTS (SELECT 1 FROM plug_postgres.lease WHERE owner <> '')")
		if err == nil && strings.TrimSpace(string(out)) == "t" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("standby did not replay the Plug lease and schema before timeout")
}

func (c *failoverContainers) killPrimary() error {
	output, err := e5Docker("kill", "-s", "SIGKILL", c.primary)
	if err != nil {
		return fmt.Errorf("primary kill: %v %s", err, output)
	}
	return nil
}

func (c *failoverContainers) promoteAndOpen(t *testing.T, primary *localFixture) (*localFixture, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if output, err := e5Docker("exec", "-u", "postgres", c.standby, "pg_ctl", "-D", "/var/lib/postgresql/data", "promote", "-w"); err != nil {
		return nil, fmt.Errorf("standby promotion: %v %s", err, output)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, err := e5Docker("exec", c.standby, "psql", "-U", "postgres", "-d", failoverDatabase, "-Atqc", "SELECT NOT pg_is_in_recovery()")
		if err == nil && strings.TrimSpace(string(out)) == "t" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	store, err := (postgres.Driver{}).Open(ctx, driver.Config{DSN: c.standbyDSN})
	if err != nil {
		return nil, fmt.Errorf("open promoted standby: %w", err)
	}
	lease, err := store.Lease(ctx, "e5-failover-standby", driver.LeaseTTL)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("acquire promoted standby lease: %w", err)
	}
	meta, err := store.Meta(ctx)
	if err != nil {
		_ = lease.Release(context.Background())
		_ = store.Close()
		return nil, fmt.Errorf("promoted standby metadata: %w", err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: model.RefByID("authority", model.Natural64(0))}
	base := primary.base
	base.Namespace = ns
	composition, err := authzrpc.New(ctx, authzrpc.Config{Store: store, Lease: lease, Provider: primary.provider, Facts: authzrpc.StaticFacts{Facts: base}, Namespace: ns, DefaultProject: "project", Mutators: primary.host.Mutators()})
	if err != nil {
		_ = lease.Release(context.Background())
		_ = store.Close()
		return nil, fmt.Errorf("compose promoted standby: %w", err)
	}
	replacement := &localFixture{store: store, lease: lease, provider: primary.provider, tokens: primary.tokens, base: base, composition: composition, host: primary.host, evidence: map[string]any{}}
	replacement.startLeaseRenewal()
	t.Cleanup(func() {
		replacement.stopRenewal()
		if err := <-replacement.renewed; err != nil {
			t.Errorf("promoted standby lease renewal: %v", err)
		}
		if err := lease.Release(context.Background()); err != nil {
			t.Errorf("promoted standby lease cleanup: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("promoted standby store cleanup: %v", err)
		}
	})
	return replacement, nil
}

func containerPort(t *testing.T, name string) string {
	t.Helper()
	output, err := e5Docker("port", name, "5432/tcp")
	if err != nil {
		t.Fatalf("container %s port lookup failed: %v %s", name, err, output)
	}
	_, port, err := net.SplitHostPort(strings.TrimSpace(string(output)))
	if err != nil || port == "" {
		t.Fatalf("container %s port=%q: %v", name, output, err)
	}
	return port
}

func waitPostgres(t *testing.T, name, database string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		// The image starts a temporary Unix-socket-only server during initdb.
		// TCP is available only after that server exits and final startup ends.
		if output, err := e5Docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", database); err == nil && strings.Contains(string(output), "accepting connections") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("container %s did not become ready", name)
}
