package e5acceptance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/server/transport/httpjson"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

const (
	publicClaimMethod       = "bead.claimNext"
	publicRetryBudget       = 64
	bulkObserverID          = "00000000-0000-4000-8000-000000000999"
	lifecycleRetryBudget    = 64
	lifecycleRequestTimeout = 5 * time.Second
	lifecyclePhaseTimeout   = 45 * time.Second
	lifecycleConcurrency    = 8
)

type publicClaimReply struct {
	key            string
	status         int
	result         json.RawMessage
	resultPresent  bool
	errorCode      string
	errorHint      string
	errorRetryable bool
	body           []byte
	winnerID       string
	commit         *model.Commit
	protocolError  string
}

type publicOutcome string

const (
	publicSuccess               publicOutcome = "success"
	publicNull                  publicOutcome = "null"
	publicWriteConflict         publicOutcome = "write_conflict"
	publicBusy                  publicOutcome = "busy"
	publicRateLimited           publicOutcome = "rate_limited"
	publicSubscriberUnavailable publicOutcome = "subscriber_unavailable"
	publicOther                 publicOutcome = "other"
)

// TestPublicHTTPClaimNextContention sends concurrent authenticated requests
// through the fixture's real HTTP handler. The same principal is deliberate:
// this is a small public observation and does not claim unique winners.
func TestPublicHTTPClaimNextContention(t *testing.T) {
	f := newLocalFixture(t)
	const n = 8
	start := make(chan struct{})
	replies := make(chan publicClaimReply, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%032x", 1000+i)
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			replies <- publicClaimNext(f, key, start)
		}(key)
	}
	close(start)
	wg.Wait()
	close(replies)

	metrics := publicWorkloadSummary{Workers: n, Launched: n, Winners: map[string]int{}}
	reader := f.reader(t)
	principal := localAgent("user:sponsor")
	smallWinners := map[string]model.PrincipalRef{}
	for reply := range replies {
		metrics.Received++
		metrics.Attempts++
		switch reply.outcome() {
		case publicWriteConflict:
			metrics.Conflicts++
			metrics.TerminalRetryable++
		case publicBusy:
			metrics.Busy++
			metrics.TerminalRetryable++
		case publicRateLimited:
			metrics.RateLimited++
			metrics.TerminalRetryable++
		case publicNull:
			metrics.Nulls++
		case publicSuccess:
			metrics.Successes++
			if err := validatePublicClaimSuccess(reply, principal, reply.key, nil, reader, model.Natural64(1)); err != nil {
				t.Fatalf("small public success validation failed: %v", err)
			}
			metrics.Winners[reply.winnerID]++
			smallWinners[reply.winnerID] = principal
		default:
			metrics.Errors++
		}
	}
	if err := metrics.validateAccounting(false); err != nil {
		t.Fatal(err)
	}
	demand(t, metrics.Successes > 0, "no authenticated claimNext request succeeded")
	for winnerID, expected := range smallWinners {
		if err := validatePersistedClaim(reader, winnerID, expected, nil, false, model.Natural64(1)); err != nil {
			t.Fatalf("small final persisted winner %s failed validation: %v", winnerID, err)
		}
	}
	t.Logf("public authenticated claimNext contention: attempts=%d successes=%d terminal_nulls=%d write_conflicts=%d busy=%d rate_limited=%d other_errors=%d; repeated same-principal observation, fixture only", metrics.Attempts, metrics.Successes, metrics.Nulls, metrics.Conflicts, metrics.Busy, metrics.RateLimited, metrics.Errors)
}

func TestPublicHTTPClaimNextRetriesPreSubscriber(t *testing.T) {
	for _, wantCode := range []string{"busy", "subscriber_unavailable"} {
		t.Run(wantCode, func(t *testing.T) {
			f := newLocalFixture(t)
			const key = "00000000000000000000000000004001"
			by := bulkAgent(0)
			issued, err := f.provider.Issue(by, time.Now().Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			principal, err := driver.PrincipalKey(by)
			if err != nil {
				t.Fatal(err)
			}
			firstCtx := context.Background()
			var deadlineObserved atomic.Bool
			if wantCode == "busy" {
				seedBulkReady(t, f, 2)
				firstCtx = pluginhost.WithWASMCallbackTimerBeginObserver(firstCtx, func(ctx context.Context, _ <-chan struct{}) {
					deadlineObserved.Store(true)
					<-ctx.Done()
				})
			}
			before := f.head(t)
			attempts := 0
			result := runPublicClaimAttempts(0, func() publicClaimReply {
				attempts++
				if attempts != 1 {
					return publicClaimNextRequest(context.Background(), f, key, issued.Token)
				}
				reply := publicClaimNextRequest(firstCtx, f, key, issued.Token)
				if got := reply.localRetryOutcome(); got != publicOutcome(wantCode) {
					t.Fatalf("first claim outcome=%s code=%q retryable=%t, want retryable %s", got, reply.errorCode, reply.errorRetryable, wantCode)
				}
				if wantCode == "busy" && !deadlineObserved.Load() {
					t.Fatal("busy response did not reach the WASI callback deadline barrier")
				}
				reader := f.reader(t)
				receipt, err := reader.IdempotencyByKey(context.Background(), principal, "", publicClaimMethod, key)
				if err != nil || receipt != nil || reader.Snapshot().At.String() != before {
					t.Fatalf("refused claim published state: before=%s after=%s receipt=%t err=%v", before, reader.Snapshot().At, receipt != nil, err)
				}
				if wantCode == "subscriber_unavailable" {
					// The source-built guest needs its separate observer at revision 2.
					// Restore it after observing the refusal on the first HTTP request.
					seedBulkReady(t, f, 2)
				}
				return reply
			})
			retries := result.conflicts + result.busy + result.rateLimited + result.subscriberUnavailable
			if retries < 1 || result.attempts != 1+retries || result.finalRetryable {
				t.Fatalf("expected a same-key retry and completion: attempts=%d retries=%d exhausted=%t", result.attempts, retries, result.finalRetryable)
			}
			reader := f.reader(t)
			if err := validatePublicClaimSuccess(result.final, by, key, seededWinnerIDs(2), reader, model.Natural64(2)); err != nil {
				t.Fatal(err)
			}
			if err := validatePersistedClaim(reader, result.final.winnerID, by, &result.final.commit.Results[0], true, model.Natural64(2)); err != nil {
				t.Fatal(err)
			}
			t.Logf("claimNext: first_code=%s retries=%d refused_head=%s final_revision=%s durable_receipt=true", wantCode, retries, before, result.final.commit.Revision)
		})
	}
}

// TestPublicHTTPClaimNextTwoHundredAgents exercises the plan-sized boundary
// through the authenticated public composition. Every worker has a distinct
// sponsored-agent identity and idempotency key. The client retains the old
// cap-8 transport concurrency, 64-attempt retry budget, and backoffs.
func TestPublicHTTPClaimNextTwoHundredAgents(t *testing.T) {
	f := newLocalFixture(t)
	const sessions = 200
	seedBulkReady(t, f, sessions)
	lifecycleIDs, lifecyclePrincipals := seedLifecycleReady(t, f, sessions)

	tokens := make([]string, sessions)
	agents := make([]model.PrincipalRef, sessions)
	principalKeys := make(map[string]struct{}, sessions)
	tokenSet := make(map[string]struct{}, sessions)
	workerKeys := make(map[string]struct{}, sessions)
	for i := 0; i < sessions; i++ {
		agents[i] = bulkAgent(i)
		principalKey, err := driver.PrincipalKey(agents[i])
		demand(t, err == nil, "bulk principal key failed")
		if _, exists := principalKeys[principalKey]; exists {
			t.Fatalf("duplicate sponsored principal at worker %d", i)
		}
		principalKeys[principalKey] = struct{}{}
		key := fmt.Sprintf("%032x", 2000+i)
		if _, exists := workerKeys[key]; exists {
			t.Fatalf("duplicate worker idempotency key at worker %d", i)
		}
		workerKeys[key] = struct{}{}
		issued, err := f.provider.Issue(agents[i], time.Now().Add(time.Minute))
		demand(t, err == nil, "bulk authenticated agent issue failed")
		tokens[i] = issued.Token
		if _, exists := tokenSet[tokens[i]]; exists {
			t.Fatalf("duplicate in-memory token at worker %d", i)
		}
		tokenSet[tokens[i]] = struct{}{}
	}
	demand(t, len(principalKeys) == sessions && len(tokenSet) == sessions && len(workerKeys) == sessions, "bulk identity denominator is not distinct")

	start := make(chan struct{})
	ready := make(chan struct{})
	close(ready)
	httpSem := make(chan struct{}, 8)
	type workerResult struct {
		publicClaimAttempts
		index int
		key   string
		agent model.PrincipalRef
	}
	results := make(chan workerResult, sessions)
	var wg sync.WaitGroup
	lifecycleStart := make(chan struct{})
	lifecycleSummaries := make(chan struct {
		method  string
		summary publicLifecycleSummary
	}, 3)
	go func() {
		<-lifecycleStart
		for phase, method := range []string{"bead.update", "bead.comment", "bead.close"} {
			summary := runPublicLifecyclePhase(t, f, lifecycleIDs, lifecyclePrincipals, method, phase)
			lifecycleSummaries <- struct {
				method  string
				summary publicLifecycleSummary
			}{method: method, summary: summary}
		}
	}()
	for i := 0; i < sessions; i++ {
		key := fmt.Sprintf("%032x", 2000+i)
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			<-start
			attempts := runPublicClaimAttempts(i, func() publicClaimReply {
				httpSem <- struct{}{}
				reply := publicClaimNextToken(f, key, tokens[i], ready)
				<-httpSem
				return reply
			})
			results <- workerResult{publicClaimAttempts: attempts, index: i, key: key, agent: agents[i]}
		}(i, key)
	}
	close(start)
	// Release both workloads together. Lifecycle operations target a separate
	// 200-record set, so this exercises concurrent storage and callback activity
	// without changing the claim winner contract.
	close(lifecycleStart)
	wg.Wait()
	close(results)

	metrics := publicWorkloadSummary{Workers: sessions, Launched: sessions, Winners: map[string]int{}}
	expectedWinners := seededWinnerIDs(sessions)
	reader := f.reader(t)
	winnerEvidence := map[string]publicWinnerEvidence{}
	errorCodes := map[string]int{}
	errorHints := map[string]int{}
	receiptRevisions := make(map[uint64]struct{}, sessions)
	var firstReceiptRevision, lastReceiptRevision uint64
	for outcome := range results {
		metrics.Received++
		metrics.Attempts += outcome.attempts
		metrics.Conflicts += outcome.conflicts
		metrics.Busy += outcome.busy
		metrics.RateLimited += outcome.rateLimited
		metrics.SubscriberUnavailable += outcome.subscriberUnavailable
		expectedKey := fmt.Sprintf("%032x", 2000+outcome.index)
		demand(t, outcome.key == expectedKey && outcome.final.key == expectedKey, fmt.Sprintf("worker %d response was correlated to the wrong key", outcome.index))
		demand(t, equalJSONValues(outcome.agent, agents[outcome.index]), fmt.Sprintf("worker %d principal correlation changed", outcome.index))
		switch outcome.final.localRetryOutcome() {
		case publicSuccess:
			demand(t, !outcome.finalRetryable, "successful response marked as retry exhaustion")
			if err := validatePublicClaimSuccess(outcome.final, outcome.agent, expectedKey, expectedWinners, reader, model.Natural64(2)); err != nil {
				t.Fatalf("bulk worker %d success validation failed: %v", outcome.index, err)
			}
			if metrics.Winners[outcome.final.winnerID] != 0 {
				t.Fatalf("bulk worker %d returned duplicate winner %s", outcome.index, outcome.final.winnerID)
			}
			revision, err := outcome.final.commit.Revision.Uint64()
			demand(t, err == nil && revision > 0, fmt.Sprintf("bulk worker %d omitted a valid receipt revision", outcome.index))
			if _, exists := receiptRevisions[revision]; exists {
				t.Fatalf("bulk worker %d returned duplicate receipt revision %d", outcome.index, revision)
			}
			if len(receiptRevisions) == 0 || revision < firstReceiptRevision {
				firstReceiptRevision = revision
			}
			lastReceiptRevision = max(lastReceiptRevision, revision)
			receiptRevisions[revision] = struct{}{}
			metrics.Successes++
			metrics.Winners[outcome.final.winnerID]++
			winnerEvidence[outcome.final.winnerID] = publicWinnerEvidence{principal: outcome.agent, result: outcome.final.commit.Results[0]}
		case publicNull:
			metrics.Nulls++
		case publicWriteConflict, publicBusy, publicRateLimited, publicSubscriberUnavailable:
			demand(t, outcome.finalRetryable, fmt.Sprintf("worker %d returned a retryable class without exhausting the budget", outcome.index))
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
	persistedChecks := 0
	persistedErrors := 0
	for winnerID, evidence := range winnerEvidence {
		persistedChecks++
		if err := validatePersistedClaim(reader, winnerID, evidence.principal, &evidence.result, true, model.Natural64(2)); err != nil {
			persistedErrors++
		}
	}
	observerRevision := "<unread>"
	if observerRows, err := reader.Get(context.Background(), []model.Ref{model.RefByID(bulkObserverID, model.Natural64(0))}); err == nil && len(observerRows) == 1 {
		observerRevision = observerRows[0].Revision.String()
	}
	t.Logf("public authenticated 200-agent claimNext partial/final: launched=%d received=%d A=%d C=%d B=%d L=%d subscriber_unavailable=%d S=%d N=%d E=%d X=%d retry_starts=%d distinct_principals=%d distinct_tokens=%d distinct_keys=%d distinct_winners=%d receipts=%d receipt_revision_min=%d receipt_revision_max=%d persisted_checks=%d persisted_errors=%d observer_revision=%s error_codes=%s error_hints=%s cap=8 retry_budget=64; source-built WASI/public fixture only", metrics.Launched, metrics.Received, metrics.Attempts, metrics.Conflicts, metrics.Busy, metrics.RateLimited, metrics.SubscriberUnavailable, metrics.Successes, metrics.Nulls, metrics.Errors, metrics.TerminalRetryable, metrics.Attempts-sessions, len(principalKeys), len(tokenSet), len(workerKeys), len(metrics.Winners), len(receiptRevisions), firstReceiptRevision, lastReceiptRevision, persistedChecks, persistedErrors, observerRevision, formatPublicCounters(errorCodes), formatPublicCounters(errorHints))
	if err := metrics.validateAccounting(true); err != nil {
		t.Fatal(err)
	}
	demand(t, validateWinnerSet(metrics.Winners, expectedWinners, sessions) == nil, "bulk winner set is not exactly the seeded distinct set")
	for winnerID, evidence := range winnerEvidence {
		if err := validatePersistedClaim(reader, winnerID, evidence.principal, &evidence.result, true, model.Natural64(2)); err != nil {
			t.Fatalf("bulk final persisted winner %s failed validation: %v", winnerID, err)
		}
	}

	observerRows, err := reader.Get(context.Background(), []model.Ref{model.RefByID(bulkObserverID, model.Natural64(0))})
	demand(t, err == nil && len(observerRows) == 1, "bulk observer record disappeared")
	demand(t, validateObserverDocument(observerRows[0].Body) == nil, "bulk observer was claimed or changed")
	demand(t, observerRows[0].Revision.String() == "2", fmt.Sprintf("bulk observer revision=%s want second changeset revision 2", observerRows[0].Revision))
	for winnerID, evidence := range winnerEvidence {
		if err := validateGuestReadRevision(evidence.result.Record, expectedBulkTitle(winnerID), observerRows[0].Revision); err != nil {
			t.Fatalf("bulk winner %s does not retain the actual observer revision: %v", winnerID, err)
		}
	}
	t.Logf("public authenticated 200-agent claimNext: launched=%d received=%d A=%d C=%d B=%d L=%d subscriber_unavailable=%d S=%d N=%d E=%d X=%d retry_starts=%d distinct_principals=%d distinct_tokens=%d distinct_keys=%d distinct_winners=%d receipts=%d cap=8 retry_budget=64; source-built WASI/public fixture only", metrics.Launched, metrics.Received, metrics.Attempts, metrics.Conflicts, metrics.Busy, metrics.RateLimited, metrics.SubscriberUnavailable, metrics.Successes, metrics.Nulls, metrics.Errors, metrics.TerminalRetryable, metrics.Attempts-sessions, len(principalKeys), len(tokenSet), len(workerKeys), len(metrics.Winners), metrics.Successes)

	// Lifecycle mutators run against a second 200-record set concurrently with
	// the claimers above. Each phase fans out all agents concurrently, uses a
	// bounded retry budget for typed write_conflict/busy responses, and gives
	// every request a cancellable deadline so a lock cycle is observable instead
	// of hanging the test.
	for range []int{0, 1, 2} {
		result := <-lifecycleSummaries
		summary := result.summary
		if summary.Attempts != summary.Operations+summary.Retries || summary.Retries != summary.Conflicts+summary.Busy {
			t.Fatalf("lifecycle %s retry accounting mismatch: %+v", result.method, summary)
		}
		if summary.MaxBusyRetries > lifecycleRetryBudget {
			t.Fatalf("lifecycle %s exceeded busy retry budget: max=%d budget=%d", result.method, summary.MaxBusyRetries, lifecycleRetryBudget)
		}
		if summary.Successes != len(lifecycleIDs) || summary.Errors != 0 || summary.Deadlines != 0 {
			t.Fatalf("lifecycle %s failed: %+v", result.method, summary)
		}
		t.Logf("E5 lifecycle %s: operations=%d successes=%d attempts=%d retries=%d write_conflicts=%d busy=%d max_busy_retries=%d errors=%d deadlines=%d deadlock_free=%t cap=%d", result.method, summary.Operations, summary.Successes, summary.Attempts, summary.Retries, summary.Conflicts, summary.Busy, summary.MaxBusyRetries, summary.Errors, summary.Deadlines, summary.Deadlines == 0, lifecycleConcurrency)
	}

	finalReader := f.reader(t)
	for index, lifecycleID := range lifecycleIDs {
		rows, err := finalReader.Get(context.Background(), []model.Ref{model.RefByID(lifecycleID, model.Natural64(0))})
		demand(t, err == nil && len(rows) == 1, fmt.Sprintf("lifecycle winner %s disappeared", lifecycleID))
		doc, err := pipeline.DecodeDocument(rows[0].Body)
		demand(t, err == nil, fmt.Sprintf("lifecycle winner %s body malformed", lifecycleID))
		demand(t, doc.Fields["title"] == fmt.Sprintf("e5-lifecycle-update-%03d", index), fmt.Sprintf("lifecycle winner %s update not durable", lifecycleID))
		demand(t, doc.Fields["status"] == "done", fmt.Sprintf("lifecycle winner %s close status=%v", lifecycleID, doc.Fields["status"]))
		commentBodies := make(map[string]bool, len(doc.Comments))
		for _, comment := range doc.Comments {
			commentBodies[comment.Body] = true
		}
		demand(t, commentBodies[fmt.Sprintf("e5-lifecycle-comment-%03d", index)], fmt.Sprintf("lifecycle winner %s comment not durable", lifecycleID))
		demand(t, commentBodies[fmt.Sprintf("e5-lifecycle-close-%03d", index)], fmt.Sprintf("lifecycle winner %s close reason not durable", lifecycleID))
	}
	t.Logf("E5 lifecycle final state: records=%d updated=%d comments=%d closed=%d concurrent_with_claimers=true deadlock_free=true", len(lifecycleIDs), len(lifecycleIDs), len(lifecycleIDs), len(lifecycleIDs))
}

type publicWriteReply struct {
	key            string
	status         int
	resultPresent  bool
	commit         *model.Commit
	errorCode      string
	errorRetryable bool
	errorHint      string
	protocolError  string
}

func (r publicWriteReply) retryable() bool {
	return r.errorRetryable && (r.errorCode == "write_conflict" || r.errorCode == "busy")
}

type publicLifecycleResult struct {
	final     publicWriteReply
	attempts  int
	retries   int
	conflicts int
	busy      int
	deadline  bool
}

type publicLifecycleSummary struct {
	Operations     int
	Successes      int
	Attempts       int
	Retries        int
	Conflicts      int
	Busy           int
	MaxBusyRetries int
	Errors         int
	Deadlines      int
	ErrorCodes     map[string]int
}

func runPublicLifecyclePhase(t *testing.T, f *localFixture, winnerIDs []string, winnerPrincipals map[string]model.PrincipalRef, method string, phase int) publicLifecycleSummary {
	t.Helper()
	phaseCtx, cancel := context.WithTimeout(context.Background(), lifecyclePhaseTimeout)
	defer cancel()
	start := make(chan struct{})
	results := make(chan publicLifecycleResult, len(winnerIDs))
	sem := make(chan struct{}, lifecycleConcurrency)
	var wg sync.WaitGroup
	for index, winnerID := range winnerIDs {
		index, winnerID := index, winnerID
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			select {
			case sem <- struct{}{}:
			case <-phaseCtx.Done():
				results <- publicLifecycleResult{deadline: true}
				return
			}
			result := runPublicLifecycleOperation(phaseCtx, f, winnerPrincipals[winnerID], winnerID, method, index, phase)
			<-sem
			results <- result
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-phaseCtx.Done():
		return publicLifecycleSummary{Operations: len(winnerIDs), Deadlines: len(winnerIDs), ErrorCodes: map[string]int{}}
	}
	close(results)

	summary := publicLifecycleSummary{Operations: len(winnerIDs), ErrorCodes: map[string]int{}}
	for result := range results {
		summary.Attempts += result.attempts
		summary.Retries += result.retries
		summary.Conflicts += result.conflicts
		summary.Busy += result.busy
		if result.busy > summary.MaxBusyRetries {
			summary.MaxBusyRetries = result.busy
		}
		if result.deadline {
			summary.Deadlines++
			continue
		}
		if result.final.protocolError == "" && result.final.resultPresent && result.final.commit != nil {
			summary.Successes++
		} else {
			summary.Errors++
			code := result.final.errorCode
			if code == "" {
				code = result.final.protocolError
			}
			summary.ErrorCodes[code]++
		}
	}
	return summary
}

func runPublicLifecycleOperation(ctx context.Context, f *localFixture, principal model.PrincipalRef, winnerID, method string, index, phase int) publicLifecycleResult {
	result := publicLifecycleResult{}
	for attempt := 1; attempt <= lifecycleRetryBudget; attempt++ {
		if ctx.Err() != nil {
			result.deadline = true
			return result
		}
		requestCtx, cancel := context.WithTimeout(ctx, lifecycleRequestTimeout)
		reply := publicLifecycleRequest(requestCtx, f, principal, winnerID, method, index, phase)
		cancel()
		result.attempts++
		result.final = reply
		if reply.retryable() {
			if reply.errorCode == "write_conflict" {
				result.conflicts++
			} else {
				result.busy++
			}
			if attempt == lifecycleRetryBudget {
				return result
			}
			result.retries++
			select {
			case <-ctx.Done():
				result.deadline = true
				return result
			case <-time.After(time.Duration(1+index%5) * time.Millisecond):
			}
			continue
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			result.deadline = true
		}
		return result
	}
	return result
}

func publicLifecycleRequest(ctx context.Context, f *localFixture, principal model.PrincipalRef, winnerID, method string, index, phase int) publicWriteReply {
	id := model.RefByID(winnerID, model.Natural64(0))
	fields := map[string]any{}
	body := ""
	reason := ""
	switch method {
	case "bead.update":
		fields = map[string]any{"title": fmt.Sprintf("e5-lifecycle-update-%03d", index)}
	case "bead.comment":
		body = fmt.Sprintf("e5-lifecycle-comment-%03d", index)
	case "bead.close":
		reason = fmt.Sprintf("e5-lifecycle-close-%03d", index)
	}
	key := fmt.Sprintf("%032x", 100000+phase*1000+index)
	meta := model.WriteMeta{Method: method, IdempotencyKey: key, Project: "project", Return: "full"}
	var outcome port.WriteOutcome
	var err error
	switch method {
	case "bead.update":
		outcome, err = f.lifecycle.UpdateWithOutcome(ctx, principal, service.UpdateRequest{ID: id, Fields: fields}, meta)
	case "bead.comment":
		outcome, err = f.lifecycle.CommentWithOutcome(ctx, principal, service.CommentRequest{ID: id, Body: body}, meta)
	case "bead.close":
		outcome, err = f.lifecycle.CloseWithOutcome(ctx, principal, service.CloseRequest{ID: id, Reason: reason}, meta)
	default:
		return publicWriteReply{key: key, protocolError: "unknown lifecycle method"}
	}
	reply := publicWriteReply{key: key, status: http.StatusOK}
	if err == nil {
		if outcome.Commit.Revision.IsZero() {
			reply.protocolError = "lifecycle result is not a committed response"
			return reply
		}
		reply.resultPresent, reply.commit = true, &outcome.Commit
		return reply
	}
	var modelErr *model.Error
	if errors.As(err, &modelErr) {
		reply.errorCode, reply.errorRetryable, reply.errorHint = string(modelErr.Data.Code), modelErr.Data.Retryable, modelErr.Data.Hint
		return reply
	}
	reply.protocolError = err.Error()
	return reply
}

// TestE5StaleLeaseFencingAfterLifecycle exercises the same backend selected by
// PLUG_E5_STORAGE. It deliberately stages a write, releases its lease, and
// requires the stale transaction to be fenced before a fresh holder can read
// the unchanged record. The standalone store keeps the public fixture lease
// and renewal goroutine untouched.
func TestE5StaleLeaseFencingAfterLifecycle(t *testing.T) {
	store := openE5Store(t, e5StorageBackend())
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	old, err := store.Lease(ctx, "e5-stale-old", driver.LeaseTTL)
	demand(t, err == nil, "stale-fence old lease unavailable")
	ref := model.RefByID("e5-stale-lifecycle", model.Natural64(0))
	body := []byte(`{"id":"e5-stale-lifecycle","type":"issue","project":"project","fields":{"title":"stale","status":"todo"}}`)
	_, err = conformance.Seed(ctx, store, old, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body}})
	demand(t, err == nil, "stale-fence seed failed")
	tx, err := store.Write(ctx, old)
	demand(t, err == nil, "stale-fence transaction unavailable")
	_, err = tx.Lock(ctx, []driver.LockRequest{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, Mode: driver.LockUpdate}})
	demand(t, err == nil, "stale-fence record lock failed")
	_, err = tx.Head(ctx)
	demand(t, err == nil, "stale-fence head allocation failed")
	oldEpoch := old.Epoch()
	demand(t, old.Release(ctx) == nil, "stale-fence old lease release failed")
	staleErr := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"id":"e5-stale-lifecycle","type":"issue","project":"project","fields":{"title":"must-not-publish","status":"done"}}`)})
	var fenced driver.ErrFenced
	demand(t, errors.As(staleErr, &fenced), fmt.Sprintf("stale transaction was not fenced: %v", staleErr))
	_ = tx.Rollback(context.Background())
	next, err := store.Lease(ctx, "e5-stale-new", driver.LeaseTTL)
	demand(t, err == nil, "stale-fence fresh lease unavailable")
	defer next.Release(context.Background())
	demand(t, next.Epoch() > oldEpoch, "fresh lease did not advance epoch")
	meta, err := store.Meta(ctx)
	demand(t, err == nil, "stale-fence metadata unavailable")
	reader, err := store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	demand(t, err == nil, "stale-fence reader unavailable")
	rows, err := reader.Get(ctx, []model.Ref{ref})
	demand(t, err == nil && len(rows) == 1 && bytes.Contains(rows[0].Body, []byte(`"title":"stale"`)), "stale transaction changed durable record")
	demand(t, reader.Close() == nil, "stale-fence reader close failed")
	t.Logf("E5 stale lease: backend=%s fenced=true old_epoch=%d new_epoch=%d unchanged=true", e5StorageBackend(), oldEpoch, next.Epoch())
}

type publicWorkloadSummary struct {
	Workers               int
	Launched              int
	Received              int
	Attempts              int
	Conflicts             int
	Busy                  int
	RateLimited           int
	SubscriberUnavailable int
	Successes             int
	Nulls                 int
	Errors                int
	TerminalRetryable     int
	Winners               map[string]int
}

type publicClaimAttempts struct {
	final                 publicClaimReply
	attempts              int
	conflicts             int
	busy                  int
	rateLimited           int
	subscriberUnavailable int
	finalRetryable        bool
}

func runPublicClaimAttempts(worker int, call func() publicClaimReply) publicClaimAttempts {
	var result publicClaimAttempts
	for result.attempts < publicRetryBudget {
		result.attempts++
		result.final = call()
		outcome := result.final.localRetryOutcome()
		switch outcome {
		case publicWriteConflict:
			result.conflicts++
		case publicBusy:
			result.busy++
		case publicRateLimited:
			result.rateLimited++
		case publicSubscriberUnavailable:
			result.subscriberUnavailable++
		default:
			return result
		}
		if result.attempts == publicRetryBudget {
			result.finalRetryable = true
			return result
		}
		if outcome == publicRateLimited {
			time.Sleep(time.Duration(3+worker%7) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(1+worker%4) * time.Millisecond)
		}
	}
	return result
}

type publicWinnerEvidence struct {
	principal model.PrincipalRef
	result    model.OpResult
}

func validateWinnerSet(winners map[string]int, expected map[string]bool, workers int) error {
	if len(winners) != workers {
		return fmt.Errorf("distinct winners=%d want=%d", len(winners), workers)
	}
	for winner, count := range winners {
		if !expected[winner] {
			return fmt.Errorf("foreign winner %s", winner)
		}
		if count != 1 {
			return fmt.Errorf("winner %s occurred %d times", winner, count)
		}
	}
	for winner := range expected {
		if winners[winner] != 1 {
			return fmt.Errorf("seeded winner %s is missing", winner)
		}
	}
	return nil
}

func (s publicWorkloadSummary) validateAccounting(requireSuccess bool) error {
	if s.Launched != s.Workers {
		return fmt.Errorf("launched=%d want workers=%d", s.Launched, s.Workers)
	}
	if s.Received != s.Workers {
		return fmt.Errorf("received=%d want workers=%d", s.Received, s.Workers)
	}
	retryable := s.Conflicts + s.Busy + s.RateLimited + s.SubscriberUnavailable
	if s.Attempts != retryable+s.Successes+s.Nulls+s.Errors {
		return fmt.Errorf("attempt accounting mismatch: A=%d retryable=%d S=%d N=%d E=%d", s.Attempts, retryable, s.Successes, s.Nulls, s.Errors)
	}
	if s.Successes+s.Nulls+s.Errors+s.TerminalRetryable != s.Workers {
		return fmt.Errorf("final outcome accounting mismatch: S=%d N=%d E=%d X=%d workers=%d", s.Successes, s.Nulls, s.Errors, s.TerminalRetryable, s.Workers)
	}
	if s.Attempts-s.Workers != retryable-s.TerminalRetryable {
		return fmt.Errorf("retry-start accounting mismatch: A-workers=%d retryable-X=%d", s.Attempts-s.Workers, retryable-s.TerminalRetryable)
	}
	if requireSuccess && s.Successes != s.Workers {
		return fmt.Errorf("successful completion required: successes=%d want=%d (nulls=%d errors=%d exhausted=%d)", s.Successes, s.Workers, s.Nulls, s.Errors, s.TerminalRetryable)
	}
	return nil
}

func (r publicClaimReply) outcome() publicOutcome {
	if r.protocolError != "" {
		return publicOther
	}
	if r.errorCode != "" {
		if !r.errorRetryable {
			return publicOther
		}
		switch r.errorCode {
		case "write_conflict":
			return publicWriteConflict
		case "busy":
			return publicBusy
		case "rate_limited":
			return publicRateLimited
		default:
			return publicOther
		}
	}
	if !r.resultPresent {
		return publicOther
	}
	if bytes.Equal(bytes.TrimSpace(r.result), []byte("null")) {
		return publicNull
	}
	return publicSuccess
}

func (r publicClaimReply) localRetryOutcome() publicOutcome {
	if r.protocolError == "" && r.errorRetryable && r.errorCode == "subscriber_unavailable" {
		return publicSubscriberUnavailable
	}
	return r.outcome()
}

func (r publicClaimReply) retryable() bool {
	switch r.outcome() {
	case publicWriteConflict, publicBusy, publicRateLimited:
		return true
	default:
		return false
	}
}

func sanitizePublicHint(hint string) string {
	hint = strings.TrimSpace(hint)
	switch hint {
	case "", "snapshot changed during all four write attempts", "bead already has a live claim", "write pre phases exceeded their deadline", "active storage lease was lost":
		if hint == "" {
			return "<empty>"
		}
		return hint
	default:
		return "<other>"
	}
}

func formatPublicCounters(counts map[string]int) string {
	if len(counts) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func decodeJSONObject(data []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(data)))
	var object map[string]json.RawMessage
	if err := dec.Decode(&object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, errors.New("JSON-RPC root is not an object")
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("JSON-RPC response has trailing values")
		}
		return nil, err
	}
	return object, nil
}

func protocolReply(key string, status int, body []byte, err error) publicClaimReply {
	return publicClaimReply{key: key, status: status, body: append([]byte(nil), body...), protocolError: err.Error()}
}

// parsePublicClaimReply is shared by the real HTTP workload and focused
// negative cases. It never turns malformed input into a valid JSON null.
func parsePublicClaimReply(key string, status int, body []byte) publicClaimReply {
	reply := publicClaimReply{key: key, status: status, body: append([]byte(nil), body...)}
	object, err := decodeJSONObject(body)
	if err != nil {
		return protocolReply(key, status, body, fmt.Errorf("JSON-RPC envelope: %w", err))
	}
	for name := range object {
		switch name {
		case "jsonrpc", "id", "result", "error", "_meta":
		default:
			return protocolReply(key, status, body, fmt.Errorf("unexpected JSON-RPC member %q", name))
		}
	}
	var version string
	if raw, ok := object["jsonrpc"]; !ok || json.Unmarshal(raw, &version) != nil || version != "2.0" {
		return protocolReply(key, status, body, errors.New("JSON-RPC version is not 2.0"))
	}
	rawID, ok := object["id"]
	if !ok {
		return protocolReply(key, status, body, errors.New("JSON-RPC response omitted id"))
	}
	var responseID string
	if json.Unmarshal(rawID, &responseID) != nil || responseID != key {
		return protocolReply(key, status, body, errors.New("JSON-RPC response id does not match worker key"))
	}
	rawResult, hasResult := object["result"]
	rawError, hasError := object["error"]
	if hasResult == hasError {
		return protocolReply(key, status, body, errors.New("JSON-RPC response must contain exactly one of result or error"))
	}
	if hasResult {
		if status != http.StatusOK {
			return protocolReply(key, status, body, fmt.Errorf("result response used HTTP status %d", status))
		}
		reply.resultPresent = true
		reply.result = append(json.RawMessage(nil), rawResult...)
		if bytes.Equal(bytes.TrimSpace(rawResult), []byte("null")) {
			return reply
		}
		var commit model.Commit
		if err := json.Unmarshal(rawResult, &commit); err != nil {
			return protocolReply(key, status, body, fmt.Errorf("claimNext result: %w", err))
		}
		if commit.Revision.IsZero() || len(commit.Results) != 1 {
			return protocolReply(key, status, body, errors.New("claimNext result must contain one committed result"))
		}
		result := commit.Results[0]
		if !result.Ref.IsID() || result.Ref.ID == "" || result.Revision.IsZero() || len(bytes.TrimSpace(result.Record)) == 0 || bytes.Equal(bytes.TrimSpace(result.Record), []byte("null")) {
			return protocolReply(key, status, body, errors.New("claimNext result omitted a full claimed record"))
		}
		reply.commit = &commit
		reply.winnerID = result.Ref.ID
		return reply
	}

	var errorObject map[string]json.RawMessage
	if err := json.Unmarshal(rawError, &errorObject); err != nil || errorObject == nil {
		return protocolReply(key, status, body, errors.New("JSON-RPC error object is malformed"))
	}
	rawCode, codeOK := errorObject["code"]
	rawMessage, messageOK := errorObject["message"]
	rawData, dataOK := errorObject["data"]
	var numericCode json.Number
	if !codeOK || len(rawCode) == 0 || json.Unmarshal(rawCode, &numericCode) != nil || numericCode.String() == "" {
		return protocolReply(key, status, body, errors.New("JSON-RPC error code is missing or not numeric"))
	}
	var message string
	if !messageOK || len(rawMessage) == 0 || json.Unmarshal(rawMessage, &message) != nil {
		return protocolReply(key, status, body, errors.New("JSON-RPC error message is malformed"))
	}
	var dataObject map[string]json.RawMessage
	if !dataOK || json.Unmarshal(rawData, &dataObject) != nil || dataObject == nil {
		return protocolReply(key, status, body, errors.New("JSON-RPC error data is malformed"))
	}
	rawTypedCode, typedCodeOK := dataObject["code"]
	rawRetryable, retryableOK := dataObject["retryable"]
	var typedCode string
	var retryable bool
	if !typedCodeOK || json.Unmarshal(rawTypedCode, &typedCode) != nil || typedCode == "" || model.ErrorCode(typedCode).Validate() != nil || !retryableOK || json.Unmarshal(rawRetryable, &retryable) != nil {
		return protocolReply(key, status, body, errors.New("JSON-RPC error data lacks a valid typed code/retryability"))
	}
	reply.errorCode = typedCode
	reply.errorRetryable = retryable
	if rawHint, ok := dataObject["hint"]; ok {
		var hint string
		if json.Unmarshal(rawHint, &hint) == nil {
			reply.errorHint = sanitizePublicHint(hint)
		}
	}
	if status != http.StatusOK && !(status == http.StatusTooManyRequests && typedCode == "rate_limited" && retryable) {
		return protocolReply(key, status, body, fmt.Errorf("typed error used unexpected HTTP status %d", status))
	}
	return reply
}

func validatePublicClaimSuccess(reply publicClaimReply, expected model.PrincipalRef, key string, allowedWinners map[string]bool, reader driver.Reader, expectedReadRevision model.Revision) error {
	if reply.outcome() != publicSuccess || reply.commit == nil {
		return fmt.Errorf("reply is not a successful committed result: outcome=%s protocol=%s", reply.outcome(), reply.protocolError)
	}
	if reply.key != key || reply.winnerID == "" {
		return errors.New("successful result is not correlated to its idempotency key/winner")
	}
	if allowedWinners != nil && !allowedWinners[reply.winnerID] {
		return fmt.Errorf("winner %s is not one of the seeded records", reply.winnerID)
	}
	result := reply.commit.Results[0]
	if err := validateClaimDocument(result, expected, reply.winnerID, expectedReadRevision); err != nil {
		return err
	}
	principal, err := driver.PrincipalKey(expected)
	if err != nil {
		return fmt.Errorf("principal key: %w", err)
	}
	receipt, err := reader.IdempotencyByKey(context.Background(), principal, "", publicClaimMethod, key)
	if err != nil {
		return fmt.Errorf("receipt lookup: %w", err)
	}
	if err := validatePublicClaimReceiptShape(receipt, expected, key, *reply.commit, result); err != nil {
		return err
	}
	return nil
}

// validatePersistedClaim checks the latest stored state after all public
// requests finish. The small repeated-principal observation deliberately
// checks only the final state; a later success may legitimately advance the
// record revision beyond an earlier response. The distinct bulk workload
// additionally requires the final record to match its one successful response.
func validatePersistedClaim(reader driver.Reader, winnerID string, expected model.PrincipalRef, response *model.OpResult, strict bool, expectedReadRevision model.Revision) error {
	rows, err := reader.Get(context.Background(), []model.Ref{model.RefByID(winnerID, model.Natural64(0))})
	if err != nil {
		return fmt.Errorf("final winner lookup: %w", err)
	}
	if len(rows) != 1 {
		return fmt.Errorf("final winner rows=%d want=1", len(rows))
	}
	return validateStoredClaimRecord(rows[0], expected, winnerID, response, strict, expectedReadRevision)
}

func validateStoredClaimRecord(row driver.Record, expected model.PrincipalRef, winnerID string, response *model.OpResult, strict bool, expectedReadRevision model.Revision) error {
	if row.Ref.ID != winnerID || row.Revision.IsZero() || len(row.Body) == 0 {
		return errors.New("final stored winner omitted its ref, revision or body")
	}
	doc, err := pipeline.DecodeDocument(row.Body)
	if err != nil {
		return fmt.Errorf("final stored winner decode: %w", err)
	}
	if doc.ID != winnerID || doc.Fields["status"] != "in_progress" {
		return fmt.Errorf("final stored winner is not in_progress: id=%s status=%v", doc.ID, doc.Fields["status"])
	}
	if doc.Lease == nil || !equalJSONValues(doc.Lease.Principal, expected) {
		return errors.New("final stored winner actor does not match authenticated principal")
	}
	title, ok := doc.Fields["title"].(string)
	if !ok || !validGuestOutput(title, expectedBulkTitle(winnerID), expectedReadRevision) {
		return fmt.Errorf("final stored winner lacks the observed guest output: %q", title)
	}
	if response == nil {
		return nil
	}
	if response.Ref.ID != winnerID || response.Revision.IsZero() || len(response.Record) == 0 {
		return errors.New("successful response cannot establish final winner correspondence")
	}
	if strict {
		if row.Revision.String() != response.Revision.String() {
			return fmt.Errorf("final stored winner revision=%s response=%s", row.Revision, response.Revision)
		}
		responseDoc, err := pipeline.DecodeDocument(response.Record)
		if err != nil {
			return fmt.Errorf("successful response decode: %w", err)
		}
		if !equalJSONValues(doc, responseDoc) {
			return errors.New("final stored winner body does not match successful response")
		}
	}
	return nil
}

func validatePublicClaimReceiptShape(receipt *driver.IdempotencyRecord, expected model.PrincipalRef, key string, response model.Commit, responseResult model.OpResult) error {
	if receipt == nil {
		return errors.New("matching durable claim receipt is missing")
	}
	if receipt.ResponsePending {
		return errors.New("matching durable claim receipt is still pending")
	}
	if receipt.Changeset.Kind != model.ChangesetWrite || receipt.Changeset.IdempotencyKey != key || !equalJSONValues(receipt.Changeset.Principal, expected) {
		return errors.New("durable receipt principal, kind or idempotency key does not match")
	}
	if len(receipt.Changeset.Mutators) != 1 || receipt.Changeset.Mutators[0] != bindingID {
		return errors.New("durable receipt omitted the expected mutator binding")
	}
	if receipt.Changeset.Revision.String() != response.Revision.String() || !equalJSONValues(receipt.Commit, response) {
		return errors.New("durable receipt commit/revision does not match the public response")
	}
	if len(receipt.Commit.Results) != 1 || !equalJSONValues(receipt.Commit.Results[0], responseResult) {
		return errors.New("durable receipt result does not match the public ref/revision/record")
	}
	if len(receipt.Changeset.Changes) != 1 || !equalJSONValues(receipt.Changeset.Changes[0].Ref, responseResult.Ref) {
		return errors.New("durable receipt change ref does not match the public winner")
	}
	return nil
}

func validateClaimDocument(result model.OpResult, expected model.PrincipalRef, winnerID string, expectedReadRevision model.Revision) error {
	if result.Ref.ID != winnerID || result.Revision.IsZero() || len(result.Record) == 0 {
		return errors.New("public claim result omitted its ref, revision or record")
	}
	doc, err := pipeline.DecodeDocument(result.Record)
	if err != nil {
		return fmt.Errorf("claimed record decode: %w", err)
	}
	if doc.ID != winnerID || doc.Fields["status"] != "in_progress" {
		return fmt.Errorf("claimed record is not in the final in_progress state: id=%s status=%v", doc.ID, doc.Fields["status"])
	}
	if doc.Lease == nil || !equalJSONValues(doc.Lease.Principal, expected) {
		return errors.New("claimed record has no matching authenticated actor lease")
	}
	title, ok := doc.Fields["title"].(string)
	if !ok || !validGuestOutput(title, expectedBulkTitle(winnerID), expectedReadRevision) {
		return fmt.Errorf("claimed record lacks the real observed guest output: %q", title)
	}
	if result.SetBy["fields.title"] != bindingID {
		return errors.New("public response omitted the actual mutator binding for fields.title")
	}
	return nil
}

func validGuestOutput(title, expectedOriginalTitle string, expectedReadRevision model.Revision) bool {
	readRevision, ok := guestObservedRevision(title, expectedOriginalTitle)
	if !ok || readRevision != expectedReadRevision.String() {
		return false
	}
	marker := "|observed|read=" + readRevision + "|base="
	markerIndex := strings.LastIndex(title, marker)
	if markerIndex < 0 {
		return false
	}
	baseAndCall := title[markerIndex+len(marker):]
	if !strings.Contains(baseAndCall, "|call=") || !strings.HasSuffix(baseAndCall, "|refused=0") {
		return false
	}
	base := baseAndCall
	if index := strings.IndexByte(baseAndCall, '|'); index >= 0 {
		base = baseAndCall[:index]
	}
	return base != ""
}

func guestObservedRevision(title, expectedOriginalTitle string) (string, bool) {
	if !strings.HasPrefix(title, strings.ToUpper(expectedOriginalTitle)+"|") {
		return "", false
	}
	marker := "|observed|read="
	markerIndex := strings.LastIndex(title, marker)
	if markerIndex < 0 {
		return "", false
	}
	readAndRest := title[markerIndex+len(marker):]
	baseIndex := strings.Index(readAndRest, "|base=")
	if baseIndex <= 0 {
		return "", false
	}
	readRevision := readAndRest[:baseIndex]
	value, err := strconv.ParseUint(readRevision, 10, 64)
	if err != nil || value == 0 {
		return "", false
	}
	return readRevision, true
}

func validateGuestReadRevision(record []byte, expectedOriginalTitle string, expected model.Revision) error {
	doc, err := pipeline.DecodeDocument(record)
	if err != nil {
		return fmt.Errorf("guest record decode: %w", err)
	}
	title, ok := doc.Fields["title"].(string)
	if !ok {
		return errors.New("guest record omitted its title")
	}
	observed, ok := guestObservedRevision(title, expectedOriginalTitle)
	if !ok || observed != expected.String() {
		return fmt.Errorf("guest observed revision=%s want=%s", observed, expected)
	}
	return nil
}

func equalJSONValues(left, right any) bool {
	leftBytes, err := json.Marshal(left)
	if err != nil {
		return false
	}
	rightBytes, err := json.Marshal(right)
	if err != nil {
		return false
	}
	decode := func(data []byte) (any, error) {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		var value any
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		return value, nil
	}
	lv, err := decode(leftBytes)
	if err != nil {
		return false
	}
	rv, err := decode(rightBytes)
	return err == nil && reflect.DeepEqual(lv, rv)
}

func seededWinnerIDs(count int) map[string]bool {
	winners := map[string]bool{targetID: true, watchedID: true}
	for i := 3; i <= count; i++ {
		winners[fmt.Sprintf("00000000-0000-4000-8000-%012d", i)] = true
	}
	return winners
}

func expectedBulkTitle(id string) string {
	switch id {
	case targetID:
		return "seed"
	case watchedID:
		return "observed"
	}
	if len(id) >= 3 {
		if value, err := strconv.Atoi(id[len(id)-3:]); err == nil && value >= 3 {
			return fmt.Sprintf("bulk-%03d", value-1)
		}
	}
	return ""
}

func validateObserverDocument(body []byte) error {
	doc, err := pipeline.DecodeDocument(body)
	if err != nil {
		return err
	}
	if doc.ID != bulkObserverID || doc.Fields["title"] != "observed" || doc.Fields["status"] != "done" || doc.Lease != nil {
		return errors.New("bulk observer is not the untouched done record")
	}
	return nil
}

// TestPublicClaimAssertionNegatives proves that the same parser, completion,
// receipt, claim-state and snapshot validators reject false-PASS evidence.
func TestPublicClaimAssertionNegatives(t *testing.T) {
	parserCases := []struct {
		name   string
		status int
		body   string
	}{
		{"malformed", http.StatusOK, "{"},
		{"null_root", http.StatusOK, "null"},
		{"wrong_version", http.StatusOK, "{\"jsonrpc\":\"1.0\",\"id\":\"k\",\"result\":null}"},
		{"wrong_id", http.StatusOK, "{\"jsonrpc\":\"2.0\",\"id\":\"other\",\"result\":null}"},
		{"result_and_error", http.StatusOK, "{\"jsonrpc\":\"2.0\",\"id\":\"k\",\"result\":null,\"error\":{\"code\":-32000,\"message\":\"bad\",\"data\":{\"code\":\"busy\",\"retryable\":true}}}"},
		{"error_without_code", http.StatusOK, "{\"jsonrpc\":\"2.0\",\"id\":\"k\",\"error\":{\"code\":-32000,\"message\":\"bad\",\"data\":{\"retryable\":true}}}"},
		{"wrong_http_status", http.StatusBadRequest, "{\"jsonrpc\":\"2.0\",\"id\":\"k\",\"result\":null}"},
		{"unknown_error_code", http.StatusOK, "{\"jsonrpc\":\"2.0\",\"id\":\"k\",\"error\":{\"code\":-32000,\"message\":\"bad\",\"data\":{\"code\":\"not_a_catalog_code\",\"retryable\":false}}}"},
	}
	for _, tc := range parserCases {
		t.Run(tc.name, func(t *testing.T) {
			reply := parsePublicClaimReply("k", tc.status, []byte(tc.body))
			if reply.protocolError == "" || reply.outcome() != publicOther {
				t.Fatalf("malformed public envelope was accepted: %+v", reply)
			}
		})
	}
	validNull := parsePublicClaimReply("k", http.StatusOK, []byte("{\"jsonrpc\":\"2.0\",\"id\":\"k\",\"result\":null}"))
	if validNull.protocolError != "" || validNull.outcome() != publicNull {
		t.Fatalf("valid terminal null was rejected or reclassified: %+v", validNull)
	}
	validConflict := parsePublicClaimReply("k", http.StatusOK, []byte("{\"jsonrpc\":\"2.0\",\"id\":\"k\",\"error\":{\"code\":-32000,\"message\":\"conflict\",\"data\":{\"code\":\"write_conflict\",\"retryable\":true}}}"))
	if validConflict.protocolError != "" || validConflict.outcome() != publicWriteConflict || !validConflict.retryable() {
		t.Fatalf("typed retryable conflict was not preserved: %+v", validConflict)
	}

	completionCases := []publicWorkloadSummary{
		{Workers: 2, Launched: 2, Received: 2, Attempts: 128, Conflicts: 128, TerminalRetryable: 2, Winners: map[string]int{}}, // exhausted-only
		{Workers: 2, Launched: 2, Received: 2, Attempts: 2, Nulls: 2, Winners: map[string]int{}},                               // null-only
		{Workers: 2, Launched: 2, Received: 2, Attempts: 2, Successes: 2, Winners: map[string]int{targetID: 2}},                // duplicate
		{Workers: 2, Launched: 2, Received: 2, Attempts: 2, Successes: 2, Winners: map[string]int{targetID: 1, "foreign": 1}},
	}
	for index, summary := range completionCases {
		err := summary.validateAccounting(true)
		if err == nil && index >= 2 {
			err = validateWinnerSet(summary.Winners, map[string]bool{targetID: true, watchedID: true}, 2)
		}
		if err == nil {
			t.Fatalf("false completion case %d passed shared accounting validator", index)
		}
	}

	expected := bulkAgent(0)
	key := fmt.Sprintf("%032x", 7000)
	ref := model.RefByID(targetID, model.Natural64(0))
	doc := pipeline.Document{ID: targetID, Type: "issue", Project: "project", Fields: map[string]any{"title": "SEED|observed|read=1|base=1|call=1|refused=0", "status": "in_progress"}, Lease: &pipeline.ClaimLease{Principal: expected, ExpiresAt: time.Now().Add(time.Minute), TtlMs: model.Natural64(300000), Epoch: model.Natural64(1)}}
	recordBody, err := json.Marshal(doc)
	demand(t, err == nil, "negative receipt document encode failed")
	responseResult := model.OpResult{Ref: ref, Revision: model.Natural64(2), Record: recordBody, SetBy: map[string]string{"fields.title": bindingID}}
	response := model.Commit{Revision: model.Natural64(2), Results: []model.OpResult{responseResult}, Warnings: []model.Warning{}}
	baseReceipt := func() *driver.IdempotencyRecord {
		return &driver.IdempotencyRecord{Commit: response, Changeset: model.Changeset{Revision: model.Natural64(2), Kind: model.ChangesetWrite, Principal: expected, IdempotencyKey: key, Mutators: []string{bindingID}, Changes: []model.ChangeRecord{{Ref: ref}}}}
	}
	receiptCases := []struct {
		name string
		edit func(*driver.IdempotencyRecord)
	}{
		{"pending", func(r *driver.IdempotencyRecord) { r.ResponsePending = true }},
		{"wrong_principal", func(r *driver.IdempotencyRecord) { r.Changeset.Principal = localAgent("user:outsider") }},
		{"wrong_key", func(r *driver.IdempotencyRecord) { r.Changeset.IdempotencyKey = "wrong" }},
		{"wrong_mutator", func(r *driver.IdempotencyRecord) { r.Changeset.Mutators = []string{"wrong"} }},
		{"wrong_revision", func(r *driver.IdempotencyRecord) { r.Commit.Revision = model.Natural64(3) }},
		{"wrong_ref", func(r *driver.IdempotencyRecord) {
			r.Changeset.Changes[0].Ref = model.RefByID(watchedID, model.Natural64(0))
		}},
	}
	for _, tc := range receiptCases {
		t.Run("receipt_"+tc.name, func(t *testing.T) {
			receipt := baseReceipt()
			tc.edit(receipt)
			if err := validatePublicClaimReceiptShape(receipt, expected, key, response, responseResult); err == nil {
				t.Fatalf("invalid receipt passed shared validator: %+v", receipt)
			}
		})
	}
	if err := validateClaimDocument(responseResult, expected, targetID, model.Natural64(1)); err != nil {
		t.Fatalf("valid claimed record rejected: %v", err)
	}
	stored := driver.Record{Ref: ref, Revision: responseResult.Revision, Body: responseResult.Record}
	if err := validateStoredClaimRecord(stored, expected, targetID, &responseResult, true, model.Natural64(1)); err != nil {
		t.Fatalf("valid final stored winner rejected: %v", err)
	}
	staleStored := stored
	staleStored.Revision = model.Natural64(3)
	if err := validateStoredClaimRecord(staleStored, expected, targetID, &responseResult, true, model.Natural64(1)); err == nil {
		t.Fatal("stale final stored revision passed winner correspondence validator")
	}
	badState := responseResult
	badState.Record = bytes.Replace(badState.Record, []byte("\"in_progress\""), []byte("\"todo\""), 1)
	if err := validateClaimDocument(badState, expected, targetID, model.Natural64(1)); err == nil {
		t.Fatal("wrong final claim state passed shared validator")
	}
	badStoredState := stored
	badStoredState.Body = badState.Record
	if err := validateStoredClaimRecord(badStoredState, expected, targetID, &responseResult, true, model.Natural64(1)); err == nil {
		t.Fatal("wrong final stored claim state passed winner validator")
	}
	badStoredActor := stored
	badStoredActor.Body = bytes.Replace(badStoredActor.Body, []byte("agent:bulk-000"), []byte("agent:bulk-999"), 1)
	if err := validateStoredClaimRecord(badStoredActor, expected, targetID, &responseResult, true, model.Natural64(1)); err == nil {
		t.Fatal("wrong final stored actor passed winner validator")
	}
	badGuest := responseResult
	badGuest.Record = bytes.Replace(badGuest.Record, []byte("read=1"), []byte("read=0"), 1)
	if err := validateClaimDocument(badGuest, expected, targetID, model.Natural64(1)); err == nil {
		t.Fatal("wrong snapshot observation passed shared validator")
	}
	wrongPositiveGuest := responseResult
	wrongPositiveGuest.Record = bytes.Replace(wrongPositiveGuest.Record, []byte("read=1"), []byte("read=2"), 1)
	if err := validateClaimDocument(wrongPositiveGuest, expected, targetID, model.Natural64(1)); err == nil {
		t.Fatal("wrong positive snapshot revision passed small shared validator")
	}
	if err := validateGuestReadRevision(responseResult.Record, "seed", model.Natural64(1)); err != nil {
		t.Fatalf("valid observed revision rejected: %v", err)
	}
	if err := validateGuestReadRevision(responseResult.Record, "seed", model.Natural64(2)); err == nil {
		t.Fatal("wrong observed revision passed shared validator")
	}
	observer, err := json.Marshal(pipeline.Document{ID: bulkObserverID, Type: "issue", Project: "project", Fields: map[string]any{"title": "observed", "description": "hidden", "status": "done", "priority": 0}})
	demand(t, err == nil, "negative observer encode failed")
	if err := validateObserverDocument(observer); err != nil {
		t.Fatalf("valid untouched observer rejected: %v", err)
	}
	badObserver := bytes.Replace(observer, []byte("\"done\""), []byte("\"todo\""), 1)
	if err := validateObserverDocument(badObserver); err == nil {
		t.Fatal("wrong snapshot observer state passed shared validator")
	}
}

func seedBulkReady(t *testing.T, f *localFixture, count int) {
	t.Helper()
	changes := make([]conformance.FixtureChange, 0, count-1)
	observerBody, err := json.Marshal(map[string]any{"id": bulkObserverID, "type": "issue", "project": "project", "fields": map[string]any{
		"title": "observed", "description": "hidden", "status": "done", "priority": 0,
	}})
	demand(t, err == nil, "bulk observer encode failed")
	changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(bulkObserverID, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: observerBody})
	for i := 2; i < count; i++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
		body, err := json.Marshal(map[string]any{"id": id, "type": "issue", "project": "project", "createdAt": "2026-10-01T00:00:00Z", "fields": map[string]any{
			"title": fmt.Sprintf("bulk-%03d", i), "description": "hidden", "status": "todo", "priority": 0,
		}})
		demand(t, err == nil, "bulk seed encode failed")
		changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	_, err = conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes)
	demand(t, err == nil, "bulk ready seed failed")
}

func seedLifecycleReady(t *testing.T, f *localFixture, count int) ([]string, map[string]model.PrincipalRef) {
	t.Helper()
	changes := make([]conformance.FixtureChange, 0, count)
	ids := make([]string, count)
	principals := make(map[string]model.PrincipalRef, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("e5-mutator-%03d", i)
		body, err := json.Marshal(map[string]any{"id": id, "type": "issue", "project": "project", "createdAt": "2026-10-01T00:00:00Z", "fields": map[string]any{
			"title": fmt.Sprintf("e5-mutator-seed-%03d", i), "description": "hidden", "status": "in_progress", "priority": 0,
		}})
		demand(t, err == nil, "lifecycle seed encode failed")
		ids[i] = id
		principals[id] = bulkAgent(1000 + i)
		changes = append(changes, conformance.FixtureChange{Ref: model.RefByID(id, model.Natural64(0)), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	_, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes)
	demand(t, err == nil, "lifecycle ready seed failed")
	return ids, principals
}

func bulkAgent(i int) model.PrincipalRef {
	return model.PrincipalRef{Kind: "agent", ID: fmt.Sprintf("agent:bulk-%03d", i), Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:sponsor"}, Caveats: map[string]any{
		"actions": []string{"modify"}, "projects": []model.Ref{model.RefByID("project", model.Natural64(0))},
	}}
}

func publicClaimNext(f *localFixture, key string, start <-chan struct{}) publicClaimReply {
	return publicClaimNextToken(f, key, f.tokens["user:sponsor"], start)
}

func publicClaimNextToken(f *localFixture, key, token string, start <-chan struct{}) publicClaimReply {
	<-start
	return publicClaimNextRequest(context.Background(), f, key, token)
}

func publicClaimNextRequest(ctx context.Context, f *localFixture, key, token string) publicClaimReply {
	params := map[string]any{"_meta": map[string]string{"idempotencyKey": key}}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": key, "method": publicClaimMethod, "params": params})
	if err != nil {
		return protocolReply(key, http.StatusInternalServerError, nil, fmt.Errorf("request encode: %w", err))
	}
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1"+httpjson.Path, bytes.NewReader(body))
	request.RemoteAddr = "127.0.0.1:12345"
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	f.composition.Handler.ServeHTTP(response, request)
	return parsePublicClaimReply(key, response.Code, response.Body.Bytes())
}
