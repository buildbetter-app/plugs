package rehearsal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/api/wire"
	"github.com/buildbetter-app/plugs/internal/cli"
	"github.com/buildbetter-app/plugs/internal/client"
	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/restore"
	"github.com/buildbetter-app/plugs/internal/core/reverseconverter"
	"github.com/buildbetter-app/plugs/internal/plugins/authnapikey"
	"github.com/buildbetter-app/plugs/internal/server/authn"
	"github.com/buildbetter-app/plugs/internal/server/composition"
	"github.com/buildbetter-app/plugs/internal/server/transport/mcp"
)

type Preparation struct {
	SourceSHA        string        `json:"source_sha"`
	SourceTree       string        `json:"source_tree"`
	Driver           string        `json:"driver"`
	Preparation      string        `json:"preparation"`
	Qualification    string        `json:"r1_qualification"`
	BaselineKind     string        `json:"baseline_kind"`
	FactoryWorkers   int           `json:"sampled_factory_workers"`
	FactoryTasks     int           `json:"sampled_factory_tasks"`
	AdminProof       string        `json:"admin_proof"`
	ResourcesCleaned bool          `json:"resources_cleaned"`
	Runs             []PreparedRun `json:"runs"`
	GateWaits        []string      `json:"gate_waits_at_execution"`
}

type PreparedRun struct {
	Started             time.Time `json:"started"`
	Finished            time.Time `json:"finished"`
	FreshEmptyTarget    bool      `json:"fresh_empty_target"`
	RestoredComparison  bool      `json:"restored_comparison"`
	AuthenticatedMCP    bool      `json:"authenticated_mcp"`
	ServiceAdminRefused bool      `json:"service_admin_refused"`
	NewIncarnation      bool      `json:"new_restore_incarnation"`
	FactoryWorkers      int       `json:"sampled_factory_workers"`
	FactoryTasks        int       `json:"sampled_factory_tasks"`
	DumpDigest          string    `json:"head_dump_sha256"`
	Baseline            Metrics   `json:"baseline"`
	Shadow              Metrics   `json:"shadow"`
	ComparisonWaits     []string  `json:"comparison_waits"`
	ReverseConversion   string    `json:"reverse_conversion"`
	ReverseSeconds      float64   `json:"reverse_attempt_seconds"`
	SoftwareRollback    string    `json:"software_rollback"`
	Failover            string    `json:"managed_failover"`
	Rollback            Rollback  `json:"rollback"`
}

// Rollback records the bounded rehearsal of every rollback input. The
// manifests and data digest are real disposable artifacts, but no image or
// plugin redeployment, Beads import, or source-system readback is attempted.
// Those omissions remain explicit WAIT evidence rather than a synthetic pass.
type Rollback struct {
	ImageDigest       string `json:"image_digest"`
	PromptDigest      string `json:"prompt_digest"`
	PluginDigest      string `json:"plugin_digest"`
	DataDumpDigest    string `json:"data_dump_digest"`
	InputsFrozen      bool   `json:"inputs_frozen"`
	Software          string `json:"software"`
	ReverseConversion string `json:"reverse_conversion"`
	Data              string `json:"data"`
	Complete          string `json:"complete"`
}

const (
	sampledFactoryWorkers    = 4
	sampledTasksPerWorker    = 2
	sampledFactoryTaskCount  = sampledFactoryWorkers * sampledTasksPerWorker
	rollbackBundleVersion    = "i8-disposable-bundle-v1"
	rollbackPromptVersion    = "rewritten-caller-prompt-v1"
	rollbackPluginSetVersion = "dry-run-plugin-set-v1"
)

var fixturePresence = map[string]any{"mode": "presence_check", "challenge": "disposable-rehearsal", "proof": "synthetic-only"}

// RunDisposable owns every resource it opens. There is intentionally no server
// URL, DSN, credential, command adapter, or cloud-provider option. Each run
// restores into a new empty store and compares HTTP-reference with MCP callers.
func RunDisposable(ctx context.Context, kind, sourceSHA, sourceTree string) (report Preparation, err error) {
	if (kind != "memory" && kind != "sqlite") || !sha40.MatchString(sourceSHA) || !sha40.MatchString(sourceTree) {
		return report, fmt.Errorf("memory/sqlite and full source SHA/tree required")
	}
	report = Preparation{SourceSHA: sourceSHA, SourceTree: sourceTree, Driver: kind, Preparation: "PASS", Qualification: "WAIT", BaselineKind: "sampled factory pool over synthetic authenticated HTTP/MCP fixtures; reference factory and staging UNQUALIFIED", FactoryWorkers: sampledFactoryWorkers, FactoryTasks: sampledFactoryTaskCount, AdminProof: "synthetic trusted-host session and presence fixture; real D32 UNQUALIFIED"}
	if err = ensureWorkRoot(); err != nil {
		return report, err
	}
	root, err := os.MkdirTemp(workRoot, "plug-swarm-i8-")
	if err != nil {
		return report, err
	}
	defer func() {
		cleanup := os.RemoveAll(root)
		report.ResourcesCleaned = cleanup == nil
		if cleanup != nil {
			err = errors.Join(err, fmt.Errorf("disposable directory cleanup failed"))
		}
	}()
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		var run PreparedRun
		run, err = runDisposable(ctx, kind, root, i, seen, sourceSHA, sourceTree)
		if err != nil {
			report.Preparation = "BLOCK"
			return report, err
		}
		report.Runs = append(report.Runs, run)
	}
	evidence := GateEvidence{SourceSHA: sourceSHA, SourceTree: sourceTree}
	for _, run := range report.Runs {
		evidence.Rehearsals = append(evidence.Rehearsals, Receipt{SourceSHA: sourceSHA, SourceTree: sourceTree, Digest: run.DumpDigest, Finished: run.Finished, Clean: false})
	}
	report.GateWaits = EvaluateGate(time.Now(), evidence)
	return report, nil
}

type fixture struct {
	runtime        *composition.Runtime
	server         *httptest.Server
	rpc            *client.Client
	transport      *client.HTTPTransport
	token          string
	mcp            *mcp.Handler
	admin          *fixtureAdmin
	adminRPC       *client.Client
	adminTransport *client.HTTPTransport
}

func openFixture(ctx context.Context, kind, path string) (*fixture, error) {
	provider := authnapikey.New(authnapikey.Config{})
	issued, err := provider.Issue(model.PrincipalRef{Kind: "service", ID: "service:rehearsal"}, time.Now().Add(time.Hour))
	if err != nil {
		return nil, fmt.Errorf("fixture credential issue failed")
	}
	admin, err := newFixtureAdmin()
	if err != nil {
		return nil, fmt.Errorf("fixture admin session unavailable")
	}
	runtime, err := composition.Open(ctx, composition.Config{
		Driver: kind, DSN: path, Project: "rehearsal", AuthMode: "authenticated", Auth: authn.Config{Provider: provider, CoreSessions: admin},
		RestorePresence: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !strings.HasPrefix(method, "admin.") || digest == "" || p["challenge"] != fixturePresence["challenge"] || p["proof"] != fixturePresence["proof"] {
				return fmt.Errorf("fixture-only presence refused")
			}
			return nil
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("fixture runtime open failed: %w", err)
	}
	f := &fixture{runtime: runtime, token: issued.Token, admin: admin}
	handler, err := mcp.New(runtime.RPC, runtime.Authenticator, mcp.Config{})
	if err != nil {
		f.close()
		return nil, err
	}
	f.mcp = handler
	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.Handle("/rpc/v1", runtime.HTTPHandler)
	f.server = httptest.NewServer(mux)
	f.transport, err = client.NewHTTP(client.HTTPConfig{Server: f.server.URL, Token: f.token})
	if err == nil {
		f.rpc, err = client.New(f.transport, "rehearsal", "rehearsal-agent")
	}
	if err == nil {
		f.adminTransport, err = client.NewHTTP(client.HTTPConfig{Server: f.server.URL, Token: admin.token})
	}
	if err == nil {
		f.adminRPC, err = client.New(f.adminTransport, "rehearsal", "fixture-admin")
	}
	if err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}

func (f *fixture) close() error {
	if f.transport != nil {
		f.transport.Close()
	}
	if f.adminTransport != nil {
		f.adminTransport.Close()
	}
	if f.admin != nil {
		f.admin.revoked.Store(true)
	}
	if f.server != nil {
		f.server.Close()
	}
	var err error
	if f.mcp != nil {
		err = f.mcp.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return errors.Join(err, f.runtime.Close(ctx))
}

func runDisposable(ctx context.Context, kind, root string, index int, seen map[string]bool, sourceSHA, sourceTree string) (run PreparedRun, err error) {
	run.Started = time.Now().UTC()
	run.FactoryWorkers = sampledFactoryWorkers
	run.FactoryTasks = sampledFactoryTaskCount
	source, err := openFixture(ctx, kind, fixtureDSN(kind, root, index, "source"))
	if err != nil {
		return run, err
	}
	defer func() { err = errors.Join(err, source.close()) }()
	target, err := openFixture(ctx, kind, fixtureDSN(kind, root, index, "target"))
	if err != nil {
		return run, err
	}
	defer func() { err = errors.Join(err, target.close()) }()
	meta, err := target.runtime.Store.Meta(ctx)
	if err != nil {
		return run, err
	}
	identity := fmt.Sprintf("%s/%s", meta.StoreID, meta.Incarnation)
	if seen[identity] || meta.Head.String() != "0" {
		return run, fmt.Errorf("target is reused or populated")
	}
	seen[identity] = true
	run.FreshEmptyTarget = true
	for i := 0; i < sampledFactoryTaskCount; i++ {
		if _, err = source.rpc.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "synthetic task", "priority": 1}}, ""); err != nil {
			return run, err
		}
	}
	if _, err = source.rpc.Call(ctx, "memory.save", map[string]any{"key": "rehearsal", "content": "synthetic caller context"}, ""); err != nil {
		return run, err
	}
	before, err := readySignature(ctx, source.rpc)
	if err != nil {
		return run, err
	}
	for _, f := range []*fixture{source, target} {
		_, denied := f.rpc.Call(ctx, "admin.dump", map[string]any{"history": "head", "pageSize": 1 << 20, "humanPresence": fixturePresence}, "")
		var remote *wire.Error
		if !errors.As(denied, &remote) || remote.Data.Code != "human_presence_required" {
			return run, fmt.Errorf("service key admin boundary was not enforced")
		}
	}
	run.ServiceAdminRefused = true
	data, archive, err := captureDump(ctx, source.adminRPC)
	if err != nil {
		return run, err
	}
	if err = restoreDump(ctx, target.adminRPC, data, archive); err != nil {
		return run, err
	}
	restoredMeta, err := target.runtime.Store.Meta(ctx)
	if err != nil {
		return run, err
	}
	if restoredMeta.Incarnation == meta.Incarnation || restoredMeta.Incarnation == archive.Manifest().NativeIncarnation {
		return run, fmt.Errorf("restore reused an incarnation")
	}
	run.NewIncarnation = true
	after, err := readySignature(ctx, target.rpc)
	if err != nil {
		return run, err
	}
	if before != after {
		return run, fmt.Errorf("restored ready identities/content differ")
	}
	run.RestoredComparison = true
	shadowOpen := func(ctx context.Context, worker int) (*client.Client, func(), error) {
		return newMCPClient(ctx, target, fmt.Sprintf("factory-mcp-%d", worker))
	}
	run.AuthenticatedMCP = true
	httpOpen := func(_ context.Context, worker int) (*client.Client, func(), error) {
		httpClient, clientErr := client.New(source.transport, "rehearsal", fmt.Sprintf("factory-http-%d", worker))
		return httpClient, func() {}, clientErr
	}
	run.Baseline, err = sampledFactoryPool(ctx, httpOpen, sampledFactoryWorkers, sampledTasksPerWorker)
	if err != nil {
		return run, fmt.Errorf("HTTP sampled factory workload: %w", err)
	}
	run.Shadow, err = sampledFactoryPool(ctx, shadowOpen, sampledFactoryWorkers, sampledTasksPerWorker)
	if err != nil {
		return run, fmt.Errorf("MCP sampled factory workload: %w", err)
	}
	if run.Baseline.Completed != sampledFactoryTaskCount || run.Shadow.Completed != sampledFactoryTaskCount {
		return run, fmt.Errorf("synthetic task completion mismatch")
	}
	before, err = readySignature(ctx, source.rpc)
	if err != nil {
		return run, err
	}
	after, err = readySignature(ctx, target.rpc)
	if err != nil {
		return run, err
	}
	if before != after {
		return run, fmt.Errorf("post-workload ready comparison differs")
	}
	// Targets for the 100,000-record design workload are not inferred from this
	// sampled pool. Missing targets are explicit WAIT rows.
	run.ComparisonWaits = Compare(run.Baseline, run.Shadow, nil)
	post, _, err := captureDump(ctx, target.adminRPC)
	if err != nil {
		return run, err
	}
	digest := sha256.Sum256(post)
	run.DumpDigest = hex.EncodeToString(digest[:])
	started := time.Now()
	_, reverseErr := reverseconverter.ConvertArchive(post)
	run.ReverseSeconds = time.Since(started).Seconds()
	run.ReverseConversion = "WAIT_reverse_output_requires_import_readback_and_loss_review"
	if reverseErr != nil {
		run.ReverseConversion = "WAIT_native_head_reverse_profile_refused"
	}
	run.Rollback, err = rehearseRollback(ctx, root, index, sourceSHA, sourceTree, post, source.rpc)
	if err != nil {
		return run, err
	}
	run.SoftwareRollback = run.Rollback.Software
	run.Failover = "UNQUALIFIED_use_separately_reviewed_A1_fixture_and_managed_drill"
	run.Finished = time.Now().UTC()
	return run, nil
}

func fixtureDSN(kind, root string, index int, name string) string {
	if kind == "memory" {
		return ""
	}
	return filepath.Join(root, fmt.Sprintf("%d-%s.db", index, name))
}

type poolClientFactory func(context.Context, int) (*client.Client, func(), error)

type poolWorkerResult struct {
	samples   []Sample
	claimed   []string
	completed int
	err       error
}

// sampledFactoryPool exercises the rewritten caller through several distinct
// agent sessions. It is deliberately small and synthetic: it catches caller
// framing, claim uniqueness, lifecycle readback and error drift without
// pretending to be the 100,000-bead factory or authenticated staging.
func sampledFactoryPool(ctx context.Context, open poolClientFactory, workers, tasksPerWorker int) (Metrics, error) {
	if open == nil || workers <= 0 || tasksPerWorker <= 0 {
		return Metrics{}, fmt.Errorf("sampled factory pool requires a client factory and positive bounds")
	}
	poolCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type openedClient struct {
		client *client.Client
		close  func()
	}
	clients := make([]openedClient, 0, workers)
	closeOpened := func() {
		for _, opened := range clients {
			if opened.close != nil {
				opened.close()
			}
		}
	}
	for worker := 0; worker < workers; worker++ {
		c, closeClient, err := open(poolCtx, worker)
		if err != nil || c == nil || closeClient == nil {
			closeOpened()
			if err == nil {
				err = fmt.Errorf("factory client unavailable")
			}
			return Metrics{}, err
		}
		clients = append(clients, openedClient{client: c, close: closeClient})
	}
	defer closeOpened()
	results := make(chan poolWorkerResult, workers)
	// The pool keeps distinct authenticated sessions and concurrent read
	// framing, while this tiny fixture serializes each write lifecycle. The
	// separate E5 contention gate owns concurrent-writer qualification; this
	// smoke must isolate caller parity from storage contention.
	var writeMu sync.Mutex
	var group sync.WaitGroup
	for worker, opened := range clients {
		group.Add(1)
		go func(worker int, opened openedClient) {
			defer group.Done()
			result := runPoolWorker(poolCtx, opened.client, worker, tasksPerWorker, &writeMu)
			if result.err != nil {
				cancel()
			}
			results <- result
		}(worker, opened)
	}
	group.Wait()
	close(results)

	var samples []Sample
	claimed := map[string]bool{}
	completed := 0
	for result := range results {
		if result.err != nil {
			return Metrics{}, result.err
		}
		samples = append(samples, result.samples...)
		completed += result.completed
		for _, id := range result.claimed {
			if id == "" || claimed[id] {
				return Metrics{}, fmt.Errorf("sampled factory pool returned a duplicate or empty claim")
			}
			claimed[id] = true
		}
	}
	if len(claimed) != workers*tasksPerWorker || completed != len(claimed) {
		return Metrics{}, fmt.Errorf("sampled factory pool did not complete every claimed task")
	}

	// Check the final queue only after every worker has finished. Checking from
	// inside a worker would race with other legitimate claims.
	check, closeCheck, err := open(poolCtx, workers)
	if err != nil || check == nil || closeCheck == nil {
		if err == nil {
			err = fmt.Errorf("factory queue-check client unavailable")
		}
		return Metrics{}, err
	}
	defer closeCheck()
	ready, err := sampledCall(poolCtx, check, &samples, "ready", "--limit", "10")
	var page struct{ Items []json.RawMessage }
	if err != nil || json.Unmarshal(ready.JSON, &page) != nil || page.Items == nil || len(page.Items) != 0 {
		return Metrics{}, fmt.Errorf("completed tasks remained ready")
	}
	return Summarize(samples, workers*tasksPerWorker, completed)
}

func runPoolWorker(ctx context.Context, c *client.Client, worker, tasks int, writeMu *sync.Mutex) poolWorkerResult {
	result := poolWorkerResult{}
	call := func(argv ...string) (client.Result, error) {
		return sampledCall(ctx, c, &result.samples, argv...)
	}
	if brief, err := call("brief", "--budget-tokens", "500"); err != nil || !bytes.Contains(brief.JSON, []byte("synthetic caller context")) {
		result.err = fmt.Errorf("brief caller failed")
		return result
	}
	if memories, err := call("memory", "list", "--limit", "10"); err != nil || !bytes.Contains(memories.JSON, []byte("synthetic caller context")) {
		result.err = fmt.Errorf("memory caller failed")
		return result
	}
	for task := 0; task < tasks; task++ {
		var taskErr error
		writeMu.Lock()
		func() {
			defer writeMu.Unlock()
			claimed, err := call("claim-next", "--type", "issue", "--idempotency-key", poolIdempotencyKey(worker, task, tasks))
			if err != nil {
				taskErr = fmt.Errorf("atomic claim caller failed")
				return
			}
			var commit model.Commit
			if json.Unmarshal(claimed.JSON, &commit) != nil || len(commit.Results) != 1 {
				taskErr = fmt.Errorf("atomic claim returned no winner")
				return
			}
			id := commit.Results[0].Ref.ID
			if id == "" {
				taskErr = fmt.Errorf("atomic claim returned an empty winner")
				return
			}
			result.claimed = append(result.claimed, id)
			for _, argv := range [][]string{{"get", id}, {"update", id, "--title", "synthetic completed task"}, {"comment", id, "--body", "synthetic progress"}, {"close", id, "--reason", "synthetic completion"}} {
				if _, err := call(argv...); err != nil {
					taskErr = fmt.Errorf("task lifecycle caller failed")
					return
				}
			}
			final, err := call("get", id)
			var record struct {
				Bead struct {
					Status string
					Fields map[string]any
				}
			}
			if err != nil || json.Unmarshal(final.JSON, &record) != nil || record.Bead.Status != "done" || record.Bead.Fields["title"] != "synthetic completed task" {
				taskErr = fmt.Errorf("completed task readback failed")
				return
			}
			result.completed++
		}()
		if taskErr != nil {
			result.err = taskErr
			return result
		}
	}
	return result
}

func sampledCall(ctx context.Context, c *client.Client, samples *[]Sample, argv ...string) (client.Result, error) {
	command, err := cli.ParseCommand(argv)
	if err != nil {
		return client.Result{}, fmt.Errorf("rewritten caller command refused")
	}
	started := time.Now()
	result, err := c.Call(ctx, command.Method, command.Params, command.Key)
	sample := Sample{Method: command.Method, Latency: time.Since(started), Completed: command.Method == "bead.close" && err == nil}
	if err != nil {
		sample.Code = "transport_error"
		var remote *wire.Error
		if errors.As(err, &remote) {
			sample.Code = remote.Data.Code
			sample.Retryable = remote.Data.Retryable
		}
	}
	*samples = append(*samples, sample)
	return result, err
}

func poolIdempotencyKey(worker, task, tasksPerWorker int) string {
	return fmt.Sprintf("%064x", uint64(worker*tasksPerWorker+task+1))
}

func rehearseRollback(ctx context.Context, root string, index int, sourceSHA, sourceTree string, post []byte, oldService *client.Client) (Rollback, error) {
	if len(post) == 0 || oldService == nil {
		return Rollback{}, fmt.Errorf("rollback rehearsal requires a nonempty dump and old service")
	}
	dir := filepath.Join(root, fmt.Sprintf("%d-rollback", index))
	if err := os.Mkdir(dir, 0700); err != nil {
		return Rollback{}, err
	}
	artifacts := map[string][]byte{
		"image.manifest":  []byte(fmt.Sprintf("%s\nsource_sha=%s\nsource_tree=%s\n", rollbackBundleVersion, sourceSHA, sourceTree)),
		"prompt.manifest": []byte(rollbackPromptVersion + "\n"),
		"plugin.manifest": []byte(rollbackPluginSetVersion + "\nmode=dry-run\n"),
		"data.dump":       append([]byte(nil), post...),
	}
	digests := map[string]string{}
	for name, data := range artifacts {
		path := filepath.Join(dir, name)
		if err := writeRollbackArtifact(path, data); err != nil {
			return Rollback{}, err
		}
		readback, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(readback, data) {
			return Rollback{}, fmt.Errorf("rollback artifact readback failed")
		}
		sum := sha256.Sum256(readback)
		digests[name] = hex.EncodeToString(sum[:])
	}
	// The old disposable service remains available as a canary. This proves
	// only that the pre-switch fixture still answers; it is not a redeployment
	// or image/prompt/plugin rollback receipt.
	if _, err := oldService.Call(ctx, "agent.brief", map[string]any{"budgetTokens": 1}, ""); err != nil {
		return Rollback{}, fmt.Errorf("old disposable canary failed")
	}
	_, reverseErr := reverseconverter.ConvertArchive(post)
	reverseStatus := "WAIT_reverse_output_requires_import_readback_and_loss_review"
	var refusal *reverseconverter.Refusal
	switch {
	case reverseErr == nil:
		reverseStatus = "WAIT_reverse_output_requires_import_readback_and_loss_review"
	case errors.As(reverseErr, &refusal):
		reverseStatus = "WAIT_reverse_conversion_refused"
	default:
		reverseStatus = "WAIT_reverse_conversion_unavailable"
	}
	return Rollback{
		ImageDigest:       digests["image.manifest"],
		PromptDigest:      digests["prompt.manifest"],
		PluginDigest:      digests["plugin.manifest"],
		DataDumpDigest:    digests["data.dump"],
		InputsFrozen:      true,
		Software:          "WAIT_pinned_bundle_canary_only_no_redeployment",
		ReverseConversion: reverseStatus,
		Data:              reverseStatus,
		Complete:          "WAIT_full_image_prompt_plugin_data_rollback_requires_redeployment_and_beads_readback",
	}, nil
}

func writeRollbackArtifact(path string, data []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".rollback-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}

func readySignature(ctx context.Context, c *client.Client) (string, error) {
	result, err := c.Call(ctx, "bead.ready", map[string]any{"limit": 100}, "")
	if err != nil {
		return "", err
	}
	var page struct {
		Items []struct {
			ID, Type, Status string
			Fields           map[string]any
		}
	}
	if err = json.Unmarshal(result.JSON, &page); err != nil {
		return "", err
	}
	rows := []string{}
	for _, item := range page.Items {
		raw, _ := json.Marshal(item)
		rows = append(rows, string(raw))
	}
	sort.Strings(rows)
	return strings.Join(rows, "\n"), nil
}

func captureDump(ctx context.Context, c *client.Client) ([]byte, *dumpformat.Archive, error) {
	var data bytes.Buffer
	cursor := ""
	for pages := 0; pages < 4096; pages++ {
		params := map[string]any{"history": "head", "pageSize": 1 << 20, "humanPresence": fixturePresence}
		if cursor != "" {
			params["cursor"] = cursor
		}
		result, err := c.Call(ctx, "admin.dump", params, "")
		if err != nil {
			return nil, nil, err
		}
		var page struct {
			DumpID   string `json:"dumpId"`
			Data     string
			Done     bool
			NextPage json.Number `json:"nextPage"`
		}
		if err = json.Unmarshal(result.JSON, &page); err != nil {
			return nil, nil, err
		}
		chunk, err := base64.StdEncoding.DecodeString(page.Data)
		if err != nil {
			return nil, nil, err
		}
		data.Write(chunk)
		if data.Len() > 32<<20 {
			return nil, nil, fmt.Errorf("synthetic dump exceeds bound")
		}
		if page.Done {
			archive, err := dumpformat.Read(bytes.NewReader(data.Bytes()))
			return data.Bytes(), archive, err
		}
		cursor = page.DumpID + ":" + page.NextPage.String()
	}
	return nil, nil, fmt.Errorf("synthetic dump pagination exceeded bound")
}

func restoreDump(ctx context.Context, c *client.Client, data []byte, archive *dumpformat.Archive) error {
	member, ok := archive.Member("manifest.json")
	if !ok {
		return fmt.Errorf("dump manifest missing")
	}
	manifest, err := wire.DecodeJSON(member)
	if err != nil {
		return err
	}
	begin, err := c.Call(ctx, "admin.restore.begin", map[string]any{"manifest": manifest, "history": "head", "humanPresence": fixturePresence}, strings.Repeat("a", 32))
	if err != nil {
		return err
	}
	var opened struct {
		RestoreID string `json:"restoreId"`
	}
	if err = json.Unmarshal(begin.JSON, &opened); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if _, err = c.Call(ctx, "admin.restore.chunk", map[string]any{"restoreId": opened.RestoreID, "page": 0, "data": base64.StdEncoding.EncodeToString(data), "sha256": hex.EncodeToString(sum[:]), "humanPresence": fixturePresence}, strings.Repeat("b", 32)); err != nil {
		return err
	}
	canonical, err := json.Marshal(archive.Manifest())
	if err != nil {
		return err
	}
	sum = sha256.Sum256(canonical)
	finished, err := c.Call(ctx, "admin.restore.finish", map[string]any{"restoreId": opened.RestoreID, "manifestSha256": hex.EncodeToString(sum[:]), "humanPresence": fixturePresence}, strings.Repeat("c", 32))
	if err != nil {
		return err
	}
	var state struct{ State string }
	if json.Unmarshal(finished.JSON, &state) != nil || state.State != "complete" {
		return fmt.Errorf("disposable restore did not complete")
	}
	return nil
}

// mcpTransport is private to owned loopback fixtures. It has no arbitrary URL
// constructor and does not inherit HTTP proxies, redirects, or retry behavior.
type mcpTransport struct {
	endpoint, token, session string
	http                     *http.Client
}

func newMCPClient(ctx context.Context, f *fixture, agent string) (*client.Client, func(), error) {
	t := &mcpTransport{endpoint: f.server.URL + "/mcp", token: f.token, http: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	close := func() { t.http.CloseIdleConnections() }
	init, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "init", "method": "initialize", "params": map[string]any{"protocolVersion": mcp.DefaultProtocolVersion, "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "disposable-shadow", "version": "1"}}})
	_, headers, err := t.exchange(ctx, init)
	if err != nil {
		close()
		return nil, nil, err
	}
	t.session = headers.Get("MCP-Session-Id")
	if t.session == "" {
		close()
		return nil, nil, fmt.Errorf("MCP initialization failed")
	}
	listed, _, err := t.exchange(ctx, []byte(`{"jsonrpc":"2.0","id":"discover","method":"tools/list","params":{}}`))
	var discovery struct {
		JSONRPC string `json:"jsonrpc"`
		ID      string
		Result  struct{ Tools []struct{ Name string } }
	}
	if err != nil || json.Unmarshal(listed, &discovery) != nil || discovery.JSONRPC != "2.0" || discovery.ID != "discover" {
		close()
		return nil, nil, fmt.Errorf("MCP tool discovery failed")
	}
	found := false
	for _, tool := range discovery.Result.Tools {
		found = found || tool.Name == "plug_call"
	}
	if !found {
		close()
		return nil, nil, fmt.Errorf("MCP canonical caller tool unavailable")
	}
	c, err := client.New(t, "rehearsal", agent)
	if err != nil {
		close()
		return nil, nil, err
	}
	return c, close, nil
}

func (t *mcpTransport) exchange(ctx context.Context, data []byte) ([]byte, http.Header, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, nil, fmt.Errorf("MCP fixture request invalid")
	}
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Plug-RPC", "1")
	request.Header.Set("Authorization", "Bearer "+t.token)
	if t.session != "" {
		request.Header.Set("MCP-Session-Id", t.session)
	}
	response, err := t.http.Do(request)
	if err != nil {
		return nil, nil, fmt.Errorf("MCP fixture transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("MCP fixture admission refused")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
	if err != nil || len(body) > 8<<20 {
		return nil, nil, fmt.Errorf("MCP fixture response exceeds bound")
	}
	return body, response.Header, nil
}

func (t *mcpTransport) RoundTrip(ctx context.Context, data []byte) ([]byte, error) {
	var request struct {
		ID     json.RawMessage
		Method string
		Params map[string]any
	}
	if json.Unmarshal(data, &request) != nil {
		return nil, fmt.Errorf("invalid caller bytes")
	}
	// The generic discovered tool preserves canonical camel-case method names
	// and also admits visible non-core methods such as memory.list. Projected
	// core tool names use a separate snake-case naming convention.
	encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "method": "tools/call", "params": map[string]any{"name": "plug_call", "arguments": map[string]any{"method": request.Method, "params": request.Params}}})
	if err != nil {
		return nil, err
	}
	body, _, err := t.exchange(ctx, encoded)
	if err != nil {
		return nil, err
	}
	var response struct {
		JSONRPC string `json:"jsonrpc"`
		ID      json.RawMessage
		Result  struct {
			Content []struct{ Type, Text string }
			IsError bool            `json:"isError"`
			Meta    json.RawMessage `json:"_meta"`
		}
	}
	if json.Unmarshal(body, &response) != nil || response.JSONRPC != "2.0" || !bytes.Equal(response.ID, request.ID) || len(response.Result.Content) != 1 || response.Result.Content[0].Type != "text" {
		return nil, fmt.Errorf("MCP fixture returned invalid framing")
	}
	value := map[string]any{"jsonrpc": "2.0", "id": response.ID}
	field := "result"
	if response.Result.IsError {
		field = "error"
	}
	value[field] = json.RawMessage(response.Result.Content[0].Text)
	if len(response.Result.Meta) > 0 {
		value["_meta"] = response.Result.Meta
	}
	return json.Marshal(value)
}
