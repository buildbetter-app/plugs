package rehearsal

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"debug/elf"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
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
	RuntimeRollback  *Rollback     `json:"runtime_rollback,omitempty"`
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
	TranscriptCompared  bool      `json:"public_transcript_compared"`
	TranscriptExchanges int       `json:"public_transcript_exchanges"`
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

// Rollback keeps bounded preparation separate from an executed transition.
// Reverse/reimport qualification belongs to I6, not this native snapshot drill.
type Rollback struct {
	ImageDigest             string               `json:"image_digest"`
	PromptDigest            string               `json:"prompt_digest"`
	PluginDigest            string               `json:"plugin_digest"`
	DataDumpDigest          string               `json:"data_dump_digest"`
	InputsFrozen            bool                 `json:"inputs_frozen"`
	Software                string               `json:"software"`
	ReverseConversion       string               `json:"reverse_conversion"`
	Data                    string               `json:"data"`
	Complete                string               `json:"complete"`
	CandidatePinned         bool                 `json:"candidate_pinned"`
	OldToCandidate          bool                 `json:"old_to_candidate"`
	CandidatePublicReadback bool                 `json:"candidate_public_readback"`
	CandidateStopped        bool                 `json:"candidate_stopped"`
	CandidateToOld          bool                 `json:"candidate_to_old"`
	RollbackPublicReadback  bool                 `json:"rollback_public_readback"`
	AtomicReplacements      bool                 `json:"atomic_replacements"`
	IncarnationBoundary     bool                 `json:"incarnation_boundary"`
	CleanupVerified         bool                 `json:"cleanup_verified"`
	WrongDigestRefused      bool                 `json:"wrong_digest_refused"`
	PinMismatchRefused      bool                 `json:"pin_mismatch_refused"`
	OldIncarnation          string               `json:"old_incarnation"`
	CandidateIncarnation    string               `json:"candidate_incarnation"`
	RollbackIncarnation     string               `json:"rollback_incarnation"`
	OldBundle               RuntimeBundle        `json:"old_bundle"`
	CandidateBundle         RuntimeBundle        `json:"candidate_bundle"`
	RollbackBundle          RuntimeBundle        `json:"rollback_bundle"`
	Observations            []RuntimeObservation `json:"runtime_observations,omitempty"`
	Failure                 string               `json:"failure,omitempty"`
	RecoveryAttempted       bool                 `json:"recovery_attempted"`
	RecoveryVerified        bool                 `json:"recovery_verified"`
	RecoveredObservation    *RuntimeObservation  `json:"recovered_observation,omitempty"`
}

// RuntimeBundle pins real Bazel-built edition artifacts from one tested tree.
// ImageDigest is the inspected Docker image ID; ImageArchiveDigest binds the
// saved, reloadable image. Profiles are not historical release binaries.
type RuntimeBundle struct {
	Role               string `json:"role"`
	SourceSHA          string `json:"source_sha"`
	SourceTree         string `json:"source_tree"`
	ExecutablePath     string `json:"-"`
	ImagePath          string `json:"-"`
	PromptPath         string `json:"-"`
	PluginPath         string `json:"-"`
	DataPath           string `json:"-"`
	ExecutableDigest   string `json:"executable_sha256"`
	ImageDigest        string `json:"image_id_sha256"`
	ImageArchiveDigest string `json:"image_archive_sha256"`
	PromptDigest       string `json:"prompt_sha256"`
	PluginDigest       string `json:"plugin_sha256"`
	DataDigest         string `json:"data_sha256"`
}

type RuntimeObservation struct {
	ContainerID         string `json:"container_id"`
	ImageID             string `json:"running_image_id"`
	ExecutableDigest    string `json:"running_executable_sha256"`
	PromptDigest        string `json:"consumed_prompt_sha256"`
	PluginDigest        string `json:"consumed_plugin_sha256"`
	DataDigest          string `json:"restored_data_sha256"`
	StartupIncarnation  string `json:"startup_incarnation"`
	RestoredIncarnation string `json:"restored_incarnation"`
	ReceiptReplayed     bool   `json:"receipt_replayed"`
	StaleCursorRefused  bool   `json:"stale_cursor_refused"`
	PublicDataReadback  bool   `json:"public_data_readback"`
	PromptCanary        bool   `json:"prompt_canary"`
	PluginCanary        bool   `json:"plugin_canary"`
}

const (
	sampledFactoryWorkers    = 4
	sampledTasksPerWorker    = 2
	sampledFactoryTaskCount  = sampledFactoryWorkers * sampledTasksPerWorker
	rollbackBundleVersion    = "i8-disposable-bundle-v1"
	rollbackPromptVersion    = "rewritten-caller-prompt-v1"
	rollbackPluginSetVersion = "dry-run-plugin-set-v1"
	runtimeExecutableName    = "plug-runtime"
	runtimeImageName         = "image.tar"
	runtimePromptName        = "prompt.txt"
	runtimePluginName        = "plugin-set.txt"
	runtimeDataName          = "data.dump"
	runtimeMarkerKey         = "rollback.bundle"
)

var fixturePresence = map[string]any{"mode": "presence_check", "challenge": "disposable-rehearsal", "proof": "synthetic-only"}

// RunDisposable owns every resource it opens. There is intentionally no server
// URL, DSN, credential, command adapter, or cloud-provider option. Each run
// restores into a new empty store and compares HTTP-reference with MCP callers.
func RunDisposable(ctx context.Context, kind, sourceSHA, sourceTree string) (report Preparation, err error) {
	if (kind != "memory" && kind != "sqlite") || !validSourcePin(sourceSHA) || !validSourcePin(sourceTree) {
		return report, fmt.Errorf("memory/sqlite and full source SHA/tree required")
	}
	report = Preparation{SourceSHA: sourceSHA, SourceTree: sourceTree, Driver: kind, Preparation: "PASS", Qualification: "WAIT", BaselineKind: "sampled factory pool over synthetic authenticated HTTP/MCP fixtures; reference factory and staging UNQUALIFIED", FactoryWorkers: sampledFactoryWorkers, FactoryTasks: sampledFactoryTaskCount, AdminProof: "synthetic trusted-host session and presence fixture; real D32 UNQUALIFIED"}
	if _, err = requireSourcePins(sourceSHA, sourceTree); err != nil {
		return report, err
	}
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
			report.Preparation = "BLOCK"
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
	// Consume the separately implemented complete edition transition at this
	// runner boundary. Descriptor-only per-run rollback rows remain WAIT; this
	// drill builds and validates actual owned artifacts and verifies cleanup.
	rollback, rollbackErr := RunDeployableRollback(ctx, kind, sourceSHA, sourceTree)
	report.RuntimeRollback = &rollback
	if rollbackErr != nil || !completeRuntimeRollback(rollback, sourceSHA, sourceTree) {
		report.Preparation = "BLOCK"
		if rollbackErr == nil {
			rollbackErr = fmt.Errorf("complete owned rollback evidence missing")
		}
		return report, rollbackErr
	}
	evidence := GateEvidence{SourceSHA: sourceSHA, SourceTree: sourceTree}
	for _, run := range report.Runs {
		evidence.Rehearsals = append(evidence.Rehearsals, Receipt{SourceSHA: sourceSHA, SourceTree: sourceTree, Digest: run.DumpDigest, Finished: run.Finished, Clean: false})
	}
	report.GateWaits = EvaluateGate(time.Now(), evidence)
	return report, nil
}

func completeRuntimeRollback(r Rollback, sha, tree string) bool {
	if r.Software != "PASS_digest_verified_old_candidate_old" ||
		r.Complete != "PASS_executable_image_prompt_plugin_data_transition" ||
		r.Data != "PASS_snapshot_restore_public_readback" ||
		!r.InputsFrozen || !r.CandidatePinned || !r.OldToCandidate || !r.CandidatePublicReadback ||
		!r.CandidateStopped || !r.CandidateToOld || !r.RollbackPublicReadback ||
		!r.AtomicReplacements || !r.IncarnationBoundary || !r.CleanupVerified ||
		!r.WrongDigestRefused || !r.PinMismatchRefused || len(r.Observations) != 3 ||
		r.OldBundle.Role != "old" || r.CandidateBundle.Role != "candidate" || r.RollbackBundle.Role != "old" ||
		r.OldIncarnation == "" || r.CandidateIncarnation == "" || r.RollbackIncarnation == "" ||
		r.OldIncarnation == r.CandidateIncarnation || r.OldIncarnation == r.RollbackIncarnation ||
		r.CandidateIncarnation == r.RollbackIncarnation ||
		r.OldBundle.ExecutableDigest != r.RollbackBundle.ExecutableDigest ||
		r.OldBundle.ImageDigest != r.RollbackBundle.ImageDigest ||
		r.OldBundle.PromptDigest != r.RollbackBundle.PromptDigest ||
		r.OldBundle.PluginDigest != r.RollbackBundle.PluginDigest ||
		r.CandidateBundle.DataDigest != r.RollbackBundle.DataDigest ||
		r.ImageDigest != r.RollbackBundle.ImageDigest ||
		r.PromptDigest != r.RollbackBundle.PromptDigest ||
		r.PluginDigest != r.RollbackBundle.PluginDigest ||
		r.DataDumpDigest != r.RollbackBundle.DataDigest {
		return false
	}
	for i, b := range []RuntimeBundle{r.OldBundle, r.CandidateBundle, r.RollbackBundle} {
		if b.SourceSHA != sha || b.SourceTree != tree || len(b.ExecutableDigest) != 64 ||
			len(b.ImageDigest) != 64 || len(b.ImageArchiveDigest) != 64 ||
			len(b.PromptDigest) != 64 || len(b.PluginDigest) != 64 || len(b.DataDigest) != 64 {
			return false
		}
		observation := r.Observations[i]
		if observation.ImageID != "sha256:"+b.ImageDigest ||
			observation.ExecutableDigest != b.ExecutableDigest ||
			observation.PromptDigest != b.PromptDigest ||
			observation.PluginDigest != b.PluginDigest ||
			observation.DataDigest != b.DataDigest ||
			!observation.PublicDataReadback || !observation.ReceiptReplayed ||
			!observation.PromptCanary || !observation.PluginCanary ||
			(i > 0 && !observation.StaleCursorRefused) {
			return false
		}
	}
	return true
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
	seededID := ""
	for i := 0; i < sampledFactoryTaskCount; i++ {
		created, createErr := source.rpc.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": "synthetic task", "priority": 1}}, "")
		if createErr != nil {
			return run, createErr
		}
		var commit model.Commit
		if json.Unmarshal(created.JSON, &commit) != nil || len(commit.Results) != 1 || commit.Results[0].Ref.ID == "" {
			return run, fmt.Errorf("seed create returned no public bead ID")
		}
		if i == 0 {
			seededID = commit.Results[0].Ref.ID
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
	run.TranscriptExchanges, err = compareOwnedPublicCalls(ctx, target, seededID)
	if err != nil || run.TranscriptExchanges != 3 {
		if err == nil {
			err = fmt.Errorf("public caller transcript incomplete")
		}
		return run, err
	}
	run.TranscriptCompared = true
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
	t, close, err := openMCPTransport(ctx, f)
	if err != nil {
		return nil, nil, err
	}
	c, err := client.New(t, "rehearsal", agent)
	if err != nil {
		close()
		return nil, nil, err
	}
	return c, close, nil
}

func openMCPTransport(ctx context.Context, f *fixture) (*mcpTransport, func(), error) {
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
	return t, close, nil
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
		JSONRPC string `json:"jsonrpc"`
		ID      json.RawMessage
		Method  string
		Params  map[string]any
	}
	if wire.DecodeInto(data, &request) != nil {
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

// RuntimeEdition is set only by the two explicit Bazel edition targets.
var RuntimeEdition string

const runtimeOwnerLabel = "plug.disposable.rollback.owner"

type runtimePrompt struct {
	Edition      string   `json:"edition"`
	Instructions string   `json:"instructions"`
	Canary       []string `json:"canary"`
}

type runtimePlugins struct {
	Edition string   `json:"edition"`
	Modules []string `json:"modules"`
}

type runtimeLaunch struct {
	Bundle RuntimeBundle `json:"bundle"`
	Driver string        `json:"driver"`
}

// Tokens and the TLS private key never enter files, Docker arguments or logs.
// This one-shot bootstrap is exchanged through an owned 0600 Unix socket.
type runtimeBootstrap struct {
	ServiceToken       string
	AdminToken         string
	Certificate        []byte
	Edition            string
	ExecutableDigest   string
	PromptDigest       string
	PluginDigest       string
	DataDigest         string
	SourceSHA          string
	SourceTree         string
	StartupIncarnation string
}

func validSourcePin(pin string) bool {
	if !sha40.MatchString(pin) {
		return false
	}
	return pin != strings.Repeat(pin[:1], 40)
}

func sourceCheckout() (root, sha, tree string, err error) {
	root = os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) {
		return "", "", "", fmt.Errorf("exact RunsOn source checkout required")
	}
	git := func(args ...string) (string, error) {
		c := exec.Command("git", append([]string{"--no-optional-locks", "-C", root}, args...)...)
		c.Env = []string{"PATH=" + os.Getenv("PATH"), "GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
		out, e := c.Output()
		if e != nil {
			return "", fmt.Errorf("source identity verification failed")
		}
		return strings.TrimSpace(string(out)), nil
	}
	if sha, err = git("rev-parse", "--verify", "HEAD"); err != nil {
		return
	}
	if tree, err = git("rev-parse", "--verify", "HEAD^{tree}"); err != nil {
		return
	}
	if !validSourcePin(sha) || !validSourcePin(tree) {
		err = fmt.Errorf("source identity is not a real full SHA/tree")
		return
	}
	if _, err = git("diff-index", "--quiet", "HEAD", "--"); err != nil {
		return
	}
	for _, pair := range [][2]string{{"R1_SOURCE_SHA", sha}, {"R1_SOURCE_TREE", tree}} {
		if pin := os.Getenv(pair[0]); pin != "" && pin != pair[1] {
			err = fmt.Errorf("requested source pin differs from tested checkout")
			return
		}
	}
	// Bazel's declared source runfiles must match this exact clean checkout,
	// tying the data binaries' build graph to the source identity we record.
	for _, name := range []string{"tools/rehearsal/disposable.go", "tools/rehearsal/transcript.go", "tools/rehearsal/transcript_consumer.go", "tools/rehearsal/cmd/rollback-runtime/main.go"} {
		runfile := filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), name)
		if os.Getenv("TEST_SRCDIR") == "" {
			err = fmt.Errorf("Bazel source runfiles required")
			return
		}
		built, e1 := os.ReadFile(runfile)
		checked, e2 := os.ReadFile(filepath.Join(root, name))
		if e1 != nil || e2 != nil || !bytes.Equal(built, checked) {
			err = fmt.Errorf("Bazel source and checkout differ")
			return
		}
	}
	return
}

func requireSourcePins(sha, tree string) (string, error) {
	root, actualSHA, actualTree, err := sourceCheckout()
	if err != nil {
		return "", err
	}
	if sha != actualSHA || tree != actualTree {
		return "", fmt.Errorf("bundle pin does not match tested source/tree")
	}
	return root, nil
}

func fileDigest(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 256<<20 {
		return "", fmt.Errorf("bounded regular artifact required")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, io.LimitReader(f, (256<<20)+1)); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func digestBytes(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

func runtimeBinary(role string) (string, error) {
	env := map[string]string{"old": "R1_OLD_RUNTIME_BINARY", "candidate": "R1_CANDIDATE_RUNTIME_BINARY"}[role]
	if env == "" {
		return "", fmt.Errorf("unsupported edition")
	}
	path := os.Getenv(env)
	expected := "tools/rehearsal/cmd/rollback-runtime/" + role + "-runtime_/" + role + "-runtime"
	if path != expected {
		return "", fmt.Errorf("explicit Bazel edition output required")
	}
	path = filepath.Join(os.Getenv("TEST_SRCDIR"), os.Getenv("TEST_WORKSPACE"), path)
	binary, err := elf.Open(path)
	if err != nil {
		return "", fmt.Errorf("real ELF executable required")
	}
	defer binary.Close()
	if binary.Type != elf.ET_EXEC {
		return "", fmt.Errorf("static ELF executable required")
	}
	for _, program := range binary.Progs {
		if program.Type == elf.PT_INTERP || program.Type == elf.PT_DYNAMIC {
			return "", fmt.Errorf("dynamic runtime is refused")
		}
	}
	return path, nil
}

type ownedDocker struct {
	owner      string
	root       string
	network    string
	images     []string
	containers []string
}

func newOwnedDocker(root string) *ownedDocker {
	return &ownedDocker{owner: filepath.Base(root), root: root}
}

func (d *ownedDocker) command(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
	deadline, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	command := exec.CommandContext(deadline, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "DOCKER_CONFIG=" + filepath.Join(d.root, "docker-config"), "DOCKER_BUILDKIT=0"}
	command.Stdin = input
	out, err := command.CombinedOutput()
	if err != nil {
		// Docker output is never reflected: bootstrap credentials are not Docker
		// inputs, but a daemon error must not turn into arbitrary log content.
		return nil, fmt.Errorf("owned Docker %s failed", args[0])
	}
	return out, nil
}

func (d *ownedDocker) owned(ctx context.Context, kind, id string) error {
	var out []byte
	var err error
	if kind != "network" {
		out, err = d.command(ctx, nil, kind, "inspect", "--format", "{{ index .Config.Labels \""+runtimeOwnerLabel+"\" }}", id)
	}
	if kind == "network" {
		out, err = d.command(ctx, nil, "network", "inspect", "--format", "{{ index .Labels \""+runtimeOwnerLabel+"\" }}", id)
	}
	if err != nil || strings.TrimSpace(string(out)) != d.owner {
		return fmt.Errorf("resource ownership check failed")
	}
	return nil
}

func (d *ownedDocker) createNetwork(ctx context.Context) error {
	d.network = d.owner + "-net"
	_, err := d.command(ctx, nil, "network", "create", "--internal", "--label", runtimeOwnerLabel+"="+d.owner, d.network)
	if err != nil {
		d.network = ""
		return err
	}
	return nil
}

func (d *ownedDocker) cleanup(ctx context.Context) error {
	var result error
	// Discover only this unguessable owner's labels. This also covers a create
	// that reached the daemon but lost its acknowledgement; no unrelated
	// container/image/network is inspected or removed.
	for _, kind := range []string{"container", "image", "network"} {
		args := []string{kind, "ls", "--quiet", "--filter", "label=" + runtimeOwnerLabel + "=" + d.owner}
		if kind == "container" {
			args = append(args, "--all")
		}
		out, err := d.command(ctx, nil, args...)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		seen := map[string]bool{}
		for _, id := range strings.Fields(string(out)) {
			if seen[id] {
				continue
			}
			seen[id] = true
			if err = d.owned(ctx, kind, id); err != nil {
				result = errors.Join(result, err)
				continue
			}
			remove := []string{kind, "rm", id}
			if kind == "container" {
				remove = []string{kind, "rm", "--force", id}
			}
			if _, err = d.command(ctx, nil, remove...); err != nil {
				result = errors.Join(result, err)
			}
		}
	}
	// A successful daemon query and an empty owner selection prove absence;
	// an inspect failure by itself never does.
	for _, kind := range []string{"container", "image", "network"} {
		args := []string{kind, "ls", "--quiet", "--filter", "label=" + runtimeOwnerLabel + "=" + d.owner}
		if kind == "container" {
			args = append(args, "--all")
		}
		out, err := d.command(ctx, nil, args...)
		if err != nil || len(bytes.TrimSpace(out)) != 0 {
			result = errors.Join(result, fmt.Errorf("owned resource cleanup not verified"))
		}
	}
	return result
}

func buildRuntimeBundle(ctx context.Context, d *ownedDocker, role, sha, tree string, data []byte) (b RuntimeBundle, err error) {
	source, err := requireSourcePins(sha, tree)
	if err != nil {
		return b, err
	}
	binary, err := runtimeBinary(role)
	if err != nil {
		return b, err
	}
	if _, err = dumpformat.Read(bytes.NewReader(data)); err != nil {
		return b, fmt.Errorf("real native snapshot required")
	}
	dir := filepath.Join(d.root, role+"-bundle")
	if err = os.Mkdir(dir, 0700); err != nil {
		return b, err
	}
	b = RuntimeBundle{Role: role, SourceSHA: sha, SourceTree: tree, ExecutablePath: filepath.Join(dir, runtimeExecutableName), ImagePath: filepath.Join(dir, runtimeImageName), PromptPath: filepath.Join(dir, runtimePromptName), PluginPath: filepath.Join(dir, runtimePluginName), DataPath: filepath.Join(dir, runtimeDataName)}
	executable, err := os.ReadFile(binary)
	if err != nil {
		return b, err
	}
	instructions, err := os.ReadFile(filepath.Join(source, "plugins/plug/SKILL.md"))
	if err != nil {
		return b, err
	}
	canary := []string{"memory", "list", "--limit", "1"}
	if role == "candidate" {
		canary = []string{"brief", "--budget-tokens", "1"}
	}
	prompt, _ := json.Marshal(runtimePrompt{role, string(instructions), canary})
	modules := []string{"authn.apikey"}
	if role == "candidate" {
		modules = append(modules, "transport.mcp")
	}
	plugins, _ := json.Marshal(runtimePlugins{role, modules})
	for path, content := range map[string][]byte{b.ExecutablePath: executable, b.PromptPath: prompt, b.PluginPath: plugins, b.DataPath: data} {
		if err = writeRollbackArtifact(path, content); err != nil {
			return b, err
		}
	}
	if err = os.Chmod(b.ExecutablePath, 0500); err != nil {
		return b, err
	}
	b.ExecutableDigest = digestBytes(executable)
	b.PromptDigest = digestBytes(prompt)
	b.PluginDigest = digestBytes(plugins)
	b.DataDigest = digestBytes(data)
	// A scratch build consumes only these supplied bytes. No package manager,
	// registry pull, network build step or nominal image descriptor is involved.
	var contextTar bytes.Buffer
	tw := tar.NewWriter(&contextTar)
	dockerfile := []byte("FROM scratch\nCOPY plug-runtime /plug-runtime\nENTRYPOINT [\"/plug-runtime\"]\n")
	for name, content := range map[string][]byte{"Dockerfile": dockerfile, runtimeExecutableName: executable} {
		mode := int64(0400)
		if name == runtimeExecutableName {
			mode = 0555
		}
		if err = tw.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(content))}); err != nil {
			return b, err
		}
		if _, err = tw.Write(content); err != nil {
			return b, err
		}
	}
	if err = tw.Close(); err != nil {
		return b, err
	}
	tag := d.owner + "-" + role
	_, err = d.command(ctx, &contextTar, "build", "--network=none", "--pull=false", "--no-cache",
		"--label", runtimeOwnerLabel+"="+d.owner, "--label", "plug.source.sha="+sha, "--label", "plug.source.tree="+tree,
		"--label", "plug.edition="+role, "--label", "plug.executable.sha256="+b.ExecutableDigest, "--tag", tag, "-")
	if err != nil {
		return b, err
	}
	out, err := d.command(ctx, nil, "image", "inspect", "--format", "{{.Id}}", tag)
	if err != nil {
		return b, err
	}
	id := strings.TrimSpace(string(out))
	if !strings.HasPrefix(id, "sha256:") || len(id) != 71 {
		return b, fmt.Errorf("built image identity missing")
	}
	d.images = append(d.images, id)
	b.ImageDigest = strings.TrimPrefix(id, "sha256:")
	if _, err = d.command(ctx, nil, "image", "save", "--output", b.ImagePath, id); err != nil {
		return b, err
	}
	b.ImageArchiveDigest, err = fileDigest(b.ImagePath)
	if err != nil {
		return b, err
	}
	return b, validateRuntimeBundle(b)
}

func runtimeArtifactPaths(b RuntimeBundle) map[string]string {
	return map[string]string{b.ExecutablePath: b.ExecutableDigest, b.ImagePath: b.ImageArchiveDigest, b.PromptPath: b.PromptDigest, b.PluginPath: b.PluginDigest, b.DataPath: b.DataDigest}
}

func validateRuntimeBundle(b RuntimeBundle) error {
	if _, err := requireSourcePins(b.SourceSHA, b.SourceTree); err != nil {
		return err
	}
	if b.Role != "old" && b.Role != "candidate" {
		return fmt.Errorf("edition pin refused")
	}
	if len(b.ImageDigest) != 64 {
		return fmt.Errorf("image pin missing")
	}
	for path, want := range runtimeArtifactPaths(b) {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("artifact must be an owned regular file")
		}
		got, err := fileDigest(path)
		if err != nil || len(want) != 64 || got != want {
			return fmt.Errorf("artifact digest mismatch")
		}
	}
	binary, err := runtimeBinary(b.Role)
	if err != nil {
		return err
	}
	built, err := fileDigest(binary)
	if err != nil || built != b.ExecutableDigest {
		return fmt.Errorf("executable differs from tested Bazel edition")
	}
	return validateSavedImage(b)
}

// installRuntimeFiles validates every input, stages a complete generation and
// changes one relative symlink atomically. Errors before rename leave the old
// executable/prompt/plugin/data tuple untouched. The fault seam is test-only.
func installRuntimeFiles(root string, b RuntimeBundle, kind string, fault func(string) error) (err error) {
	if kind != "memory" && kind != "sqlite" {
		return fmt.Errorf("disposable driver required")
	}
	if err = validateRuntimeBundle(b); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Join(root, "generations"), 0700); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(filepath.Join(root, "generations"), b.Role+"-")
	if err != nil {
		return err
	}
	activated := false
	defer func() {
		if !activated {
			err = errors.Join(err, os.RemoveAll(stage))
		}
	}()
	for _, item := range []struct{ name, path string }{
		{runtimeExecutableName, b.ExecutablePath}, {runtimePromptName, b.PromptPath}, {runtimePluginName, b.PluginPath}, {runtimeDataName, b.DataPath},
	} {
		content, e := os.ReadFile(item.path)
		if e != nil {
			return e
		}
		if err = writeRollbackArtifact(filepath.Join(stage, item.name), content); err != nil {
			return err
		}
		if item.name == runtimeExecutableName {
			if err = os.Chmod(filepath.Join(stage, item.name), 0500); err != nil {
				return err
			}
		}
		if fault != nil {
			if err = fault(item.name); err != nil {
				return err
			}
		}
	}
	launch, _ := json.Marshal(runtimeLaunch{Bundle: b, Driver: kind})
	if err = writeRollbackArtifact(filepath.Join(stage, "launch.json"), launch); err != nil {
		return err
	}
	// Re-read the staged bytes before publishing; source mutation or a partial
	// copy cannot create a mixed generation.
	for name, want := range map[string]string{runtimeExecutableName: b.ExecutableDigest, runtimePromptName: b.PromptDigest, runtimePluginName: b.PluginDigest, runtimeDataName: b.DataDigest} {
		got, e := fileDigest(filepath.Join(stage, name))
		if e != nil || got != want {
			return fmt.Errorf("staged artifact digest mismatch")
		}
	}
	relative, err := filepath.Rel(root, stage)
	if err != nil {
		return err
	}
	link := filepath.Join(root, ".next")
	if err = os.Symlink(relative, link); err != nil {
		return err
	}
	defer os.Remove(link)
	if fault != nil {
		if err = fault("activate"); err != nil {
			return err
		}
	}
	if err = os.Rename(link, filepath.Join(root, "current")); err != nil {
		return err
	}
	activated = true
	return nil
}

// ServeRollbackRuntime is intentionally a separate rehearsal-only executable.
// It accepts no external address, DSN, credentials, cloud configuration or
// production mode. All mutable state is below its single owned /owned mount.
func ServeRollbackRuntime(ctx context.Context) (err error) {
	const owned = "/owned"
	generation, err := filepath.EvalSymlinks(owned + "/current")
	if err != nil || !strings.HasPrefix(generation, owned+"/generations/") {
		return fmt.Errorf("owned generation required")
	}
	raw, err := os.ReadFile(generation + "/launch.json")
	if err != nil {
		return err
	}
	var launch runtimeLaunch
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&launch) != nil || decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid owned launch")
	}
	b := launch.Bundle
	if (launch.Driver != "memory" && launch.Driver != "sqlite") || b.Role != RuntimeEdition || (RuntimeEdition != "old" && RuntimeEdition != "candidate") || !validSourcePin(b.SourceSHA) || !validSourcePin(b.SourceTree) {
		return fmt.Errorf("runtime pin refused")
	}
	for name, want := range map[string]string{runtimeExecutableName: b.ExecutableDigest, runtimePromptName: b.PromptDigest, runtimePluginName: b.PluginDigest, runtimeDataName: b.DataDigest} {
		got, e := fileDigest(filepath.Join(generation, name))
		if e != nil || len(want) != 64 || got != want {
			return fmt.Errorf("installed runtime input digest mismatch")
		}
	}
	self, err := fileDigest("/proc/self/exe")
	if err != nil || self != b.ExecutableDigest {
		return fmt.Errorf("running executable digest mismatch")
	}
	promptRaw, err := os.ReadFile(filepath.Join(generation, runtimePromptName))
	if err != nil {
		return err
	}
	pluginRaw, err := os.ReadFile(filepath.Join(generation, runtimePluginName))
	if err != nil {
		return err
	}
	var prompt runtimePrompt
	var plugins runtimePlugins
	if json.Unmarshal(promptRaw, &prompt) != nil || json.Unmarshal(pluginRaw, &plugins) != nil || prompt.Edition != RuntimeEdition || plugins.Edition != RuntimeEdition || prompt.Instructions == "" {
		return fmt.Errorf("runtime profile mismatch")
	}
	command, err := cli.ParseCommand(prompt.Canary)
	if err != nil || (command.Method != "memory.list" && command.Method != "agent.brief") {
		return fmt.Errorf("unsafe rehearsal prompt")
	}
	modules := strings.Join(plugins.Modules, ",")
	if modules != "authn.apikey" && modules != "authn.apikey,transport.mcp" {
		return fmt.Errorf("unsupported plugin set")
	}
	dataDir := owned + "/active-data"
	entries, err := os.ReadDir(dataDir)
	if err != nil || len(entries) != 0 {
		return fmt.Errorf("fresh empty runtime data required")
	}
	provider := authnapikey.New(authnapikey.Config{})
	issued, err := provider.Issue(model.PrincipalRef{Kind: "service", ID: "service:rehearsal"}, time.Now().Add(time.Hour))
	if err != nil {
		return fmt.Errorf("runtime API key issue failed")
	}
	admin, err := newFixtureAdmin()
	if err != nil {
		return err
	}
	defer admin.revoked.Store(true)
	dsn := ""
	if launch.Driver == "sqlite" {
		dsn = filepath.Join(dataDir, "runtime.db")
	}
	r, err := composition.Open(ctx, composition.Config{
		Driver: launch.Driver, DSN: dsn, Project: "rehearsal", AuthMode: "authenticated",
		Auth: authn.Config{Provider: provider, CoreSessions: admin},
		// Public cursor rejection across native restore is checked with a stable,
		// public test signing key. It is never a production signing key.
		CursorKey: bytes.Repeat([]byte{0x4a}, 32),
		RestorePresence: restore.PresenceVerifierFunc(func(ctx context.Context, method, digest string, p restore.Presence) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !strings.HasPrefix(method, "admin.") || digest == "" || p["challenge"] != fixturePresence["challenge"] || p["proof"] != fixturePresence["proof"] {
				return fmt.Errorf("synthetic test presence refused")
			}
			return nil
		}),
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = errors.Join(err, r.Close(closeCtx))
	}()
	mux := http.NewServeMux()
	mux.Handle("/rpc/v1", r.HTTPHandler)
	if modules == "authn.apikey,transport.mcp" {
		handler, e := mcp.New(r.RPC, r.Authenticator, mcp.Config{})
		if e != nil {
			return e
		}
		defer func() { err = errors.Join(err, handler.Close()) }()
		mux.Handle("/mcp", handler)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	cert, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rollback.invalid"},
		DNSNames: []string{"rollback.invalid"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rollback.invalid"}}, public, private)
	if err != nil {
		return err
	}
	server, err := r.HTTPHandler.ListenHandler(ctx, "0.0.0.0:8077", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{cert}, PrivateKey: private}}}, mux)
	if err != nil {
		return err
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve() }()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err = errors.Join(err, server.Shutdown(shutdown))
		if serveErr := <-served; serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			err = errors.Join(err, serveErr)
		}
	}()
	meta, err := r.Store.Meta(ctx)
	if err != nil || !meta.Head.IsZero() {
		return fmt.Errorf("runtime target was not empty")
	}
	socket, err := net.Listen("unix", owned+"/bootstrap.sock")
	if err != nil {
		return err
	}
	defer socket.Close()
	if err = os.Chmod(owned+"/bootstrap.sock", 0600); err != nil {
		return err
	}
	go func() { <-ctx.Done(); socket.Close() }()
	connection, err := socket.Accept()
	if err != nil {
		return fmt.Errorf("owned bootstrap failed")
	}
	_ = connection.SetWriteDeadline(time.Now().Add(10 * time.Second))
	boot := runtimeBootstrap{issued.Token, admin.token, cert, RuntimeEdition, self, digestBytes(promptRaw), digestBytes(pluginRaw), b.DataDigest, b.SourceSHA, b.SourceTree, meta.Incarnation}
	err = json.NewEncoder(connection).Encode(boot)
	err = errors.Join(err, connection.Close(), socket.Close())
	if err != nil {
		return fmt.Errorf("owned bootstrap exchange failed")
	}
	// The rewritten prompt is consumed again by the parent canary over public
	// RPC; exposing a digest alone cannot mark the canary as passed.
	<-ctx.Done()
	return nil
}

type runtimeHTTP struct {
	endpoint, token string
	session         string
	http            *http.Client
}

func (t *runtimeHTTP) exchange(ctx context.Context, path string, body []byte) ([]byte, int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, fmt.Errorf("owned RPC request invalid")
	}
	request.GetBody = nil
	request.Host = "rollback.invalid"
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Plug-RPC", "1")
	if t.token != "" {
		request.Header.Set("Authorization", "Bearer "+t.token)
	}
	if path == "/mcp" && t.session != "" {
		request.Header.Set("MCP-Session-Id", t.session)
	}
	response, err := t.http.Do(request)
	if err != nil {
		return nil, 0, fmt.Errorf("owned runtime HTTP failed")
	}
	defer response.Body.Close()
	if path == "/mcp" && response.Header.Get("MCP-Session-Id") != "" {
		t.session = response.Header.Get("MCP-Session-Id")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
	if err != nil || len(data) > 8<<20 {
		return nil, response.StatusCode, fmt.Errorf("owned RPC response invalid")
	}
	return data, response.StatusCode, nil
}
func (t *runtimeHTTP) RoundTrip(ctx context.Context, body []byte) ([]byte, error) {
	raw, status, err := t.exchange(ctx, "/rpc/v1", body)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("owned RPC admission status %d", status)
	}
	return raw, nil
}

type ownedRuntime struct {
	id, endpoint, root        string
	docker                    *ownedDocker
	transport, adminTransport *runtimeHTTP
	rpc, adminRPC             *client.Client
	bootstrap                 runtimeBootstrap
	stopped                   bool
	stopAttempted             bool
	forwarder                 *runtimeForwarder
}

func startOwnedRuntime(ctx context.Context, d *ownedDocker, root string, b RuntimeBundle) (h *ownedRuntime, err error) {
	if err = validateRuntimeBundle(b); err != nil {
		return nil, err
	}
	if _, err = d.command(ctx, nil, "image", "load", "--input", b.ImagePath); err != nil {
		return nil, err
	}
	id := "sha256:" + b.ImageDigest
	if err = d.owned(ctx, "image", id); err != nil {
		return nil, err
	}
	out, err := d.command(ctx, nil, "image", "inspect", "--format", "{{.Id}}|{{ index .Config.Labels \"plug.source.sha\" }}|{{ index .Config.Labels \"plug.source.tree\" }}|{{ index .Config.Labels \"plug.edition\" }}|{{ index .Config.Labels \"plug.executable.sha256\" }}", id)
	if err != nil || strings.TrimSpace(string(out)) != strings.Join([]string{id, b.SourceSHA, b.SourceTree, b.Role, b.ExecutableDigest}, "|") {
		return nil, fmt.Errorf("loaded image pin mismatch")
	}
	if err = os.Mkdir(filepath.Join(root, "active-data"), 0700); err != nil {
		return nil, fmt.Errorf("runtime data must be newly created")
	}
	name := d.owner + "-" + b.Role + "-" + strconv.Itoa(len(d.containers))
	out, err = d.command(ctx, nil, "container", "create", "--name", name, "--label", runtimeOwnerLabel+"="+d.owner,
		"--network", d.network, "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,noexec",
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"--mount", "type=bind,source="+root+",target=/owned", id)
	if err != nil {
		return nil, err
	}
	containerID := strings.TrimSpace(string(out))
	d.containers = append(d.containers, containerID)
	h = &ownedRuntime{id: containerID, root: root, docker: d}
	if _, err = d.command(ctx, nil, "container", "start", containerID); err != nil {
		return h, err
	}
	inspected, err := d.command(ctx, nil, "container", "inspect", containerID)
	var state []struct {
		Image           string
		State           struct{ Running bool }
		NetworkSettings struct {
			Networks map[string]struct{ IPAddress, NetworkID string }
		}
	}
	if err != nil || json.Unmarshal(inspected, &state) != nil || len(state) != 1 || !state[0].State.Running || state[0].Image != id {
		return h, fmt.Errorf("running image pin mismatch")
	}
	network, ok := state[0].NetworkSettings.Networks[d.network]
	ip := net.ParseIP(network.IPAddress)
	if !ok || ip == nil || !ip.IsPrivate() {
		return h, fmt.Errorf("owned internal container address missing")
	}
	networkInfo, e := d.command(ctx, nil, "network", "inspect", "--format", "{{.Id}}|{{.Internal}}", d.network)
	if e != nil || strings.TrimSpace(string(networkInfo)) != network.NetworkID+"|true" {
		return h, fmt.Errorf("runtime network is not owned/internal")
	}
	h.forwarder, err = newRuntimeForwarder(ctx, net.JoinHostPort(ip.String(), "8077"))
	if err != nil {
		return h, err
	}
	h.endpoint = "https://" + h.forwarder.listener.Addr().String()
	// Only readiness of the private bootstrap is polled. RPC writes, restore,
	// build and failed transitions are never retried.
	ready, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var connection net.Conn
	for {
		connection, err = (&net.Dialer{}).DialContext(ready, "unix", filepath.Join(root, "bootstrap.sock"))
		if err == nil {
			break
		}
		select {
		case <-ready.Done():
			return h, fmt.Errorf("owned runtime bootstrap deadline")
		case <-time.After(20 * time.Millisecond):
		}
	}
	defer connection.Close()
	_ = connection.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err = json.NewDecoder(io.LimitReader(connection, 16<<10)).Decode(&h.bootstrap); err != nil {
		return h, fmt.Errorf("owned bootstrap invalid")
	}
	boot := h.bootstrap
	if boot.Edition != b.Role || boot.ExecutableDigest != b.ExecutableDigest || boot.PromptDigest != b.PromptDigest || boot.PluginDigest != b.PluginDigest || boot.DataDigest != b.DataDigest || boot.SourceSHA != b.SourceSHA || boot.SourceTree != b.SourceTree || boot.StartupIncarnation == "" {
		return h, fmt.Errorf("running artifact pin mismatch")
	}
	cert, err := x509.ParseCertificate(boot.Certificate)
	if err != nil {
		return h, fmt.Errorf("bootstrap TLS certificate invalid")
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	httpClient := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "rollback.invalid"}, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	h.transport = &runtimeHTTP{endpoint: h.endpoint, token: boot.ServiceToken, http: httpClient}
	h.adminTransport = &runtimeHTTP{endpoint: h.endpoint, token: boot.AdminToken, http: httpClient}
	h.rpc, err = client.New(h.transport, "rehearsal", "rollback-canary")
	if err == nil {
		h.adminRPC, err = client.New(h.adminTransport, "rehearsal", "fixture-admin")
	}
	return h, err
}

func (h *ownedRuntime) stop(ctx context.Context) error {
	if h == nil || h.stopped || h.stopAttempted {
		return fmt.Errorf("runtime stop requires an active owned service")
	}
	h.stopAttempted = true
	if err := h.docker.owned(ctx, "container", h.id); err != nil {
		return err
	}
	if _, err := h.docker.command(ctx, nil, "container", "stop", "--time", "10", h.id); err != nil {
		return err
	}
	out, err := h.docker.command(ctx, nil, "container", "inspect", "--format", "{{.State.Running}}|{{.State.ExitCode}}", h.id)
	if err != nil || strings.TrimSpace(string(out)) != "false|0" {
		return fmt.Errorf("owned service did not stop cleanly")
	}
	if h.transport != nil {
		h.transport.http.CloseIdleConnections()
	}
	if h.forwarder != nil {
		if err = h.forwarder.close(); err != nil {
			return err
		}
	}
	address := strings.TrimPrefix(h.endpoint, "https://")
	connection, dialErr := net.DialTimeout("tcp", address, time.Second)
	if dialErr == nil {
		connection.Close()
		return fmt.Errorf("stopped service endpoint still accepts connections")
	}
	if _, err = os.Lstat(filepath.Join(h.root, "bootstrap.sock")); !os.IsNotExist(err) {
		return fmt.Errorf("runtime bootstrap socket not cleaned")
	}
	if err = os.Rename(filepath.Join(h.root, "active-data"), filepath.Join(h.root, "retired-"+h.id[:12])); err != nil {
		return err
	}
	h.stopped = true
	return nil
}

// A failed stop never starts a successor. A failed install/start/readback tries
// one owned recovery and still returns the original failure, even if recovery
// succeeds. A recovery failure cannot be rewritten into a passing transition.
func performRuntimeTransition(stop, install, start, readback, recoverOld func() error) error {
	if err := stop(); err != nil {
		return fmt.Errorf("owned stop failed: %w", err)
	}
	for _, step := range []struct {
		name string
		run  func() error
	}{{"install", install}, {"start", start}, {"readback", readback}} {
		if err := step.run(); err != nil {
			return errors.Join(fmt.Errorf("transition %s failed: %w", step.name, err), recoverOld())
		}
	}
	return nil
}

func validateSavedImage(b RuntimeBundle) error {
	file, err := os.Open(b.ImagePath)
	if err != nil {
		return err
	}
	defer file.Close()
	var config struct {
		Config struct{ Labels map[string]string }
		RootFS struct {
			Type    string
			DiffIDs []string `json:"diff_ids"`
		} `json:"rootfs"`
	}
	type descriptor struct {
		MediaType string `json:"mediaType"`
		Digest    string `json:"digest"`
		Platform  *struct {
			Architecture string `json:"architecture"`
			OS           string `json:"os"`
		} `json:"platform,omitempty"`
	}
	type imageManifest struct {
		Config    descriptor   `json:"config"`
		Layers    []descriptor `json:"layers"`
		Manifests []descriptor `json:"manifests"`
	}
	var legacyManifests []struct {
		Config string
		Layers []string
	}
	members := map[string]string{}
	memberBytes := map[string][]byte{}
	reader := tar.NewReader(file)
	for {
		header, e := reader.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return fmt.Errorf("saved image archive invalid")
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if (header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA) || header.Size > 256<<20 {
			return fmt.Errorf("saved image member must be a bounded regular file")
		}
		if _, duplicate := members[header.Name]; duplicate {
			return fmt.Errorf("saved image member repeated")
		}
		var raw []byte
		if header.Size <= 64<<20 {
			raw, err = io.ReadAll(io.LimitReader(reader, header.Size))
			if err != nil || int64(len(raw)) != header.Size {
				return fmt.Errorf("saved image member incomplete")
			}
			memberBytes[header.Name] = raw
			members[header.Name] = digestBytes(raw)
		} else {
			hash := sha256.New()
			if _, err = io.CopyN(hash, reader, header.Size); err != nil {
				return fmt.Errorf("saved image member incomplete")
			}
			members[header.Name] = hex.EncodeToString(hash.Sum(nil))
		}
		if strings.HasPrefix(header.Name, "blobs/sha256/") && header.Name != "blobs/sha256/"+members[header.Name] {
			return fmt.Errorf("saved image blob digest mismatch")
		}
	}
	// Docker 29 saves OCI indexes whose top-level digest is the inspected image
	// ID. Legacy archives carry the same pin in <image-id>.json. Require that
	// exact member before accepting the archive so a caller cannot replace only
	// the in-memory image digest.
	if memberBytes["blobs/sha256/"+b.ImageDigest] == nil {
		legacyName := b.ImageDigest + ".json"
		if memberBytes[legacyName] == nil || members[legacyName] != b.ImageDigest {
			return fmt.Errorf("saved runnable image does not match image pin")
		}
	}

	configName := ""
	layers := []string{}
	if raw := memberBytes["manifest.json"]; raw != nil {
		if json.Unmarshal(raw, &legacyManifests) != nil || len(legacyManifests) != 1 {
			return fmt.Errorf("saved image manifest invalid")
		}
		configName, layers = legacyManifests[0].Config, legacyManifests[0].Layers
	} else if raw := memberBytes["index.json"]; raw != nil {
		var index imageManifest
		if json.Unmarshal(raw, &index) != nil || len(index.Manifests) == 0 {
			return fmt.Errorf("saved image index invalid")
		}
		selectDescriptor := func(items []descriptor) descriptor {
			for _, item := range items {
				if item.Platform != nil && item.Platform.OS == "linux" && item.Platform.Architecture == "amd64" {
					return item
				}
			}
			return items[0]
		}
		current := selectDescriptor(index.Manifests)
		for depth := 0; depth < 3; depth++ {
			name := "blobs/sha256/" + strings.TrimPrefix(current.Digest, "sha256:")
			raw := memberBytes[name]
			if raw == nil {
				return fmt.Errorf("saved image manifest blob missing")
			}
			var candidate imageManifest
			if json.Unmarshal(raw, &candidate) != nil {
				return fmt.Errorf("saved image manifest invalid")
			}
			if candidate.Config.Digest != "" {
				configName = "blobs/sha256/" + strings.TrimPrefix(candidate.Config.Digest, "sha256:")
				for _, layer := range candidate.Layers {
					layers = append(layers, "blobs/sha256/"+strings.TrimPrefix(layer.Digest, "sha256:"))
				}
				break
			}
			if len(candidate.Manifests) == 0 {
				return fmt.Errorf("saved image manifest invalid")
			}
			current = selectDescriptor(candidate.Manifests)
		}
	}
	if configName == "" || len(layers) == 0 && memberBytes["manifest.json"] == nil {
		return fmt.Errorf("saved runnable image does not match image pin")
	}
	// ImageDigest follows Docker image-ID semantics. For an OCI archive that
	// means the selected manifest's config digest, rather than its manifest
	// descriptor digest or an arbitrary blob digest.
	selectedConfigDigest := strings.TrimSuffix(filepath.Base(configName), ".json")
	if strings.HasPrefix(configName, "blobs/sha256/") {
		selectedConfigDigest = strings.TrimPrefix(selectedConfigDigest, "sha256:")
	}
	if selectedConfigDigest != b.ImageDigest {
		return fmt.Errorf("saved image config digest does not match image pin")
	}
	rawConfig := memberBytes[configName]
	if rawConfig == nil || members[configName] != strings.TrimPrefix(filepath.Base(configName), "sha256:") && strings.HasPrefix(configName, "blobs/sha256/") {
		return fmt.Errorf("saved image config digest or content mismatch")
	}
	if json.Unmarshal(rawConfig, &config) != nil {
		return fmt.Errorf("saved image config digest or content mismatch")
	}
	for key, want := range map[string]string{"plug.source.sha": b.SourceSHA, "plug.source.tree": b.SourceTree, "plug.edition": b.Role, "plug.executable.sha256": b.ExecutableDigest} {
		if config.Config.Labels[key] != want {
			return fmt.Errorf("saved image source/executable pin mismatch")
		}
	}
	// Docker image-save emits uncompressed layer tars. Bind every required
	// member, in order, to the rootfs digests inside the pinned image config.
	// A config-only or rehashed corrupt archive must fail before activation.
	if config.RootFS.Type != "layers" || len(layers) == 0 || len(layers) != len(config.RootFS.DiffIDs) {
		return fmt.Errorf("saved image layer set incomplete")
	}
	for i, name := range layers {
		layer := memberBytes[name]
		if layer == nil || members[name] == "" {
			return fmt.Errorf("saved image layer missing or digest mismatch")
		}
		content := layer
		if len(layer) >= 2 && layer[0] == 0x1f && layer[1] == 0x8b {
			gzipReader, err := gzip.NewReader(bytes.NewReader(layer))
			if err != nil {
				return fmt.Errorf("saved image layer invalid")
			}
			content, err = io.ReadAll(io.LimitReader(gzipReader, 256<<20))
			closeErr := gzipReader.Close()
			if err != nil || closeErr != nil {
				return fmt.Errorf("saved image layer invalid")
			}
		}
		if "sha256:"+digestBytes(content) != config.RootFS.DiffIDs[i] {
			return fmt.Errorf("saved image layer missing or digest mismatch")
		}
	}
	return nil
}

func publicState(ctx context.Context, c *client.Client) (string, error) {
	ready, err := readySignature(ctx, c)
	if err != nil {
		return "", err
	}
	result, err := c.Call(ctx, "memory.list", map[string]any{"limit": 1000}, "")
	if err != nil {
		return "", err
	}
	var page struct {
		Items      []struct{ ID string }
		NextCursor string
	}
	if json.Unmarshal(result.JSON, &page) != nil || page.NextCursor != "" {
		return "", fmt.Errorf("bounded public memory readback invalid")
	}
	rows := []string{ready}
	for _, item := range page.Items {
		got, e := c.Call(ctx, "memory.get", map[string]any{"id": item.ID}, "")
		if e != nil {
			return "", e
		}
		var row map[string]any
		if json.Unmarshal(got.JSON, &row) != nil {
			return "", fmt.Errorf("public memory readback invalid")
		}
		delete(row, "revision")
		raw, _ := json.Marshal(row)
		rows = append(rows, string(raw))
	}
	sort.Strings(rows)
	return strings.Join(rows, "\n"), nil
}

func currentRuntimeIncarnation(ctx context.Context, h *ownedRuntime) (string, error) {
	// server.hello takes no _meta. The generic command client always adds it,
	// so use the exact public frame through the same authenticated TLS transport.
	hello, err := h.transport.RoundTrip(ctx, []byte(`{"jsonrpc":"2.0","id":"hello","method":"server.hello","params":{}}`))
	if err != nil {
		return "", err
	}
	var envelope struct {
		JSONRPC string
		ID      string
		Error   json.RawMessage
		Result  struct{ Incarnation, AuthMode string }
	}
	if json.Unmarshal(hello, &envelope) != nil || envelope.JSONRPC != "2.0" || envelope.ID != "hello" || len(envelope.Error) != 0 || envelope.Result.Incarnation == "" || envelope.Result.AuthMode != "authenticated" {
		return "", fmt.Errorf("public runtime identity invalid")
	}
	identity := envelope.Result
	c := h.rpc
	ready, err := c.Call(ctx, "bead.ready", map[string]any{"limit": 1}, "")
	if err != nil {
		return "", err
	}
	var page struct{ AsOf struct{ Incarnation string } }
	if json.Unmarshal(ready.JSON, &page) != nil || page.AsOf.Incarnation != identity.Incarnation {
		return "", fmt.Errorf("public ready incarnation differs from hello")
	}
	return identity.Incarnation, nil
}

func memoryCursor(ctx context.Context, c *client.Client) (string, error) {
	result, err := c.Call(ctx, "memory.list", map[string]any{"limit": 1}, "")
	if err != nil {
		return "", err
	}
	var page struct{ NextCursor string }
	if json.Unmarshal(result.JSON, &page) != nil || page.NextCursor == "" {
		return "", fmt.Errorf("public memory cursor missing")
	}
	next, err := c.Call(ctx, "memory.list", map[string]any{"limit": 1, "cursor": page.NextCursor}, "")
	if err != nil || len(next.JSON) == 0 {
		return "", fmt.Errorf("current incarnation cursor did not continue")
	}
	return page.NextCursor, nil
}

func restoreAndReadRuntime(ctx context.Context, h *ownedRuntime, b RuntimeBundle, wantState, staleCursor string) (observation RuntimeObservation, err error) {
	observation = RuntimeObservation{ContainerID: h.id, ImageID: "sha256:" + b.ImageDigest, ExecutableDigest: h.bootstrap.ExecutableDigest, PromptDigest: h.bootstrap.PromptDigest, PluginDigest: h.bootstrap.PluginDigest, DataDigest: h.bootstrap.DataDigest, StartupIncarnation: h.bootstrap.StartupIncarnation}
	// Service keys must remain unable to run the synthetic test-admin path.
	_, denied := h.rpc.Call(ctx, "admin.dump", map[string]any{"history": "head", "pageSize": 1 << 20, "humanPresence": fixturePresence}, "")
	var remote *wire.Error
	if !errors.As(denied, &remote) || remote.Data.Code != "human_presence_required" {
		return observation, fmt.Errorf("runtime service-key admin boundary failed")
	}
	// Restore the installed generation, not a bundle source that may have
	// changed since activation. Check these exact bytes before admitting any
	// restore work so bootstrap-time validation cannot mask later corruption.
	data, err := os.ReadFile(filepath.Join(h.root, "current", runtimeDataName))
	if err != nil {
		return observation, fmt.Errorf("installed native snapshot unavailable")
	}
	if digestBytes(data) != b.DataDigest {
		return observation, fmt.Errorf("installed native snapshot digest mismatch")
	}
	archive, err := dumpformat.Read(bytes.NewReader(data))
	if err != nil {
		return observation, err
	}
	if err = restoreDump(ctx, h.adminRPC, data, archive); err != nil {
		return observation, fmt.Errorf("public native snapshot restore failed: %w", err)
	}
	observation.RestoredIncarnation, err = currentRuntimeIncarnation(ctx, h)
	if err != nil {
		return observation, err
	}
	if observation.RestoredIncarnation == observation.StartupIncarnation || observation.RestoredIncarnation == archive.Manifest().NativeIncarnation {
		return observation, fmt.Errorf("native restore did not mint a new incarnation")
	}
	got, err := publicState(ctx, h.rpc)
	if err != nil || got != wantState {
		return observation, fmt.Errorf("post-restore public state differs from pinned snapshot")
	}
	observation.PublicDataReadback = true
	if staleCursor != "" {
		_, denied = h.rpc.Call(ctx, "memory.list", map[string]any{"limit": 1, "cursor": staleCursor}, "")
		if !errors.As(denied, &remote) || remote.Data.Code != "incarnation_changed" {
			return observation, fmt.Errorf("stale public cursor did not fail with incarnation_changed")
		}
		observation.StaleCursorRefused = true
	}
	if _, err = memoryCursor(ctx, h.rpc); err != nil {
		return observation, err
	}
	return observation, nil
}

func runtimeCanary(ctx context.Context, h *ownedRuntime, b RuntimeBundle, observation *RuntimeObservation) error {
	installed := filepath.Join(h.root, "current")
	raw, err := os.ReadFile(filepath.Join(installed, runtimePromptName))
	if err != nil {
		return err
	}
	if digestBytes(raw) != b.PromptDigest {
		return fmt.Errorf("canary prompt pin mismatch")
	}
	var prompt runtimePrompt
	if json.Unmarshal(raw, &prompt) != nil {
		return fmt.Errorf("installed prompt invalid")
	}
	command, err := cli.ParseCommand(prompt.Canary)
	if err != nil {
		return err
	}
	if _, err = h.rpc.Call(ctx, command.Method, command.Params, command.Key); err != nil {
		return fmt.Errorf("installed prompt public canary failed")
	}
	observation.PromptCanary = true
	raw, err = os.ReadFile(filepath.Join(installed, runtimePluginName))
	if err != nil {
		return err
	}
	if digestBytes(raw) != b.PluginDigest {
		return fmt.Errorf("canary plugin pin mismatch")
	}
	var modules runtimePlugins
	if json.Unmarshal(raw, &modules) != nil {
		return fmt.Errorf("installed plugin set invalid")
	}
	enabled := strings.Join(modules.Modules, ",") == "authn.apikey,transport.mcp"
	init := []byte(`{"jsonrpc":"2.0","id":"canary","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"rollback-canary","version":"1"}}}`)
	module := *h.transport
	valid, status, err := module.exchange(ctx, "/mcp", init)
	if err != nil {
		return err
	}
	if enabled {
		if status != http.StatusOK || !bytes.Contains(valid, []byte("protocolVersion")) {
			return fmt.Errorf("configured MCP module did not initialize")
		}
		listed, code, e := module.exchange(ctx, "/mcp", []byte(`{"jsonrpc":"2.0","id":"tools","method":"tools/list","params":{}}`))
		if e != nil || code != http.StatusOK || !bytes.Contains(listed, []byte("plug_call")) {
			return fmt.Errorf("configured MCP module tools unavailable")
		}
	} else if status != http.StatusNotFound {
		return fmt.Errorf("removed MCP module remains mounted")
	}
	module.token = ""
	module.session = ""
	_, status, err = module.exchange(ctx, "/mcp", init)
	if err != nil || (enabled && status != http.StatusUnauthorized) || (!enabled && status != http.StatusNotFound) {
		return fmt.Errorf("module authentication/removal boundary failed")
	}
	observation.PluginCanary = true
	// The same explicit key has no receipt in this new native incarnation.
	// Its first request must commit normally; the identical second request must
	// replay exactly. These are intentional receipt assertions, never retries.
	marker := strings.Join([]string{b.Role, b.SourceSHA, b.SourceTree, b.ExecutableDigest, b.ImageDigest, b.PromptDigest, b.PluginDigest}, "/")
	params := map[string]any{"key": runtimeMarkerKey, "content": marker}
	key := digestBytes([]byte("rollback-runtime-canary"))[:32]
	first, err := h.rpc.Call(ctx, "memory.save", params, key)
	if err != nil || first.Replayed {
		return fmt.Errorf("receipt incorrectly survived native incarnation boundary")
	}
	replay, err := h.rpc.Call(ctx, "memory.save", params, key)
	if err != nil || !replay.Replayed || !bytes.Equal(first.JSON, replay.JSON) {
		return fmt.Errorf("runtime public write receipt did not replay")
	}
	state, err := publicState(ctx, h.rpc)
	if err != nil || !strings.Contains(state, marker) {
		return fmt.Errorf("runtime marker public readback failed")
	}
	observation.ReceiptReplayed = true
	return nil
}

// RunDeployableRollback performs only the approved local-on-RunsOn drill.
// Both editions are built from this same tested SHA/tree; this does not prove
// compatibility with any historical release or any Beads reverse/reimport.
func RunDeployableRollback(ctx context.Context, kind, sha, tree string) (Rollback, error) {
	return runDeployableRollback(ctx, kind, sha, tree, nil)
}

// fault exists only inside this package for destructive, owned failure tests;
// no public runner accepts a command, endpoint or external resource adapter.
func runDeployableRollback(ctx context.Context, kind, sha, tree string, fault func(string, *ownedRuntime) error) (report Rollback, err error) {
	report.Software = "BLOCK"
	report.Complete = "BLOCK"
	report.Data = "BLOCK"
	report.ReverseConversion = "WAIT_I6_public_reverse_reimport_readback"
	if kind != "memory" && kind != "sqlite" {
		return report, fmt.Errorf("disposable memory/sqlite required")
	}
	if _, err = requireSourcePins(sha, tree); err != nil {
		return report, err
	}
	if err = ensureWorkRoot(); err != nil {
		return report, err
	}
	root, err := os.MkdirTemp(workRoot, "plug-swarm-rollback-")
	if err != nil {
		return report, err
	}
	d := newOwnedDocker(root)
	var active *ownedRuntime
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		var closeErr error
		if active != nil && !active.stopAttempted {
			closeErr = active.stop(cleanupCtx)
		}
		if active != nil && active.forwarder != nil {
			closeErr = errors.Join(closeErr, active.forwarder.close())
		}
		cleanupErr := d.cleanup(cleanupCtx)
		removeErr := os.RemoveAll(root)
		err = finishRuntimeReport(&report, err, closeErr, cleanupErr, removeErr)
	}()
	seed, err := openFixture(ctx, kind, fixtureDSN(kind, root, 0, "seed"))
	if err != nil {
		return report, err
	}
	seedOpen := true
	defer func() {
		if seedOpen {
			err = errors.Join(err, seed.close())
		}
	}()
	for i := 0; i < 2; i++ {
		if _, err = seed.rpc.Call(ctx, "bead.create", map[string]any{"type": "issue", "fields": map[string]any{"title": fmt.Sprintf("public disposable seed %d", i), "priority": 1}}, ""); err != nil {
			return report, err
		}
		if _, err = seed.rpc.Call(ctx, "memory.save", map[string]any{"key": fmt.Sprintf("rollback.seed.%d", i), "content": fmt.Sprintf("public disposable state %d", i)}, ""); err != nil {
			return report, err
		}
	}
	seedState, err := publicState(ctx, seed.rpc)
	if err != nil {
		return report, err
	}
	snapshot, _, err := captureDump(ctx, seed.adminRPC)
	if err != nil {
		return report, err
	}
	err = seed.close()
	seedOpen = false
	if err != nil {
		return report, err
	}
	old, err := buildRuntimeBundle(ctx, d, "old", sha, tree, snapshot)
	if err != nil {
		return report, err
	}
	if err = d.createNetwork(ctx); err != nil {
		return report, err
	}
	serviceRoot := filepath.Join(root, "service")
	if err = installRuntimeFiles(serviceRoot, old, kind, nil); err != nil {
		return report, err
	}
	active, err = startOwnedRuntime(ctx, d, serviceRoot, old)
	if err != nil {
		return report, err
	}
	oldObservation, err := restoreAndReadRuntime(ctx, active, old, seedState, "")
	if err != nil {
		return report, err
	}
	if err = runtimeCanary(ctx, active, old, &oldObservation); err != nil {
		return report, err
	}
	report.OldBundle = old
	report.OldIncarnation = oldObservation.RestoredIncarnation
	checkpoint, _, err := captureDump(ctx, active.adminRPC)
	if err != nil {
		return report, err
	}
	oldState, err := publicState(ctx, active.rpc)
	if err != nil {
		return report, err
	}
	oldCursor, err := memoryCursor(ctx, active.rpc)
	if err != nil {
		return report, err
	}
	rollback := old
	rollback.DataPath = filepath.Join(root, "old-checkpoint.dump")
	if err = writeRollbackArtifact(rollback.DataPath, checkpoint); err != nil {
		return report, err
	}
	rollback.DataDigest = digestBytes(checkpoint)
	candidate, err := buildRuntimeBundle(ctx, d, "candidate", sha, tree, checkpoint)
	if err != nil {
		return report, err
	}
	if old.ExecutableDigest == candidate.ExecutableDigest || old.ImageDigest == candidate.ImageDigest || old.PromptDigest == candidate.PromptDigest || old.PluginDigest == candidate.PluginDigest {
		return report, fmt.Errorf("edition profiles did not produce distinct runnable artifacts")
	}
	report.CandidateBundle = candidate
	report.RollbackBundle = rollback
	// Reject digest/pin faults while old is still serving, and prove refusal
	// cannot publish even part of a replacement generation.
	currentBefore, err := os.Readlink(filepath.Join(serviceRoot, "current"))
	if err != nil {
		return report, err
	}
	bad := candidate
	bad.PromptDigest = strings.Repeat("0", 64)
	report.WrongDigestRefused = installRuntimeFiles(serviceRoot, bad, kind, nil) != nil
	bad = candidate
	bad.SourceTree = strings.Repeat("e", 40)
	report.PinMismatchRefused = installRuntimeFiles(serviceRoot, bad, kind, nil) != nil
	bad = candidate
	bad.ImageDigest = strings.Repeat("f", 64)
	if installRuntimeFiles(serviceRoot, bad, kind, nil) == nil {
		return report, fmt.Errorf("wrong image pin accepted")
	}
	currentAfter, e := os.Readlink(filepath.Join(serviceRoot, "current"))
	if !report.WrongDigestRefused || !report.PinMismatchRefused || e != nil || currentBefore != currentAfter {
		return report, fmt.Errorf("failed bundle validation changed active generation")
	}
	report.InputsFrozen = true
	report.CandidatePinned = true
	// Recovery is attempted once after a failed candidate transition. The
	// original failure remains BLOCK; failure of recovery is also preserved.
	recoverOld := func() error {
		report.RecoveryAttempted = true
		if active != nil && !active.stopped {
			if e := active.stop(ctx); e != nil {
				return fmt.Errorf("rollback owned stop failed: %w", e)
			}
		}
		if e := installRuntimeFiles(serviceRoot, rollback, kind, nil); e != nil {
			return fmt.Errorf("rollback install failed: %w", e)
		}
		var e error
		active, e = startOwnedRuntime(ctx, d, serviceRoot, rollback)
		if e != nil {
			return fmt.Errorf("rollback start failed: %w", e)
		}
		recovery, e := restoreAndReadRuntime(ctx, active, rollback, oldState, "")
		if e != nil {
			return fmt.Errorf("rollback readback failed: %w", e)
		}
		if e = runtimeCanary(ctx, active, rollback, &recovery); e != nil {
			return fmt.Errorf("rollback canary failed: %w", e)
		}
		report.RecoveryVerified = true
		report.RecoveredObservation = &recovery
		return nil
	}
	var candidateObservation RuntimeObservation
	err = performRuntimeTransition(
		func() error { return active.stop(ctx) },
		func() error { return installRuntimeFiles(serviceRoot, candidate, kind, nil) },
		func() error { var e error; active, e = startOwnedRuntime(ctx, d, serviceRoot, candidate); return e },
		func() error {
			var e error
			candidateObservation, e = restoreAndReadRuntime(ctx, active, candidate, oldState, oldCursor)
			if e != nil {
				return e
			}
			if fault != nil {
				if e = fault("candidate-canary", active); e != nil {
					return e
				}
			}
			return runtimeCanary(ctx, active, candidate, &candidateObservation)
		}, recoverOld,
	)
	if err != nil {
		return report, err
	}
	report.OldToCandidate = true
	report.CandidatePublicReadback = true
	report.CandidateIncarnation = candidateObservation.RestoredIncarnation
	if _, err = active.rpc.Call(ctx, "memory.save", map[string]any{"key": "rollback.candidate-only", "content": candidate.ExecutableDigest}, ""); err != nil {
		return report, err
	}
	candidateState, err := publicState(ctx, active.rpc)
	if err != nil || candidateState == oldState {
		return report, fmt.Errorf("candidate data did not diverge")
	}
	candidateCursor, err := memoryCursor(ctx, active.rpc)
	if err != nil {
		return report, err
	}
	var rollbackObservation RuntimeObservation
	err = performRuntimeTransition(
		func() error { e := active.stop(ctx); report.CandidateStopped = e == nil; return e },
		func() error { return installRuntimeFiles(serviceRoot, rollback, kind, nil) },
		func() error { var e error; active, e = startOwnedRuntime(ctx, d, serviceRoot, rollback); return e },
		func() error {
			var e error
			rollbackObservation, e = restoreAndReadRuntime(ctx, active, rollback, oldState, candidateCursor)
			if e != nil {
				return e
			}
			if fault != nil {
				if e = fault("rollback-canary", active); e != nil {
					return e
				}
			}
			return runtimeCanary(ctx, active, rollback, &rollbackObservation)
		}, func() error { return fmt.Errorf("rollback transition failed; qualification blocked") },
	)
	if err != nil {
		return report, err
	}
	report.CandidateToOld = true
	report.RollbackPublicReadback = true
	report.AtomicReplacements = true
	report.RollbackIncarnation = rollbackObservation.RestoredIncarnation
	if report.OldIncarnation == report.CandidateIncarnation || report.OldIncarnation == report.RollbackIncarnation || report.CandidateIncarnation == report.RollbackIncarnation {
		return report, fmt.Errorf("runtime incarnation boundary reused")
	}
	report.IncarnationBoundary = true
	report.Observations = []RuntimeObservation{oldObservation, candidateObservation, rollbackObservation}
	report.ImageDigest = rollback.ImageDigest
	report.PromptDigest = rollback.PromptDigest
	report.PluginDigest = rollback.PluginDigest
	report.DataDumpDigest = rollback.DataDigest
	return report, nil
}

func finishRuntimeReport(report *Rollback, runErr, stopErr, cleanupErr, removeErr error) error {
	err := errors.Join(runErr, stopErr, cleanupErr, removeErr)
	report.CleanupVerified = stopErr == nil && cleanupErr == nil && removeErr == nil
	if err != nil {
		report.Software = "BLOCK"
		report.Data = "BLOCK"
		report.Complete = "BLOCK"
		report.Failure = err.Error()
	} else {
		report.Software = "PASS_digest_verified_old_candidate_old"
		report.Data = "PASS_snapshot_restore_public_readback"
		report.Complete = "PASS_executable_image_prompt_plugin_data_transition"
	}
	return err
}

// Docker internal bridges do not publish ports on every supported daemon.
// This owned byte forwarder binds only loopback and dials only the inspected
// address of our labeled container. TLS and authenticated public HTTP still
// terminate in the real container; no request authority is rewritten here.
type runtimeForwarder struct {
	listener    net.Listener
	mu          sync.Mutex
	connections map[net.Conn]bool
	closed      bool
	group       sync.WaitGroup
}

func newRuntimeForwarder(ctx context.Context, target string) (*runtimeForwarder, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f := &runtimeForwarder{listener: listener, connections: map[net.Conn]bool{}}
	f.group.Add(1)
	go func() {
		defer f.group.Done()
		for {
			incoming, e := listener.Accept()
			if e != nil {
				return
			}
			f.mu.Lock()
			if f.closed {
				f.mu.Unlock()
				incoming.Close()
				return
			}
			f.connections[incoming] = true
			f.group.Add(1)
			f.mu.Unlock()
			go func() {
				defer f.group.Done()
				defer func() { incoming.Close(); f.mu.Lock(); delete(f.connections, incoming); f.mu.Unlock() }()
				outgoing, e := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", target)
				if e != nil {
					return
				}
				f.mu.Lock()
				if f.closed {
					f.mu.Unlock()
					outgoing.Close()
					return
				}
				f.connections[outgoing] = true
				f.mu.Unlock()
				defer func() { outgoing.Close(); f.mu.Lock(); delete(f.connections, outgoing); f.mu.Unlock() }()
				copied := make(chan struct{}, 1)
				go func() { _, _ = io.Copy(outgoing, incoming); copied <- struct{}{} }()
				_, _ = io.Copy(incoming, outgoing)
				incoming.Close()
				outgoing.Close()
				<-copied
			}()
		}
	}()
	return f, nil
}
func (f *runtimeForwarder) close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	var err error
	addClose := func(e error) {
		if e != nil && !errors.Is(e, net.ErrClosed) {
			err = errors.Join(err, e)
		}
	}
	addClose(f.listener.Close())
	for connection := range f.connections {
		addClose(connection.Close())
	}
	f.mu.Unlock()
	f.group.Wait()
	return err
}
