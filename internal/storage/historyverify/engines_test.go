package historyverify_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/historyverify"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const postgresImage = "postgres@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3"

var pgDSN string

func id() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func docker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host=unix:///var/run/docker.sock"}, args...)...)
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "DOCKER_HOST=") && !strings.HasPrefix(v, "DOCKER_CONTEXT=") && !strings.HasPrefix(v, "DOCKER_TLS=") && !strings.HasPrefix(v, "DOCKER_TLS_VERIFY=") && !strings.HasPrefix(v, "DOCKER_CERT_PATH=") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	return cmd.CombinedOutput()
}

// Every run owns a pinned real PostgreSQL engine. There is no external DSN,
// absent-engine skip, or in-memory SQL replacement.
func TestMain(m *testing.M) {
	code := func() int {
		name := "plug-swarm-stream-engines-" + id()
		out, err := docker("run", "-d", "--rm", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=plug_test", "-p", "127.0.0.1::5432", postgresImage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "real PostgreSQL start: %v %s\n", err, out)
			return 1
		}
		defer func() {
			if out, err := docker("rm", "-f", name); err != nil {
				fmt.Fprintf(os.Stderr, "owned PostgreSQL cleanup: %v %s\n", err, out)
			}
		}()
		out, err = docker("port", name, "5432/tcp")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		address := strings.TrimSpace(string(out))
		host, _, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" {
			fmt.Fprintln(os.Stderr, "PostgreSQL must bind loopback")
			return 1
		}
		pgDSN = "postgres://postgres@" + address + "/plug_test?sslmode=disable"
		db, err := sql.Open("pgx", pgDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for db.PingContext(ctx) != nil {
			if ctx.Err() != nil {
				fmt.Fprintln(os.Stderr, ctx.Err())
				return 1
			}
			time.Sleep(30 * time.Millisecond)
		}
		var version string
		if err = db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("Streaming real engine: %s\nImage: %s\n", version, postgresImage)
		return m.Run()
	}()
	os.Exit(code)
}

type engine struct {
	name      string
	store     driver.Store
	dsn       string
	open      func() (driver.Store, error)
	newStage  func(driver.WriteTx, historyverify.Checkpoint) (driver.OfflineHistoryStreamStage, error)
	lifetime  driver.CheckpointLifetime
	sqlDriver string
}

func fresh(t *testing.T, name string) *engine {
	t.Helper()
	ctx := context.Background()
	e := &engine{name: name, lifetime: driver.CheckpointLifetimeRestart}
	switch name {
	case "memory":
		e.open = func() (driver.Store, error) { return memory.New(), nil }
		e.newStage = memory.NewOfflineHistoryStreamStage
		e.lifetime = driver.CheckpointLifetimeProcess
	case "sqlite":
		e.dsn = filepath.Join(t.TempDir(), "stream.db")
		e.open = func() (driver.Store, error) { return (sqlite.Driver{}).Open(ctx, driver.Config{DSN: e.dsn}) }
		e.newStage = sqlite.NewOfflineHistoryStreamStage
		e.sqlDriver = "sqlite"
	case "postgres":
		admin, err := sql.Open("pgx", pgDSN)
		if err != nil {
			t.Fatal(err)
		}
		dbName := "plug_stream_" + id()
		if _, err = admin.ExecContext(ctx, `CREATE DATABASE "`+dbName+`"`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if _, err := admin.ExecContext(ctx, `DROP DATABASE "`+dbName+`" WITH (FORCE)`); err != nil {
				t.Error(err)
			}
			_ = admin.Close()
		})
		u, err := url.Parse(pgDSN)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + dbName
		e.dsn = u.String()
		e.open = func() (driver.Store, error) { return postgres.Open(ctx, driver.Config{DSN: e.dsn}) }
		e.newStage = postgres.NewOfflineHistoryStreamStage
		e.sqlDriver = "pgx"
	default:
		t.Fatal("unknown engine")
	}
	var err error
	e.store, err = e.open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.store.Close(); err != nil {
			t.Error(err)
		}
	})
	return e
}

func engines(t *testing.T, run func(*testing.T, *engine)) {
	t.Helper()
	for _, name := range []string{"memory", "sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) { run(t, fresh(t, name)) })
	}
}

func sum(data []byte) string  { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func ref(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
func record(id, kind, body string) driver.Record {
	return driver.Record{Ref: ref(id), Kind: kind, Body: []byte(body)}
}

type delta struct {
	name          string
	before, after *driver.Record
}
type unit struct {
	baseline                 bool
	sequence                 uint64
	deltas                   []delta
	commits, parents, events []string
}
type fixture struct {
	header        driver.OfflineHistoryStreamHeader
	gaps          []driver.SourceHistoryGap
	boundary      *driver.NativeHistoryBoundary
	units         []unit
	at            func(int) unit
	count         int
	head, current []driver.Record
	physical      bool
}

func basicFixture() fixture {
	a := record("stream-a", "bead", `{"title":"a","n":9007199254740993,"lex":1.2300}`)
	b := record("stream-a", "bead", `{"title":"b","n":9007199254740994,"lex":2e+03}`)
	c := record("stream-a", "bead", `{"title":"recreated","nullable":null}`)
	gone := record("stream-z", "memory", `{"body":"deleted"}`)
	units := []unit{{sequence: 1, deltas: []delta{{"a", nil, &a}, {"z", nil, &gone}}, commits: []string{"c1"}, events: []string{"e1"}}, {sequence: 2, deltas: []delta{{"a", &a, &b}}, commits: []string{"c2"}, parents: []string{"c1", "merge-parent"}, events: []string{"e2"}}, {sequence: 3, commits: []string{"empty"}, parents: []string{"c2"}}, {sequence: 4, deltas: []delta{{"a", &b, nil}, {"z", &gone, nil}}, commits: []string{"c4"}, parents: []string{"empty"}}, {sequence: 5, deltas: []delta{{"a", nil, &c}}, commits: []string{"c5"}, parents: []string{"c4"}}}
	return fixture{header: driver.OfflineHistoryStreamHeader{Version: driver.OfflineHistoryStreamVersion, Source: "engine-contract", Verified: true, Complete: true, First: 1, Head: 5}, units: units, count: len(units), head: []driver.Record{c}, current: []driver.Record{c}}
}

func (f fixture) unit(i int) unit {
	if f.at != nil {
		return f.at(i)
	}
	return f.units[i]
}

type source struct {
	fixture           fixture
	prefix            bool
	unit              int
	frames            []driver.OfflineHistoryStreamFrame
	next              int
	stats             driver.OfflineHistoryStreamEnd
	ended             bool
	closeCount, pulls int
	failAt            int
	fail, closeErr    error
	edit              func(*driver.OfflineHistoryStreamFrame)
	inspect           func()
	buffer            []byte
	cancel            context.CancelFunc
}

func (s *source) Next(ctx context.Context, limits driver.OfflineHistoryStreamLimits) (driver.OfflineHistoryStreamFrame, error) {
	s.pulls++
	if s.inspect != nil {
		s.inspect()
	}
	if err := ctx.Err(); err != nil {
		return driver.OfflineHistoryStreamFrame{}, err
	}
	if s.failAt > 0 && s.pulls == s.failAt {
		return driver.OfflineHistoryStreamFrame{}, s.fail
	}
	if s.cancel != nil && s.pulls == 4 {
		s.cancel()
		return driver.OfflineHistoryStreamFrame{}, context.Canceled
	}
	if !s.prefix {
		s.prefix = true
		for _, g := range s.fixture.gaps {
			copy := g
			s.frames = append(s.frames, driver.OfflineHistoryStreamFrame{Kind: "gap", Gap: &copy})
		}
		if s.fixture.boundary != nil {
			s.frames = append(s.frames, driver.OfflineHistoryStreamFrame{Kind: "boundary", Boundary: s.fixture.boundary})
		}
	}
	if s.next == len(s.frames) {
		if s.unit < s.fixture.count {
			s.frames = s.unitFrames(s.fixture.unit(s.unit), s.unit, limits.MaxChunkBytes)
			s.next = 0
			s.unit++
		} else if !s.ended {
			s.ended = true
			end := s.stats
			return driver.OfflineHistoryStreamFrame{Kind: "end", End: &end}, nil
		} else {
			return driver.OfflineHistoryStreamFrame{}, io.EOF
		}
	}
	frame := s.frames[s.next]
	s.next++
	if frame.Chunk != nil {
		n := len(frame.Chunk.Data)
		if cap(s.buffer) < n {
			s.buffer = make([]byte, n)
		}
		s.buffer = s.buffer[:n]
		copy(s.buffer, frame.Chunk.Data)
		chunk := *frame.Chunk
		chunk.Data = s.buffer
		frame.Chunk = &chunk
	}
	if s.edit != nil {
		s.edit(&frame)
	}
	return frame, nil
}
func (s *source) Close() error {
	s.closeCount++
	for i := range s.buffer {
		s.buffer[i] = 0
	}
	return s.closeErr
}

func (s *source) unitFrames(u unit, index, chunkBytes int) []driver.OfflineHistoryStreamFrame {
	c := driver.OfflineHistoryStreamCommit{Baseline: u.baseline, Sequence: u.sequence, CommitIDs: uint64(len(u.commits)), ParentIDs: uint64(len(u.parents)), EventIDs: uint64(len(u.events)), Tables: uint64(len(u.deltas))}
	if !u.baseline {
		c.SourceStoreID = "source"
		c.Project = "p"
		c.SourceSequence = u.sequence
		s.stats.Commits++
	}
	if s.fixture.physical {
		c.Physical = &driver.OfflineHistorySourceCommit{Root: fmt.Sprint("root-", index), Tables: c.Tables, TablesSHA256: sum([]byte(fmt.Sprint("physical-", index)))}
	}
	frames := []driver.OfflineHistoryStreamFrame{{Kind: "commit", Commit: &c}}
	hash := sha256.New()
	_, _ = hash.Write([]byte("plug-offline-history-commit/1\n"))
	hashFrame := func(f driver.OfflineHistoryStreamFrame) {
		raw, _ := json.Marshal(f)
		_, _ = hash.Write(raw)
		_, _ = hash.Write([]byte{'\n'})
	}
	hashFrame(frames[0])
	for i, ids := range [][]string{u.commits, u.parents, u.events} {
		for _, value := range ids {
			f := driver.OfflineHistoryStreamFrame{Kind: "id", ID: &driver.OfflineHistoryStreamID{Kind: []string{"commit", "parent", "event"}[i], Value: value}}
			frames = append(frames, f)
			hashFrame(f)
		}
	}
	for _, d := range u.deltas {
		r := d.after
		if r == nil {
			r = d.before
		}
		table := driver.OfflineHistoryStreamTable{Name: d.name, Ref: r.Ref, Kind: r.Kind}
		for i, v := range []*driver.Record{d.before, d.after} {
			if v != nil {
				image := &driver.OfflineHistoryStreamImage{Bytes: uint64(len(v.Body)), SHA256: sum(v.Body)}
				if i == 0 {
					table.Before = image
				} else {
					table.After = image
				}
			}
		}
		if s.fixture.physical {
			table.PhysicalAfter = &driver.OfflineHistorySourceTable{Present: d.after != nil, TableSHA256: sum([]byte("table"))}
			if d.after != nil {
				table.PhysicalAfter.Rows = 1
				table.PhysicalAfter.SchemaSHA256 = sum([]byte("schema"))
			}
		}
		f := driver.OfflineHistoryStreamFrame{Kind: "table", Table: &table}
		frames = append(frames, f)
		hashFrame(f)
		s.stats.Tables++
		for i, v := range []*driver.Record{d.before, d.after} {
			if v == nil {
				continue
			}
			for at := 0; at < len(v.Body); at += chunkBytes {
				end := at + chunkBytes
				if end > len(v.Body) {
					end = len(v.Body)
				}
				frames = append(frames, driver.OfflineHistoryStreamFrame{Kind: "chunk", Chunk: &driver.OfflineHistoryStreamChunk{Image: []string{"before", "after"}[i], Offset: uint64(at), Data: v.Body[at:end]}})
				s.stats.DecodedBytes += uint64(end - at)
			}
		}
		frames = append(frames, driver.OfflineHistoryStreamFrame{Kind: "table-end"})
	}
	frames = append(frames, driver.OfflineHistoryStreamFrame{Kind: "commit-end", CommitEnd: &driver.OfflineHistoryStreamCommitEnd{SHA256: hex.EncodeToString(hash.Sum(nil))}})
	return frames
}

func request(t *testing.T, e *engine, f fixture) driver.OfflineHistoryStreamRequest {
	t.Helper()
	meta, err := e.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 11, 11, 1, 30, 0, 0, time.UTC)
	p := driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: "authenticated", Actor: model.PrincipalRef{Kind: "user", ID: "engine-admin"}, Authority: "restore-admin", AdmissionDigest: sum([]byte("admitted"))}, SourceStoreID: "source", Method: "offline.restore.import", IdempotencyKey: "stream-engine-receipt-" + id(), ArchiveDigest: sum([]byte("archive")), DerivedDigest: sum([]byte("exact private derived-image preimage is verifier-owned")), HistoryMode: driver.RestoreHistoryModeFull, HistoryTime: when.Format(time.RFC3339Nano)}
	if f.header.First > 1 {
		p.HistoryMode = driver.RestoreHistoryModePartial
	}
	r := driver.OfflineHistoryStreamRequest{Current: meta, Profile: p, Header: f.header, When: when, Lifetime: e.lifetime, Limits: driver.OfflineHistoryStreamLimits{MaxFrameBytes: 4096, MaxChunkBytes: 113}}
	rebind(t, &r, f, nil)
	return r
}
func rebind(t *testing.T, r *driver.OfflineHistoryStreamRequest, f fixture, edit func(*driver.OfflineHistoryStreamFrame)) {
	t.Helper()
	s := &source{fixture: f, edit: edit}
	h := sha256.New()
	_, _ = h.Write([]byte(driver.OfflineHistoryStreamVersion + "\n"))
	raw, _ := json.Marshal(r.Header)
	_, _ = h.Write(raw)
	_, _ = h.Write([]byte{'\n'})
	for {
		frame, err := s.Next(context.Background(), r.Limits)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if frame.Kind != "end" {
			raw, err := json.Marshal(frame)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = h.Write(raw)
			_, _ = h.Write([]byte{'\n'})
		}
	}
	r.Profile.HistoryDigest = hex.EncodeToString(h.Sum(nil))
	_ = s.Close()
}

func lockSet(f fixture) []driver.LockKey {
	keys := map[string]driver.LockKey{}
	add := func(k driver.LockKey) { id, _ := driver.EncodeLockKey(k); keys[id] = k }
	for i := 0; i < f.count; i++ {
		for _, d := range f.unit(i).deltas {
			r := d.after
			if r == nil {
				r = d.before
			}
			copy := r.Ref
			add(driver.LockKey{Kind: driver.LockRecord, Ref: &copy})
			if r.Kind == "bead" {
				v, _ := driver.NewBeadSentinel(r.Ref.ID)
				add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: v})
			}
		}
	}
	for _, r := range f.current {
		copy := r.Ref
		add(driver.LockKey{Kind: driver.LockRecord, Ref: &copy})
		if r.Kind == "bead" {
			v, _ := driver.NewBeadSentinel(r.Ref.ID)
			add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: v})
		}
	}
	marker := ref(driver.OfflineHistoryEvidenceID)
	add(driver.LockKey{Kind: driver.LockRecord, Ref: &marker})
	result := make([]driver.LockKey, 0, len(keys))
	for _, key := range keys {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(result[i])
		b, _ := driver.EncodeLockKey(result[j])
		return a < b
	})
	return result
}

type active struct {
	tx    driver.WriteTx
	lease driver.ActiveLease
	keys  []driver.LockKey
	aux   historyverify.Auxiliary
	stop  func()
}

func writer(t *testing.T, e *engine, keys []driver.LockKey) *active {
	t.Helper()
	ctx := context.Background()
	lease, err := e.store.Lease(ctx, "stream-test-"+id(), driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := e.store.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	requests := make([]driver.LockRequest, len(keys))
	for i, k := range keys {
		requests[i] = driver.LockRequest{Key: k, Mode: driver.LockUpdate}
	}
	if _, err = tx.Lock(ctx, requests); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	a := &active{tx: tx, lease: lease, keys: keys, aux: historyverify.Auxiliary{Bumps: map[string]driver.LockKey{}}}
	renewCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var once sync.Once
	a.stop = func() { once.Do(func() { cancel(); <-done }) }
	go func() {
		defer close(done)
		ticker := time.NewTicker(driver.LeaseRenewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-lease.Lost():
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					if renewCtx.Err() == nil {
						t.Errorf("normal lease renewal: %v", err)
					}
					return
				}
			}
		}
	}()
	t.Cleanup(func() { a.stop(); _ = lease.Release(context.Background()) })

	for _, k := range keys {
		if k.Kind == driver.LockSentinel {
			id, _ := driver.EncodeLockKey(k)
			a.aux.Bumps[id] = k
		}
	}
	return a
}
func (a *active) release(ctx context.Context) error { a.stop(); return a.lease.Release(ctx) }
func header(t *testing.T, r driver.OfflineHistoryStreamRequest, completion driver.OfflineHistoryStreamCompletion, keys []driver.LockKey, current []driver.Record) driver.ChangesetHeader {
	t.Helper()
	hash, err := r.Profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	h := driver.ChangesetHeader{Branch: model.Natural64(0), Incarnation: completion.Meta.Incarnation, Kind: model.ChangesetRestore, Time: r.When, Principal: r.Profile.Admission.Actor, Method: r.Profile.Method, AgentLabel: r.Profile.AgentLabel(), IdempotencyKey: r.Profile.IdempotencyKey, RequestHash: hash, Response: model.Commit{Revision: completion.Meta.Head, Results: []model.OpResult{}, Warnings: []model.Warning{}}, LockKeys: keys}
	for _, record := range current {
		h.Changes = append(h.Changes, model.ChangeRecord{Ref: record.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
	}
	h.Changes = append(h.Changes, model.ChangeRecord{Ref: ref(driver.OfflineHistoryEvidenceID), Op: model.OpCreate, FromRevision: model.Natural64(0)})
	return h
}
func stageCurrent(t *testing.T, a *active, current []driver.Record) {
	t.Helper()
	ctx := context.Background()
	for _, r := range current {
		if err := a.tx.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	var bumps []driver.Sentinel
	for _, k := range a.aux.Bumps {
		if k.Kind == driver.LockSentinel {
			bumps = append(bumps, k.Sentinel)
		}
	}
	sort.Slice(bumps, func(i, j int) bool { return bumps[i] < bumps[j] })
	if err := a.tx.BumpSentinels(ctx, bumps); err != nil {
		t.Fatal(err)
	}
}

func assertEmpty(t *testing.T, e *engine, meta driver.Meta) {
	t.Helper()
	ctx := context.Background()
	actual, err := e.store.Meta(ctx)
	if err != nil || actual != meta {
		t.Fatalf("visible prefix metadata: %+v %v", actual, err)
	}
	r, err := e.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	page, err := r.Query(ctx, driver.Query{Limit: 10000})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("visible prefix records: %d %v", len(page.Items), err)
	}
	entries, err := r.Changesets(ctx, nil, 100)
	if err != nil || len(entries) != 0 {
		t.Fatalf("visible prefix log: %d %v", len(entries), err)
	}
}

// The remaining behavior tests below use only the real engine factories above.

func mint(t *testing.T, r driver.OfflineHistoryStreamRequest, f fixture, a *active) historyverify.Checkpoint {
	t.Helper()
	p, err := historyverify.MintForTesting(r, f.head, f.current, a.aux, f.physical)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func completion(t *testing.T, p *driver.PreparedOfflineHistoryStream) driver.OfflineHistoryStreamCompletion {
	t.Helper()
	c, err := p.Completion()
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func scratch(t *testing.T) { t.Helper(); t.Setenv("TMPDIR", t.TempDir()) }
func noScratch(t *testing.T) {
	t.Helper()
	names, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if strings.HasPrefix(name.Name(), "plug-history-stage-") {
			t.Fatal("unreleased private stage", name.Name())
		}
	}
}

func verifyPublished(t *testing.T, e *engine, f fixture, r driver.OfflineHistoryStreamRequest, meta driver.Meta, h driver.ChangesetHeader) {
	t.Helper()
	ctx := context.Background()
	actual, err := e.store.Meta(ctx)
	if err != nil || actual != meta || meta.StoreID != r.Profile.SourceStoreID || meta.Incarnation == r.Current.Incarnation {
		t.Fatalf("published metadata: %+v %v", actual, err)
	}
	reader, err := e.store.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, want := range f.current {
		got, err := reader.Get(ctx, []driver.Ref{want.Ref})
		if err != nil || len(got) != 1 || got[0].Kind != want.Kind || !bytes.Equal(got[0].Body, want.Body) {
			t.Fatalf("final image %s: %+v %v", want.Ref.ID, got, err)
		}
	}
	entries, err := reader.Changesets(ctx, nil, 10000)
	if err != nil {
		t.Fatal(err)
	}
	head, _ := meta.Head.Uint64()
	if uint64(len(entries)) != head {
		t.Fatalf("complete log: %d want%d", len(entries), head)
	}
	for i, entry := range entries {
		if entry.Revision != model.Natural64(uint64(i+1)) || !entry.Time.Equal(r.When) || !reflect.DeepEqual(entry.Principal, r.Profile.Admission.Actor) || entry.Incarnation != meta.Incarnation || entry.Kind != model.ChangesetRestore {
			t.Fatalf("attribution/order: %+v", entry)
		}
	}
	principal, _ := driver.PrincipalKey(r.Profile.Admission.Actor)
	receipt, err := reader.IdempotencyByKey(ctx, principal, r.Profile.AgentLabel(), r.Profile.Method, r.Profile.IdempotencyKey)
	if err != nil || receipt == nil || receipt.RequestHash != h.RequestHash || receipt.Commit.Revision != meta.Head || receipt.Changeset.IdempotencyKey != r.Profile.IdempotencyKey || receipt.ResponsePending {
		t.Fatalf("original receipt: %+v %v", receipt, err)
	}
	marker, err := reader.Get(ctx, []driver.Ref{ref(driver.OfflineHistoryEvidenceID)})
	if err != nil || len(marker) != 1 {
		t.Fatal("history evidence", err)
	}
	var evidence struct {
		Gaps    []driver.SourceHistoryGap `json:"gaps"`
		Mapping []struct {
			Sequence  uint64         `json:"sequence"`
			Revision  model.Revision `json:"revision"`
			CommitIDs []string       `json:"commitIds"`
			ParentIDs []string       `json:"parentIds"`
			EventIDs  []string       `json:"eventIds"`
		}
		NativeBoundaries []driver.NativeHistoryBoundary `json:"nativeBoundaries"`
		SourceFrames     []struct {
			Revision model.Revision
			Frame    driver.OfflineHistoryStreamFrame
		} `json:"sourceFrames"`
	}
	if err = json.Unmarshal(marker[0].Body, &evidence); err != nil {
		t.Fatal("streamed evidence JSON", err)
	}
	if uint64(len(evidence.Mapping)) != f.header.Head-f.header.First+1 || !reflect.DeepEqual(evidence.Gaps, append([]driver.SourceHistoryGap{}, f.gaps...)) {
		t.Fatalf("mapping/gaps: %+v", evidence)
	}
	boundaries := []driver.NativeHistoryBoundary{}
	if f.boundary != nil {
		boundaries = append(boundaries, *f.boundary)
	}
	if !reflect.DeepEqual(evidence.NativeBoundaries, boundaries) {
		t.Fatalf("native boundaries: %+v", evidence.NativeBoundaries)
	}
	for index, mapping := range evidence.Mapping {
		want := model.Natural64(uint64(index + 1))
		if f.header.BaselineVerified {
			want = model.Natural64(uint64(index + 2))
		}
		if mapping.Revision != want {
			t.Fatalf("source revision mapping: %+v", mapping)
		}
	}
	if len(evidence.SourceFrames) == 0 {
		t.Fatal("physical/ordered provenance metadata missing")
	}
	for index, mapping := range evidence.Mapping {
		unitIndex := index
		if f.header.BaselineVerified {
			unitIndex++
		}
		want := f.unit(unitIndex)
		if want.baseline || mapping.Sequence != want.sequence {
			t.Fatalf("source mapping sequence: %+v", mapping)
		}
		if !reflect.DeepEqual(mapping.CommitIDs, append([]string{}, want.commits...)) || !reflect.DeepEqual(mapping.ParentIDs, append([]string{}, want.parents...)) || !reflect.DeepEqual(mapping.EventIDs, append([]string{}, want.events...)) {
			t.Fatalf("ordered provenance: %+v want %+v", mapping, want)
		}
	}
}

func TestStreamingEngineCreateUpdateDeleteRecreateEmptyCommitAndReceipt(t *testing.T) {
	engines(t, func(t *testing.T, e *engine) {
		scratch(t)
		f := basicFixture()
		r := request(t, e, f)
		a := writer(t, e, lockSet(f))
		stage, err := e.newStage(a.tx, mint(t, r, f, a))
		if err != nil {
			t.Fatal(err)
		}
		source := &source{fixture: f, inspect: func() { assertEmpty(t, e, r.Current) }}
		p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, source, stage)
		if err != nil {
			t.Fatal(err)
		}
		c := completion(t, p)
		if source.closeCount != 1 || c.Stats.Commits != 5 || c.Stats.PeakChunkBytes > r.Limits.MaxChunkBytes {
			t.Fatalf("pull/cleanup: %+v close=%d", c, source.closeCount)
		}
		assertEmpty(t, e, r.Current)
		h := header(t, r, c, a.keys, f.current)
		stageCurrent(t, a, f.current)
		if _, err = a.tx.Commit(context.Background(), h); err == nil {
			t.Fatal("ordinary Commit published a prepared stream")
		}
		if _, err = stage.Publish(context.Background(), driver.OfflineHistoryStreamPublication{}, h); err == nil {
			t.Fatal("zero publication proof accepted")
		}
		entry, err := p.Commit(context.Background(), h)
		if err != nil || entry.Revision != model.Natural64(6) {
			t.Fatalf("publication: %+v %v", entry, err)
		}
		if _, err = p.Commit(context.Background(), h); err == nil {
			t.Fatal("consumed handle republished")
		}
		noScratch(t)
		verifyPublished(t, e, f, r, c.Meta, h)
		ctx := context.Background()
		for revision, want := range map[uint64]string{1: `{"title":"a","n":9007199254740993,"lex":1.2300}`, 2: `{"title":"b","n":9007199254740994,"lex":2e+03}`, 3: `{"title":"b","n":9007199254740994,"lex":2e+03}`, 4: "", 5: `{"title":"recreated","nullable":null}`} {
			at := model.Natural64(revision)
			reader, err := e.store.Read(ctx, driver.View{Incarnation: c.Meta.Incarnation, At: &at})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := reader.Get(ctx, []driver.Ref{ref("stream-a")})
			_ = reader.Close()
			if err != nil || want == "" && len(rows) != 0 || want != "" && (len(rows) != 1 || string(rows[0].Body) != want) {
				t.Fatalf("asOf %d: %+v %v", revision, rows, err)
			}
		}
		reader, err := e.store.Read(ctx, driver.View{Incarnation: c.Meta.Incarnation})
		if err != nil {
			t.Fatal(err)
		}
		versions, err := reader.Versions(ctx, ref("stream-a"), driver.PageReq{Limit: 100})
		if err != nil || len(versions.Items) != 4 {
			t.Fatalf("versions: %+v %v", versions, err)
		}
		logs, err := reader.Changesets(ctx, nil, 100)
		_ = reader.Close()
		if err != nil || len(logs[2].Changes) != 0 {
			t.Fatal("explicit empty source commit lost", err)
		}
		if e.name != "memory" {
			if err = a.release(ctx); err != nil {
				t.Fatal(err)
			}
			if err = e.store.Close(); err != nil {
				t.Fatal(err)
			}
			e.store, err = e.open()
			if err != nil {
				t.Fatal(err)
			}
			verifyPublished(t, e, f, r, c.Meta, h)
		}
	})
}

func TestStreamingEnginePartialBaselineFloorAndExactGaps(t *testing.T) {
	engines(t, func(t *testing.T, e *engine) {
		scratch(t)
		aRecord := record("stream-a", "bead", `{"title":"baseline"}`)
		bRecord := record("stream-a", "bead", `{"title":"suffix"}`)
		f := fixture{header: driver.OfflineHistoryStreamHeader{Version: driver.OfflineHistoryStreamVersion, Source: "engine-contract", Verified: true, First: 8, Head: 9, BaselineVerified: true}, gaps: []driver.SourceHistoryGap{{First: 1, Last: 6, Reason: "unavailable"}, {First: 7, Last: 7, Reason: "not-retained"}}, units: []unit{{baseline: true, sequence: 7, deltas: []delta{{"a", nil, &aRecord}}}, {sequence: 8, commits: []string{"c8"}, deltas: []delta{{"a", &aRecord, &bRecord}}}, {sequence: 9, commits: []string{"empty9"}, parents: []string{"c8"}}}, count: 3, head: []driver.Record{bRecord}, current: []driver.Record{bRecord}}
		r := request(t, e, f)
		a := writer(t, e, lockSet(f))
		stage, err := e.newStage(a.tx, mint(t, r, f, a))
		if err != nil {
			t.Fatal(err)
		}
		source := &source{fixture: f}
		p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, source, stage)
		if err != nil {
			t.Fatal(err)
		}
		c := completion(t, p)
		stageCurrent(t, a, f.current)
		h := header(t, r, c, a.keys, f.current)
		if _, err = p.Commit(context.Background(), h); err != nil {
			t.Fatal(err)
		}
		if c.Meta.HistoryFloor != model.Natural64(1) || c.Meta.Head != model.Natural64(4) {
			t.Fatalf("partial metadata: %+v", c.Meta)
		}
		verifyPublished(t, e, f, r, c.Meta, h)
		zero := model.Natural64(0)
		_, err = e.store.Read(context.Background(), driver.View{Incarnation: c.Meta.Incarnation, At: &zero})
		var truncated driver.ErrTruncated
		if !errors.As(err, &truncated) || truncated.Floor != model.Natural64(1) {
			t.Fatalf("below-floor read: %v", err)
		}
		noScratch(t)
	})
}

func TestStreamingEngineMissingWrongAndUnreconciledVerifierValues(t *testing.T) {
	for _, name := range []string{"memory", "sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) {
			for _, fault := range []string{"missing", "wrong-archive", "wrong-history", "wrong-derived", "wrong-target", "wrong-admission", "wrong-time", "wrong-lifetime", "wrong-source", "physical-not-reconciled"} {
				t.Run(fault, func(t *testing.T) {
					e := fresh(t, name)
					scratch(t)
					f := basicFixture()
					if fault == "physical-not-reconciled" {
						f.physical = true
					}
					r := request(t, e, f)
					a := writer(t, e, lockSet(f))
					proofRequest := r
					switch fault {
					case "wrong-archive":
						proofRequest.Profile.ArchiveDigest = sum([]byte("other"))
					case "wrong-history":
						proofRequest.Profile.HistoryDigest = sum([]byte("other"))
					case "wrong-derived":
						proofRequest.Profile.DerivedDigest = sum([]byte("other"))
					case "wrong-target":
						proofRequest.Current.Incarnation = "foreign-incarnation"
					case "wrong-admission":
						proofRequest.Profile.Admission.Actor.ID = "different-admin"
					case "wrong-time":
						proofRequest.When = proofRequest.When.Add(time.Second)
					case "wrong-lifetime":
						if e.lifetime == driver.CheckpointLifetimeProcess {
							proofRequest.Lifetime = driver.CheckpointLifetimeRestart
						} else {
							proofRequest.Lifetime = driver.CheckpointLifetimeProcess
						}
					case "wrong-source":
						proofRequest.Header.Source = "other"
					}
					var p historyverify.Checkpoint
					var err error
					if fault != "missing" {
						p, err = historyverify.MintForTesting(proofRequest, f.head, f.current, a.aux, false)
						if err != nil {
							t.Fatal(err)
						}
					}
					stage, err := e.newStage(a.tx, p)
					if err != nil {
						t.Fatal(err)
					}
					source := &source{fixture: f}
					prepared, err := driver.PrepareOfflineHistoryStream(context.Background(), r, source, stage)
					var refusal historyverify.ErrCheckpoint
					if prepared != nil || !errors.As(err, &refusal) || source.closeCount != 1 {
						t.Fatalf("unverified evidence accepted: %v close%d", err, source.closeCount)
					}
					assertEmpty(t, e, r.Current)
					noScratch(t)
				})
			}
		})
	}
}

func TestStreamingEnginePrefixHashCloseStageAndCancellationNeverPublish(t *testing.T) {
	for _, name := range []string{"memory", "sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) {
			for _, fault := range []string{"prefix-EOF", "source-error", "wrong-chunk-hash", "source-close", "invalid-JSON", "invalid-body-UTF8", "unproven-preimage", "duplicate-provenance", "stage-failure", "cancel-during-pull"} {
				t.Run(fault, func(t *testing.T) {
					e := fresh(t, name)
					scratch(t)
					f := basicFixture()
					if fault == "invalid-JSON" || fault == "invalid-body-UTF8" {
						bad := record("stream-a", "bead", `{"title":`)
						if fault == "invalid-body-UTF8" {
							bad.Body = []byte{'"', 0xff, '"'}
						}
						f.units[0].deltas[0].after = &bad
					} else if fault == "unproven-preimage" {
						bad := record("stream-a", "bead", `{"title":"forged"}`)
						f.units[1].deltas[0].before = &bad
					} else if fault == "duplicate-provenance" {
						f.units[1].commits = []string{"c1"}
					}
					r := request(t, e, f)
					a := writer(t, e, lockSet(f))
					stage, err := e.newStage(a.tx, mint(t, r, f, a))
					if err != nil {
						t.Fatal(err)
					}
					source := &source{fixture: f, inspect: func() { assertEmpty(t, e, r.Current) }}
					sentinel := errors.New("injected streaming failure")
					switch fault {
					case "prefix-EOF":
						source.failAt = 7
						source.fail = io.EOF
					case "source-error":
						source.failAt = 7
						source.fail = sentinel
					case "wrong-chunk-hash":
						source.edit = func(f *driver.OfflineHistoryStreamFrame) {
							if f.Chunk != nil {
								f.Chunk.Data[0] ^= 1
							}
						}
					case "source-close":
						source.closeErr = sentinel
					case "stage-failure":
						stage = &guardedStage{actual: stage, failAfter: 5, err: sentinel}
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if fault == "cancel-during-pull" {
						source.cancel = cancel
					}
					prepared, err := driver.PrepareOfflineHistoryStream(ctx, r, source, stage)
					if err == nil || prepared != nil || source.closeCount != 1 {
						t.Fatalf("failed prefix authorized publication: %v close%d", err, source.closeCount)
					}
					if fault == "cancel-during-pull" && !errors.Is(err, context.Canceled) {
						t.Fatal("cancellation identity lost", err)
					}
					assertEmpty(t, e, r.Current)
					noScratch(t)
				})
			}
		})
	}
}

type guardedStage struct {
	actual           driver.OfflineHistoryStreamStage
	failAfter, count int
	err              error
	replyLoss        bool
	key              string
}

func (s *guardedStage) Begin(ctx context.Context, b driver.OfflineHistoryStreamBinding) error {
	return s.actual.Begin(ctx, b)
}
func (s *guardedStage) Stage(ctx context.Context, r model.Revision, f driver.OfflineHistoryStreamFrame) error {
	s.count++
	if s.failAfter == s.count {
		return s.err
	}
	return s.actual.Stage(ctx, r, f)
}
func (s *guardedStage) Publish(ctx context.Context, p driver.OfflineHistoryStreamPublication, h driver.ChangesetHeader) (model.Changeset, error) {
	entry, err := s.actual.Publish(ctx, p, h)
	if err == nil && s.replyLoss {
		return model.Changeset{}, driver.ErrIndeterminate{IdempotencyKey: s.key}
	}
	return entry, err
}
func (s *guardedStage) Abort() error {
	err := s.actual.Abort()
	if s.replyLoss {
		return errors.Join(err, context.Canceled, driver.ErrFenced{})
	}
	return err
}

func TestStreamingEnginePreparedAbortAndCanceledPublicationReleasePrivateState(t *testing.T) {
	for _, name := range []string{"memory", "sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) {
			for _, fault := range []string{"abort", "canceled-commit", "lease-lost", "wrong-receipt", "wrong-HEAD-root", "wrong-current-root", "missing-record-lock", "missing-sentinel-bump", "changed-projection"} {
				t.Run(fault, func(t *testing.T) {
					e := fresh(t, name)
					scratch(t)
					f := basicFixture()
					r := request(t, e, f)
					keys := lockSet(f)
					if fault == "missing-record-lock" {
						var filtered []driver.LockKey
						for _, k := range keys {
							if k.Ref == nil || k.Ref.ID != "stream-z" {
								filtered = append(filtered, k)
							}
						}
						keys = filtered
					}
					a := writer(t, e, keys)
					proofFixture := f
					if fault == "wrong-HEAD-root" {
						proofFixture.head = []driver.Record{record("stream-a", "bead", `{"other":true}`)}
					}
					if fault == "wrong-current-root" {
						proofFixture.current = []driver.Record{record("stream-a", "bead", `{"other":true}`)}
					}
					stage, err := e.newStage(a.tx, mint(t, r, proofFixture, a))
					if err != nil {
						t.Fatal(err)
					}
					source := &source{fixture: f}
					p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, source, stage)
					if err != nil {
						t.Fatal(err)
					}
					c := completion(t, p)
					h := header(t, r, c, a.keys, f.current)
					if fault == "abort" {
						if err = p.Close(); err != nil {
							t.Fatal(err)
						}
						if _, err = p.Commit(context.Background(), h); err == nil {
							t.Fatal("closed handle committed")
						}
					} else {
						if fault == "missing-sentinel-bump" {
							for _, r := range f.current {
								if err = a.tx.Put(context.Background(), r); err != nil {
									t.Fatal(err)
								}
							}
						} else {
							stageCurrent(t, a, f.current)
						}
						ctx, cancel := context.WithCancel(context.Background())
						if fault == "canceled-commit" {
							cancel()
						}
						defer cancel()
						if fault == "lease-lost" {
							_ = a.release(context.Background())
						}
						if fault == "wrong-receipt" {
							h.IdempotencyKey = "different-original-key"
						}
						if fault == "changed-projection" {
							if _, err = a.tx.PutBlob(context.Background(), []byte("unverified late artifact")); err != nil {
								t.Fatal(err)
							}
						}
						if _, err = p.Commit(ctx, h); err == nil {
							t.Fatal("invalid publication succeeded")
						}
						if fault == "canceled-commit" && !errors.Is(err, context.Canceled) {
							t.Fatal("cancellation identity", err)
						}
					}
					assertEmpty(t, e, r.Current)
					noScratch(t)
				})
			}
		})
	}
}

func TestStreamingEngineSQLPublicationErrorRollsBackWrittenPrefix(t *testing.T) {
	for _, name := range []string{"sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) {
			e := fresh(t, name)
			scratch(t)
			f := basicFixture()
			r := request(t, e, f)
			a := writer(t, e, lockSet(f))
			stage, err := e.newStage(a.tx, mint(t, r, f, a))
			if err != nil {
				t.Fatal(err)
			}
			p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, &source{fixture: f}, stage)
			if err != nil {
				t.Fatal(err)
			}
			c := completion(t, p)
			stageCurrent(t, a, f.current)
			h := header(t, r, c, a.keys, f.current)
			db, err := sql.Open(e.sqlDriver, e.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			sqlText := `CREATE TRIGGER stream_fail BEFORE INSERT ON versions BEGIN SELECT RAISE(ABORT,'stream-injected-publication'); END;`
			if name == "postgres" {
				sqlText = `CREATE FUNCTION plug_postgres.stream_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'stream-injected-publication'; END $$; CREATE TRIGGER stream_fail BEFORE INSERT ON plug_postgres.versions FOR EACH ROW EXECUTE FUNCTION plug_postgres.stream_fail();`
			}
			if _, err = db.ExecContext(context.Background(), sqlText); err != nil {
				t.Fatal(err)
			}
			_, err = p.Commit(context.Background(), h)
			if err == nil || !strings.Contains(err.Error(), "stream-injected-publication") {
				t.Fatal("actual engine failure not observed", err)
			}
			assertEmpty(t, e, r.Current)
			noScratch(t)
			if err = a.release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err = e.store.Close(); err != nil {
				t.Fatal(err)
			}
			e.store, err = e.open()
			if err != nil {
				t.Fatal(err)
			}
			assertEmpty(t, e, r.Current)
		})
	}
}

func TestStreamingEngineCommittedReplyLossRecoversOriginalKeyWithoutRepublish(t *testing.T) {
	engines(t, func(t *testing.T, e *engine) {
		scratch(t)
		f := basicFixture()
		r := request(t, e, f)
		a := writer(t, e, lockSet(f))
		actual, err := e.newStage(a.tx, mint(t, r, f, a))
		if err != nil {
			t.Fatal(err)
		}
		stage := &guardedStage{actual: actual, replyLoss: true, key: r.Profile.IdempotencyKey}
		p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, &source{fixture: f}, stage)
		if err != nil {
			t.Fatal(err)
		}
		c := completion(t, p)
		stageCurrent(t, a, f.current)
		h := header(t, r, c, a.keys, f.current)
		_, err = p.Commit(context.Background(), h)
		var uncertain driver.ErrIndeterminate
		if !errors.As(err, &uncertain) || uncertain.IdempotencyKey != r.Profile.IdempotencyKey || errors.Is(err, context.Canceled) {
			t.Fatalf("uncertain original key/classification: %v", err)
		}
		var fenced driver.ErrFenced
		if errors.As(err, &fenced) {
			t.Fatal("cleanup fencing contaminated indeterminate error", err)
		}
		if _, err = p.Commit(context.Background(), h); err == nil {
			t.Fatal("uncertain token republished")
		}
		verifyPublished(t, e, f, r, c.Meta, h)
		noScratch(t)
	})
}

func TestStreamingEngineIntermediateCommentAuthorityRefusal(t *testing.T) {
	for _, name := range []string{"memory", "sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) {
			for _, fault := range []string{"principal", "time", "source-time", "source-author", "valid-large-body"} {
				t.Run(fault, func(t *testing.T) {
					e := fresh(t, name)
					scratch(t)
					f := basicFixture()
					r := request(t, e, f)
					comment := map[string]any{"body": strings.Repeat("large discarded comment body ", 8192), "principal": r.Profile.Admission.Actor, "origin": "import", "time": r.When, "sourceAuthor": "source-author", "sourceTime": r.When.Add(-time.Hour)}
					switch fault {
					case "principal":
						comment["principal"] = model.PrincipalRef{Kind: "user", ID: "source-controlled"}
					case "time":
						comment["time"] = r.When.Add(time.Second)
					case "source-time":
						comment["sourceTime"] = nil
					case "source-author":
						comment["sourceAuthor"] = "trailing "
					}
					raw, err := json.Marshal(map[string]any{"title": "intermediate only", "comments": []any{comment}})
					if err != nil {
						t.Fatal(err)
					}
					intermediate := record("stream-a", "bead", string(raw))
					f.units[0].deltas[0].after = &intermediate
					f.units[1].deltas[0].before = &intermediate
					r.Limits = driver.OfflineHistoryStreamLimits{MaxFrameBytes: 64 * 1024, MaxChunkBytes: 16 * 1024}
					rebind(t, &r, f, nil)
					a := writer(t, e, lockSet(f))
					stage, err := e.newStage(a.tx, mint(t, r, f, a))
					if err != nil {
						t.Fatal(err)
					}
					source := &source{fixture: f}
					prepared, err := driver.PrepareOfflineHistoryStream(context.Background(), r, source, stage)
					if fault == "valid-large-body" {
						if err != nil {
							t.Fatal("bounded historical projection refused valid body", err)
						}
						c := completion(t, prepared)
						stageCurrent(t, a, f.current)
						h := header(t, r, c, a.keys, f.current)
						if _, err = prepared.Commit(context.Background(), h); err != nil {
							t.Fatal(err)
						}
						verifyPublished(t, e, f, r, c.Meta, h)
					} else {
						if err == nil || prepared != nil {
							t.Fatal("intermediate comment authority accepted", err)
						}
						assertEmpty(t, e, r.Current)
					}
					if source.closeCount != 1 {
						t.Fatal("source cleanup", source.closeCount)
					}
					noScratch(t)
				})
			}
		})
	}
}

// Cancellation is armed only for the real engine's table-end work. The context
// closes itself after twenty cooperative checks, rather than relying on a
// machine-speed-dependent timer or canceling before the work starts.
type scanCancelContext struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int64
}

func (c *scanCancelContext) Err() error {
	if c.checks.Add(1) == 20 {
		c.cancel()
	}
	return c.Context.Err()
}

type scanCancelStage struct {
	driver.OfflineHistoryStreamStage
	armed *scanCancelContext
	used  bool
}

func (s *scanCancelStage) Stage(ctx context.Context, r model.Revision, f driver.OfflineHistoryStreamFrame) error {
	if f.Kind == "table-end" && !s.used {
		s.used = true
		return s.OfflineHistoryStreamStage.Stage(s.armed, r, f)
	}
	return s.OfflineHistoryStreamStage.Stage(ctx, r, f)
}
func TestStreamingEngineCancellationInsideCompletedImageScan(t *testing.T) {
	engines(t, func(t *testing.T, e *engine) {
		scratch(t)
		f := basicFixture()
		big := record("stream-a", "bead", `{"title":"`+strings.Repeat("x", 2*1024*1024)+`"}`)
		f.units[0].deltas[0].after = &big
		f.units[1].deltas[0].before = &big
		r := request(t, e, f)
		r.Limits = driver.OfflineHistoryStreamLimits{MaxFrameBytes: 64 * 1024, MaxChunkBytes: 16 * 1024}
		rebind(t, &r, f, nil)
		a := writer(t, e, lockSet(f))
		actual, err := e.newStage(a.tx, mint(t, r, f, a))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		during := &scanCancelContext{Context: ctx, cancel: cancel}
		stage := &scanCancelStage{OfflineHistoryStreamStage: actual, armed: during}
		source := &source{fixture: f}
		p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, source, stage)
		if p != nil || !errors.Is(err, context.Canceled) || !stage.used || during.checks.Load() < 20 || source.closeCount != 1 {
			t.Fatalf("mid-scan cancellation: %v checks%d close%d", err, during.checks.Load(), source.closeCount)
		}
		assertEmpty(t, e, r.Current)
		noScratch(t)
	})
}

func jointProfile(t *testing.T, r *driver.OfflineHistoryStreamRequest) driver.SourcePlanePlanV1 {
	t.Helper()
	plan := driver.SourcePlanePlanV1{Version: driver.SourcePlanePlanVersionV1, AddressContract: driver.SourcePlaneAddressContractV1, SourceStoreID: string(r.Profile.SourceStoreID), ArchiveSHA256: r.Profile.ArchiveDigest, Target: driver.SourcePlaneTargetV1{StoreID: string(r.Current.StoreID), Incarnation: r.Current.Incarnation, RequireEmpty: true, Lifetime: r.Lifetime, RuntimeContract: driver.SourcePlaneRuntimeContractV1}, Providers: []driver.SourcePlaneProviderV1{{Alias: "workflow", Instance: "default", Namespace: "plugin/workflow/default/durable", Schema: "workflow/source-state/1", Codec: "workflow/json/1", RuntimeContract: driver.SourcePlaneRuntimeContractV1, Lifetime: r.Lifetime, LogicalImageSize: 2, LogicalImageSHA256: sum([]byte(`{}`)), OriginAdapter: "beads-workflow-origins/1"}}}
	binding, err := driver.NewSourcePlaneImportBindingV1(plan)
	if err != nil {
		t.Fatal(err)
	}
	r.Profile.SourcePlanes = &binding
	return plan
}
func jointEvidence(t *testing.T, r driver.OfflineHistoryStreamRequest, c driver.OfflineHistoryStreamCompletion, plan driver.SourcePlanePlanV1) (driver.SourcePlaneCommitBindingV1, []byte, driver.Record) {
	t.Helper()
	request, err := r.Profile.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	provider := plan.Providers[0]
	origin := driver.SourcePlaneOriginBindingV1{Version: driver.SourcePlaneOriginVersionV1, JointPlanDigest: r.Profile.SourcePlanes.JointPlanDigest, RequestHash: request, StoreID: c.Meta.StoreID, Incarnation: c.Meta.Incarnation, Namespace: provider.Namespace, InitialGeneration: 1, LogicalImageSHA256: provider.LogicalImageSHA256, Lifetime: r.Lifetime}
	envelope, err := json.Marshal(map[string]any{"values": map[string]any{"workflow/state": map[string]any{"revision": 1, "value": json.RawMessage(`{}`)}}, "sourceOrigin": origin})
	if err != nil {
		t.Fatal(err)
	}
	binding := driver.SourcePlaneCommitBindingV1{Version: driver.SourcePlaneCommitVersionV1, JointPlanDigest: r.Profile.SourcePlanes.JointPlanDigest, RequestHash: request, StoreID: c.Meta.StoreID, Incarnation: c.Meta.Incarnation, Revision: c.Meta.Head, Providers: []driver.SourcePlaneProviderCommitV1{{Provider: provider, Origin: origin, EnvelopeSHA256: sum(envelope)}}}
	body, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	return binding, envelope, record(driver.SourcePlaneCommitRecordID, "instance_config", string(body))
}
func TestStreamingEngineJointNativeBoundariesParticipantsAndOriginalReceipt(t *testing.T) {
	for _, name := range []string{"memory", "sqlite", "postgres"} {
		t.Run(name, func(t *testing.T) {
			for _, fault := range []string{"valid", "missing-reconciled-value", "missing-binding", "missing-checkpoint", "changed-checkpoint", "missing-receipt"} {
				t.Run(fault, func(t *testing.T) {
					e := fresh(t, name)
					scratch(t)
					first := record(driver.NativeTableHistoryRecordPrefix+"p/tables", "beads-archive", `{"presence":true,"rows":[[9007199254740993,null],[9007199254740993,null]],"dolt_nonlocal_tables":[["inert"]]}`)
					last := record(driver.NativeTableHistoryRecordPrefix+"p/tables", "beads-archive", `{"presence":true,"rows":[],"dolt_nonlocal_tables":[["inert"]]}`)
					f := fixture{header: driver.OfflineHistoryStreamHeader{Version: driver.OfflineHistoryStreamVersion, Source: driver.NativeTableHistorySource, Verified: true, First: 1, Head: 3, NativeBoundaries: 1}, boundary: &driver.NativeHistoryBoundary{SourceStoreID: "source", Project: "p", VerifiedCommitCount: 3, BoundarySHA256: sum([]byte("authenticated unavailable prefix"))}, count: 3, units: []unit{{sequence: 1, commits: []string{"native1"}, deltas: []delta{{"tables", nil, &first}}}, {sequence: 2, commits: []string{"native2"}, parents: []string{"native1", "ordered-merge-parent"}, deltas: []delta{{"tables", &first, &last}}}, {sequence: 3, commits: []string{"native-empty3"}, parents: []string{"native2"}}}, head: []driver.Record{last}, current: []driver.Record{last}, physical: true}
					r := request(t, e, f)
					plan := jointProfile(t, &r)
					locks := f
					locks.current = append(append([]driver.Record{}, f.current...), record(driver.SourcePlaneCommitRecordID, "instance_config", `{}`))
					a := writer(t, e, lockSet(locks))
					seal := mint(t, r, f, a)
					if fault == "missing-reconciled-value" {
						var err error
						seal, err = historyverify.MintForTesting(r, f.head, f.current, a.aux, false)
						if err != nil {
							t.Fatal(err)
						}
					}
					actual, err := e.newStage(a.tx, seal)
					if err != nil {
						t.Fatal(err)
					}
					source := &source{fixture: f}
					p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, source, actual)
					provider := plan.Providers[0]
					checkAbsent := func() {
						t.Helper()
						assertEmpty(t, e, r.Current)
						cp, err := e.store.(driver.CheckpointStore).LoadCheckpoint(context.Background(), provider.Namespace)
						if err != nil || cp.Generation != 0 || cp.Payload != nil {
							t.Fatalf("participant prefix: %+v %v", cp, err)
						}
					}
					if fault == "missing-reconciled-value" {
						var refusal historyverify.ErrCheckpoint
						if p != nil || !errors.As(err, &refusal) {
							t.Fatal("source-plane physical proof absent", err)
						}
						checkAbsent()
						noScratch(t)
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					checkAbsent()
					c := completion(t, p)
					binding, envelope, evidence := jointEvidence(t, r, c, plan)
					current := append(append([]driver.Record{}, f.current...), evidence)
					stageCurrent(t, a, current)
					h := header(t, r, c, a.keys, current)
					if fault != "missing-receipt" {
						h.Response.Results = []model.OpResult{{Ref: evidence.Ref, Revision: c.Meta.Head, Record: bytes.Clone(evidence.Body)}}
					}
					if fault != "missing-binding" {
						if err = a.tx.(driver.SourcePlaneRestoreTx).BindSourcePlaneImport(context.Background(), binding); err != nil {
							t.Fatal(err)
						}
					}
					if fault != "missing-checkpoint" {
						payload := bytes.Clone(envelope)
						if fault == "changed-checkpoint" {
							payload = append(payload, ' ')
						}
						if err = a.tx.(driver.CheckpointWriteTx).StageCheckpoint(context.Background(), provider.Namespace, 0, payload); err != nil {
							t.Fatal(err)
						}
					}
					_, err = p.Commit(context.Background(), h)
					if fault != "valid" {
						if err == nil {
							t.Fatal("incomplete joint publication accepted")
						}
						checkAbsent()
						noScratch(t)
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					verify := func() {
						t.Helper()
						verifyPublished(t, e, f, r, c.Meta, h)
						cp, err := e.store.(driver.CheckpointStore).LoadCheckpoint(context.Background(), provider.Namespace)
						if err != nil || cp.Generation != 1 || !bytes.Equal(cp.Payload, envelope) {
							t.Fatalf("atomic provider payload: %+v %v", cp, err)
						}
						reader, err := e.store.Read(context.Background(), driver.View{Incarnation: c.Meta.Incarnation})
						if err != nil {
							t.Fatal(err)
						}
						defer reader.Close()
						rows, err := reader.Get(context.Background(), []driver.Ref{evidence.Ref})
						if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Body, evidence.Body) {
							t.Fatal("atomic source-plane commit evidence", err)
						}
						principal, _ := driver.PrincipalKey(r.Profile.Admission.Actor)
						receipt, err := reader.IdempotencyByKey(context.Background(), principal, r.Profile.AgentLabel(), r.Profile.Method, r.Profile.IdempotencyKey)
						if err != nil || receipt == nil || !reflect.DeepEqual(receipt.Commit.Results, h.Response.Results) {
							t.Fatal("original participant receipt", err)
						}
					}
					verify()
					noScratch(t)
					if name != "memory" {
						if err = a.release(context.Background()); err != nil {
							t.Fatal(err)
						}
						if err = e.store.Close(); err != nil {
							t.Fatal(err)
						}
						e.store, err = e.open()
						if err != nil {
							t.Fatal(err)
						}
						verify()
					}
				})
			}
		})
	}
}

func TestStreamingEngineLazyHistoryBeyondAggregateSerializedCeiling(t *testing.T) {
	engines(t, func(t *testing.T, e *engine) {
		scratch(t)
		const commits = 140
		const payload = 256 * 1024
		image := func(i int, id string) driver.Record {
			return record(id, "memory", fmt.Sprintf(`{"step":%d,"data":"%s"}`, i, strings.Repeat("x", payload)))
		}
		f := fixture{header: driver.OfflineHistoryStreamHeader{Version: driver.OfflineHistoryStreamVersion, Source: "engine-lazy-contract", Verified: true, Complete: true, First: 1, Head: commits}, count: commits}
		f.at = func(i int) unit {
			a, z := image(i, "lazy-a"), image(i, "lazy-z")
			u := unit{sequence: uint64(i + 1), commits: []string{fmt.Sprintf("commit%d", i)}, deltas: []delta{{"a", nil, &a}, {"z", nil, &z}}}
			if i > 0 {
				beforeA, beforeZ := image(i-1, "lazy-a"), image(i-1, "lazy-z")
				u.deltas[0].before = &beforeA
				u.deltas[1].before = &beforeZ
				u.parents = []string{fmt.Sprintf("commit%d", i-1)}
			}
			return u
		}
		f.head = []driver.Record{image(commits-1, "lazy-a"), image(commits-1, "lazy-z")}
		f.current = f.head
		r := request(t, e, f)
		r.Limits = driver.OfflineHistoryStreamLimits{MaxFrameBytes: 64 * 1024, MaxChunkBytes: 16 * 1024}
		rebind(t, &r, f, nil)
		a := writer(t, e, lockSet(f))
		stage, err := e.newStage(a.tx, mint(t, r, f, a))
		if err != nil {
			t.Fatal(err)
		}
		source := &source{fixture: f}
		source.inspect = func() {
			if source.pulls%100 == 1 {
				assertEmpty(t, e, r.Current)
			}
			if len(source.frames) > 160 {
				t.Fatalf("source retained more than one bounded commit: %d", len(source.frames))
			}
		}
		p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, source, stage)
		if err != nil {
			t.Fatal(err)
		}
		c := completion(t, p)
		if c.Stats.Commits != commits || c.Stats.Tables != 2*commits || c.Stats.DecodedBytes <= 128*1024*1024 || c.Stats.PeakChunkBytes > r.Limits.MaxChunkBytes || source.closeCount != 1 {
			t.Fatalf("lazy physical-engine mechanics: %+v close%d", c.Stats, source.closeCount)
		}
		stageCurrent(t, a, f.current)
		h := header(t, r, c, a.keys, f.current)
		if _, err = p.Commit(context.Background(), h); err != nil {
			t.Fatal(err)
		}
		verifyPublished(t, e, f, r, c.Meta, h)
		noScratch(t)
		t.Logf("actual engine=%s commits=%d tables=%d decodedBytes=%d peakChunk=%d; process RSS NOT_MEASURED", e.name, c.Stats.Commits, c.Stats.Tables, c.Stats.DecodedBytes, c.Stats.PeakChunkBytes)
	})
}

func TestStreamingEngineCurrentProjectionDerivedAllocatorAndBinaryBlob(t *testing.T) {
	engines(t, func(t *testing.T, e *engine) {
		scratch(t)
		f := basicFixture()
		r := request(t, e, f)
		derived := driver.DerivedRow{Key: "stream-reference", Value: []byte(`{"target":"stream-a","lex":9007199254740993}`)}
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedReferenceIndex, Key: derived.Key}}
		allocator := driver.OfflineAllocator{Prefix: "stream", Mode: driver.AllocatorSequential, LastID: 9}
		allocatorKey, err := driver.AllocatorLock(allocator.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		keys := append(lockSet(f), key, allocatorKey)
		sort.Slice(keys, func(i, j int) bool {
			a, _ := driver.EncodeLockKey(keys[i])
			b, _ := driver.EncodeLockKey(keys[j])
			return a < b
		})
		a := writer(t, e, keys)
		rowID, _ := driver.EncodeLockKey(key)
		allocatorID, _ := driver.EncodeLockKey(allocatorKey)
		blob := []byte{0, 255, 128, 'x', '\n'}
		blobID := driver.Hash(sum(blob))
		a.aux.Derived = map[string]driver.LockState{rowID: {Key: key, Present: true, Derived: &derived}}
		a.aux.Allocators = map[string]driver.OfflineAllocator{allocator.Prefix: allocator}
		a.aux.Blobs = map[driver.Hash][]byte{blobID: bytes.Clone(blob)}
		a.aux.Bumps[allocatorID] = allocatorKey
		stage, err := e.newStage(a.tx, mint(t, r, f, a))
		if err != nil {
			t.Fatal(err)
		}
		p, err := driver.PrepareOfflineHistoryStream(context.Background(), r, &source{fixture: f}, stage)
		if err != nil {
			t.Fatal(err)
		}
		c := completion(t, p)
		stageCurrent(t, a, f.current)
		if err = a.tx.PutDerived(context.Background(), driver.DerivedReferenceIndex, nil, []driver.DerivedRow{derived}); err != nil {
			t.Fatal(err)
		}
		if err = a.tx.(driver.AllocatorContinuation).RestoreAllocators(context.Background(), []driver.OfflineAllocator{allocator}); err != nil {
			t.Fatal(err)
		}
		actual, err := a.tx.PutBlob(context.Background(), blob)
		if err != nil || actual != blobID {
			t.Fatal("staged blob", err)
		}
		h := header(t, r, c, a.keys, f.current)
		if _, err = p.Commit(context.Background(), h); err != nil {
			t.Fatal(err)
		}
		verify := func() {
			t.Helper()
			verifyPublished(t, e, f, r, c.Meta, h)
			reader, err := e.store.Read(context.Background(), driver.View{Incarnation: c.Meta.Incarnation})
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			rows, err := reader.DerivedRows(context.Background(), driver.DerivedReferenceIndex, "")
			if err != nil || len(rows) != 1 || rows[0].Key != derived.Key || !bytes.Equal(rows[0].Value, derived.Value) {
				t.Fatal("atomic derived bytes", err)
			}
			v, err := reader.Blob(context.Background(), blobID)
			if err != nil || !bytes.Equal(v, blob) {
				t.Fatal("atomic binary blob", err)
			}
			allocation, err := reader.(driver.AllocatorReader).LoadAllocator(context.Background(), allocator.Prefix)
			if err != nil || allocation.LastID != 9 || allocation.Mode != driver.AllocatorSequential {
				t.Fatalf("allocator continuation: %+v %v", allocation, err)
			}
		}
		verify()
		noScratch(t)
		if e.name != "memory" {
			if err = a.release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err = e.store.Close(); err != nil {
				t.Fatal(err)
			}
			e.store, err = e.open()
			if err != nil {
				t.Fatal(err)
			}
			verify()
		}
	})
}
