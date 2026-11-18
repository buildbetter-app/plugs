//go:build linux

package publicnative

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestListenerParserKeepsOnlyRequestedListeningPort(t *testing.T) {
	input := "sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n" +
		"0: 0100007F:0CEA 00000000:0000 0A 0 0 0 0 0 12345\n" +
		"1: 0100007F:0CEB 00000000:0000 0A 0 0 0 0 0 67890\n" +
		"2: 0100007F:0CEA 00000000:0000 01 0 0 0 0 0 99999\n"
	got, err := parseListeners(strings.NewReader(input), 3306)
	if err != nil || len(got) != 1 || got[0].address != "0100007F" || got[0].inode != "12345" {
		t.Fatalf("listeners = %+v, error %v", got, err)
	}
}

func TestListenerOwnershipRejectsOtherProcess(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	owned, present, err := ownedLoopbackListener(os.Getpid(), port)
	if err != nil || !present || !owned {
		t.Fatalf("our listener: owned=%t present=%t error=%v", owned, present, err)
	}
	// A same-UID child has a readable fd table but owns no listener.
	child := exec.Command("/bin/sleep", "5")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	owned, present, err = ownedLoopbackListener(child.Process.Pid, port)
	if err != nil || !present || owned {
		t.Fatalf("unrelated child listener: owned=%t present=%t error=%v", owned, present, err)
	}
}

func TestChildEnvBindsPhysicalRoot(t *testing.T) {
	s := &Server{Port: 4301, DataRoot: "/owned/data", doltRoot: "/owned/config"}
	got := s.ChildEnv([]string{"PATH=/usr/bin", "HOME=/owned/home", "BEADS_DIR=/owned/workspace/.beads",
		"BEADS_ACTOR=public-recorder", "BEADS_NO_DAEMON=1", "GIT_AUTHOR_NAME=Public recorder",
		"UNRELATED=must-not-leak",
		"BEADS_DOLT_DATA_DIR=/foreign", "DOLT_ROOT_PATH=/foreign",
		"BEADS_DOLT_AUTO_START=1", "BEADS_DOLT_SERVER_HOST=foreign", "BEADS_DOLT_SERVER_PORT=1234",
		"BEADS_DOLT_SERVER_SOCKET=/foreign/socket", "BEADS_DOLT_SERVER_DATABASE=foreign", "BEADS_DOLT_SERVER_PASSWORD=secret"})
	joined := fmt.Sprint(got)
	for _, want := range []string{"HOME=/owned/home", "BEADS_DIR=/owned/workspace/.beads",
		"BEADS_ACTOR=public-recorder", "BEADS_NO_DAEMON=1", "GIT_AUTHOR_NAME=Public recorder",
		"BEADS_DOLT_DATA_DIR=/owned/data", "DOLT_ROOT_PATH=/owned/config", "BEADS_DOLT_AUTO_START=0",
		"BEADS_DOLT_SERVER_HOST=127.0.0.1", "BEADS_DOLT_SERVER_PORT=4301", "BEADS_DOLT_PORT=4301"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %s in %s", want, joined)
		}
	}
	if strings.Contains(joined, "foreign") || strings.Contains(joined, "1234") || strings.Contains(joined, "secret") || strings.Contains(joined, "must-not-leak") {
		t.Fatalf("foreign root retained: %s", joined)
	}
}

func TestRetainStartupFailureEvidence(t *testing.T) {
	evidence := t.TempDir()
	root := filepath.Join(evidence, "public-native-dolt-failure")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	s := &Server{Root: root, evidence: evidence, receipt: filepath.Join(root, "server.jsonl"),
		logPath: filepath.Join(root, "dolt.log"), configPath: filepath.Join(root, "dolt-root", ".dolt", "config_global.json")}
	want := []byte("version failed after output was recorded\n")
	if err := os.WriteFile(s.receipt, want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(s.configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.configPath, []byte("private settings\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.logPath, []byte("startup log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.retainFiles(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(s.receipt)
	if err != nil || string(got) != string(want) {
		t.Fatalf("retained startup receipt=%q, error=%v", got, err)
	}
	for path, want := range map[string]string{s.logPath: "startup log\n", s.configPath: "private settings\n"} {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != want {
			t.Fatalf("retained startup artifact %s=%q, error=%v", path, got, err)
		}
	}
}

func TestBeadsProxyArgsMatchExactRootAndShape(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, raw string
		want      bool
	}{
		{"split", "dolt\x00db-proxy-child\x00--root\x00" + root + "\x00", true},
		{"equals", "dolt\x00db-proxy-child\x00--root=" + root + "\x00", true},
		{"prefix", "dolt\x00db-proxy-child\x00--root\x00" + root + "-other\x00", false},
		{"wrong-command", "dolt\x00other\x00--root\x00" + root + "\x00", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := beadsProxyArgsMatch(tc.raw, root); got != tc.want {
				t.Fatalf("match=%v want %v", got, tc.want)
			}
		})
	}
}

func TestBeadsProxyPIDsFakeProc(t *testing.T) {
	proc := t.TempDir()
	root := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"self", "abc", "12"} {
		if err := os.Mkdir(filepath.Join(proc, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(proc, "12", "cmdline"), []byte("dolt\x00db-proxy-child\x00--root\x00"+root+"\x00"), 0600); err != nil {
		t.Fatal(err)
	}
	pids, err := beadsProxyPIDsIn(proc, root)
	if err != nil || len(pids) != 1 || pids[0] != 12 {
		t.Fatalf("pids=%v err=%v", pids, err)
	}
	if err := os.Remove(filepath.Join(proc, "12", "cmdline")); err != nil {
		t.Fatal(err)
	}
	if _, err := beadsProxyPIDsIn(proc, root); err != nil {
		t.Fatalf("vanished pid should be ignored: %v", err)
	}
	if err := os.Mkdir(filepath.Join(proc, "12", "cmdline"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := beadsProxyPIDsIn(proc, root); err == nil {
		t.Fatal("unreadable cmdline did not fail closed")
	}
}

func TestBeadsProxyArgsMatchNormalizesSymlinks(t *testing.T) {
	base := t.TempDir()
	data := filepath.Join(base, "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "alias")
	if err := os.Symlink(data, link); err != nil {
		t.Fatal(err)
	}
	raw := "dolt\x00db-proxy-child\x00--root=" + link + "/.\x00"
	if !beadsProxyArgsMatch(raw, data) {
		t.Fatal("normalized equivalent roots did not match")
	}
}

func TestBeadsProxyWorkspaceDoesNotMatchDataRoot(t *testing.T) {
	workspace := t.TempDir()
	data := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	raw := "dolt\x00db-proxy-child\x00--root\x00" + data + "\x00"
	if beadsProxyArgsMatch(raw, workspace) {
		t.Fatal("workspace unexpectedly matched proxy data root")
	}
	if !beadsProxyArgsMatch(raw, data) {
		t.Fatal("data root did not match proxy root")
	}
}
