//go:build linux

package publicnative

import (
	"fmt"
	"net"
	"os"
	"os/exec"
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
	got := s.ChildEnv([]string{"PATH=/usr/bin", "BEADS_DOLT_DATA_DIR=/foreign", "DOLT_ROOT_PATH=/foreign",
		"BEADS_DOLT_AUTO_START=1", "BEADS_DOLT_SERVER_HOST=foreign", "BEADS_DOLT_SERVER_PORT=1234",
		"BEADS_DOLT_SERVER_SOCKET=/foreign/socket", "BEADS_DOLT_SERVER_DATABASE=foreign", "BEADS_DOLT_SERVER_PASSWORD=secret"})
	joined := fmt.Sprint(got)
	for _, want := range []string{"BEADS_DOLT_DATA_DIR=/owned/data", "DOLT_ROOT_PATH=/owned/config", "BEADS_DOLT_AUTO_START=0",
		"BEADS_DOLT_SERVER_HOST=127.0.0.1", "BEADS_DOLT_SERVER_PORT=4301", "BEADS_DOLT_PORT=4301"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %s in %s", want, joined)
		}
	}
	if strings.Contains(joined, "foreign") || strings.Contains(joined, "1234") || strings.Contains(joined, "secret") {
		t.Fatalf("foreign root retained: %s", joined)
	}
}
