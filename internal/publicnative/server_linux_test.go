//go:build linux

package publicnative

import (
	"fmt"
	"net"
	"os"
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
	// PID 1 is not this test process. Seeing its listener would fail closed.
	if os.Getpid() != 1 {
		owned, present, err = ownedLoopbackListener(1, port)
		if err != nil || !present || owned {
			t.Fatalf("unrelated PID listener: owned=%t present=%t error=%v", owned, present, err)
		}
	}
}

func TestChildEnvBindsPhysicalRoot(t *testing.T) {
	s := &Server{DataRoot: "/owned/data"}
	got := s.ChildEnv([]string{"PATH=/usr/bin", "BEADS_DOLT_DATA_DIR=/foreign", "DOLT_ROOT_PATH=/foreign", "BEADS_DOLT_AUTO_START=1"})
	joined := fmt.Sprint(got)
	for _, want := range []string{"BEADS_DOLT_DATA_DIR=/owned/data", "DOLT_ROOT_PATH=/owned/data", "BEADS_DOLT_AUTO_START=0"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %s in %s", want, joined)
		}
	}
	if strings.Contains(joined, "/foreign") {
		t.Fatalf("foreign root retained: %s", joined)
	}
}
