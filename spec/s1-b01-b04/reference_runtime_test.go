package s1b01b04

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

//go:embed record_reference.py
var runtimeRecorderSource []byte

func runtimeTestArchive(t *testing.T, headers ...*tar.Header) []byte {
	t.Helper()
	var output bytes.Buffer
	compressed := gzip.NewWriter(&output)
	writer := tar.NewWriter(compressed)
	for _, header := range headers {
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeReg {
			if _, err := writer.Write(bytes.Repeat([]byte("x"), int(header.Size))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestCaptureRuntimeRejectsUnpinnedArchiveBeforeWriting(t *testing.T) {
	archive := runtimeTestArchive(t, &tar.Header{Name: referenceDoltMember, Typeflag: tar.TypeReg, Size: 1, Mode: 0o755})
	destination := filepath.Join(t.TempDir(), "dolt")
	if _, err := installReferenceDolt(archive, destination); err == nil {
		t.Fatal("accepted a synthetic archive as the pinned runtime")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("untrusted archive created an executable: %v", err)
	}
}

func TestCaptureRuntimeRejectsUnsafeOrAmbiguousExecutable(t *testing.T) {
	valid := runtimeTestArchive(t, &tar.Header{Name: referenceDoltMember, Typeflag: tar.TypeReg, Size: 1})
	if payload, err := referenceDoltPayload(valid); err != nil || !bytes.Equal(payload, []byte("x")) {
		t.Fatalf("valid regular-member control: payload=%q error=%v", payload, err)
	}
	for name, headers := range map[string][]*tar.Header{
		"symlink":       {{Name: referenceDoltMember, Typeflag: tar.TypeSymlink, Linkname: "/bin/sh"}},
		"hardlink":      {{Name: referenceDoltMember, Typeflag: tar.TypeLink, Linkname: "other"}},
		"traversalOnly": {{Name: "../" + referenceDoltMember, Typeflag: tar.TypeReg, Size: 1}},
		"duplicate": {
			{Name: referenceDoltMember, Typeflag: tar.TypeReg, Size: 1},
			{Name: referenceDoltMember, Typeflag: tar.TypeReg, Size: 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := referenceDoltPayload(runtimeTestArchive(t, headers...)); err == nil {
				t.Fatal("accepted unsafe or ambiguous Dolt member")
			}
		})
	}
}

func TestCaptureRecorderFailureRetainsRawBytesAndStopsOwnedRuntime(t *testing.T) {
	root := t.TempDir()
	recorder := filepath.Join(root, "record_reference.py")
	if err := os.WriteFile(recorder, runtimeRecorderSource, 0o600); err != nil {
		t.Fatal(err)
	}
	// This is a negative harness probe, never a reference/parity producer.
	probe := filepath.Join(root, "capture-negative-probe")
	if err := os.WriteFile(probe, []byte(`#!/bin/sh
case "$1" in
  init) mkdir -p .beads; exit 0 ;;
  dolt) printf 'stopped\n'; exit 0 ;;
  *) printf '\000raw\n'; printf 'refused\r\n' >&2; exit 7 ;;
esac
`), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("python3", "-B", "-c", `
import base64, importlib.util, json, os, pathlib, shutil, sys
source, probe, root = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("negative_recorder_probe", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
os.makedirs("/tmp/plug-swarm/work", exist_ok=True)
transcript = root / "negative-transcript.jsonl"
recorder = module.Recorder(probe, root, root,
    {"mode": "managed-proxied-server", "version": "negative-probe"}, transcript)
try:
    recorder.record()
except RuntimeError as error:
    assert "exit 7, expected 0" in str(error), str(error)
else:
    raise AssertionError("failed producer was accepted")
events = [json.loads(line) for line in transcript.read_bytes().splitlines()]
assert [event["label"] for event in events] == ["init", "B01-create-p0", "cleanup-stop"], events
assert base64.b64decode(events[1]["stdoutBase64"]) == b"\x00raw\n"
assert base64.b64decode(events[1]["stderrBase64"]) == b"refused\r\n"
assert events[1]["exitCode"] == 7 and events[2]["exitCode"] == 0
assert not pathlib.Path(events[0]["workspace"]).exists()
assert not (root / "recording.json").exists()
# A failed shutdown must retain the owned workspace and its raw refusal.
probe.write_text("#!/bin/sh\ncase \"$1\" in\n init) mkdir -p .beads; exit 0 ;;\n dolt) printf 'stop refused' >&2; exit 8 ;;\n *) exit 7 ;;\nesac\n")
failed_stop = root / "failed-stop.jsonl"
recorder = module.Recorder(probe, root, root,
    {"mode": "managed-proxied-server", "version": "negative-probe"}, failed_stop)
try:
    recorder.record()
except RuntimeError:
    pass
else:
    raise AssertionError("failed producer was accepted")
events = [json.loads(line) for line in failed_stop.read_bytes().splitlines()]
workspace = pathlib.Path(events[0]["workspace"])
assert workspace.exists() and (workspace / ".beads").is_dir()
assert events[-1]["label"] == "cleanup-stop" and events[-1]["exitCode"] == 8
assert base64.b64decode(events[-1]["stderrBase64"]) == b"stop refused"
# No real process exists in this negative probe; remove only its own directory.
shutil.rmtree(workspace)
# Inject the real TimeoutExpired shape without changing the 120s recorder
# timeout or waiting. Setup and shutdown still execute the labelled probe.
probe.write_text("#!/bin/sh\ncase \"$1\" in\n init) mkdir -p .beads; exit 0 ;;\n dolt) exit 0 ;;\n *) exit 7 ;;\nesac\n")
real_run = module.subprocess.run
def timeout_create(argv, **kwargs):
    if argv[1] == "create":
        raise module.subprocess.TimeoutExpired(argv, 120,
            output=b"partial\x00stdout\n", stderr=b"partial stderr\r\n")
    return real_run(argv, **kwargs)
module.subprocess.run = timeout_create
timed = root / "timeout.jsonl"
try:
    module.Recorder(probe, root, root,
        {"mode": "managed-proxied-server", "version": "negative-probe"}, timed).record()
except RuntimeError as error:
    assert "timed out after 120 seconds" in str(error), str(error)
else:
    raise AssertionError("timed-out producer was accepted")
finally:
    module.subprocess.run = real_run
events = [json.loads(line) for line in timed.read_bytes().splitlines()]
assert [event["label"] for event in events] == ["init", "B01-create-p0", "cleanup-stop"]
assert events[1]["outcome"] == "timeout" and events[1]["exitCode"] is None
assert events[1]["timeoutSeconds"] == 120
assert base64.b64decode(events[1]["stdoutBase64"]) == b"partial\x00stdout\n"
assert base64.b64decode(events[1]["stderrBase64"]) == b"partial stderr\r\n"
assert not pathlib.Path(events[0]["workspace"]).exists()
`, recorder, probe, root)
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + root,
		"PYTHONNOUSERSITE=1", "PYTHONDONTWRITEBYTECODE=1", "LC_ALL=C.UTF-8", "TZ=UTC",
	}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("negative capture probe: %v: %s", err, output)
	}
}

func TestCaptureRecorderPreservesCompleteOuterStreams(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprintf("exit_%d", code), func(t *testing.T) {
			root := t.TempDir()
			command := exec.Command("python3", "-B", "-c", fmt.Sprintf(
				"import os,sys;os.write(1,b'out'+b'x'*65536);os.write(2,b'err'+b'y'*65536);sys.exit(%d)", code))
			command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root, "LC_ALL=C.UTF-8"}
			stdout, stderr, err := runReferenceRecorder(command, root)
			if (err != nil) != (code != 0) {
				t.Fatalf("producer exit=%d error=%v", code, err)
			}
			for name, want := range map[string][]byte{
				"recorder.stdout": append([]byte("out"), bytes.Repeat([]byte("x"), 65536)...),
				"recorder.stderr": append([]byte("err"), bytes.Repeat([]byte("y"), 65536)...),
			} {
				got, readErr := os.ReadFile(filepath.Join(root, name))
				if readErr != nil || !bytes.Equal(got, want) {
					t.Fatalf("retained %s length=%d want=%d error=%v", name, len(got), len(want), readErr)
				}
			}
			if len(stdout) != 65539 || len(stderr) != 65539 {
				t.Fatalf("outer streams truncated: stdout=%d stderr=%d", len(stdout), len(stderr))
			}
		})
	}
}
