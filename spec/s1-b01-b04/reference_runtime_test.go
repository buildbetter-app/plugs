package s1b01b04

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

//go:embed record_reference.py
var runtimeRecorderSource []byte

//go:embed extract_reference_packet.py
var runtimeExtractorSource []byte

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

func TestCapturePacketSurvivesLogOnlyCollector(t *testing.T) {
	root, retained := t.TempDir(), t.TempDir()
	want := append([]byte("partial\x00raw\r\n"), bytes.Repeat([]byte("x"), 65536)...)
	if err := os.WriteFile(filepath.Join(root, "raw-commands.jsonl"), want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "recorder.stderr"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	if err := retainReferenceCapture(root, retained, &log, false); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(log.Bytes(), []byte("\n"))
	if len(lines) != 4 {
		t.Fatalf("log-only collector lost raw packet: %d bytes logged", log.Len())
	}
	var size int
	var digest string
	if _, err := fmt.Sscanf(string(lines[0]), capturePacketBegin+" %d %s", &size, &digest); err != nil {
		t.Fatal(err)
	}
	if size != len(lines[1]) || digest != referenceDigest(lines[1]) ||
		string(lines[2]) != fmt.Sprintf("%s %d %s", capturePacketEnd, size, digest) {
		t.Fatal("raw frame length/digest/footer differs")
	}
	var packet struct {
		Format string `json:"format"`
		Files  map[string]struct {
			Present bool   `json:"present"`
			Bytes   []byte `json:"bytesBase64"`
			SHA256  string `json:"sha256"`
		} `json:"files"`
	}
	if err := json.Unmarshal(lines[1], &packet); err != nil {
		t.Fatal(err)
	}
	if packet.Format != "plug.s1-b01-b04.raw-packet.v1" || len(packet.Files) != len(capturePacketFiles) {
		t.Fatalf("incomplete raw packet: format=%q files=%d", packet.Format, len(packet.Files))
	}
	for name, data := range map[string][]byte{"raw-commands.jsonl": want, "recorder.stderr": {}} {
		entry := packet.Files[name]
		if !entry.Present || !bytes.Equal(entry.Bytes, data) || entry.SHA256 != referenceDigest(data) {
			t.Fatalf("log-only collector changed %s", name)
		}
		got, err := os.ReadFile(filepath.Join(retained, name))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("retained copy differs for %s: %v", name, err)
		}
	}
	if entry, ok := packet.Files["reference-recording.json"]; !ok || entry.Present {
		t.Fatal("failed producer's missing recording is not explicit")
	}
}

func TestCapturePacketFailureStillLogsAvailableBytes(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "raw-commands.jsonl"), []byte("available"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "recorder.stderr"), 0o700); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	if err := retainReferenceCapture(root, filepath.Join(root, "missing-output-directory"), &log, false); err == nil {
		t.Fatal("read/copy failure accepted")
	}
	if !bytes.Contains(log.Bytes(), []byte(`"bytesBase64":"YXZhaWxhYmxl"`)) || !bytes.Contains(log.Bytes(), []byte(`"error":`)) {
		t.Fatalf("available bytes or retention error lost from log: %s", log.Bytes())
	}
	if err := retainReferenceCapture(root, t.TempDir(), captureLogFailure{}, false); err == nil || !strings.Contains(err.Error(), "test log") {
		t.Fatalf("log-write failure accepted: %v", err)
	}
}

type captureLogFailure struct{}

func (captureLogFailure) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestCapturePacketCompleteAndMissingOutcome(t *testing.T) {
	root, retained := t.TempDir(), t.TempDir()
	for source := range capturePacketFiles {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, source)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, source), []byte("capture-negative-probe"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var log bytes.Buffer
	if err := retainReferenceCapture(root, retained, &log, true); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(log.Bytes(), []byte(`"status":"CAPTURED"`)) {
		t.Fatal("complete negative harness packet lost its declared outcome")
	}
	if err := os.Remove(filepath.Join(root, "capture-profile.json")); err != nil {
		t.Fatal(err)
	}
	log.Reset()
	if err := retainReferenceCapture(root, retained, &log, true); err == nil {
		t.Fatal("completed packet accepted a missing profile")
	}
	if !bytes.Contains(log.Bytes(), []byte(`"status":"BLOCK"`)) {
		t.Fatal("missing profile was not a BLOCK packet")
	}
}

func TestCapturePacketIndependentExtractorFailsClosed(t *testing.T) {
	root := t.TempDir()
	extractor := filepath.Join(root, "extract_reference_packet.py")
	if err := os.WriteFile(extractor, runtimeExtractorSource, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "recorder.stderr"), []byte("partial\x00failure\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	if err := retainReferenceCapture(root, t.TempDir(), &log, false); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "capture.log")
	if err := os.WriteFile(logPath, log.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	// This tests framing/retention only; no reference producer is executed.
	command := exec.Command("python3", "-B", "-c", `
import copy, importlib.util, json, pathlib, subprocess, sys
source, log_path, root = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("independent_extractor", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
raw = log_path.read_bytes()
packet, payload = module.unframe(raw)
files = module.validate_packet(packet)
assert files["recorder.stderr"] == b"partial\x00failure\r\n"
assert packet["status"] == "BLOCK"

def reject(operation):
    try:
        operation()
    except (ValueError, KeyError, TypeError):
        return
    raise AssertionError("invalid packet accepted")

reject(lambda: module.accept_capture(packet, files))
for malformed in (b"", raw[:-1], raw[:len(raw)//2], raw+raw,
        raw.replace(b"\n", b"\r\n"), raw.replace(b'"status":"BLOCK"', b'"status":"OTHER"'),
        raw.replace(b"\n", b"\nextra\n", 1)):
    reject(lambda: module.unframe(malformed))

def frame(payload):
    identity = str(len(payload)).encode() + b" " + module.digest(payload).encode()
    return module.BEGIN+b" "+identity+b"\n"+payload+b"\n"+module.END+b" "+identity+b"\n"

reject(lambda: module.unframe(frame(b'{"duplicate":1,"duplicate":2}')))
reject(lambda: module.unframe(frame(b'{ "format": "not-canonical" }')))
missing = copy.deepcopy(packet)
missing["files"].pop("raw-commands.jsonl")
reject(lambda: module.validate_packet(missing))
changed = copy.deepcopy(packet)
changed["files"]["recorder.stderr"]["size"] += 1
reject(lambda: module.validate_packet(changed))
changed = copy.deepcopy(packet)
changed["files"]["recorder.stderr"]["bytesBase64"] = "AA=="
reject(lambda: module.validate_packet(changed))
read_error = copy.deepcopy(packet)
read_error["status"] = "CAPTURED"
read_error["files"]["recorder.stderr"]["error"] = "read refused"
reject(lambda: module.accept_capture(read_error, files))
# Default positive extraction refuses BLOCK before creating any output.
accepted = root / "must-not-be-accepted"
refused = subprocess.run([sys.executable,"-B",str(source),str(log_path),"--output",str(accepted)], capture_output=True)
assert refused.returncode != 0 and not accepted.exists()
# Only an explicit diagnostic route preserves a BLOCK packet, under that label.
diagnostic = root / "diagnostic"
retained = subprocess.run([sys.executable,"-B",str(source),str(log_path),"--output",str(diagnostic),"--retain-block"], capture_output=True)
assert retained.returncode == 0, retained.stderr
assert (diagnostic / "packet.json").read_bytes() == payload
assert (diagnostic / "recorder.stderr").read_bytes() == b"partial\x00failure\r\n"
assert json.loads((diagnostic / "extraction.json").read_bytes())["classification"] == "diagnostic-unqualified"
`, extractor, logPath, root)
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root,
		"PYTHONNOUSERSITE=1", "PYTHONDONTWRITEBYTECODE=1", "LC_ALL=C.UTF-8"}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("independent raw packet extractor: %v: %s", err, output)
	}
}

func TestCaptureDoltVersionSetupIsPrivateStrictAndRetained(t *testing.T) {
	for _, scenario := range []string{"success", "config-failure", "second-config-failure", "identity-failure", "version-failure", "version-warning"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			probe := filepath.Join(root, "capture-runtime-negative-probe")
			configExit, versionExit, extra, failSecond := "0", "0", "", "0"
			failIdentity := "0"
			configStderr := strings.Repeat("e", 65537) + "\r\n"
			if scenario == "config-failure" {
				configExit = "7"
			}
			if scenario == "version-failure" {
				versionExit = "9"
			}
			if scenario == "version-warning" {
				extra = "online warning\n"
			}
			if scenario == "second-config-failure" {
				failSecond = "1"
			}
			if scenario == "identity-failure" {
				failIdentity = "1"
			}
			body := fmt.Sprintf(`#!/bin/sh
test "$DOLT_ROOT_PATH" = "$HOME" || exit 23
test "$DOLT_DISABLE_EVENT_FLUSH" = 1 || exit 24
case "$1" in
 config)
  test "$2:$3" = "--global:--add" || exit 20
  printf 'online version diagnostic\n'
  printf '%s' >&2
  test %s = 0 || exit %s
  if test "$4" = versioncheck.disabled && test %s = 1; then exit 8; fi
  if test "$4" = user.email && test %s = 1; then exit 10; fi
  mkdir -p "$DOLT_ROOT_PATH/.dolt"
  case "$4" in
   metrics.disabled) test "$5" = true || exit 27; printf 'true' > "$HOME/metrics.disabled" ;;
   versioncheck.disabled)
    test "$5" = true || exit 28
    test -f "$HOME/metrics.disabled" || exit 25
    printf '{"metrics.disabled":"true","versioncheck.disabled":"true"}\n' > "$DOLT_ROOT_PATH/.dolt/config_global.json"
    ;;
   user.name)
    test "$5" = beads || exit 29
    printf '{"metrics.disabled":"true","user.name":"beads","versioncheck.disabled":"true"}\n' > "$DOLT_ROOT_PATH/.dolt/config_global.json"
    ;;
   user.email)
    test "$5" = beads@localhost || exit 30
    printf '{"metrics.disabled":"true","user.email":"beads@localhost","user.name":"beads","versioncheck.disabled":"true"}\n' > "$DOLT_ROOT_PATH/.dolt/config_global.json"
    ;;
   *) exit 26 ;;
  esac
  ;;
 version)
  test -f "$DOLT_ROOT_PATH/.dolt/config_global.json" || exit 21
  printf 'dolt version 2.2.0\n%s'
  test %s = 0 || { printf 'version stderr\r\n' >&2; exit %s; }
  ;;
 *) exit 22 ;;
esac
`, configStderr, configExit, configExit, failSecond, failIdentity, extra, versionExit, versionExit)
			if err := os.WriteFile(probe, []byte(body), 0o700); err != nil {
				t.Fatal(err)
			}
			_, _, err := prepareReferenceDoltVersion(probe, root)
			if (err == nil) != (scenario == "success") {
				t.Fatalf("scenario=%s error=%v", scenario, err)
			}
			trace, err := os.ReadFile(filepath.Join(root, "runtime-commands.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			lines := bytes.Split(bytes.TrimSuffix(trace, []byte("\n")), []byte("\n"))
			wantEvents := 5
			if scenario == "config-failure" {
				wantEvents = 1
			} else if scenario == "second-config-failure" {
				wantEvents = 2
			} else if scenario == "identity-failure" {
				wantEvents = 4
			}
			if len(lines) != wantEvents {
				t.Fatalf("runtime command order/count=%d want=%d", len(lines), wantEvents)
			}
			for i, line := range lines {
				var event struct {
					Label           string `json:"label"`
					Exit            int    `json:"exitCode"`
					Stdout          string `json:"stdoutBase64"`
					Stderr          string `json:"stderrBase64"`
					OutSHA          string `json:"stdoutSHA256"`
					ErrSHA          string `json:"stderrSHA256"`
					OutBytes        int    `json:"stdoutBytes"`
					ErrBytes        int    `json:"stderrBytes"`
					PrivateHome     bool   `json:"privateHome"`
					PrivateDoltRoot bool   `json:"doltRootMatchesHome"`
					FlushDisabled   bool   `json:"eventFlushDisabled"`
				}
				if err := json.Unmarshal(line, &event); err != nil {
					t.Fatal(err)
				}
				stdout, outErr := base64.StdEncoding.DecodeString(event.Stdout)
				stderr, errErr := base64.StdEncoding.DecodeString(event.Stderr)
				if outErr != nil || errErr != nil || event.OutSHA != referenceDigest(stdout) || event.ErrSHA != referenceDigest(stderr) || event.OutBytes != len(stdout) || event.ErrBytes != len(stderr) || !event.PrivateHome || !event.PrivateDoltRoot || !event.FlushDisabled {
					t.Fatal("runtime setup trace lost stream bytes/digests")
				}
				if i < 4 {
					wantLabel := "dolt-config-" + []string{"metrics.disabled", "versioncheck.disabled", "user.name", "user.email"}[i]
					if event.Label != wantLabel || string(stdout) != "online version diagnostic\n" || string(stderr) != configStderr {
						t.Fatal("configuration command raw streams lost")
					}
					if scenario == "config-failure" && event.Exit != 7 {
						t.Fatal("config refusal exit lost")
					}
					if scenario == "second-config-failure" && i == 1 && event.Exit != 8 {
						t.Fatal("second configuration refusal exit lost")
					}
					if scenario == "identity-failure" && i == 3 && event.Exit != 10 {
						t.Fatal("identity configuration refusal exit lost")
					}
				} else if event.Label != "dolt-version" || (scenario == "version-failure" && (event.Exit != 9 || string(stderr) != "version stderr\r\n")) {
					t.Fatal("version command refusal metadata/streams lost")
				}
			}
			if scenario == "success" || scenario == "version-failure" || scenario == "version-warning" {
				configPath := filepath.Join(root, ".dolt", "config_global.json")
				if data, err := os.ReadFile(configPath); err != nil || string(data) != "{\"metrics.disabled\":\"true\",\"user.email\":\"beads@localhost\",\"user.name\":\"beads\",\"versioncheck.disabled\":\"true\"}\n" {
					t.Fatal("version configuration escaped the disposable HOME")
				}
				if info, err := os.Stat(configPath); err != nil || info.Mode().Perm() != 0o600 {
					t.Fatal("configuration is not private")
				}
			}
		})
	}
}
