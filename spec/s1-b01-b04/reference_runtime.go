package s1b01b04

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// These are the Linux/amd64 release pins in the reference source's
// tools/bazel/dolt.bzl at 9d6fff760b7a314b476539198504d77ce0959656.
const (
	referenceDoltVersion = "2.2.0"
	referenceDoltURL     = "https://github.com/dolthub/dolt/releases/download/v2.2.0/dolt-linux-amd64.tar.gz"
	referenceDoltSHA256  = "1f7ad8c2622995789420a3fb0f2d16b4aa7430000a825dd91d5938f36480cbf6"
	referenceDoltMember  = "dolt-linux-amd64/bin/dolt"
	referenceSizeLimit   = 256 << 20
)

func referenceDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func fetchReferenceDolt(ctx context.Context) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, referenceDoltURL, nil)
	if err != nil {
		return nil, err
	}
	// No GitHub/App credentials are added to this public release request.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Timeout:   2 * time.Minute,
		Transport: transport,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 5 || request.URL.Scheme != "https" || request.URL.User != nil || request.URL.Port() != "" {
				return fmt.Errorf("unsupported pinned runtime redirect")
			}
			switch request.URL.Hostname() {
			case "release-assets.githubusercontent.com", "objects.githubusercontent.com":
				return nil
			default:
				return fmt.Errorf("unsupported pinned runtime redirect host")
			}
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pinned Dolt release HTTP status %d", response.StatusCode)
	}
	archive, err := io.ReadAll(io.LimitReader(response.Body, referenceSizeLimit+1))
	if err != nil {
		return nil, err
	}
	if len(archive) > referenceSizeLimit {
		return nil, fmt.Errorf("pinned Dolt release exceeds size limit")
	}
	return archive, nil
}

func referenceDoltPayload(archive []byte) ([]byte, error) {
	stream, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	reader := tar.NewReader(stream)
	var payload []byte
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if header.Name != referenceDoltMember {
			continue
		}
		if payload != nil || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > referenceSizeLimit {
			return nil, fmt.Errorf("pinned Dolt member is duplicate, empty, oversized or not a regular file")
		}
		payload, err = io.ReadAll(io.LimitReader(reader, referenceSizeLimit+1))
		if err != nil {
			return nil, err
		}
		if int64(len(payload)) != header.Size {
			return nil, fmt.Errorf("pinned Dolt member is truncated")
		}
	}
	if payload == nil {
		return nil, fmt.Errorf("pinned Dolt archive has no exact %s member", referenceDoltMember)
	}
	return payload, nil
}

func installReferenceDolt(archive []byte, destination string) (string, error) {
	// Authenticate the entire compressed archive before parsing or writing it.
	if got := referenceDigest(archive); got != referenceDoltSHA256 {
		return "", fmt.Errorf("pinned Dolt archive SHA-256=%s want=%s", got, referenceDoltSHA256)
	}
	payload, err := referenceDoltPayload(archive)
	if err != nil {
		return "", err
	}
	file, err := os.OpenFile(filepath.Clean(destination), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		return "", err
	}
	_, writeErr := file.Write(payload)
	closeErr := file.Close()
	if writeErr != nil {
		return "", writeErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	return referenceDigest(payload), nil
}

func runReferenceRecorder(command *exec.Cmd, root string) ([]byte, []byte, error) {
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, runErr := command.Output()
	// An explicit stderr buffer preserves successful stderr and avoids
	// exec.ExitError.Stderr's bounded prefix/suffix capture on failure.
	outErr := os.WriteFile(filepath.Join(root, "recorder.stdout"), stdout, 0o600)
	errErr := os.WriteFile(filepath.Join(root, "recorder.stderr"), stderr.Bytes(), 0o600)
	return stdout, stderr.Bytes(), errors.Join(runErr, outErr, errErr)
}

const capturePacketBegin = "S1_B01_B04_RAW_PACKET_BEGIN"
const capturePacketEnd = "S1_B01_B04_RAW_PACKET_END"
const capturePacketLimit = 32 << 20

var capturePacketFiles = map[string]string{
	"recording.json":                 "reference-recording.json",
	"raw-commands.jsonl":             "raw-commands.jsonl",
	"capture-runtime.json":           "capture-runtime.json",
	"capture-profile.json":           "capture-profile.json",
	"public-reference-manifest.json": "public-reference-manifest.json",
	"record_reference.py":            "record_reference.py",
	"recorder.stdout":                "recorder.stdout",
	"recorder.stderr":                "recorder.stderr",
	"dolt-version.stdout":            "dolt-version.stdout",
	"dolt-version.stderr":            "dolt-version.stderr",
}

func retainReferenceCapture(root, retained string, log io.Writer, captured bool) error {
	var errs []error
	files := make(map[string]any)
	for source, name := range capturePacketFiles {
		entry := map[string]any{"present": false, "bytesBase64": nil, "size": 0}
		files[name] = entry
		data, err := os.ReadFile(filepath.Join(root, source))
		if os.IsNotExist(err) {
			if captured {
				errs = append(errs, fmt.Errorf("completed capture is missing %s", source))
			}
			continue
		}
		if err != nil {
			entry["error"] = err.Error()
			errs = append(errs, fmt.Errorf("retain %s: %w", source, err))
			continue
		}
		entry["present"], entry["bytesBase64"] = true, data
		entry["size"], entry["sha256"] = len(data), referenceDigest(data)
		if err := os.WriteFile(filepath.Join(retained, name), data, 0o600); err != nil {
			entry["error"] = err.Error()
			errs = append(errs, fmt.Errorf("retain %s: %w", name, err))
		}
	}
	status := "BLOCK"
	if captured && len(errs) == 0 {
		status = "CAPTURED"
	}
	// Use the existing B09 framing convention with this lane's own markers.
	// The trusted collector archives logs/XML, not undeclared output files.
	var payload bytes.Buffer
	encoder := json.NewEncoder(&payload)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(map[string]any{
		"format": "plug.s1-b01-b04.raw-packet.v1", "status": status, "files": files,
	}); err != nil {
		return errors.Join(append(errs, err)...)
	}
	packet := bytes.TrimSuffix(payload.Bytes(), []byte("\n"))
	if len(packet) > capturePacketLimit {
		return errors.Join(append(errs, fmt.Errorf("raw capture packet exceeds %d bytes", capturePacketLimit))...)
	}
	digest := referenceDigest(packet)
	if _, err := fmt.Fprintf(log, "%s %d %s\n%s\n%s %d %s\n",
		capturePacketBegin, len(packet), digest, packet, capturePacketEnd, len(packet), digest); err != nil {
		errs = append(errs, fmt.Errorf("retain raw packet in test log: %w", err))
	}
	return errors.Join(errs...)
}
