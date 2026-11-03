package s1b01b04

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
