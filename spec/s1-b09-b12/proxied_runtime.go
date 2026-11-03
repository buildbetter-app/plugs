package s1b09b12

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Adapted from the B01 owner's spec-local provisioner at 56fa6812ff88e81fbb78352ec8bc763099d3be05:
// spec/s1-b01-b04/reference_runtime.go. Only the fixed public archive is fetched;
// Python independently rehashes it before extracting any executable bytes.
// Archive/version pins are from original9d6 tools/bazel/dolt.bzl.
const (
	proxiedDoltURL    = "https://github.com/dolthub/dolt/releases/download/v2.2.0/dolt-linux-amd64.tar.gz"
	proxiedDoltSHA256 = "1f7ad8c2622995789420a3fb0f2d16b4aa7430000a825dd91d5938f36480cbf6"
	proxiedSizeLimit  = 256 << 20
)

func fetchProxiedDoltArchive(ctx context.Context) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxiedDoltURL, nil)
	if err != nil {
		return nil, err
	}
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
	archive, err := io.ReadAll(io.LimitReader(response.Body, proxiedSizeLimit+1))
	if err != nil {
		return nil, err
	}
	if len(archive) > proxiedSizeLimit {
		return nil, fmt.Errorf("pinned Dolt release exceeds size limit")
	}
	digest := sha256.Sum256(archive)
	if hex.EncodeToString(digest[:]) != proxiedDoltSHA256 {
		return nil, fmt.Errorf("pinned Dolt release digest differs")
	}
	return archive, nil
}
