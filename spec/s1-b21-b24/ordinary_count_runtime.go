package s1b21b24

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	ordinaryCountDoltURL    = "https://github.com/dolthub/dolt/releases/download/v2.2.0/dolt-linux-amd64.tar.gz"
	ordinaryCountDoltSHA256 = "1f7ad8c2622995789420a3fb0f2d16b4aa7430000a825dd91d5938f36480cbf6"
	ordinaryCountDoltLimit  = 256 << 20
)

// fetchOrdinaryCountDoltArchive is the only runtime download route. It uses
// the public pinned release, strips ambient proxy/credentials, rejects
// unapproved redirects and authenticates the complete archive before Python
// receives it for member extraction.
func fetchOrdinaryCountDoltArchive(ctx context.Context) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, ordinaryCountDoltURL, nil)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Timeout:   2 * time.Minute,
		Transport: transport,
		CheckRedirect: func(next *http.Request, via []*http.Request) error {
			if len(via) >= 5 || next.URL.Scheme != "https" || next.URL.User != nil || next.URL.Port() != "" {
				return fmt.Errorf("unsupported pinned runtime redirect")
			}
			switch next.URL.Hostname() {
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
	archive, err := io.ReadAll(io.LimitReader(response.Body, ordinaryCountDoltLimit+1))
	if err != nil {
		return nil, err
	}
	if len(archive) > ordinaryCountDoltLimit {
		return nil, fmt.Errorf("pinned Dolt release exceeds size limit")
	}
	digest := sha256.Sum256(archive)
	if hex.EncodeToString(digest[:]) != ordinaryCountDoltSHA256 {
		return nil, fmt.Errorf("pinned Dolt release digest differs")
	}
	return archive, nil
}
