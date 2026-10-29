package authnapikey

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/server/authn"
)

type lookupContext struct {
	context.Context
	started chan struct{}
}

func (c lookupContext) Err() error {
	err := c.Context.Err()
	select {
	case c.started <- struct{}{}:
	default:
	}
	return err
}

func TestCanceledLookupDoesNotWaitForKeyRing(t *testing.T) {
	p := New(Config{})
	issued, err := p.Issue(model.PrincipalRef{Kind: "service", ID: "service:lookup"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// Hold the actual ring lock across cancellation. Authentication must return
	// without waiting for the trusted provisioning operation to finish.
	p.mu.Lock()
	base, cancel := context.WithCancel(context.Background())
	ctx := lookupContext{Context: base, started: make(chan struct{}, 1)}
	defer cancel()
	done := make(chan error, 1)
	go func() {
		principal, err := p.Authenticate(ctx, issued.Token)
		if principal.ID != "" {
			done <- errors.New("canceled lookup returned a principal")
			return
		}
		done <- err
	}()
	// The first context check has observed a live budget. The held ring gate
	// prevents lookup from finishing while we cancel it.
	<-ctx.started
	cancel()
	select {
	case err := <-done:
		p.mu.Unlock()
		if !errors.Is(err, authn.ErrUnavailable) || err.Error() != "authn_unavailable" {
			t.Fatal("canceled lookup did not return a credential-free unavailable error")
		}
	case <-time.After(time.Second):
		p.mu.Unlock()
		<-done
		t.Fatal("canceled lookup remained blocked behind key provisioning")
	}
	if _, err := p.Authenticate(context.Background(), issued.Token); err != nil {
		t.Fatal("cancellation retained the key-ring lock")
	}
}

func TestIssuedKeysRetainOnlyDigestsAndOmitSecretsFromJSON(t *testing.T) {
	p := New(Config{})
	issued, err := p.Issue(model.PrincipalRef{Kind: "service", ID: "service:provisioned"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	record := p.keys[issued.ID]
	if record.digest != sha256.Sum256([]byte(issued.Token)) {
		t.Fatal("key ring did not retain the issued token digest")
	}
	data, err := json.Marshal(issued)
	if err != nil {
		t.Fatal(err)
	}
	_, secret, _ := strings.Cut(strings.TrimPrefix(issued.Token, "plg_k_"), "_")
	if strings.Contains(string(data), issued.Token) || strings.Contains(string(data), secret) {
		t.Fatal("serializing an issuance exposed bearer key material")
	}
}
