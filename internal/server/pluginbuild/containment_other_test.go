//go:build !linux || !amd64

package pluginbuild

import (
	"errors"
	"testing"
)

func TestUnsupportedBuildContainmentRefuses(t *testing.T) {
	run, err := containedGo("go", "/source", "/output", "")
	var typed *Error
	if run != nil || !errors.As(err, &typed) || typed.Code != ErrUnsupported {
		t.Fatalf("unsupported local mode-2 build was admitted: %v", err)
	}
}
