//go:build !linux || !amd64

package pluginbuild

import "runtime"

func containedGo(_, _, _, _ string) (goRunner, error) {
	return nil, refusal(ErrUnsupported, "local wasip1 builds require the Linux/amd64 containment profile; host is %s/%s", runtime.GOOS, runtime.GOARCH)
}
