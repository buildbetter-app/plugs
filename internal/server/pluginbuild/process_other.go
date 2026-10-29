//go:build !linux && !darwin && !freebsd

package pluginbuild

import "os/exec"

// Build refuses these platforms before starting a command.
func configureBuildProcess(*exec.Cmd) {}
