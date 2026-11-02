package s1b13b16

import (
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	script := ""
	for _, root := range []string{os.Getenv("RUNFILES_DIR"), filepath.Dir(os.Args[0])} {
		for _, candidate := range []string{
			filepath.Join(root, "plugs", "spec", "s1-b13-b16", "reference_capture.py"),
			filepath.Join(root, "spec", "s1-b13-b16", "reference_capture.py"),
			filepath.Join(root, "reference_capture.py"),
		} {
			if candidate != "" {
				if _, err := os.Stat(candidate); err == nil {
					script = candidate
					break
				}
			}
		}
		if script != "" {
			break
		}
	}
	if script == "" {
		panic("reference_capture.py is not present in runfiles")
	}
	command := exec.Command("python3", append([]string{script}, os.Args[1:]...)...)
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		if status, ok := err.(*exec.ExitError); ok {
			os.Exit(status.ExitCode())
		}
		panic(err)
	}
}
