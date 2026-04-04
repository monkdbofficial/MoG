package version

import (
	"fmt"
	"runtime"
)

// These variables are intended to be overridden at build time via -ldflags.
//
// Example:
//
//	go build -ldflags "-X mog/internal/version.Version=v0.1.0 -X mog/internal/version.Commit=$(git rev-parse --short HEAD) -X mog/internal/version.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" ./cmd/mog
var (
	// Version is the semantic version of the MoG binary.
	Version = "v0.1.0"
	// Commit is the git commit SHA (usually short) used for the build.
	Commit = "dev"
	// BuildDate is an RFC3339 timestamp (UTC recommended) for when the binary was produced.
	BuildDate = "unknown"
)

// Info is a helper used by the adapter.
func Info() string {
	commit := Commit
	if commit == "" {
		commit = "dev"
	}
	date := BuildDate
	if date == "" {
		date = "unknown"
	}
	ver := Version
	if ver == "" {
		ver = "dev"
	}
	return fmt.Sprintf("%s (commit %s, built %s, %s)", ver, commit, date, runtime.Version())
}
