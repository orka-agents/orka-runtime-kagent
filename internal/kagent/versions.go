package kagent

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

// verifiedCommits are the kagent controller builds this release passed
// conformance/release-gate.sh against. The broker depends on behavior read
// from these exact builds, so it refuses work on any other.
var verifiedCommits = map[string][]string{
	// v0.10.3
	config.KagentV0: {"8878c39ac64421cb665c8e62ecc6411dd12f2fc2"},
	// main at 1eb89fb1, between v1.0.0-alpha13 and v1.0.0-alpha14. kagent v1
	// support is experimental until kagent v1.0.0.
	config.KagentV1: {"1eb89fb1b670571e25d17e9f55551c567a28f500"},
}

// ErrUnverifiedBuild reports a kagent controller build that neither this
// release nor the configuration accepts.
var ErrUnverifiedBuild = errors.New("kagent controller build is not verified")

var shortCommit = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

// verifiedBuild accepts commit, as a controller reports it (an abbreviated
// git commit), if it abbreviates a verified or accepted commit.
func verifiedBuild(version, commit string, accepted []string) error {
	commit = strings.TrimSpace(commit)
	if !shortCommit.MatchString(commit) {
		return fmt.Errorf("%w: it reports no git commit", ErrUnverifiedBuild)
	}
	for _, known := range append(append([]string(nil), verifiedCommits[version]...), accepted...) {
		if strings.HasPrefix(known, commit) || strings.HasPrefix(commit, known) {
			return nil
		}
	}
	return fmt.Errorf("%w: commit %s", ErrUnverifiedBuild, commit)
}
