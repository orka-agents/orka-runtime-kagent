// Command orka-runtime-kagent runs either the ACP child that Orka's
// supervisor launches for a wrapped kagent Agent (--protocol acp) or the
// lifecycle broker sidecar that owns the kagent identity (--protocol broker).
package main

import (
	"fmt"
	"os"
	"slices"

	"github.com/orka-agents/orka-runtime-kagent/internal/acp"
	"github.com/orka-agents/orka-runtime-kagent/internal/broker"
)

func main() {
	args := os.Args[1:]
	var err error
	switch {
	case slices.Contains(args, "broker") || slices.Contains(args, "--protocol=broker"):
		err = broker.Run(args, os.Getenv)
	default:
		err = acp.Main(args)
	}
	if err != nil {
		// Errors are fixed strings; configuration content is never echoed.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
