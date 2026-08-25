// Command tape captures and replays market data.
package main

import (
	"fmt"
	"os"
)

const usage = `tape — market data capture and deterministic replay

usage:
  tape capture [flags]            capture the live feed to local tape files
  tape replay [flags] <window>    replay a window to stdout as canonical NDJSON
  tape verify [flags] <window>    read a window and report what is in it
  tape stat [flags] <window>      measure what a window costs to store
  tape bench [flags] <window>     push a window back through capture under load
  tape help                       show this message

a window is a directory of .tape files, or a single .tape file.
run "tape <subcommand> -h" for its flags
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "capture":
		err = runCapture(os.Args[2:])
	case "replay":
		err = runReplay(os.Args[2:])
	case "verify":
		err = runVerify(os.Args[2:])
	case "stat":
		err = runStat(os.Args[2:])
	case "bench":
		err = runBench(os.Args[2:])
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "tape: unknown subcommand %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "tape: %v\n", err)
		os.Exit(1)
	}
}
