// Command tape captures and (from M3) replays market data.
package main

import (
	"fmt"
	"os"
)

const usage = `tape — market data capture and deterministic replay

usage:
  tape capture [flags]    capture the live feed to local tape files
  tape help               show this message

run "tape capture -h" for capture flags
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
