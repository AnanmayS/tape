package main

import (
	"flag"
	"os"

	"github.com/AnanmayS/tape/internal/termui"
)

// termFlags is the one flag every command that draws anything takes.
//
// One, not three. Whether to use colour is already answered by the output: a
// pipe gets none, NO_COLOR gets none, TERM=dumb gets none. -no-color exists for
// the case none of those cover — a terminal whose owner does not want it — and
// there is no -color to force it back on, because forcing escape codes into a
// redirect is not a thing anyone should be able to ask for by accident.
type termFlags struct {
	noColor bool
}

func (t *termFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&t.noColor, "no-color", false,
		"never use colour, even on a terminal (also NO_COLOR in the environment)")
}

// caps reports what f can do under these flags.
func (t *termFlags) caps(f *os.File) termui.Caps {
	c := termui.Detect(f)
	if t.noColor {
		c = c.Plain()
	}
	return c
}
