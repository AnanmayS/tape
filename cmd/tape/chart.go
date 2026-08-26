package main

import (
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/AnanmayS/tape/internal/replay"
	"github.com/AnanmayS/tape/internal/termui"
)

// The verify chart draws the shape of a window under the summary that counts it.
//
// The counts alone cannot answer the question verify exists to answer. A window
// that lost its connection for ten seconds has healthy counts either side of
// the hole; "records 84,201, gaps 1" says a gap happened and says nothing about
// where, or how much of the window is on the far side of it. Density against
// time puts the hole where it is, and puts a red mark under it.
//
// The chart is additive and never replaces a line of the existing summary.
// Something may be parsing that summary — it is the same text it has always
// been — and it prints regardless; only the chart needs a terminal.

// chart accumulates a window's shape as it is replayed. Feeding it is one call
// per record on a path that is already reading every record, and it holds a
// bounded histogram rather than the records themselves.
type chart struct {
	tl termui.Timeline

	// seen marks the file indexes already given a boundary mark, so a window's
	// files are marked once each rather than once per record.
	seen map[int]bool
}

func newChart() *chart { return &chart{seen: map[int]bool{}} }

// add folds one replayed record into the shape.
func (c *chart) add(rec replay.Record) {
	t := rec.Time()
	switch rec.Kind {
	case replay.KindGap:
		c.tl.Mark(t, termui.MarkGap)
	case replay.KindReseed:
		// The reseed that opens a window is where the window starts, not a
		// break in it. Marking it would put a warning on every healthy window.
		if !rec.Opening {
			c.tl.Mark(t, termui.MarkReseed)
		}
	default:
		c.tl.Add(t)
	}
	if i := rec.Position.FileIndex; !c.seen[i] {
		c.seen[i] = true
		c.tl.Mark(t, termui.MarkFile)
	}
}

// empty reports whether there is anything to draw.
func (c *chart) empty() bool { return c.tl.Empty() }

// print writes the chart under the summary, using the summary's own label
// column so the two read as one block.
func (c *chart) print(w io.Writer, caps termui.Caps) {
	if c.empty() {
		return
	}
	width := caps.Width - labelWidth - 2
	if width < termui.MinWidth {
		width = termui.MinWidth
	}
	rows := c.tl.Render(caps, width)

	fmt.Fprintf(w, "  %s%s\n", termui.Pad("shape", labelWidth), rows[0])
	indent := strings.Repeat(" ", labelWidth+2)
	for _, row := range rows[1:] {
		if row == "" {
			continue
		}
		fmt.Fprintf(w, "%s%s\n", indent, row)
	}
}

// chartFlags are the chart's two switches. Both default to drawing, because a
// terminal is where a person is and a person is who the chart is for; both turn
// it off without touching the summary above it.
type chartFlags struct {
	on bool
	termFlags
}

func (c *chartFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&c.on, "chart", true,
		"draw the window's shape under the summary; only ever on a terminal")
	c.termFlags.register(fs)
}

// drawing reports whether the chart should be drawn to f.
func (c *chartFlags) drawing(caps termui.Caps) bool { return c.on && caps.TTY }
