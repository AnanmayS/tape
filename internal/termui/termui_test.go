package termui

import (
	"bytes"
	"strings"
	"testing"
)

// env builds the lookup function detect takes, so a test states the environment
// it means rather than mutating the process's.
func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestDetect(t *testing.T) {
	utf8 := map[string]string{"LANG": "en_US.UTF-8"}

	t.Run("not a terminal draws nothing", func(t *testing.T) {
		c := detect(false, env(utf8))
		if c.TTY {
			t.Fatal("TTY set for a non-terminal")
		}
		if c.Color {
			t.Fatal("colour enabled off a terminal: escape codes would reach a pipe")
		}
	})

	t.Run("terminal enables colour", func(t *testing.T) {
		if c := detect(true, env(utf8)); !c.Color {
			t.Fatal("colour disabled on a terminal")
		}
	})

	t.Run("NO_COLOR wins over a terminal", func(t *testing.T) {
		for _, v := range []string{"1", "0", "yes", "anything"} {
			c := detect(true, env(map[string]string{"NO_COLOR": v}))
			if c.Color {
				t.Fatalf("NO_COLOR=%q left colour on", v)
			}
			if !c.TTY {
				t.Fatalf("NO_COLOR=%q turned off drawing as well as colour", v)
			}
		}
	})

	t.Run("NO_COLOR empty is not set", func(t *testing.T) {
		if c := detect(true, env(map[string]string{"NO_COLOR": ""})); !c.Color {
			t.Fatal("empty NO_COLOR disabled colour; the convention says it must not")
		}
	})

	t.Run("TERM=dumb disables colour", func(t *testing.T) {
		if c := detect(true, env(map[string]string{"TERM": "dumb"})); c.Color {
			t.Fatal("TERM=dumb left colour on")
		}
	})

	t.Run("unicode from the locale", func(t *testing.T) {
		cases := map[string]bool{
			"LANG=en_US.UTF-8": true,
			"LANG=en_US.utf8":  true,
			"LANG=C":           false,
			"LANG=POSIX":       false,
			"":                 false,
		}
		for spec, want := range cases {
			kv := map[string]string{}
			if k, v, ok := strings.Cut(spec, "="); ok {
				kv[k] = v
			}
			if got := detect(true, env(kv)).Unicode; got != want {
				t.Errorf("%q: unicode=%v, want %v", spec, got, want)
			}
		}
	})

	t.Run("LC_ALL beats LANG", func(t *testing.T) {
		c := detect(true, env(map[string]string{"LC_ALL": "C", "LANG": "en_US.UTF-8"}))
		if c.Unicode {
			t.Fatal("LANG overrode LC_ALL")
		}
	})

	t.Run("width", func(t *testing.T) {
		cases := map[string]int{
			"":       DefaultWidth,
			"not":    DefaultWidth,
			"0":      DefaultWidth,
			"-5":     DefaultWidth,
			"100":    100,
			" 100 ":  100,
			"3":      MinWidth,
			"100000": MaxWidth,
		}
		for in, want := range cases {
			if got := detect(true, env(map[string]string{"COLUMNS": in})).Width; got != want {
				t.Errorf("COLUMNS=%q: width=%d, want %d", in, got, want)
			}
		}
	})
}

func TestPaintOnlyWhenColorIsOn(t *testing.T) {
	on := Caps{TTY: true, Color: true, Unicode: true, Width: 80}
	off := on.Plain()

	if got := off.Paint(ColorRed, "gap"); got != "gap" {
		t.Fatalf("Paint with colour off returned %q", got)
	}
	if got := off.Bold(ColorRed, "gap"); got != "gap" {
		t.Fatalf("Bold with colour off returned %q", got)
	}
	if got := on.Paint(ColorRed, "gap"); !strings.Contains(got, "\x1b[31m") {
		t.Fatalf("Paint with colour on returned %q", got)
	}
	if got := on.Paint(ColorNone, "x"); got != "x" {
		t.Fatalf("ColorNone painted: %q", got)
	}
	if got := on.Paint(ColorRed, ""); got != "" {
		t.Fatalf("empty string painted: %q", got)
	}
}

func TestLevel(t *testing.T) {
	cases := []struct {
		v, max float64
		want   int
	}{
		{0, 100, 0},  // nothing is its own level
		{-5, 100, 0}, // and so is nonsense
		{1, 100, 1},  // anything at all is not nothing
		{0.001, 100, 1},
		{100, 100, RampLevels - 1},
		{500, 100, RampLevels - 1}, // over the scale clamps rather than panicking
		{50, 100, 4},
		{5, 0, 0}, // no scale to measure against
	}
	for _, c := range cases {
		if got := Level(c.v, c.max); got != c.want {
			t.Errorf("Level(%v, %v) = %d, want %d", c.v, c.max, got, c.want)
		}
	}
}

func TestSparkline(t *testing.T) {
	uni := Caps{Unicode: true, Width: 80}
	ascii := Caps{Width: 80}

	t.Run("empty data draws nothing", func(t *testing.T) {
		if got := uni.Sparkline(nil, 20); got != "" {
			t.Fatalf("nil series drew %q", got)
		}
		if got := uni.Sparkline([]float64{}, 20); got != "" {
			t.Fatalf("empty series drew %q", got)
		}
	})

	t.Run("zero width draws nothing", func(t *testing.T) {
		if got := uni.Sparkline([]float64{1, 2, 3}, 0); got != "" {
			t.Fatalf("zero width drew %q", got)
		}
		if got := uni.Sparkline([]float64{1, 2, 3}, -4); got != "" {
			t.Fatalf("negative width drew %q", got)
		}
	})

	t.Run("single point is full scale", func(t *testing.T) {
		if got := uni.Sparkline([]float64{7}, 20); got != "█" {
			t.Fatalf("single point drew %q, want █", got)
		}
	})

	t.Run("all zeros are all floor", func(t *testing.T) {
		if got := uni.Sparkline([]float64{0, 0, 0}, 20); got != "▁▁▁" {
			t.Fatalf("all zeros drew %q", got)
		}
	})

	t.Run("negatives clamp to the floor", func(t *testing.T) {
		if got := uni.Sparkline([]float64{-1, 0, 10}, 20); got != "▁▁█" {
			t.Fatalf("drew %q", got)
		}
	})

	t.Run("scales against the maximum", func(t *testing.T) {
		got := uni.Sparkline([]float64{0, 25, 50, 75, 100}, 20)
		want := "▁▃▅▇█"
		if got != want {
			t.Fatalf("drew %q, want %q", got, want)
		}
	})

	t.Run("more values than cells keeps the newest", func(t *testing.T) {
		vals := []float64{100, 100, 100, 0, 1}
		got := uni.Sparkline(vals, 2)
		if len([]rune(got)) != 2 {
			t.Fatalf("drew %q, want 2 cells", got)
		}
		// The dropped 100s must not set the scale for the cells that remain.
		if got != "▁█" {
			t.Fatalf("drew %q, want ▁█: the scale came from values not drawn", got)
		}
		if m := SparkMax(vals, 2); m != 1 {
			t.Fatalf("SparkMax = %v, want 1", m)
		}
	})

	t.Run("fewer values than cells draws only what it has", func(t *testing.T) {
		if got := uni.Sparkline([]float64{1, 2}, 40); len([]rune(got)) != 2 {
			t.Fatalf("drew %q", got)
		}
	})

	t.Run("ascii ramp when the locale is not utf-8", func(t *testing.T) {
		got := ascii.Sparkline([]float64{0, 50, 100}, 20)
		if strings.ContainsAny(got, "▁▂▃▄▅▆▇█") {
			t.Fatalf("ascii caps drew block characters: %q", got)
		}
		if got != "_=#" {
			t.Fatalf("drew %q, want _=#", got)
		}
	})
}

func TestBar(t *testing.T) {
	uni := Caps{Unicode: true}
	ascii := Caps{}

	cases := []struct {
		name   string
		v, max float64
		width  int
		want   string
	}{
		{"empty", 0, 4096, 10, "░░░░░░░░░░"},
		{"full", 4096, 4096, 10, "██████████"},
		{"over the ceiling clamps", 99999, 4096, 10, "██████████"},
		{"below zero clamps", -5, 4096, 10, "░░░░░░░░░░"},
		{"half", 50, 100, 10, "█████░░░░░"},
		{"a trace still shows", 1, 4096, 10, "█░░░░░░░░░"},
		{"no scale", 15, 0, 10, "░░░░░░░░░░"},
		{"zero width", 15, 100, 0, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := uni.Bar(c.v, c.max, c.width); got != c.want {
				t.Fatalf("Bar(%v, %v, %d) = %q, want %q", c.v, c.max, c.width, got, c.want)
			}
		})
	}

	t.Run("ascii fallback", func(t *testing.T) {
		if got := ascii.Bar(50, 100, 10); got != "#####-----" {
			t.Fatalf("drew %q", got)
		}
	})

	t.Run("every bar is exactly its width", func(t *testing.T) {
		for v := -10; v <= 110; v += 7 {
			got := uni.Bar(float64(v), 100, 13)
			if n := len([]rune(got)); n != 13 {
				t.Fatalf("Bar(%d) drew %d cells, want 13", v, n)
			}
		}
	})
}

func TestPadAndTruncate(t *testing.T) {
	if got := Pad("ab", 5); got != "ab   " {
		t.Fatalf("Pad = %q", got)
	}
	if got := Pad("abcdef", 3); got != "ab…" {
		t.Fatalf("Pad over width = %q", got)
	}
	if got := Pad("abc", 3); got != "abc" {
		t.Fatalf("Pad exact = %q", got)
	}
	if got := Pad("abc", 0); got != "" {
		t.Fatalf("Pad to nothing = %q", got)
	}
	if got := Truncate("abcdef", 1); got != "a" {
		t.Fatalf("Truncate to 1 = %q", got)
	}
	if got := Truncate("▁▂▃▄▅", 3); got != "▁▂…" {
		t.Fatalf("Truncate counts bytes, not runes: %q", got)
	}
}

func TestFrameRedrawsInPlace(t *testing.T) {
	var buf bytes.Buffer
	f := NewFrame(&buf, Caps{TTY: true, Color: true, Width: 80})

	if err := f.Draw([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	first := buf.String()
	if strings.Contains(first, "\x1b[A") || strings.Contains(first, "2A") {
		t.Fatalf("first frame moved the cursor up over lines it never drew: %q", first)
	}
	if strings.Contains(first, "\x1b[2J") || strings.Contains(first, "\x1b[H") {
		t.Fatal("frame cleared the screen; scrollback is not ours to destroy")
	}

	buf.Reset()
	if err := f.Draw([]string{"c", "d"}); err != nil {
		t.Fatal(err)
	}
	second := buf.String()
	if !strings.HasPrefix(second, "\x1b[2A") {
		t.Fatalf("second frame did not move up over the first: %q", second)
	}
	if !strings.Contains(second, "\x1b[2K") {
		t.Fatalf("second frame did not clear the lines it rewrote: %q", second)
	}

	// A frame that shrank has to erase what it no longer covers, or stale
	// numbers sit under fresh ones.
	buf.Reset()
	if err := f.Draw([]string{"e"}); err != nil {
		t.Fatal(err)
	}
	third := buf.String()
	if !strings.HasPrefix(third, "\x1b[2A") {
		t.Fatalf("shrunk frame did not move up over the whole previous block: %q", third)
	}
	if strings.Count(third, "\x1b[2K") != 2 {
		t.Fatalf("shrunk frame cleared %d lines, want 2: %q", strings.Count(third, "\x1b[2K"), third)
	}
	if !strings.HasSuffix(third, "\x1b[1A") {
		t.Fatalf("shrunk frame left the cursor below its own block: %q", third)
	}
}

func TestFrameOffATerminalIsPlainText(t *testing.T) {
	var buf bytes.Buffer
	f := NewFrame(&buf, Caps{TTY: false, Width: 80})
	f.HideCursor()
	if err := f.Draw([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := f.Draw([]string{"c"}); err != nil {
		t.Fatal(err)
	}
	f.ShowCursor()

	if got := buf.String(); got != "a\nb\nc\n" {
		t.Fatalf("off a terminal a frame wrote %q, want plain appended lines", got)
	}
	if strings.Contains(buf.String(), "\x1b") {
		t.Fatal("escape code reached a non-terminal writer")
	}
}
