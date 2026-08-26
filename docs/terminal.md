# The terminal layer

Three commands can draw as well as print. This is what they draw, and the four
rules they are not allowed to break.

Nothing here changes what the system stores, what it counts, or what it
guarantees. It is presentation, and it is separated from everything else on
purpose: `internal/termui` holds the primitives and knows nothing about market
data, and every renderer above it is a function from values to strings, tested
by writing into a buffer rather than by finding a terminal.

## The four rules

**Nothing decorative reaches a pipe.** Whether the output is a terminal is
decided by `os.Stdout.Stat()` and the `os.ModeCharDevice` bit — no dependency,
no ioctl. When it is not, every one of these features turns itself off and the
command produces the output it produced before any of this existed. That is
checked by piping, not by assertion: `tape capture -live > file` writes zero
bytes to the file and the same log lines to stderr, and `tape verify | cat`
prints the summary and no chart.

**Red means this data is untrustworthy.** It is used for gaps, drops, decode
errors and exchange errors, and for nothing else. The one exception is the trade
side in `replay -pretty`, where green-for-buy and red-for-sell is the convention
every reader of market data already has and spelling it differently would cost
more than the rule is worth. Everything structural keeps the rule.

**A missing capability degrades rather than fails.** No `COLUMNS` is eighty
columns. No UTF-8 in `LC_ALL`, `LC_CTYPE` or `LANG` is an ASCII ramp instead of
block characters — conservative on purpose, since a bare container sets none of
them and mojibake in a log nobody can re-run is worse than a plain-looking
chart. `NO_COLOR` set to anything non-empty, or `TERM=dumb`, or `-no-color`, is
plain text that still draws.

**The capture path pays nothing.** See below.

## `tape capture -live`

A panel that redraws in place, four times a second, instead of a progress line
every five thousand messages.

```
tape capture   coinbase BTC-USD   columnar   → live80
  elapsed     0:00:22     messages 671       avg 30/s
  rate        ▃▃▃█▃▄▃▃█▃▂▃▃▃▃▃▃▃▃▃▃▃▂▃▃▃▃▆▄▃▄▃▄▃▅▄▃▃▄▃▃▄▃▄  now 36/s  peak 104/s
  written     362 KiB     records 672        rotations 0
  file        date=2026-08-26/hour=03/20260826T030000Z.tape
  queue       ░░░░░░░░░░░░░░░░ 0/4096   write p50 4.61µs p99 20.48µs
  gaps        0   reseeds 1   stale 0   decode 0   exchange 0
```

That is a real twenty-two seconds of BTC-USD at eighty columns, with `-no-color`
so it survives being pasted. Before the first columnar batch closes, the file
row reads `… (buffering; not on disk yet)` and `written` is `0 B` — which is the
truth: the records are counted and the bytes are not on disk yet.

The moment the gap count leaves zero it turns bold red and a full-width red
banner appears under the row, and both stay for the rest of the session.

**How it reads the counters without racing them.** The counters belong to the
capture writer goroutine and are deliberately unsynchronised, because
synchronising them would put a lock on the path every message takes. So the
sample is not taken from outside: `capture.Config.Progress` adds a ticker
*beside the flush ticker the writer already has*, and on that tick the writer
copies its own counters into a `capture.Progress` value and offers it to the
channel with a non-blocking send. The display goroutine only ever sees values.

Nothing is added per message — not a lock, not an atomic, not an allocation, not
a branch. Four times a second the writer copies a dozen integers between two
records it was going to write anyway, and a display too slow to keep up loses a
frame rather than stalling the queue. The 49,800 msg/s in the README is measured
with all of this compiled in and is the same number with `-live` as without,
because `-live` does no work per message.

**Warnings are not swallowed.** While the panel holds the terminal, log records
below WARN are suppressed — they are the progress lines the panel replaces — and
every warning and error is buffered and shown inside the panel. The counts
beside them come from the session's own counters, so nothing structural depends
on the buffer. When the panel stops, logging goes back to normal and the counted
session summary prints on stderr exactly as it always did.

**It never clears the screen.** Redraw is cursor-up plus clear-line over its own
block. A capture that has been running for an hour sits under whatever the
terminal was doing before it started, and that scrollback belongs to whoever
started it. The final frame is drawn from the `Summary` — the counted answer —
and left on screen.

## `tape verify` and the window's shape

The counts cannot answer the question `verify` exists to answer. A window that
lost its connection for ten seconds has healthy counts either side of the hole;
"records 84,201, gaps 1" says one happened and nothing about where.

```
  continuity  BROKEN — 3 discontinuities
  digest      sha256:5a03cc21ee350574a47fb7633f5d0718f29e576c98369f91a60c6e4da51e6879
  ! gap at …/20260825T140000Z.tape record 302: expected sequence 302, got 951 (649 missing)
  ! reseed at …/20260825T140000Z.tape record 503: reconnect: connection reset by peer
  ! gap at …/20260825T140100Z.tape record 103: expected sequence 1351, got 1877 (526 missing)
  shape       ██████████████████████████████████████████████████████████████████
              │               !          ^    │     !                          │
              14:00:00──────────────────────────────────────────────────14:02:00
              ▁▂▃▄▅▆▇█ 1-19 per 1.818s   ! gap   ^ reseed   │ file
```

The two red columns are the gaps, at their position in the window. A gap that
falls in a column no message reached is drawn as a full red block rather than
the space it would otherwise be — that is the most important column on the
chart and it would have been the invisible one.

The summary above the chart is unchanged and prints either way; something may be
parsing it. `-chart=false` leaves only the summary, and so does a pipe.

The accumulator streams. Records arrive one at a time and the window's span is
not known until the last one, so buckets start at one millisecond and halve
whenever they would outgrow 4,096. A day's window costs the same memory as a
minute's, from the single pass `verify` was already making.

## `tape replay -pretty`

A second, separate rendering: aligned columns for reading, with gaps and reseeds
as full-width banners.

```
time         type          sequence      side price       size
━━━━━━━━━━━━━━━━━━ window opens  subscribed  at 14:00:00.000 ━━━━━━━━━━━━━━━━━━━
14:00:30.000 match         300           sell    80299.53   0.00000001
14:00:30.100 match         301           buy     80300.53   0.00000001
━━━━━━ GAP  649 messages missing  expected 302, got 951  at 14:00:30.200 ━━━━━━━
14:00:30.200 match         951           sell    80301.53   0.00000001
```

(Eighty columns, so the channel column has been dropped; the type implies it.)

Two things about it are not negotiable.

**The canonical NDJSON is untouched.** It is the serialized shape the
determinism invariant is stated about, and the only safe relationship between it
and a display format is none: `-pretty` is a different encoder, sharing no code,
reached by a branch outside `NewCanonicalEncoder` rather than inside it. No flag
on `tape replay` can change a byte of the default output.

**Prices are the exchange's characters.** `PriceText` and `SizeText` are printed
rather than the `float64`s beside them. A float cannot tell `80691.5` from
`80691.50`, the columnar format goes to real trouble to keep that distinction,
and a display is downstream of it. The row drops whole columns on a narrow
terminal — the channel first, since the type implies it — but never shrinks
below the four that carry the record, because the only way to make it narrower
would be to truncate a decimal.

## What was deliberately not built

No live order-book depth view. Reconstructing the book from `level2_batch`
updates is a real piece of work with its own correctness questions, it is on the
README's non-goals list, and the sibling limit-order-book simulator is where it
belongs. Drawing a book here would mean this project owning a second definition
of what the book is, and the first thing that would happen is the two
disagreeing.

No web UI, no HTTP server, no TUI framework. `go.mod` is unchanged: this is
escape codes and the standard library.
