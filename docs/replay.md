# Replay: the total order, and what it promises

Replaying the same stored window twice produces byte-identical output. This
note is the definition behind that sentence — the order records come out in,
what happens at a gap, and what "output" means exactly.

The code is `internal/replay`. The commands are `tape replay` and `tape verify`.

## The order

Records are delivered sorted on, in order:

| # | key | direction |
|---|---|---|
| 1 | exchange timestamp | ascending, Unix nanoseconds |
| 2 | sequence rank — 0 without a sequence, 1 with one | ascending |
| 3 | sequence number | ascending |
| 4 | channel | ascending, byte-wise |
| 5 | file index in the window's sorted file list | ascending |
| 6 | record ordinal inside that file | ascending |

Keys 5 and 6 together are the **arrival index**. Every field above comes from
bytes on disk or from a record's position in the file list. None of them comes
from map iteration, goroutine scheduling, a wall clock, or the order in which
anything happened to be read. The arrival index is unique within a window, so
the key is a strict total order: there is never a residual tie for a sort to
break arbitrarily, and every machine breaks every tie the same way.

Files are ordered by their path relative to the window root, compared byte-wise.
Capture names them `{symbol}/{date}/{20060102T150405Z}.tape`, and a fixed-width
UTC timestamp sorts lexicographically into chronological order — sorting the
names needs no clock and no parsing.

A window is one symbol.

### Records with no sequence

Every `level2_batch` message arrives without a sequence number, as does every
control frame. Absence is not sequence zero. Treating it as zero would sort a
book update as though it preceded every trade ever recorded, so unsequenced
records carry rank 0 and sequenced records rank 1, and rank is compared before
the number.

The effect is that at one instant, unsequenced records are delivered before
sequenced ones — a book update stamped at T before the trades stamped at T.
That grouping is a convention. What matters about it is that it is decided by a
stored fact rather than by a zero value, that it is written down here, and that
it does not change.

### Records with no exchange timestamp

Coinbase `snapshot` and `subscriptions` frames carry no `time` field. Gap and
reseed records carry only a local receive time, which is a different clock from
the exchange's and must not be mixed into an exchange-time key — the difference
between the two is network latency, and sorting on a mixture of them would let
latency reorder the stream.

Such a record is **pinned**: it inherits the ordering content (keys 1–4) of the
last record before it in arrival order that had ordering information of its
own, and its own arrival index places it immediately after that record. So a
reseed lands exactly where the reconnect happened rather than at the front of
the window, and the inheritance chain runs across file boundaries — a window
that rotated in the middle of a reconnect does not fling the reseed back to the
start of time.

This is why the window cursor is sequential rather than a k-way merge across
files: pinning is a property of arrival order, and arrival order is a property
of the file sequence.

## Streaming, and how misordering is caught

A caller never holds a window in memory. Files are read in order through a
bounded reorder buffer holding at most `ReorderWindow` records, 4096 by
default.

Stored order is arrival order; delivered order is exchange order; the two differ
by however far a message can be displaced, which on Coinbase is one
`level2_batch` interval — tens of milliseconds, a handful of records. The buffer
absorbs that.

Every record the buffer emits is compared against the last one emitted. If one
ever comes out lower, the buffer was too small for that window and `Next`
returns `ErrOutOfOrder`. There is no path that emits a misordered stream
quietly, because a stream that is quietly in the wrong order is exactly the kind
of output that looks fine and is wrong.

## Gaps

Reading past a break in continuity is a caller's decision, never a default.

- A **gap record** stops replay with an error naming it.
- A **reseed record** stops replay too, unless it is the subscription that opens
  the window. A reseed with no message before it in arrival order is where the
  window starts, not a break in it — there is nothing before it to be
  discontinuous with. It is delivered like any other record, so it is not
  hidden either.
- `WithContinueOnGap` (`-continue-on-gap` on the command line) converts stopping
  into delivery. The gap and reseed records still come back through the
  iterator, and `Discontinuities()` lists every one crossed.

There is no third path. Nothing can consume a window and not know whether it
contained a gap.

`tape verify` always continues — stopping at the first discontinuity would
report one gap and hide the rest — and exits non-zero when it finds any.

## Canonical NDJSON

The serialized form: one JSON object per record, in replay order, each
terminated by a single `\n`. This is what `tape replay` writes and what the
determinism test hashes, so it is part of the invariant rather than a
formatting preference.

- Field order is struct declaration order. Nothing in the encoder is a map, so
  no field position is ever decided by map iteration.
- Which fields appear is decided by the record's kind.
- Times are RFC 3339 with nanoseconds, in UTC, or `""` when absent.
- A missing sequence is `null`, never `0`.
- HTML escaping is off, so a raw frame's bytes survive unchanged except for
  JSON whitespace compaction.
- A frame that is not valid JSON goes out base64 in `raw_b64` instead of `raw`.
  Capture stores unparseable frames because they are still evidence; replay
  neither drops them nor lets them corrupt the stream.
- Paths are relative to the window root, so the same window replayed from two
  different directories produces identical bytes.

## What is verified

`TestDeterminism` replays the checked-in fixture twice and requires the two byte
streams to be equal, then requires their digest to equal a constant checked into
the test. The run-to-run comparison is the invariant; the constant is the
regression guard, because two replays in one binary would still agree if the
ordering rules changed underneath them.

`TestDeterminismAcrossCodePaths` replays the same window a second way: read
every record out, shuffle it with five different seeds, sort it with
`sort.Slice` on the same comparator. `sort.Slice` is not stable. If the ordering
key left any two records comparing equal, the sort would be free to keep
whichever the shuffle put first and the five runs would disagree. They agree.

Neither test is ever skipped.

The fixture is 2,340 real Coinbase BTC-USD frames across three files, stored
verbatim. Its gap record was produced by the production capture path reacting to
the real sequence numbers on either side of a severed connection — 1,464
missing sequence numbers — not written by hand. `internal/replay/fixture_test.go`
has the provenance and rebuilds it.

## Measured

On the fixture, AMD Ryzen 7 3700X, Go 1.27, `-benchtime 50x -count 3`:

| | events/sec | vs. the window's wall clock |
|---|---|---|
| Iterator alone | 198,000 – 204,000 | ~6,500x |
| Iterator + canonical NDJSON | 108,000 – 109,000 | ~3,550x |

`tape verify` on a live 3m20s BTC-USD capture — 6,434 records, 5.4 MB, four
files, including the 1.1 MB level2 snapshot — replays in 72 ms: 89,844
events/sec, 2,790x wall-clock.
