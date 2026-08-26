# Tape

**Market data capture and deterministic replay.**

## What this is

Every trade on an exchange, and every change to its order book, goes out over a
public feed as it happens. Nobody keeps a copy for you. Tape records that feed,
stores it about five times smaller than the raw JSON, and plays it back the same
way every time.

## What it's for

Suppose you want to test a trading idea: buy when the price crosses above its
20-minute average. To learn how it would have done last Tuesday, you need last
Tuesday's market data, trade by trade.

Exchanges hand you the live feed for free and charge for the history, so you
record it yourself. Then your recorder loses its connection for ten seconds one
afternoon. The file it leaves behind looks fine and is missing a hundred trades.
Your backtest reports a profit, and nothing in the file tells you otherwise.

Tape records the free public feed and writes every hole it finds into the data,
where replay stops at it. Replay one window twice and you get the same bytes
both times, so a backtest that changes its answer has told you something about
your strategy and nothing about the ground under it.

The project is built around that property. If two replays of one window could
disagree, every result above them would be worthless, so CI checks the digests
on every push.

Backtesting is one use. The same recordings answer questions that are hard to
get at any other way:

- **Debugging a live bot.** Your program did something strange on Tuesday
  afternoon. Replay that window into it and watch it do the same thing again,
  as many times as you need.
- **Regression tests for trading code.** A test that feeds in real market data
  and asserts an exact result depends on the data arriving the same way every
  run. Otherwise it fails at random and people learn to ignore it.
- **Microstructure research.** Questions about spreads, queue position, or how
  fast the book refills after a large trade need tick-level history, and vendors
  sell that by the month.
- **Incident reconstruction.** After a crash or an outage, the recording is your
  account of what the exchange published and when.

## Quick start

```bash
go build -o tape ./cmd/tape
```

Capture two minutes of live BTC-USD (the feed is free and needs no account).
`-live` draws a status panel while it runs — rate, queue depth, and the gap
count in red the moment it leaves zero:

```bash
./tape capture -dir data -duration 2m -live
```

See what landed — message counts, gaps, files:

```bash
./tape verify data/v1/symbol=BTC-USD
```

Then prove the core promise yourself. Replay the window twice and compare the
hashes; they match, every time:

```bash
./tape replay data/v1/symbol=BTC-USD | sha256sum && ./tape replay data/v1/symbol=BTC-USD | sha256sum
```

`go test ./...` runs the whole suite, including the determinism test, and needs
no AWS account.

Or run [`./demo.sh`](demo.sh), which does all of the above against the live feed
in about seventy seconds — capturing twice so the second session opens with a
real discontinuity, then checking that the window replays identically.

## The three rules

1. **Replay is deterministic.** The same window replays byte-identically, every
   run, on any machine. Ties in timestamp order break on a stored field, never
   on map iteration or goroutine scheduling.
2. **Gaps are never silent.** When a reconnect loses messages, the sequence
   numbers say so, and the hole is written *into the data* as a gap record — not
   into a log line that disappears. Replay stops at a gap unless you explicitly
   opt in to crossing it.
3. **Capture is append-only.** Stored data is never rewritten. Files are opened
   once and never reopened; corrections are new records. In S3 the bucket itself
   enforces it via conditional writes.

## The five numbers

Every one comes from running the system on live data. None is estimated. The
full workings are in [docs/results.md](docs/results.md).

| | |
|---|---|
| **Sustained throughput** | **49,800 msg/s** columnar (244,600 raw) at saturation — 1,330x what the live feed produces |
| **Compression vs raw JSON** | **5.25x**, measured on a live capture, with a per-column breakdown |
| **Replay throughput** | **81,313 events/sec**, 2,580x faster than the wall-clock time it took to record |
| **Gaps per session** | **0** in clean sessions; 3 gaps of 649, 526 and 3,240 missing sequences when the connection was severed every 25s |
| **Determinism** | Two replays: **7,612,411 bytes both times, identical SHA-256.** The golden fixture holds its digest through S3, through the columnar format, and through a mixed-format window |

## How it works

```
Coinbase public WebSocket feed          free, no credentials
              │
              ▼
      Go ingester                       one ECS Fargate task
              │                         · sequence-gap detection on reconnect
              │                         · reader and writer are separate goroutines
              ▼
   delta-encoded columnar batches       ~5x smaller than the raw frames
              │
              ▼
            S3                          partitioned by symbol / date / hour
              │
              ▼
    Go replay library  ──────────────▶  downstream backtests
              │
              ▼
       CloudWatch                       gap alarms, ingest lag
```

Three AWS services, each load-bearing, all provisioned with Terraform.

**Files.** A capture writes to `v1/symbol=BTC-USD/date=2026-08-25/hour=14/...tape`
— the same layout on local disk and in S3, so a file's path *is* its object key
and uploading is just copying it there. Every component is fixed-width and
sorts correctly as a string, which makes "give me this symbol on this day" a
plain prefix scan. That is the only query this project has.

**Formats.** Each file starts with a magic-and-version header. **v1** is
length-prefixed records, each carrying the exchange's raw frame verbatim. **v2**
is the same records as delta-encoded columnar batches, 5.2x smaller
([design note](docs/columnar.md)). Replay reads either, decided per file, so a
window can hold both. Capture writes v2 by default.

The raw frames are always kept rather than reconstructed from the columns, even
though that would nearly double the compression. Byte-exact reconstruction of
Coinbase JSON isn't provable, and a reconstruction that is 99.99% right hands
back something the exchange never sent while looking completely healthy.

**Backpressure blocks.** When the writer falls behind, the reader waits. All
three candidate policies were built and measured at saturation: dropping lost
52% of the feed while being *slower*, and buffering grew the heap 537 MiB in
sixteen seconds on its way to an OOM kill that loses data with no record of it.
Blocking's worst case — the exchange disconnecting a slow consumer — is already
covered by rule 2. `tape capture` has no policy flag; `tape bench` can still
reproduce the comparison.

## Commands

| | |
|---|---|
| `tape capture` | record the live feed to local files, optionally uploading to S3 |
| `tape replay` | write a window to stdout as canonical NDJSON, in a fixed total order |
| `tape verify` | read a window back and report what is in it; exits non-zero on a gap |
| `tape stat` | measure what a window costs to store, column by column |
| `tape bench` | push a window back through the capture path under load |

Run `tape <command> -h` for flags. Ctrl-C stops a capture cleanly and prints a
counted session summary.

Three flags draw rather than print, and each does nothing at all unless stdout
is a terminal, so a pipe gets exactly the bytes it always got. `capture -live`
replaces the progress log with a status panel — rate, sparkline, queue depth,
and the gap count in red the moment it leaves zero. `verify` draws the window's
shape under its summary, with every gap marked where it happened; `-chart=false`
turns that off. `replay -pretty` writes a readable event stream instead of
canonical NDJSON, with gaps as banners you cannot scroll past. `-no-color` and
`NO_COLOR` apply to all three. [docs/terminal.md](docs/terminal.md) has the
details and the rules they follow.

The exchange, product and channels are fixed for v1 and are deliberately not
flags: Coinbase Exchange, `BTC-USD`, channels `level2_batch` and `matches`. The
unbatched `level2` channel now requires authentication, and this project takes
no credentialed or paid data.

Two optional flags reach AWS, and both are off by default so local capture never
needs an account. `-s3-bucket` uploads each file as it closes — local disk stays
the durable copy, so an unreachable bucket costs a re-upload, never a frame.
`-metrics-namespace` publishes five metrics a minute to CloudWatch: messages,
message rate, gaps, ingest lag and peak queue depth.

## Scope

**v1 is one exchange, one symbol, capture and replay only.** A narrow version
that runs beats a broad one that is half-finished.

| Milestone | | |
|---|---|---|
| M1 | WebSocket ingester writing to local disk | done |
| M2 | Sequence-gap detection and reconnect | done |
| M3 | Replay library, byte-identical across runs | done |
| M4 | S3 storage, partitioned by symbol and date | done |
| M5 | Delta-encoded columnar format | done |
| M6 | Terraform, ECS Fargate, CloudWatch alarms | written, not applied |
| M7 | Backpressure, decided by measurement | done |

M6 is the one caveat: the Terraform, the image, the metrics and the CI are all
here and checked as far as they can be without an AWS account, but nothing has
been deployed. [docs/results.md](docs/results.md#m6--deployment) lists exactly
which claims are still unproven — chiefly that a gap on a live deployment fires
its alarm end to end.

**Deliberately not built:** no queue, no database, no Kubernetes, no query
engine, no second exchange, no paid data, no live trading, no multi-region. Each
would be a service to justify, and at this volume none of them is.

## Documentation

| | |
|---|---|
| [docs/results.md](docs/results.md) | every measurement, milestone by milestone |
| [docs/decisions.md](docs/decisions.md) | the architecture rules and what closed each open question |
| [docs/columnar.md](docs/columnar.md) | the columnar format: byte layout and codec measurements |
| [docs/replay.md](docs/replay.md) | the total order replay guarantees, written down |
| [docs/deploy.md](docs/deploy.md) | build, push, apply, verify, and the teardown that leaves nothing billable |
| [docs/terminal.md](docs/terminal.md) | the three things that draw, and what they may never do |
