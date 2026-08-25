# Tape

Market data capture and deterministic replay.

Captures live order-book and trade feeds, stores them compactly, and replays them
so that a backtest over the same window produces **the same result every time**.

## Why this exists

Six existing projects — futures backtesting, options pricing, earnings-surprise
prediction, a Bollinger-bands strategy, a Polymarket paper-trader, and a limit
order book simulator in C — all need historical market data, and all currently
improvise it. Tape is the layer underneath them.

Non-reproducible backtests are worthless. If replaying the same window twice can
produce two different answers, no result built on top of it means anything. That
property is the point of this project, not a nice-to-have.

## Architecture

```
Coinbase / Binance / Polymarket        public WebSocket feeds (free tier)
              │
              ▼
      Go ingester                      ECS Fargate, long-lived connections
              │                        · sequence-gap detection on reconnect
              │                        · backpressure when feed outruns writes
              ▼
   delta-encoded columnar batches
              │
              ▼
            S3                         partitioned by symbol / date / hour
              │
              ▼
    Go replay library  ──────────────▶ downstream backtests
              │
              ▼
       CloudWatch                      gap alarms, ingest lag
```

Provisioned with Terraform. Three AWS services, each load-bearing.

## Scope

**v1 — one exchange, one symbol, capture and replay only.**
Everything else is deliberately deferred. A narrow version that runs beats a
broad one that is half-finished.

| Milestone | Contents |
|---|---|
| M1 | WebSocket ingester, one feed, write raw batches to local disk |
| M2 | Sequence-gap detection and reconnect handling |
| M3 | Replay library — byte-identical output across runs, verified by test |
| M4 | S3 storage with symbol/date/hour partitioning |
| M5 | Delta-encoded columnar format, measured against raw JSON |
| M6 | Terraform + ECS deployment, CloudWatch gap alarms |
| M7 | Backpressure handling, throughput measured under load |

## Non-goals

- No paid market data. Public feeds only.
- No live trading, order routing, or execution.
- No query engine. Range scans by symbol and date are enough.
- No multi-region, no HA. This is one ingester.

## Usage

```
go build -o tape ./cmd/tape
./tape capture -dir data -window 5m
./tape capture -dir data -s3-bucket my-tape-bucket
./tape verify data/v1/symbol=BTC-USD/date=2026-08-25
./tape verify -s3-bucket my-tape-bucket v1/symbol=BTC-USD/date=2026-08-25
./tape replay -continue-on-gap data/v1/symbol=BTC-USD/date=2026-08-25 > window.ndjson
```

Ctrl-C stops the capture cleanly and prints a counted session summary. The
exchange, product and channels are fixed for v1 and are not flags: Coinbase
Exchange, `BTC-USD`, channels `level2_batch` and `matches`. The unbatched
`level2` channel now requires authentication and this project takes no
credentialed or paid data.

Captured files land at the data directory plus their storage key:

```
v1/symbol=BTC-USD/date=2026-08-25/hour=14/20260825T140000Z.tape
```

One layout everywhere. A file's local path is its S3 object key under the root,
so uploading it is copying it to the key its own path already spells, and a
window replayed from a bucket is named the same as the same window on disk.
Every component is fixed-width and byte-sortable, so a prefix scan by symbol,
day or hour is a literal string prefix and sorting keys is sorting windows by
time — which is the only query this project has.

Each file is a magic-and-version header followed by length-prefixed records:
message records carrying the raw frame verbatim plus a receive timestamp, gap
records, and reseed records. Files are opened once with `O_APPEND` and never
reopened.

With `-s3-bucket`, each file is uploaded as it closes. Local disk stays the
durable copy: an unreachable bucket costs a re-upload, never a frame. Uploads
run off the capture path, retry with backoff, and are logged at error level if
they run out of attempts. Region, endpoint and credentials come from the ambient
AWS configuration — the ECS task role in production — and are never flags.

`replay` writes a window to stdout as canonical NDJSON, in a fixed total order
documented in [docs/replay.md](docs/replay.md). It stops at a gap or a
reconnect unless told otherwise, and `verify` exits non-zero on a window that
contains one.

## Status

M1 through M4 done: WebSocket ingest, sequence-gap detection, reconnect with
backoff, deterministic replay, and S3 storage partitioned by symbol, date and
hour. M5 (delta-encoded columnar format) is next.

### M1 and M2 — capture

Measured on a live BTC-USD capture, 100s, one-minute windows:

| | |
|---|---|
| Messages | 3,103 |
| Sustained rate | 31.0 msg/s |
| Bytes written | 4,082,288 |
| Peak reader-to-writer queue depth | 32 of 4096 |
| Files / rotations | 3 / 2 |

Reading those files back gives 3,103 message records and 4,082,288 bytes,
matching the session summary exactly.

Reconnect behaviour was measured against the live feed with the connection
severed every 25s: three reconnects produced three reseed records and three
gap records, of 649, 526 and 3,240 missing sequence numbers. Coinbase offers
no backfill on the public feed, so those windows are marked untrustworthy
rather than silently continued.

### M3 — replay

Determinism is verified, not asserted. The test fixture is 2,340 real BTC-USD
frames across three files, with a reseed and a gap of 1,464 missing sequence
numbers produced by the capture path itself. Replaying it twice gives
2,197,803 bytes of canonical NDJSON both times, `sha256
ee9576040361b07272db0cb6e614b02cef53dec1fcc772aeea1fa609b4fb7a21`, and that
digest is checked into the test. A second test reads the same window, shuffles
every record with five seeds and sorts it with an unstable sort: all five agree
with the streaming replay, which is what says the ordering key leaves no tie
behind. Neither test is skippable.

Replay throughput, measured on that fixture:

| | |
|---|---|
| Iterator alone | 198,000–204,000 events/sec, ~6,500x wall-clock |
| Iterator + canonical NDJSON | 108,000–109,000 events/sec, ~3,550x wall-clock |

On a live 3m20s capture — 6,434 records, 5.4 MB across four files, level2
snapshot included — `tape verify` replays the whole window in 72 ms: 89,844
events/sec, 2,790x the wall-clock time it took to record.

Memory is bounded by a reorder buffer of 4,096 records rather than by the
window, and a window whose stored order is displaced further than that fails
with `ErrOutOfOrder` instead of emitting a stream that is quietly misordered.

### M4 — S3 storage

Storage is one interface with three operations — put an immutable object, list
a prefix, stream an object — behind both the writer and the reader. There is no
delete and no overwrite, because a method that can break the append-only
invariant is a method that eventually will. Two implementations: the local
filesystem, which is the path every test runs on, and S3.

**Determinism survived the move.** The M3 golden fixture replayed out of a
bucket produces 2,197,803 bytes of canonical NDJSON and `sha256
ee9576040361b07272db0cb6e614b02cef53dec1fcc772aeea1fa609b4fb7a21` — byte for
byte the same as replaying it off local disk, and the same digest M3 recorded
before any of this existed. The bucket replay streams three objects across two
`ListObjectsV2` pages; the local one opens three files. Three tests hold that
line, and none of them is skippable.

**Append-only is enforced by the bucket, not by the caller.** Every `PutObject`
carries `If-None-Match: "*"`, so S3 itself refuses to overwrite. A retried
upload whose first attempt landed but lost its response comes back 412, which
the uploader reads as "already stored" and counts as success. Measured against
the fake: sixteen concurrent puts of one key leave **exactly one object**, one
writer succeeding and fifteen told the key is taken; an upload retried after its
object had already landed leaves **exactly one object**, with the first
attempt's bytes intact. The filesystem store gets the same guarantee from a hard
link, so the store CI runs on is a real test of the behaviour, not a stand-in
for it.

**Capture does not depend on S3 being up.** Uploads run off the capture path on
a worker goroutine; handing a file over never blocks and never fails. Against a
store that is never reachable, a 179-message capture finishes with all 179
messages on disk, complete and readable, and three upload failures logged at
error level. The error is reported from `Run` but joined rather than
substituted: a session that captured everything and uploaded nothing is not a
failed capture.

**No credentials anywhere in the test suite.** The S3 path is tested against an
in-process fake — `httptest`, about four hundred lines, implementing exactly
`PutObject` with `If-None-Match`, `ListObjectsV2` with pagination, and
`GetObject`. No MinIO, no container, no AWS account. What is substituted is the
far end of the socket; the client, the signing, the pagination and the streaming
are the real ones. A test suite that needs a bucket is a test suite that stops
being run, and the conditional put is exactly the behaviour nobody would notice
had broken.

The only new dependency is `aws-sdk-go-v2` (config, credentials, s3).
