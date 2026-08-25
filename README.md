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
            S3                         partitioned by symbol / date
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
| M4 | S3 storage with symbol/date partitioning |
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
./tape verify data/BTC-USD/2026-08-25
./tape replay -continue-on-gap data/BTC-USD/2026-08-25 > window.ndjson
```

Ctrl-C stops the capture cleanly and prints a counted session summary. The
exchange, product and channels are fixed for v1 and are not flags: Coinbase
Exchange, `BTC-USD`, channels `level2_batch` and `matches`. The unbatched
`level2` channel now requires authentication and this project takes no
credentialed or paid data.

Captured files land at `data/{symbol}/{date}/{window start}.tape`. Each is a
magic-and-version header followed by length-prefixed records: message records
carrying the raw frame verbatim plus a receive timestamp, gap records, and
reseed records. Files are opened once with `O_APPEND` and never reopened.

`replay` writes a window to stdout as canonical NDJSON, in a fixed total order
documented in [docs/replay.md](docs/replay.md). It stops at a gap or a
reconnect unless told otherwise, and `verify` exits non-zero on a window that
contains one.

## Status

M1, M2 and M3 done: WebSocket ingest to local disk, sequence-gap detection,
reconnect with backoff, and deterministic replay. M4 (S3 storage) is next.

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
