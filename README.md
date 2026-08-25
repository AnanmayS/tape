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

## Status

M1 and M2 done: WebSocket ingest to local disk, sequence-gap detection,
reconnect with backoff. M3 (the replay library) is next.

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
