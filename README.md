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
./tape capture -dir data -format columnar
./tape capture -dir data -s3-bucket my-tape-bucket
./tape capture -dir data -s3-bucket my-tape-bucket -metrics-namespace Tape
./tape verify data/v1/symbol=BTC-USD/date=2026-08-25
./tape verify -s3-bucket my-tape-bucket v1/symbol=BTC-USD/date=2026-08-25
./tape replay -continue-on-gap data/v1/symbol=BTC-USD/date=2026-08-25 > window.ndjson
./tape stat data/v1/symbol=BTC-USD/date=2026-08-25
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

Each file is a magic-and-version header followed by its records. There are two
formats and the version byte decides which: **v1** is length-prefixed records —
message records carrying the raw frame verbatim plus a receive timestamp, gap
records, and reseed records — and **v2** is the same records as delta-encoded
columnar batches, 5.2x smaller, described in [docs/columnar.md](docs/columnar.md).
Replay reads either, per file, so a window can hold both. Files are opened once
with `O_APPEND` and never reopened.

`capture` writes v1 unless given `-format columnar`; the reasoning for that
default, and the condition for changing it, is in the M5 results below.

With `-s3-bucket`, each file is uploaded as it closes. Local disk stays the
durable copy: an unreachable bucket costs a re-upload, never a frame. Uploads
run off the capture path, retry with backoff, and are logged at error level if
they run out of attempts. Region, endpoint and credentials come from the ambient
AWS configuration — the ECS task role in production — and are never flags.

With `-metrics-namespace`, five numbers a minute go to CloudWatch: messages,
message rate, gaps, ingest lag and peak queue depth. It is off unless asked for,
so a local capture never needs AWS, and a publish that fails is a log line
rather than a lost frame. Deployment — Terraform, the image, and the teardown
that has to leave nothing billable — is in [docs/deploy.md](docs/deploy.md).

`replay` writes a window to stdout as canonical NDJSON, in a fixed total order
documented in [docs/replay.md](docs/replay.md). It stops at a gap or a
reconnect unless told otherwise, and `verify` exits non-zero on a window that
contains one. `stat` measures what a window costs to store, column by column;
it is where the compression ratio below comes from.

## Status

M1 through M5 done: WebSocket ingest, sequence-gap detection, reconnect with
backoff, deterministic replay, S3 storage partitioned by symbol, date and hour,
and a delta-encoded columnar format measured at 5.2x against the raw feed.

M6 is written but not applied. The Terraform, the metrics the ingester
publishes, the image and the CI are all in the repository and all of them are
checked as far as they can be checked without an AWS account; nothing has been
deployed, and the section below says exactly which claims are still unproven.
M7 (backpressure under load) is next, and is the milestone that turns the open
question in CLAUDE.md into a number.

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

### M5 — columnar storage

Measured on three minutes of live BTC-USD, 9,189 records, captured twice
concurrently — once per format — so the two numbers are the same market and not
two different minutes:

| | |
|---|---|
| Raw frames — what NDJSON would store | 5,130,188 bytes |
| v1 tape files | 5,249,687 bytes |
| v2 columnar | 1,002,748 bytes |
| **Compression ratio** | **5.12x against the frames, 5.24x against v1** |

Where the columnar bytes go:

| column | encoded | raw | share of file |
|---|---|---|---|
| frames | 914,906 | 5,148,505 | 91.2% |
| recv timestamp | 30,904 | 32,399 | 3.1% |
| exchange timestamp | 29,377 | 32,760 | 2.9% |
| size | 11,947 | 14,179 | 1.2% |
| sequence | 4,820 | 6,801 | 0.5% |
| price | 1,676 | 7,123 | 0.2% |
| message type | 1,494 | 9,336 | 0.1% |
| presence bitsets, scales, kind, reseed | 6,645 | 28,466 | 0.7% |
| batch and block framing | 979 | — | 0.1% |

The columnar figure is what `tape stat` computed for those exact records; the
concurrent columnar capture wrote 1,003,688 bytes for its own 9,207, which is
the same answer arrived at from the other side.

**The raw frames are kept.** Reconstructing them from the columns instead would
roughly double the ratio, since they are 91% of the file, and it is not done:
byte-exact reconstruction of Coinbase JSON would mean reproducing its field
order, its full key set and its number formatting forever, and a reconstruction
that is 99.99% right hands back something the exchange never sent while looking
completely healthy. The frame is what settles an argument. Everything
structured costs 8.7% of the file and buys scans that never inflate a frame.

**Determinism survived again.** The M3 golden fixture, transcoded to columnar
and replayed, produces 2,197,803 bytes of canonical NDJSON and `sha256
ee9576040361b07272db0cb6e614b02cef53dec1fcc772aeea1fa609b4fb7a21` — the digest
M3 recorded and M4 carried through the storage move, unchanged. A window with
some files in each format produces it too, because format is a property of a
file and the reader hands the replay layer byte-identical records either way.

Reading costs 21%: the same fixture window replays at 109,940 events/sec from
v1 and 87,317 from v2, both including canonical NDJSON. On the live window
above, `tape verify` runs at 111,170 events/sec over v1 and 91,308 over v2 —
1,783x the wall-clock time it took to record.

**No new dependency.** `klauspost/compress` was allowed for this milestone and
was measured against stdlib deflate on the real frames. At comparable speed zstd
compressed this data slightly worse — 6.00x at 141 MB/s for its "better" level
against 5.99x at 99 MB/s for deflate level 7 — so the format uses
`compress/flate` at level 8 and the dependency list is unchanged.

**Prices are exact.** Coinbase sends decimal strings and a float64 cannot hold
them: it cannot represent 80691.53 exactly and cannot remember whether the wire
said "80691.5" or "80691.50". Prices and sizes are stored as scaled integers,
and a value takes that path only if re-rendering it reproduces the exchange's
characters exactly; anything that fails — a leading zero, an exponent, more
digits than an int64 holds — is stored as its own string instead. Nothing is
normalised on the way to disk.

**Batches close on records, never on the clock.** The first live columnar
capture measured 4.29x rather than the 5.7x the fixture predicted, because the
one-second durability flush was closing a batch every time it fired and 23
records is not a compression window. A batch now closes on 4096 rows, 4 MiB of
frames, or a 30-second span of receive timestamps — so what a session writes is
a function of the frames that went into it, and a hard kill loses at most that
much. A clean stop loses nothing.

**Raw is still the default.** Capture is the half of this project that cannot be
re-run, and the columnar writer has not yet been measured under the load M7
exists to apply. M7 measures both; if columnar sustains the same rate it becomes
the default then, on a number.

The full design note, the codec measurements and the column-by-column reasoning
are in [docs/columnar.md](docs/columnar.md).

### M6 — deployment

Everything in [`terraform/`](terraform), the [`Dockerfile`](Dockerfile), the
five metrics the ingester publishes, and the CI that checks all of it. The full
lifecycle — build, push, apply, verify, pause, destroy — is in
[docs/deploy.md](docs/deploy.md).

**Three services, and the count is the design.** S3 holds the captures, one
Fargate task runs the ingester, CloudWatch carries its logs, metrics and alarms.
ECR and IAM are in the stack because a container has to come from somewhere and
run as someone. There is no queue between the socket and the writer, no table,
no cluster orchestrator, and no VPC — a private subnet would need a NAT gateway,
which is about $32 a month to hide a task that accepts no inbound connections at
all. Running, the whole thing is roughly $0.40 a day, and `-var desired_count=0`
stops nearly all of that without destroying anything.

**The task role is the interesting file.** It has two statements:
`s3:PutObject` on one key prefix of one bucket, and `cloudwatch:PutMetricData`
conditioned on one namespace. No `GetObject`, because the capture path never
reads an object back. No `DeleteObject`, because nothing in this project deletes
a capture and a role that cannot delete one states that more firmly than a code
review does. No `ListBucket`, because a conditional put needs no listing.
`PutMetricData` takes no resource ARN, so the namespace condition is the only
thing standing between "publish our own five metrics" and "write into any
namespace in the account, including `AWS/ECS`".

**Five metrics, aggregated locally.** Messages, message rate, gaps, ingest lag
and peak queue depth, folded client-side and sent in one `PutMetricData` call a
minute. Emission is off unless `-metrics-namespace` is given, so a local capture
never touches AWS. Ingest lag is measured from the exchange's timestamp to the
moment the record is written rather than to the socket read, so the time a frame
spends waiting in the reader-to-writer channel is inside the number rather than
hidden behind it — that wait is what grows when the feed outruns the writer, and
it is what M7 exists to characterise. It is published as a StatisticSet, because
the interesting event is one message arriving late among thousands that did not
and an average over a minute buries it.

Nothing unmeasured is published as a zero: an interval in which no frame carried
an exchange timestamp has no lag, and no observation of the queue is not an
observation of an empty queue. An interval in which nothing happened does
publish its zero counts, because a flat line and a hole in the graph mean
different things.

**The gap alarm's threshold is zero,** because the right number of gaps is zero.
A gap means a reconnect lost frames the public feed will not sell back, and
there is no acceptable rate to tune that to.

**`force_destroy = false`,** so `terraform destroy` fails while captures are in
the bucket. Teardown is a command this project runs between every session, and
one that quietly deleted the data would be the most dangerous line in the repo.
The emptying procedure — sync, count, replay the local copy and compare digests,
then delete — is in the deploy doc.

**Two packages, no AWS in the tested one.** `internal/metrics` holds every
decision and imports no SDK; `internal/metrics/cwmetrics` is a translator whose
single CloudWatch call sits behind a two-line interface. The split is the same
one `storage` and `s3store` already use, for the same reason: a package that
needs a credential to test is a package that stops being tested. The whole suite
still runs with no AWS account and no `-short` flag anywhere.

**What is not verified.** This has never been applied, and the claims that
depend on an apply are not made. No `plan`, no `apply`: CI runs `terraform fmt
-check`, `init -backend=false` and `validate`, which catch syntax, type and
reference errors and cannot catch a resource AWS rejects at create time or an
IAM policy that is valid but too narrow. The task role has not been proven
sufficient. The image has not been built here — the machine it was written on
cannot reach a Docker daemon — though both the amd64 and arm64 static builds
succeed, so CI's `docker` job is the first real build. And the end-to-end check
that matters most, inducing a gap on a live deployment and watching the alarm
deliver, can only happen after a deploy. Every link in that chain is tested in
isolation; none of it is tested joined up. [docs/deploy.md](docs/deploy.md)
keeps the same list.
