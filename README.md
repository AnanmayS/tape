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
./tape capture -dir data -format raw
./tape capture -dir data -s3-bucket my-tape-bucket
./tape capture -dir data -s3-bucket my-tape-bucket -metrics-namespace Tape
./tape verify data/v1/symbol=BTC-USD/date=2026-08-25
./tape verify -s3-bucket my-tape-bucket v1/symbol=BTC-USD/date=2026-08-25
./tape replay -continue-on-gap data/v1/symbol=BTC-USD/date=2026-08-25 > window.ndjson
./tape stat data/v1/symbol=BTC-USD/date=2026-08-25
./tape bench -repeat 20 data/v1/symbol=BTC-USD/date=2026-08-25
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

`capture` writes v2 unless given `-format raw`. M5 left raw the default with one
condition attached — the columnar writer had not met a load — and M7 measured
it at 49,800 messages a second against a live feed that produces 31 to 100. The
number and the reasoning are in the M7 results below.

`bench` pushes a captured window back through the capture path at saturation and
reports what each backpressure policy costs. It is how the policy was chosen and
how that choice can be re-checked; it is also the only place the policies that
were not chosen can still be selected.

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

M1 through M5 and M7 are done: WebSocket ingest, sequence-gap detection,
reconnect with backoff, deterministic replay, S3 storage partitioned by symbol,
date and hour, a delta-encoded columnar format measured at 5.2x against the raw
feed, and a backpressure policy chosen by measuring all three candidates at
saturation.

M6 is written but not applied. The Terraform, the metrics the ingester
publishes, the image and the CI are all in the repository and all of them are
checked as far as they can be checked without an AWS account; nothing has been
deployed, and the M6 section below says exactly which claims are still unproven.

### The five numbers

These are the deliverables `docs/decisions.md` commits to. Every one comes from running
the thing; none is estimated. Hardware for the rate-dependent ones: **AMD Ryzen
7 3700X** (8 cores, 16 threads), 31 GiB RAM, ext4 on NVMe, Go 1.27, Linux.

| | |
|---|---|
| **Sustained messages/sec under backpressure** | **49,800/s** columnar, **244,600/s** raw, at saturation under the chosen block policy — 1,330x and 6,540x the live feed's 37.4/s (M7) |
| **Compression ratio versus raw JSON** | **5.25x** against the frames on a live columnar capture; 5.12x measured against a concurrent raw capture of the same minutes (M5) |
| **Replay throughput versus wall-clock** | **81,313 events/sec, 2,580x wall-clock** on a live 4-minute columnar window, including canonical NDJSON (M3, M5) |
| **Gaps detected per capture session** | **0** in each clean session here (6m raw, 4m columnar); **3 gaps of 649, 526 and 3,240 missing sequence numbers** in a session with the connection severed every 25s (M1/M2) |
| **Determinism** | Two replays of the live window: **7,612,411 bytes both times, `sha256 8f203076…`**. The M3 golden fixture still replays to `sha256 ee957604…` through S3, through the columnar format, and through a mixed-format window |

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

**Columnar is now the default.** This section left raw the default with one
condition attached, and M7 discharged it: the columnar writer sustains 49,800
messages a second, 1,330 times the live rate, and a four-minute live columnar
capture at 31.5 msg/s peaked at a queue depth of 15 out of 4,096. The full
measurement is in M7 below.

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

### M7 — backpressure, decided by measurement

The reader and the writer have always been separate goroutines with a queue
between them, and what that queue does when it fills was the last open decision
in `docs/decisions.md`. There were three candidates — block, drop, buffer — and the
milestone's job was to make it a number rather than a preference.

All three are implemented as one `feed.Sink` with three sends. `tape bench`
reads a captured window, holds it in memory, and offers it back to `capture.Run`
through that sink, so the sequence tracking, the gap detection, the rotation and
the writing under load are the production ones and the socket is the only thing
substituted. The load below is a **six-minute live BTC-USD capture — 13,459
messages, 7.7 MiB of frames, 37.4 msg/s, zero gaps — pushed through twenty
times**, 269,180 messages and 154.1 MiB per run. Each pass advances every
sequence number past the pass before it and copies the frame it sends, because a
repeated window that restarted its sequence would be a wall of regression
records, and frames that all aliased one buffer would let a policy hold a million
of them for the price of a million pointers — which is exactly the cost one of
the three is being measured on.

Hardware, since these numbers are machine-dependent: **AMD Ryzen 7 3700X**
(8 cores, 16 threads), 31 GiB RAM, ext4 on NVMe, Go 1.27, Linux. Runs on tmpfs
measure memory bandwidth and are not reported.

**At saturation** — the feed offering frames as fast as the sink accepts them,
median of three runs:

| format | policy | offered/s | written/s | dropped | loss | peak queue | heap | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|---|---|---|---|
| raw | block | 244.6k | 244.6k | 0 | 0 | 4,097 | +10 MiB | 175ns | 1.3µs | 74µs | 1.07ms |
| raw | drop | 2.24M | 78.7k | 259,711 | 96.5% | 4,096 | +32 MiB | 159ns | 2.6µs | 393µs | 0.91ms |
| raw | buffer | 248.2k | 248.2k | 0 | 0 | 253,849 | +268 MiB | 159ns | 1.2µs | 74µs | 0.95ms |
| columnar | block | 49.8k | 49.8k | 0 | 0 | 4,097 | +22 MiB | 2.6µs | 15.4µs | 123µs | 55.7ms |
| columnar | drop | 740.0k | 14.7k | 263,827 | 98.0% | 4,097 | +26 MiB | 2.6µs | 30.7µs | 12.6ms | 56.3ms |
| columnar | buffer | 50.8k | 50.8k | 0 | 0 | 267,430 | +323 MiB | 2.6µs | 15.4µs | 106µs | 59.6ms |

That table is not yet a fair comparison and it is worth saying why. The offered
rate differs by policy — 2.24M/s for drop, 248k for block — because blocking
throttles the reader and dropping does not, and no exchange offers two million
messages a second. Comparing throughput across rows of that table would be
comparing three different experiments.

**The fair comparison pins the offered rate.** Each policy is given the same
load — the window replayed at a multiple of its own wall clock, paced to about
100,200 msg/s against the columnar writer and 484,700 against the raw one, twice
what each can take — so the only thing that varies is what a policy does with
the excess. Block's offered column reads lower than the pacing asked for because
blocking is precisely what throttles the reader: the load was offered and
refused, which is the policy working.

| format | policy | offered/s | written/s | dropped | loss | peak queue | heap |
|---|---|---|---|---|---|---|---|
| columnar | block | 49.6k | 49.6k | 0 | 0 | 4,097 | +23 MiB |
| columnar | drop | 100.2k | 47.9k | 421,422 | 52.2% | 4,097 | +25 MiB |
| columnar | buffer | 51.1k | 51.1k | 0 | 0 | 399,034 | +537 MiB |
| raw | block | 244.6k | 244.6k | 0 | 0 | 4,097 | +19 MiB |
| raw | drop | 484.7k | 228.5k | 1,067,145 | 52.9% | 4,097 | +17 MiB |
| raw | buffer | 253.4k | 253.4k | 0 | 0 | 956,193 | +1.34 GiB |

**Drop is not faster, and it loses half the feed.** At twice the writer's
capacity it wrote 47,900 messages a second against block's 49,600 and discarded
52.2% of what arrived. The policy exists to protect the writer and it does not:
the writer was already going as fast as it could, and the reader spinning
alongside it took CPU rather than giving it. What drop buys is nothing, and what
it costs is 421,422 frames.

**Buffer is not faster either, and it converts a throughput problem into an OOM.**
It wrote the same as block and grew the heap by 537 MiB in sixteen seconds on
the columnar writer, 1.34 GiB in eight on the raw one — the excess arrival rate
times the size of a frame, which is what an unbounded queue is. A burst it would
absorb; a sustained overload it cannot, and the end of that is a SIGKILL, which
loses the batch the writer had not flushed. The policy that discards nothing has
the failure that discards the most, and it leaves no record of it.

**Block's worst case is the only one already covered by invariant 2.** A reader
that stops draining its socket eventually has the exchange disconnect it, and
that is a path this project has handled since M2: reconnect, reseed record, gap
record, window marked untrustworthy. Nothing new has to be built and nothing can
be lost silently. **Block is the default, and `tape capture` has no policy
flag.** The other two stay in the tree, selectable from `tape bench` and nowhere
else, because a decision whose measurement cannot be re-run is an opinion with a
number attached.

**Every drop is a gap record.** A drop policy that merely counted what it threw
away would produce a window missing messages that reads as complete, which is
invariant 2 exactly — so it would not have been a legal candidate to measure. A
run of discarded frames is written into the stream as a gap record carrying the
count, placed ahead of the frame that resumed, and counted as a gap everywhere a
gap counts: the session summary, the `GapsDetected` metric, and the CloudWatch
alarm whose threshold is zero. On a monotonic feed nothing else could reveal it,
because there a skipped sequence number proves nothing. The 421,422 drops above
are covered by 1,752 gap records; the 1,067,145 by 34,367. A test asserts the
accounting under all three policies: every frame a session accepted is on disk
or inside a gap record's count.

**The queue's approach to the edge,** replaying the same window at multiples of
its own wall clock under the block policy — peak queue depth, out of 4,096:

| offered msg/s | raw | columnar |
|---|---|---|
| 3.7k | 153 | 274 |
| 11.2k | 174 | 548 |
| 22.4k | 222 | 1,551 |
| 33.7k | 238 | 1,767 |
| 44.9k | 272 | 3,311 |
| 56.1k requested | 379 | **4,097 — pinned; only 49.7k delivered** |

Those depths include the harness's own pacing burstiness, which is why the
honest low-rate number is the live one: **a four-minute live columnar capture at
31.5 msg/s peaked at a queue depth of 15 out of 4,096**, with zero drops and
zero gaps. Earlier live raw sessions: 37 of 4,096 at 37.4 msg/s, 32 at 31.0, 9
at 75.9. The feed is three orders of magnitude from the edge, which is why the
decision is about which failure is survivable there and not about which policy
is quickest.

**Write latency is where the columnar format's cost shows up.** Per record, raw
writes at a p50 of 175 ns and a worst case of 1.2 ms; columnar at a p50 of
2.6 µs and a worst case of 58 ms. That worst case is one record in four
thousand — the one that closes a batch and pays for encoding and compressing it.
At the live rate a batch of 4,096 rows takes about 110 seconds to fill, so that
58 ms lands once every 110 seconds and the queue grows by two frames while it
does. The live capture's own histogram says the same thing from the other side:
`p50 4.61µs p90 7.68µs p99 20.48µs p99.9 3.93ms max 53.33ms`.

**Columnar became the default here.** M5 left it optional pending exactly this
measurement. It sustains 49,800 messages a second — 1,330x the live rate — stores
the same records 5.25x smaller, and replays byte-identically; raw is 4.9x faster
and that is the wrong comparison, because what matters is columnar against the
exchange rather than columnar against raw. The four-minute live columnar
capture — 7,556 messages, 1,039,671 bytes, two files, zero gaps — replays twice
to **7,612,411 bytes and `sha256
8f203076e909f30d9791a4a88f62bbeb11150ff874fc7b7d4e4dd05df67720fd`** both times,
at 81,313 events/sec and 2,580x wall-clock.

**What is instrumented, and where it came from.** Queue depth was already a
metric; M7 added the drop counter and a bucketed write-latency histogram with
3 bits of mantissa — 512 buckets, no allocation, always on, because a number
that only exists when a flag was passed is a number nobody has when they need
it. The queue-depth-over-time series `tape bench -v` prints comes from the same
`metrics.Collector` the production build publishes to CloudWatch, so the graph
the harness draws is the graph an operator would see. No sixth CloudWatch metric
was added: drops are gaps, and `GapsDetected` already carries them.
