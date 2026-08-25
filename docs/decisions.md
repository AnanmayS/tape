# Tape — build decisions

Read this before writing code in this repo. Decisions made here are binding until
a measurement overturns them.

## What we are building

A Go service that captures live market data feeds, stores them compactly on S3,
and replays them deterministically. See `README.md` for the architecture diagram.

## The governing rule: architecture must match the problem

This project exists partly as portfolio work, and the failure mode that comes
with that is **over-architecting to collect impressive service names**. A
reviewer who sees five AWS services behind a problem that needs two asks why,
and the honest answer would be "for the resume". That is the thing to avoid.

Concretely:

- Do not add SQS. At this volume the ingester writes directly. If throughput
  ever justifies decoupling, that is a real decision made with a real number —
  not a default.
- Do not add DynamoDB. There is no state here that S3 and local files cannot
  hold.
- Do not add Kubernetes. One ECS Fargate task.
- Do not call it "distributed". It is one ingester.

If a service is added, it must be because something measured demanded it, and
the reason belongs in this file.

## Invariants — these are the project

1. **Deterministic replay.** Replaying the same stored window twice must produce
   byte-identical output. Ordering must be stable when timestamps tie. There is
   a test for this and it must never be skipped.
2. **Gaps are never silent.** If a reconnect loses messages, the sequence numbers
   say so. Either backfill, or mark the window untrustworthy. Silently continuing
   past a gap is the bug that quietly poisons every downstream result.
3. **Capture is append-only.** Stored data is never rewritten. Corrections are
   new records.

## Design decisions already made

**Sequence-gap detection over assuming continuity.** Public feeds provide
sequence numbers; a reconnect without a gap check produces data that looks fine
and is wrong.

**Columnar, delta-encoded storage.** Tick data is large and highly repetitive.
Timestamps and prices delta-encode against the previous tick. Measure the ratio
against raw JSON — that number is a deliverable. Done in M5: **5.16x against
the raw frames, 5.24x against the v1 tape files**, measured on three minutes of
live BTC-USD. Design note and the per-column breakdown: `docs/columnar.md`.

**The raw frames are stored, not reconstructed.** The columnar format keeps
every frame verbatim in a column of its own and delta-encodes the structured
fields beside it. Reconstructing frames from the columns would roughly double
the ratio and is refused: byte-exact reconstruction of Coinbase JSON is not
provable, and a reconstruction that is nearly right hands back something the
exchange never sent while looking healthy. The structured columns cost 6.4% of
the file and are an index, not the record.

**Prices are exact decimals, never floats.** Stored as scaled integers, and
only when re-rendering the integer reproduces the exchange's characters; a
value that fails that test is stored as its own string. A float cannot tell
"80691.5" from "80691.50", and a stored price that lost a digit is the quiet
kind of wrong this project exists to prevent.

**Compression is stdlib `compress/flate` at level 8.** `klauspost/compress` was
measured against it on the real frames and did not earn the dependency: at
comparable speed zstd compressed this data slightly worse. The table is in
`docs/columnar.md`. The dependency list is still `aws-sdk-go-v2` and
`coder/websocket`.

**SNS is in the stack, and the service count is still three.** This file says a
service that gets added has to be justified here, so: a CloudWatch alarm has
exactly one way to notify a human, and it is an SNS topic. There was no
alternative to weigh. The topic carries the gap and ingest-lag alarms and
nothing else. ECR and IAM are in the Terraform for the same shape of reason — a
container has to come from somewhere and run as someone. The three load-bearing
services are still S3, ECS Fargate and CloudWatch, and there is still no queue,
no table and no orchestrator.

**Five custom metrics, and no more.** CloudWatch bills per metric per month, so
each one has to earn its place: messages, message rate, gaps, ingest lag, peak
queue depth. Everything else a session counts is already in the summary it logs.
Adding a sixth means saying here what question it answers that the log does not.

**Backpressure blocks, and the number that decided it is 52.2%.** All three
policies were built and measured at saturation with `tape bench` (M7 in
`docs/results.md` has the full tables). The measurement that settles it is the one
where every policy is offered the same rate — twice what the writer can take:
**block wrote 49,600 messages a second and lost nothing; drop wrote 47,900 and
lost 52.2% of the feed; buffer wrote 51,100 and grew the heap by 537 MiB in
sixteen seconds.** Drop is not faster and it throws away half the data. Buffer
is not faster either, and it converts a throughput problem into an OOM kill —
which is a SIGKILL, which loses the batch the writer had not flushed, so the
policy that discards nothing has the failure that discards the most and leaves
no record of it. Block's overload path, by contrast, ends somewhere this project
already handles: a consumer that stops draining its socket is disconnected by
the exchange, which produces a reconnect, a reseed record and a gap record. It
is the only one of the three whose worst case is already covered by invariant 2
without new machinery.

The headroom makes this a decision about the cliff rather than the cruise. The
live feed runs at 31–100 messages a second; the columnar writer sustains 49,800
and the raw one 244,600, and at the live rate the queue peaks at 37 of 4,096.
Nothing here is close to the edge, so what matters is only which failure is
survivable at the edge.

Drop and buffer stay in the tree, selectable from `tape bench` and nowhere else,
because a decision whose measurement cannot be re-run is an opinion with a
number attached. `tape capture` offers no policy flag. Every frame the drop
policy discards is written into the stream as a gap record with its count — a
drop that is not a gap record is a window missing messages that reads as
complete, which is exactly what invariant 2 forbids, so a drop policy without
that record would not have been a legal candidate to measure.

**Capture defaults to the columnar format,** on the condition M5 attached to
leaving it raw. Columnar sustains 49,800 messages a second, 1,330 times the live
rate, while storing the same records 5.2x smaller and replaying to the same
digest. Raw is 4.9x faster and that is the wrong comparison: what matters is
columnar against the exchange, not columnar against raw. The cost is a tail —
the record that closes a batch pays 58 ms for encoding and compressing four
thousand rows, against raw's 1.2 ms worst case — and at the live rate a batch
closes every 110 seconds and the queue grows by two frames while it does.

## Stack

- Go (1.27 installed locally)
- AWS: ECS Fargate, S3, CloudWatch — nothing else without justification
- Terraform for all infrastructure. No console clicking; if it is not in
  Terraform it does not exist.
- GitHub Actions for CI

## Numbers to measure

These are real deliverables, not decoration. Every one must come from running
the thing — none may be estimated or assumed.

- Sustained messages/sec under backpressure
- Compression ratio versus raw JSON
- Replay throughput versus wall-clock
- Gaps detected per capture session
- Determinism: two replays of one window, diffed, must be identical

## Cost discipline

Public feeds are free. ECS Fargate on one small task is a few dollars a month.
Tear infrastructure down between capture sessions. Nothing here should ever
require paid market data.

## Working style

- Small commits as work happens. The commit history is part of what this repo
  is for; do not batch a milestone into one large commit.
- Milestones M1–M7 in `README.md` are the order of work. Finish one before
  starting the next.
- Tests alongside code, especially for the determinism invariant.
