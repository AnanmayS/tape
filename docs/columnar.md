# The columnar format: what it stores, and what it measured

Tape files come in two formats. v1 is a record log: a header, then every frame
written as it arrived. v2 is delta-encoded columnar batches, 5.2x smaller on a
real BTC-USD window, and it replays to the same bytes.

The code is `internal/colfmt`. The commands are `tape capture -format columnar`
and `tape stat`.

This note is the design and the numbers behind it. Every number here came from
running the thing; none is estimated.

## The decision that mattered: the raw frames stay

Two designs were on the table.

**A — columns beside the frames.** Delta-encode the structured fields into
columns, keep every raw frame verbatim in a column of its own, compress both.

**B — columns only.** Store the structured fields and rebuild each frame from
them on read. Roughly twice the ratio, since the frames are 91% of the file.

This is A, and the reason is not conservatism. Rebuilding a Coinbase frame
byte-exactly means reproducing its field order, its full key set — `trade_id`,
two order ids, fields this project does not decode — and its number formatting,
forever, across whatever the exchange changes without telling anyone. A
reconstruction that is 99.99% right is worse than useless here: it hands back
something the exchange never sent, and it looks completely healthy doing it.
The frame is the only artefact that can settle an argument about what arrived.
That is what capture is *for*, so trading it for a compression ratio would be
trading the project for a number.

So the columns are an index and the frames are the record. Replay decodes the
frames, exactly as it does for a v1 file — the v2 reader hands the layer above
it byte-identical v1 payloads, and everything columnar stops there. The columns
buy scans: which batches carry a gap, what time range a file covers, where a
sequence range lives, all without inflating a megabyte of JSON. On a live
window they cost **8.7% of the file**.

## Layout

```
file    = header | batch*
header  = magic[4] "TAPE" | version uint16 LE (=2) | reserved uint16 LE
batch   = bodyLen uint32 LE | body[bodyLen] | footer[36]
body    = block*
block   = id uint8 | codec uint8 | rawLen uvarint | encLen uvarint | data[encLen]
footer  = magic[4] "TAPB" | version uint16 LE | flags uint16 LE | rows uint32 LE
        | minRecv int64 LE | maxRecv int64 LE | bodyLen uint32 LE | crc32c uint32 LE
```

The header is v1's header with a different version byte. Each format's reader
refuses the other outright — `tapefile` reads v1 and nothing else,
`colfmt` reads v2 and nothing else — and `colfmt.OpenRecords` is the single
place that dispatches, on that byte alone. Replay calls it per file, so a
window whose files were written by different builds replays as one window.

The footer is fixed-size, last, and repeats the body length, so a scan reads
four bytes, skips the body and reads thirty-six more. `Scan` does exactly that.

**The CRC is load-bearing.** The failure mode of a delta-encoded column is not
a crash. One flipped byte shifts every value after it and produces prices that
are wrong and entirely plausible. `TestChecksumCatchesCorruption` flips every
byte of a real batch body in turn and requires each one to be caught — by the
checksum, or by a decoder that noticed the columns stopped lining up.

A batch is self-contained: every delta chain restarts at zero, so a batch
decodes without the ones before it and a file can be appended to after a crash.

## Columns

| column | encoding |
|---|---|
| kind | one byte per row |
| recv | zigzag varint delta against the previous row |
| exchange time | presence bitset + zigzag varint delta against the previous present value |
| sequence | presence bitset + zigzag varint delta |
| side | presence bitset + a second bitset for sell, and an exception list |
| price | presence bitset + scale byte + zigzag varint delta of the scaled integer |
| size | presence bitset + scale byte + zigzag varint of the scaled integer, **not** a delta |
| type | dictionary of the distinct message types, then one index per row |
| gap, reseed | the payload fields, per row of that kind |
| frames | the raw frame, length-prefixed, per message row |

Absence is a bit, never a sentinel: "no sequence" and "sequence 0" are different
facts and the ordering rules treat them differently.

### Prices are scaled integers, and are verified as such

Coinbase sends decimals as strings. A float64 cannot store them: it cannot
represent `80691.53` exactly, and it cannot remember whether the string was
`80691.5` or `80691.50`. So a price is parsed into an integer and a digit count.

The parse is accepted **only if re-rendering it reproduces the string character
for character**. Not a list of rules — the round trip itself. `0000.5`, `1e-8`,
`-0.0`, `+3`, twenty digits of integer part: each fails that test and is stored
as its own characters in an exception column instead. Nothing gets normalised
on the way to disk, so "unusual" and "corrupted" never become the same thing.

`event.Event` keeps the floats as a convenience view and gained `PriceText` and
`SizeText` for the exact strings. The canonical replay form still serialises the
floats — changing that would move the golden digest, which is a decision and not
a side effect of this milestone.

### Sizes are not delta-encoded, and that is measured

| column | delta | direct | delta, compressed | direct, compressed |
|---|---|---|---|---|
| recv | 8,870 | 21,060 | — | — |
| sequence | 1,430 | 6,540 | 1,152 | — |
| price | 1,452 | 4,287 | 843 | 1,261 |
| **size** | **1,974** | **1,736** | **1,748** | **1,366** |

Bytes, over 2,340 real BTC-USD frames. Prices walk, so their deltas are tiny.
Trade sizes do not: they are uncorrelated draws, and differencing an
uncorrelated series adds a bit of magnitude instead of removing one — 14% more
bytes raw, 28% more after compression. So sizes are stored outright.

## Compression: stdlib deflate, no new dependency

`klauspost/compress` was allowed for this milestone. It was measured and not
taken. Over the 1.5 MB of real frames in the golden fixture, single core:

| codec | bytes | ratio | time | throughput |
|---|---|---|---|---|
| deflate -1 | 330,207 | 4.56x | 6.5 ms | 233 MB/s |
| deflate -5 | 270,890 | 5.56x | 10.4 ms | 146 MB/s |
| deflate -6 | 260,623 | 5.78x | 10.6 ms | 143 MB/s |
| deflate -7 | 251,321 | 5.99x | 15.2 ms | 99 MB/s |
| **deflate -8** | **242,050** | **6.22x** | **27.6 ms** | **55 MB/s** |
| deflate -9 | 232,717 | 6.47x | 152.3 ms | 10 MB/s |
| zstd fastest | 278,354 | 5.41x | 6.9 ms | 218 MB/s |
| zstd default | 273,196 | 5.51x | 7.8 ms | 194 MB/s |
| zstd better | 250,855 | 6.00x | 10.7 ms | 141 MB/s |
| zstd best | 238,343 | 6.32x | 40.8 ms | 37 MB/s |

zstd's "better" level is 6.00x at 141 MB/s; deflate level 7 is 5.99x at 99 MB/s.
On this data the two are the same trade, and JSON frames are exactly what
deflate was designed for. A dependency that buys nothing measurable is a
dependency that does not go in.

Level 8 is the pick: everything up to it trades CPU for ratio smoothly and 9 is
the deflate cliff — 5.5x the CPU for 4% more ratio. 55 MB/s is about 180 times
what this feed produces.

Each column is compressed on its own and only if compressing it helped. Most of
the small ones stay stored: a two-hundred-byte bitset is not worth a deflate
header.

## Batching, and the number that caught it

The first live columnar capture came back at **4.29x**, against 5.7x on the
fixture, in 92 batches for 2,094 records.

The cause was `Flush`. Capture calls it on a one-second ticker to bound how long
a record sits in a buffer, and it was encoding the pending batch — so the
compression window was however many frames arrived in one second. On this feed
that is twenty-three records, which is not a compression window.

A batch now closes on **4096 rows, 4 MiB of frames, or a 30-second span of
receive timestamps** — all three read off the records, none off the clock. Two
consequences, both wanted:

- The bytes a session writes are a function of the frames that went into it, the
  same way a replay's bytes are. Nothing stored depends on when a ticker fired.
- `tape stat` on a v1 window computes what capturing it columnar *would* have
  produced. Checked against two concurrent live captures of the same feed:
  1,002,748 bytes computed, 1,003,688 actually written for eighteen more
  records, 0.09% apart.

What it costs: a hard kill loses the pending batch, at most 4096 records, 4 MiB,
or 30 seconds of feed. SIGINT and an ECS task stop both arrive as a clean
shutdown, which flushes it. `Flush` still pushes everything already encoded.

## Results

**Live, three minutes of BTC-USD, 9,189 records.** Two captures ran
concurrently against the same feed, one per format, so the comparison is over
the same market rather than over two different minutes.

| | |
|---|---|
| Raw frames (what NDJSON would store) | 5,130,188 bytes |
| v1 tape files | 5,249,687 bytes |
| v2 columnar | 1,002,748 bytes |
| **Ratio** | **5.12x against the frames, 5.24x against v1** |

The columnar figure is what `tape stat` computed for those exact records. The
concurrent columnar capture wrote 1,003,688 bytes for its own 9,207 — the same
answer from the other direction.

Per column, as a share of the columnar file:

| column | encoded | raw | share | ratio |
|---|---|---|---|---|
| frames | 914,906 | 5,148,505 | 91.2% | 5.63x |
| recv | 30,904 | 32,399 | 3.1% | 1.05x |
| exchange | 29,377 | 32,760 | 2.9% | 1.12x |
| size | 11,947 | 14,179 | 1.2% | 1.19x |
| sequence | 4,820 | 6,801 | 0.5% | 1.41x |
| price | 1,676 | 7,123 | 0.2% | 4.25x |
| type | 1,494 | 9,336 | 0.1% | 6.25x |
| presence bitsets (6) | 5,458 | 6,918 | 0.5% | 1.27x |
| scale bytes (2) | 1,080 | 12,348 | 0.1% | 11.43x |
| kind | 96 | 9,189 | 0.0% | 95.72x |
| reseed | 11 | 11 | 0.0% | 1.00x |
| framing | 979 | — | 0.1% | — |

The frames are 91.2% of the file and everything structured together is 8.7%.
That is the price of the index, and it is the honest argument for design A:
option B would have removed 91% of the bytes and all of the evidence.

The timestamp columns are the only structured ones that cost real bytes, and
both for the same reason — nanosecond resolution on a feed delivering tens of
messages a second leaves about 3.5 bytes of varint per row. The price column is
the delta encoding working as intended: 7,123 bytes of scaled integers into
1,676, on top of the 6,174 bytes of scale digits that compress 17x because they
never change.

**Read cost.** The same fixture window, replayed out of both formats:

| | raw | columnar |
|---|---|---|
| Iterator alone | 203,466 events/sec | 142,576 events/sec |
| Iterator + canonical NDJSON | 109,940 events/sec | 87,317 events/sec |

21% slower to replay, 5.2x smaller to store. The frames have to be inflated
before anything can decode them, and that is the whole of the difference. On
the live windows above, `tape verify` reads v1 at 111,170 events/sec and v2 at
91,308 — 1,783 times the wall-clock time the window took to record.

**Determinism.** The golden fixture, transcoded to columnar and replayed,
produces 2,197,803 bytes of canonical NDJSON and
`sha256 ee9576040361b07272db0cb6e614b02cef53dec1fcc772aeea1fa609b4fb7a21` — the
digest M3 wrote down and M4 carried through the storage move, unchanged. A
window with some files in each format produces it too.

## Why raw is still the default

`tape capture` writes v1 unless told otherwise.

Capture is the half of this project that cannot be re-run: a window that was
never recorded is gone. The columnar writer has now survived live captures, but
it has not been measured under the load M7 exists to apply, and it holds records
in memory that the v1 writer would already have handed to the kernel.

M7 measures both under backpressure. If columnar sustains the same rate, it
becomes the default then — on a number, which is the deal this project makes
with itself about every other decision too.
