package capture

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/colfmt"
	"github.com/AnanmayS/tape/internal/feed"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// syntheticFeed returns two identical feeds, so the same session can be
// captured twice in two formats and the results compared.
func syntheticFeed() *feed.Synthetic {
	return &feed.Synthetic{
		ProductID: "BTC-USD",
		Mode:      feed.SeqContiguous,
		StartSeq:  1,
		Count:     500,
		Now:       stepClock(time.Date(2026, 8, 25, 4, 0, 0, 0, time.UTC), time.Second),
	}
}

// captureOnce runs one session and returns the summary and the records on disk,
// read back through the format dispatcher.
func captureOnce(t *testing.T, format Format) (Summary, []storedRecord, int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sum, err := Run(ctx, syntheticFeed(), Config{
		Root:          t.TempDir(),
		Format:        format,
		Window:        time.Minute,
		FlushInterval: time.Hour, // Close flushes; no ticker needed here.
		Log:           quietLogger(),
	})
	if err != nil {
		t.Fatalf("Run(%s): %v", format, err)
	}

	var out []storedRecord
	var bytesOnDisk int64
	for _, p := range sum.Files {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		bytesOnDisk += fi.Size()

		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		rd, err := colfmt.OpenRecords(f)
		if err != nil {
			t.Fatalf("OpenRecords %s: %v", p, err)
		}
		for {
			typ, payload, err := rd.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				rd.Close()
				t.Fatalf("read %s: %v", p, err)
			}
			out = append(out, storedRecord{typ: typ, payload: payload})
		}
		rd.Close()
	}
	return sum, out, bytesOnDisk
}

type storedRecord struct {
	typ     tapefile.RecordType
	payload []byte
}

// TestFormatsCaptureTheSameSession runs one synthetic session through each
// format and requires the stored records to be identical, byte for byte. The
// format is a storage decision; if it were visible in what comes back out it
// would be a decision about the data.
func TestFormatsCaptureTheSameSession(t *testing.T) {
	rawSum, raw, rawBytes := captureOnce(t, FormatRaw)
	colSum, col, colBytes := captureOnce(t, FormatColumnar)

	if rawSum.Messages != colSum.Messages || rawSum.Records != colSum.Records {
		t.Fatalf("summaries differ: raw %d/%d messages/records, columnar %d/%d",
			rawSum.Messages, rawSum.Records, colSum.Messages, colSum.Records)
	}
	if len(raw) != len(col) {
		t.Fatalf("raw stored %d records, columnar stored %d", len(raw), len(col))
	}
	if len(raw) != int(rawSum.Records) {
		t.Fatalf("read back %d records, the summary counted %d", len(raw), rawSum.Records)
	}
	for i := range raw {
		if raw[i].typ != col[i].typ || !bytes.Equal(raw[i].payload, col[i].payload) {
			t.Fatalf("record %d differs between formats:\n raw %s %q\n col %s %q",
				i, raw[i].typ, raw[i].payload, col[i].typ, col[i].payload)
		}
	}
	if len(rawSum.Files) != len(colSum.Files) {
		t.Fatalf("raw wrote %d files, columnar wrote %d", len(rawSum.Files), len(colSum.Files))
	}
	if colBytes >= rawBytes {
		t.Errorf("columnar session is %d bytes against raw's %d", colBytes, rawBytes)
	}
	t.Logf("%d records: raw %d bytes, columnar %d bytes, %.2fx",
		len(raw), rawBytes, colBytes, float64(rawBytes)/float64(colBytes))
}

// TestColumnarStatsMatchDisk: the counters a session reports have to be the
// bytes and records actually written, batching or no batching.
func TestColumnarStatsMatchDisk(t *testing.T) {
	sum, recs, onDisk := captureOnce(t, FormatColumnar)
	if sum.Bytes != onDisk {
		t.Errorf("summary says %d bytes, %d on disk", sum.Bytes, onDisk)
	}
	if int64(len(recs)) != sum.Records {
		t.Errorf("summary says %d records, %d on disk", sum.Records, len(recs))
	}
	if sum.Format != FormatColumnar {
		t.Errorf("summary format = %q", sum.Format)
	}
}

// TestUnknownFormatIsRefused: a format nobody implements must fail at the start
// of a session, not by writing a file nothing can read.
func TestUnknownFormatIsRefused(t *testing.T) {
	_, err := Run(context.Background(), syntheticFeed(), Config{
		Root:   t.TempDir(),
		Format: Format("parquet"),
		Log:    quietLogger(),
	})
	if err == nil {
		t.Fatal("expected an error for an unknown format")
	}
}
