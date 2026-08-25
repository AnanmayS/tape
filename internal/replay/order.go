package replay

import (
	"cmp"
	"strings"
)

// orderKey is the total ordering key for one stored record. Every field in it
// comes from bytes on disk or from a record's position in the file list, so two
// machines reading the same window build identical keys.
//
// The comparison order is:
//
//	exchange  ascending  exchange timestamp, in Unix nanoseconds
//	seqRank   ascending  0 for records with no sequence, 1 for records with one
//	sequence  ascending  the feed's sequence number
//	channel   ascending  byte-wise, e.g. "control" < "level2_batch" < "matches"
//	file      ascending  index of the file in the window's sorted file list
//	record    ascending  0-based ordinal of the record inside that file
//
// The last two fields together are the arrival index. They are derived from
// position, never from map iteration, goroutine scheduling or wall-clock time,
// and they are unique across a window — so the key is a strict total order and
// no tie is ever left for a sort to break arbitrarily.
type orderKey struct {
	exchange int64
	seqRank  uint8
	sequence uint64
	channel  string
	file     int
	record   int64
}

// compareKeys returns -1, 0 or 1. Zero is only possible for a key compared with
// itself: the arrival index is unique within a window.
//
// Every field is compared, never subtracted. Subtracting two int64 timestamps
// and taking the sign is the usual way to write this and it is wrong: two
// timestamps far enough apart overflow, and the overflowed difference has the
// wrong sign, which would silently invert the order of the two records.
func compareKeys(a, b orderKey) int {
	if c := cmp.Compare(a.exchange, b.exchange); c != 0 {
		return c
	}
	if c := cmp.Compare(a.seqRank, b.seqRank); c != 0 {
		return c
	}
	if c := cmp.Compare(a.sequence, b.sequence); c != 0 {
		return c
	}
	if c := strings.Compare(a.channel, b.channel); c != 0 {
		return c
	}
	if c := cmp.Compare(a.file, b.file); c != 0 {
		return c
	}
	return cmp.Compare(a.record, b.record)
}

// content copies only the ordering content of a key, leaving the arrival index
// zero. It is what a record with no ordering information of its own inherits.
func (k orderKey) content() orderKey {
	return orderKey{
		exchange: k.exchange,
		seqRank:  k.seqRank,
		sequence: k.sequence,
		channel:  k.channel,
	}
}
