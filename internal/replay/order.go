package replay

import "strings"

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
func compareKeys(a, b orderKey) int {
	switch {
	case a.exchange != b.exchange:
		return sign64(a.exchange - b.exchange)
	case a.seqRank != b.seqRank:
		return signInt(int(a.seqRank) - int(b.seqRank))
	case a.sequence != b.sequence:
		if a.sequence < b.sequence {
			return -1
		}
		return 1
	case a.channel != b.channel:
		return strings.Compare(a.channel, b.channel)
	case a.file != b.file:
		return signInt(a.file - b.file)
	case a.record != b.record:
		return sign64(a.record - b.record)
	default:
		return 0
	}
}

// sign64 avoids the overflow that a bare subtraction of two int64 timestamps
// can produce.
func sign64(d int64) int {
	switch {
	case d < 0:
		return -1
	case d > 0:
		return 1
	default:
		return 0
	}
}

func signInt(d int) int {
	switch {
	case d < 0:
		return -1
	case d > 0:
		return 1
	default:
		return 0
	}
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
