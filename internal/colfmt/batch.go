package colfmt

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/AnanmayS/tape/internal/event"
	"github.com/AnanmayS/tape/internal/tapefile"
)

// Column ids. They are written into the file, so they are append-only: a new
// column takes the next number and an old one is never reused for something
// else. A reader that meets an id it does not know skips the block, which is
// what lets a later build add a column without orphaning today's files.
const (
	colKind         uint8 = iota + 1 // one byte per row: the record type
	colRecv                          // varint delta of the local receive time, per row
	colExchangeMask                  // bitset: does this row carry an exchange timestamp
	colExchange                      // varint delta of the exchange timestamp, per present row
	colSequenceMask                  // bitset: does this row carry a sequence number
	colSequence                      // varint delta of the sequence, per present row
	colSideMask                      // bitset: does this row carry a side
	colSideSell                      // bitset: the side is sell rather than buy
	colSideText                      // sides that are neither, by row
	colPriceMask                     // bitset: does this row carry a price
	colPriceScale                    // one byte per present row: fractional digits
	colPriceUnits                    // varint delta of the scaled price, per present row
	colPriceText                     // prices that do not survive the decimal round trip
	colSizeMask                      // bitset: does this row carry a size
	colSizeScale                     // one byte per present row: fractional digits
	colSizeUnits                     // varint of the scaled size, per present row — not a delta
	colSizeText                      // sizes that do not survive the decimal round trip
	colType                          // dictionary of message types, then one index per row
	colGap                           // expected and got, per gap row
	colReseed                        // reason, per reseed row
	colFrames                        // the raw feed frame, per message row
)

// columnName is for the measurement harness: a per-column byte breakdown is
// only useful if the columns have names.
func columnName(id uint8) string {
	switch id {
	case colKind:
		return "kind"
	case colRecv:
		return "recv"
	case colExchangeMask:
		return "exchange.mask"
	case colExchange:
		return "exchange"
	case colSequenceMask:
		return "sequence.mask"
	case colSequence:
		return "sequence"
	case colSideMask:
		return "side.mask"
	case colSideSell:
		return "side.sell"
	case colSideText:
		return "side.text"
	case colPriceMask:
		return "price.mask"
	case colPriceScale:
		return "price.scale"
	case colPriceUnits:
		return "price"
	case colPriceText:
		return "price.text"
	case colSizeMask:
		return "size.mask"
	case colSizeScale:
		return "size.scale"
	case colSizeUnits:
		return "size"
	case colSizeText:
		return "size.text"
	case colType:
		return "type"
	case colGap:
		return "gap"
	case colReseed:
		return "reseed"
	case colFrames:
		return "frames"
	default:
		return fmt.Sprintf("unknown(%d)", id)
	}
}

// row is one record on its way into or out of a batch: what tapefile stores,
// plus the decoded fields the columns index it by.
//
// The decoded fields are a view of raw and never a replacement for it. A frame
// that will not parse still becomes a row — with raw intact and every index
// field empty — because a frame this process cannot read is exactly the frame a
// later reader will most want to see.
type row struct {
	kind tapefile.RecordType

	// at is the local clock: receive time for a message, the moment a gap or a
	// reseed was noticed. Every row has one.
	at time.Time

	// Message rows.
	raw         []byte
	msgType     string
	side        string
	exchange    time.Time
	hasExchange bool
	sequence    uint64
	hasSequence bool
	price       string // the wire decimal, verbatim
	size        string // the wire decimal, verbatim

	// Gap rows.
	expected, got uint64

	// Reseed rows.
	reason string
}

// messageRow builds a row from a stored message, decoding the frame for the
// index columns. A frame that will not decode still yields a row.
func messageRow(m tapefile.Message) row {
	r := row{kind: tapefile.RecordMessage, at: m.Recv, raw: m.Raw}
	e, err := event.Decode(m.Raw, m.Recv)
	if err != nil {
		return r
	}
	r.msgType = e.Type
	r.side = e.Side
	r.price, r.size = e.PriceText, e.SizeText
	r.sequence, r.hasSequence = e.Sequence, e.HasSequence
	if !e.ExchangeTime.IsZero() {
		r.exchange, r.hasExchange = e.ExchangeTime, true
	}
	return r
}

func gapRow(g tapefile.Gap) row {
	return row{kind: tapefile.RecordGap, at: g.At, expected: g.Expected, got: g.Got}
}

func reseedRow(r tapefile.Reseed) row {
	return row{kind: tapefile.RecordReseed, at: r.At, reason: r.Reason}
}

// payload renders the row back into the v1 record payload for its type. This is
// where the two formats meet: a v2 file hands the layer above it exactly the
// bytes the v1 file holding the same record would have handed it, so nothing
// above this line has to know which format it is reading.
func (r row) payload() []byte {
	switch r.kind {
	case tapefile.RecordMessage:
		return tapefile.EncodeMessage(tapefile.Message{Recv: r.at, Raw: r.raw})
	case tapefile.RecordGap:
		return tapefile.EncodeGap(tapefile.Gap{At: r.at, Expected: r.expected, Got: r.got})
	default:
		return tapefile.EncodeReseed(tapefile.Reseed{At: r.at, Reason: r.reason})
	}
}

// encodeBatch encodes rows into a complete batch: length prefix, body, footer.
//
// Every delta chain starts at zero here, which is what makes a batch decodable
// on its own. binary.AppendVarint is a zigzag varint: a delta of a few
// milliseconds or a few cents costs one or two bytes and a negative one costs
// no more than a positive one.
func encodeBatch(rows []row) ([]byte, error) {
	n := len(rows)
	if n == 0 {
		return nil, fmt.Errorf("colfmt: refusing to encode an empty batch")
	}
	if n > MaxRows {
		return nil, fmt.Errorf("%w: %d rows", ErrBatchTooBig, n)
	}

	kinds := make([]byte, n)
	exchangeMask, sequenceMask := newBitset(n), newBitset(n)
	sideMask, sideSell := newBitset(n), newBitset(n)
	priceMask, sizeMask := newBitset(n), newBitset(n)

	var recv, exchange, sequence []byte
	var priceScale, priceUnits, sizeScale, sizeUnits []byte
	var gaps, reseeds, frames []byte
	var sideText, priceText, sizeText exceptions
	var types dict

	var flags uint16
	var prevRecv, prevExchange, prevPrice int64
	var prevSequence uint64
	minRecv, maxRecv := int64(0), int64(0)

	for i, r := range rows {
		kinds[i] = byte(r.kind)

		at := r.at.UnixNano()
		if i == 0 || at < minRecv {
			minRecv = at
		}
		if i == 0 || at > maxRecv {
			maxRecv = at
		}
		recv = binary.AppendVarint(recv, at-prevRecv)
		prevRecv = at

		types.add(r.msgType)

		switch r.kind {
		case tapefile.RecordMessage:
			frames = binary.AppendUvarint(frames, uint64(len(r.raw)))
			frames = append(frames, r.raw...)
		case tapefile.RecordGap:
			flags |= FlagGap
			gaps = binary.AppendUvarint(gaps, r.expected)
			gaps = binary.AppendUvarint(gaps, r.got)
		case tapefile.RecordReseed:
			flags |= FlagReseed
			reseeds = binary.AppendUvarint(reseeds, uint64(len(r.reason)))
			reseeds = append(reseeds, r.reason...)
		default:
			return nil, fmt.Errorf("colfmt: unknown record type %s", r.kind)
		}

		if r.hasExchange {
			exchangeMask.set(i)
			t := r.exchange.UnixNano()
			exchange = binary.AppendVarint(exchange, t-prevExchange)
			prevExchange = t
		}
		if r.hasSequence {
			sequenceMask.set(i)
			// Signed: a reseed hands back a sequence lower than the last one,
			// and a subtraction that assumed otherwise would wrap.
			sequence = binary.AppendVarint(sequence, int64(r.sequence)-int64(prevSequence))
			prevSequence = r.sequence
		}
		if r.side != "" {
			sideMask.set(i)
			switch r.side {
			case "sell":
				sideSell.set(i)
			case "buy":
			default:
				sideText.add(i, r.side)
			}
		}
		if r.price != "" {
			priceMask.set(i)
			d, ok := parseDecimal(r.price)
			if !ok {
				// The delta chain still gets an entry, so that decoding does
				// not have to know which rows were exceptions to stay in step.
				priceText.add(i, r.price)
				d = decimal{units: prevPrice}
			}
			priceScale = append(priceScale, d.scale)
			priceUnits = binary.AppendVarint(priceUnits, d.units-prevPrice)
			prevPrice = d.units
		}
		if r.size != "" {
			sizeMask.set(i)
			d, ok := parseDecimal(r.size)
			if !ok {
				sizeText.add(i, r.size)
				d = decimal{}
			}
			sizeScale = append(sizeScale, d.scale)
			// Not a delta. Measured on a real BTC-USD window: trade sizes are
			// not autocorrelated, and delta-encoding them cost 14% more bytes
			// than storing them outright — 28% more after compression.
			sizeUnits = binary.AppendVarint(sizeUnits, d.units)
		}
	}

	var body []byte
	add := func(id uint8, b []byte) error {
		var err error
		body, err = appendBlock(body, id, b)
		return err
	}
	addMask := func(id uint8, b bitset) error {
		if !b.any() {
			return nil
		}
		return add(id, b)
	}
	for _, step := range []func() error{
		func() error { return add(colKind, kinds) },
		func() error { return add(colRecv, recv) },
		func() error { return addMask(colExchangeMask, exchangeMask) },
		func() error { return add(colExchange, exchange) },
		func() error { return addMask(colSequenceMask, sequenceMask) },
		func() error { return add(colSequence, sequence) },
		func() error { return addMask(colSideMask, sideMask) },
		func() error { return addMask(colSideSell, sideSell) },
		func() error { return add(colSideText, sideText.encode()) },
		func() error { return addMask(colPriceMask, priceMask) },
		func() error { return add(colPriceScale, priceScale) },
		func() error { return add(colPriceUnits, priceUnits) },
		func() error { return add(colPriceText, priceText.encode()) },
		func() error { return addMask(colSizeMask, sizeMask) },
		func() error { return add(colSizeScale, sizeScale) },
		func() error { return add(colSizeUnits, sizeUnits) },
		func() error { return add(colSizeText, sizeText.encode()) },
		func() error { return add(colType, types.encode()) },
		func() error { return add(colGap, gaps) },
		func() error { return add(colReseed, reseeds) },
		func() error { return add(colFrames, frames) },
	} {
		if err := step(); err != nil {
			return nil, err
		}
	}
	if len(body) > MaxBatchBody {
		return nil, fmt.Errorf("%w: body is %d bytes", ErrBatchTooBig, len(body))
	}

	f := Footer{
		Version: Version,
		Flags:   flags,
		Rows:    uint32(n),
		MinRecv: minRecv,
		MaxRecv: maxRecv,
		BodyLen: uint32(len(body)),
		BodyCRC: checksum(body),
	}
	out := make([]byte, 0, batchLenSize+len(body)+FooterSize)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(body)))
	out = append(out, body...)
	return append(out, f.encode()...), nil
}

// decodeBatch decodes a batch body against its footer.
func decodeBatch(body []byte, f Footer) ([]row, error) {
	blocks, err := splitBlocks(body)
	if err != nil {
		return nil, err
	}
	n := int(f.Rows)

	kinds := blocks[colKind]
	if len(kinds) != n {
		return nil, fmt.Errorf("%w: kind column has %d entries, footer says %d rows",
			ErrCorrupt, len(kinds), n)
	}
	exchangeMask := bitset(blocks[colExchangeMask])
	sequenceMask := bitset(blocks[colSequenceMask])
	sideMask, sideSell := bitset(blocks[colSideMask]), bitset(blocks[colSideSell])
	priceMask, sizeMask := bitset(blocks[colPriceMask]), bitset(blocks[colSizeMask])

	recv := &cursor{b: blocks[colRecv]}
	exchange := &cursor{b: blocks[colExchange]}
	sequence := &cursor{b: blocks[colSequence]}
	priceUnits := &cursor{b: blocks[colPriceUnits]}
	sizeUnits := &cursor{b: blocks[colSizeUnits]}
	gaps := &cursor{b: blocks[colGap]}
	reseeds := &cursor{b: blocks[colReseed]}
	frames := &cursor{b: blocks[colFrames]}
	priceScale, sizeScale := blocks[colPriceScale], blocks[colSizeScale]

	sideText, err := decodeExceptions(blocks[colSideText])
	if err != nil {
		return nil, err
	}
	priceText, err := decodeExceptions(blocks[colPriceText])
	if err != nil {
		return nil, err
	}
	sizeText, err := decodeExceptions(blocks[colSizeText])
	if err != nil {
		return nil, err
	}
	types, err := decodeDict(blocks[colType], n)
	if err != nil {
		return nil, err
	}

	rows := make([]row, n)
	var prevRecv, prevExchange, prevPrice int64
	var prevSequence uint64
	var priceIdx, sizeIdx int

	for i := 0; i < n; i++ {
		r := row{kind: tapefile.RecordType(kinds[i]), msgType: types[i]}

		prevRecv += recv.varint()
		r.at = time.Unix(0, prevRecv).UTC()

		switch r.kind {
		case tapefile.RecordMessage:
			r.raw = frames.bytes()
		case tapefile.RecordGap:
			r.expected, r.got = gaps.uvarint(), gaps.uvarint()
		case tapefile.RecordReseed:
			r.reason = string(reseeds.bytes())
		default:
			return nil, fmt.Errorf("%w: row %d has record type %d", ErrCorrupt, i, kinds[i])
		}

		if exchangeMask.get(i) {
			prevExchange += exchange.varint()
			r.exchange, r.hasExchange = time.Unix(0, prevExchange).UTC(), true
		}
		if sequenceMask.get(i) {
			prevSequence = uint64(int64(prevSequence) + sequence.varint())
			r.sequence, r.hasSequence = prevSequence, true
		}
		if sideMask.get(i) {
			if s, ok := sideText[i]; ok {
				r.side = s
			} else if sideSell.get(i) {
				r.side = "sell"
			} else {
				r.side = "buy"
			}
		}
		if priceMask.get(i) {
			if priceIdx >= len(priceScale) {
				return nil, fmt.Errorf("%w: price scale column ran out at row %d", ErrCorrupt, i)
			}
			prevPrice += priceUnits.varint()
			r.price = decimal{units: prevPrice, scale: priceScale[priceIdx]}.String()
			if s, ok := priceText[i]; ok {
				r.price = s
			}
			priceIdx++
		}
		if sizeMask.get(i) {
			if sizeIdx >= len(sizeScale) {
				return nil, fmt.Errorf("%w: size scale column ran out at row %d", ErrCorrupt, i)
			}
			r.size = decimal{units: sizeUnits.varint(), scale: sizeScale[sizeIdx]}.String()
			if s, ok := sizeText[i]; ok {
				r.size = s
			}
			sizeIdx++
		}
		rows[i] = r
	}

	// Every column must have been consumed exactly. A column that still has
	// bytes left is a column that was written against different rows than the
	// ones just decoded, and the values already handed back cannot be trusted
	// either — this is the check that turns a silent misdecode into an error.
	for _, col := range []struct {
		id uint8
		c  *cursor
	}{
		{colRecv, recv}, {colExchange, exchange}, {colSequence, sequence},
		{colPriceUnits, priceUnits}, {colSizeUnits, sizeUnits},
		{colGap, gaps}, {colReseed, reseeds}, {colFrames, frames},
	} {
		if err := col.c.done(); err != nil {
			return nil, fmt.Errorf("%w: %s column: %v", ErrCorrupt, columnName(col.id), err)
		}
	}
	if priceIdx != len(priceScale) || sizeIdx != len(sizeScale) {
		return nil, fmt.Errorf("%w: scale columns have %d/%d entries for %d/%d present values",
			ErrCorrupt, len(priceScale), len(sizeScale), priceIdx, sizeIdx)
	}
	return rows, nil
}

// exceptions is a sparse string column: the rows whose value could not go down
// the fast path, by row index. Sides that are neither buy nor sell and decimals
// that will not round-trip land here, so that an unexpected value is stored
// exactly rather than forced into a shape that would change it.
type exceptions struct {
	buf   []byte
	count int
	prev  int
}

func (e *exceptions) add(i int, s string) {
	e.buf = binary.AppendUvarint(e.buf, uint64(i-e.prev))
	e.prev = i
	e.buf = binary.AppendUvarint(e.buf, uint64(len(s)))
	e.buf = append(e.buf, s...)
	e.count++
}

func (e *exceptions) encode() []byte {
	if e.count == 0 {
		return nil
	}
	return append(binary.AppendUvarint(nil, uint64(e.count)), e.buf...)
}

func decodeExceptions(b []byte) (map[int]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	c := &cursor{b: b}
	// Each exception costs at least two bytes to encode, so a count larger than
	// the column is a corrupt count and not a large one. Checked before the
	// allocation it would otherwise size.
	n64 := c.uvarint()
	if c.err != nil || n64 > uint64(len(b)/2) {
		return nil, fmt.Errorf("%w: exception column claims %d entries in %d bytes",
			ErrCorrupt, n64, len(b))
	}
	n := int(n64)
	out := make(map[int]string, n)
	idx := 0
	for i := 0; i < n; i++ {
		idx += int(c.uvarint())
		out[idx] = string(c.bytes())
	}
	if err := c.done(); err != nil {
		return nil, fmt.Errorf("%w: exception column: %v", ErrCorrupt, err)
	}
	return out, nil
}

// dict is a dictionary column: the distinct strings, then one index per row.
// Message types repeat endlessly — a window is thousands of l2updates and
// matches — so the column is the handful of names once and a byte per row.
type dict struct {
	index map[string]uint64
	words []string
	rows  []uint64
}

func (d *dict) add(s string) {
	if d.index == nil {
		d.index = make(map[string]uint64)
	}
	i, ok := d.index[s]
	if !ok {
		i = uint64(len(d.words))
		d.index[s] = i
		d.words = append(d.words, s)
	}
	d.rows = append(d.rows, i)
}

func (d *dict) encode() []byte {
	if len(d.rows) == 0 {
		return nil
	}
	b := binary.AppendUvarint(nil, uint64(len(d.words)))
	for _, w := range d.words {
		b = binary.AppendUvarint(b, uint64(len(w)))
		b = append(b, w...)
	}
	for _, i := range d.rows {
		b = binary.AppendUvarint(b, i)
	}
	return b
}

func decodeDict(b []byte, rows int) ([]string, error) {
	out := make([]string, rows)
	if len(b) == 0 {
		return out, nil
	}
	c := &cursor{b: b}
	// A dictionary cannot hold more distinct words than there are rows to use
	// them, so a larger count is corruption rather than a large allocation.
	words64 := c.uvarint()
	if c.err != nil || words64 > uint64(rows) {
		return nil, fmt.Errorf("%w: dictionary claims %d words for %d rows", ErrCorrupt, words64, rows)
	}
	words := make([]string, int(words64))
	for i := range words {
		words[i] = string(c.bytes())
	}
	for i := 0; i < rows; i++ {
		j := c.uvarint()
		if j >= uint64(len(words)) {
			return nil, fmt.Errorf("%w: row %d points at dictionary word %d of %d",
				ErrCorrupt, i, j, len(words))
		}
		out[i] = words[j]
	}
	if err := c.done(); err != nil {
		return nil, fmt.Errorf("%w: type column: %v", ErrCorrupt, err)
	}
	return out, nil
}
