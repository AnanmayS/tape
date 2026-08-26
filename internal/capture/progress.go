package capture

import "time"

// Progress is what a running session looks like at one instant.
//
// It exists because a status display has to read counters that belong to the
// writer goroutine, and reading them from anywhere else would be a data race —
// the counters are deliberately unsynchronised, because synchronising them
// would put a lock on the path every message takes.
//
// So the sample is taken by the goroutine that owns the counters, on a ticker
// beside the flush ticker it already has, and handed over as a value. That
// costs the write path nothing at all: not a lock, not an atomic, not an
// allocation, not a branch per message. Four times a second the writer copies a
// dozen integers between two records it was about to write anyway. The 49,800
// messages a second in README.md is measured with none of this compiled out,
// and it is the same number with -live as without because -live does no work
// per message.
//
// Everything in here is counted rather than derived. Rates belong to whoever is
// drawing, because a rate is a difference between two of these and this is one.
type Progress struct {
	// At is when the sample was taken, and Started when the session began.
	At      time.Time
	Started time.Time

	// Messages counts data frames received; Records counts everything written,
	// including gap and reseed records.
	Messages int64
	Records  int64

	Bytes     int64
	Rotations int
	Reseeds   int64

	// Gaps is the count that decides whether this window is worth anything. A
	// display that shows one number prominently shows this one.
	Gaps int64

	StaleMessages  int64
	DecodeErrors   int64
	ExchangeErrors int64

	// Dropped counts frames the backpressure policy discarded. Under the block
	// policy — which is the only one `tape capture` offers — it is always zero.
	Dropped int64

	// QueueDepth is the reader-to-writer queue right now and QueueCapacity is
	// the ceiling it is measured against. The pair is the backpressure signal:
	// a depth that sits near the ceiling is a writer that is not keeping up.
	QueueDepth    int
	QueueCapacity int
	MaxQueueDepth int

	// File is the tape file this session is writing to. FilePending says the
	// file does not exist yet: the columnar writer holds a batch in memory
	// until it closes, so for the first few thousand records the path is where
	// the data is going rather than where it already is. Reporting the path
	// without the flag would claim bytes on disk that are not there.
	File        string
	FilePending bool

	// WriteLatency is the session's per-record write distribution so far.
	WriteLatency Latency
}

// Elapsed is how long the session has been running at the moment of the sample.
func (p Progress) Elapsed() time.Duration { return p.At.Sub(p.Started) }

// MessagesPerSecond is the average receive rate over the whole session. The
// instantaneous rate is a difference between two samples and belongs to the
// caller holding both.
func (p Progress) MessagesPerSecond() float64 {
	d := p.Elapsed().Seconds()
	if d <= 0 {
		return 0
	}
	return float64(p.Messages) / d
}

// DefaultProgressInterval is how often a session samples itself when a Progress
// channel is configured without one. Four a second is fast enough that a rate
// looks live and slow enough that the terminal is never the bottleneck.
const DefaultProgressInterval = 250 * time.Millisecond

// Progress returns the summary as a final sample, so that whoever was drawing
// the running session can draw the finished one with the same code. The
// authoritative end-of-session counts are the summary's, and this is them.
func (s Summary) Progress() Progress {
	return Progress{
		At:             s.Ended,
		Started:        s.Started,
		Messages:       s.Messages,
		Records:        s.Records,
		Bytes:          s.Bytes,
		Rotations:      s.Rotations,
		Reseeds:        s.Reseeds,
		Gaps:           s.Gaps,
		StaleMessages:  s.StaleMessages,
		DecodeErrors:   s.DecodeErrors,
		ExchangeErrors: s.ExchangeErrors,
		Dropped:        s.Dropped,
		MaxQueueDepth:  s.MaxQueueDepth,
		WriteLatency:   s.WriteLatency,
	}
}

// sample takes one reading and offers it to out.
//
// The send never blocks. A display that is slow to redraw — a terminal over a
// slow link, a process stopped in the foreground — must not be able to stall
// the goroutine writing market data to disk, so a sample nobody was ready for
// is dropped. Dropping one costs a display frame; blocking would cost the
// queue, and then the socket, and then the session.
func (s *sink) sample(out chan<- Progress, q *queue, capacity int, at time.Time) {
	p := Progress{
		At:             at,
		Started:        s.sum.Started,
		Messages:       s.sum.Messages,
		Reseeds:        s.sum.Reseeds,
		Gaps:           s.sum.Gaps,
		StaleMessages:  s.sum.StaleMessages,
		DecodeErrors:   s.sum.DecodeErrors,
		ExchangeErrors: s.sum.ExchangeErrors,
		MaxQueueDepth:  s.sum.MaxQueueDepth,
		Dropped:        q.dropCount(),
		QueueDepth:     q.depth(),
		QueueCapacity:  capacity,
		File:           s.w.Path(),
		WriteLatency:   s.hist.summary(),
	}
	if p.File == "" {
		p.File, p.FilePending = s.w.PathFor(at), true
	}
	st := s.w.Stats()
	p.Records, p.Bytes, p.Rotations = st.Records, st.Bytes, st.Rotations

	select {
	case out <- p:
	default:
	}
}
