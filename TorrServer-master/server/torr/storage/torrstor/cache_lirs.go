package torrstor

import (
	"sort"
	"sync"
	"time"
)

const (
	lirsLIRFraction  = 0.85
	lirsHNRBufferSize = 256
	lirsThrottleNs   = int64(1_000_000_000)
)

type lirsState uint8

const (
	lirsHIR lirsState = 0
	lirsLIR lirsState = 1
)

type lirsMeta struct {
	tLast int64
	tPrev int64
	state lirsState
}

type hnrRing struct {
	buf  [lirsHNRBufferSize]int32
	head int
	n    int
}

func (r *hnrRing) push(idx int) {
	r.buf[r.head] = int32(idx)
	r.head = (r.head + 1) % lirsHNRBufferSize
	if r.n < lirsHNRBufferSize {
		r.n++
	}
}

func (r *hnrRing) contains(idx int) bool {
	id32 := int32(idx)
	for i := 0; i < r.n; i++ {
		if r.buf[i] == id32 {
			return true
		}
	}
	return false
}

type LIRSStore struct {
	mu          sync.RWMutex
	meta        []lirsMeta
	pieceCount  int
	lirCapacity int64
	lirFilled   int64
	hnr         hnrRing
	lirWindow   int
}

var scoredPool = sync.Pool{
	New: func() interface{} {
		s := make(scoredPieceSlice, 0, 64)
		return &s
	},
}

func NewLIRSStore(pieceCount int, cacheCapacity int64) *LIRSStore {
	lw := pieceCount / 8
	if lw < 64 {
		lw = 64
	}
	if lw > pieceCount {
		lw = pieceCount
	}
	return &LIRSStore{
		meta:        make([]lirsMeta, pieceCount),
		pieceCount:  pieceCount,
		lirCapacity: int64(lirsLIRFraction * float64(cacheCapacity)),
		lirWindow:   lw,
	}
}

func (s *LIRSStore) UpdateCapacity(cacheCapacity int64) {
	s.mu.Lock()
	s.lirCapacity = int64(lirsLIRFraction * float64(cacheCapacity))
	s.mu.Unlock()
}

func (s *LIRSStore) tryPromoteToLIR(m *lirsMeta, pieceSize int64) {
	if m.state == lirsHIR && m.tPrev > 0 && m.tLast > 0 {
		irdNs := m.tLast - m.tPrev
		if irdNs > 0 {
			irdSec := irdNs / 1_000_000_000
			if int(irdSec) < s.lirWindow && s.lirFilled+pieceSize <= s.lirCapacity {
				m.state = lirsLIR
				s.lirFilled += pieceSize
			}
		}
	}
}

func (s *LIRSStore) RecordAccess(idx int, pieceSize int64, nowNs int64) {
	if idx < 0 || idx >= s.pieceCount {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := &s.meta[idx]
	if m.tLast == 0 {
		m.tLast = nowNs
		m.state = lirsHIR
		return
	}
	if nowNs-m.tLast < lirsThrottleNs {
		return
	}
	m.tPrev = m.tLast
	m.tLast = nowNs
	s.tryPromoteToLIR(m, pieceSize)
}

func (s *LIRSStore) RecordWrite(idx int, pieceSize int64, nowNs int64) {
	if idx < 0 || idx >= s.pieceCount {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := &s.meta[idx]
	wasHNR := s.hnr.contains(idx)
	if m.tLast == 0 {
		m.tLast = nowNs
		m.tPrev = 0
		m.state = lirsHIR
		return
	}
	if nowNs-m.tLast < lirsThrottleNs {
		return
	}
	m.tPrev = m.tLast
	m.tLast = nowNs
	if wasHNR {
		s.tryPromoteToLIR(m, pieceSize)
	}
}

func (s *LIRSStore) RecordEviction(idx int, pieceSize int64) {
	if idx < 0 || idx >= s.pieceCount {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := &s.meta[idx]
	if m.state == lirsLIR {
		s.lirFilled -= pieceSize
		if s.lirFilled < 0 {
			s.lirFilled = 0
		}
	}
	m.state = lirsHIR
	m.tLast = 0
	m.tPrev = 0
	s.hnr.push(idx)
}

type scoredPiece struct {
	p     *Piece
	score float64
}

type scoredPieceSlice []scoredPiece

func (s scoredPieceSlice) Len() int           { return len(s) }
func (s scoredPieceSlice) Less(i, j int) bool { return s[i].score > s[j].score }
func (s scoredPieceSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *LIRSStore) SortCandidates(candidates []*Piece, nowNs int64) {
	n := len(candidates)
	if n == 0 {
		return
	}
	lirWindowNs := int64(s.lirWindow) * 1_000_000_000

	var maxRecencyNs int64 = 1
	s.mu.RLock()
	for _, p := range candidates {
		if p.Id < 0 || p.Id >= s.pieceCount {
			continue
		}
		m := &s.meta[p.Id]
		if m.tLast > 0 {
			rec := nowNs - m.tLast
			if rec > maxRecencyNs {
				maxRecencyNs = rec
			}
		}
	}

	scoredPtr := scoredPool.Get().(*scoredPieceSlice)
	scored := (*scoredPtr)[:0]
	if cap(scored) < n {
		scored = make(scoredPieceSlice, n)
	} else {
		scored = scored[:n]
	}

	for i, p := range candidates {
		if p.Id < 0 || p.Id >= s.pieceCount {
			scored[i] = scoredPiece{p: p, score: 0}
			continue
		}
		m := &s.meta[p.Id]
		var irdNorm float64
		if m.tPrev == 0 || m.tLast == 0 {
			irdNorm = 1.0
		} else {
			irdNs := m.tLast - m.tPrev
			if irdNs <= 0 {
				irdNs = 0
			}
			if lirWindowNs > 0 {
				irdNorm = float64(irdNs) / float64(lirWindowNs)
			} else {
				irdNorm = 1.0
			}
			if irdNorm > 1.0 {
				irdNorm = 1.0
			}
		}
		var recencyNorm float64
		if m.tLast == 0 {
			recencyNorm = 1.0
		} else {
			rec := nowNs - m.tLast
			if rec < 0 {
				rec = 0
			}
			recencyNorm = float64(rec) / float64(maxRecencyNs)
		}
		score := irdNorm * recencyNorm
		if m.state == lirsLIR {
			score *= 0.1
		}
		scored[i] = scoredPiece{p: p, score: score}
	}
	s.mu.RUnlock()

	sort.Sort(scored)
	for i := range candidates {
		candidates[i] = scored[i].p
	}

	*scoredPtr = scored[:0]
	scoredPool.Put(scoredPtr)
}

func lirsNowNs() int64 {
	return time.Now().UnixNano()
}