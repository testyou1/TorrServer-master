package torrstor

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type MemPiece struct {
	piece  *Piece
	buffer []byte
	mu     sync.RWMutex
}

func NewMemPiece(p *Piece) *MemPiece {
	return &MemPiece{piece: p}
}

func (p *MemPiece) WriteAt(b []byte, off int64) (n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.buffer == nil {
		go p.piece.cache.cleanPieces()
		p.buffer = p.piece.cache.getBuffer()
	}
	n = copy(p.buffer[off:], b)
	newSize := atomic.LoadInt64(&p.piece.Size) + int64(n)
	if newSize > p.piece.cache.pieceLength {
		newSize = p.piece.cache.pieceLength
	}
	atomic.StoreInt64(&p.piece.Size, newSize)
	atomic.StoreInt64(&p.piece.Accessed, time.Now().Unix())
	p.piece.cache.NotifyWrite(p.piece.Id, newSize)
	return
}

func (p *MemPiece) ReadAt(b []byte, off int64) (n int, err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.buffer == nil || int(off) >= len(p.buffer) {
		return 0, io.EOF
	}
	size := len(b)
	if int(off)+size > len(p.buffer) {
		size = len(p.buffer) - int(off)
	}
	if size <= 0 {
		return 0, io.EOF
	}
	n = copy(b, p.buffer[int(off):int(off)+size])
	pieceSize := atomic.LoadInt64(&p.piece.Size)
	atomic.StoreInt64(&p.piece.Accessed, time.Now().Unix())
	p.piece.cache.NotifyRead(p.piece.Id, pieceSize)
	if int64(len(b))+off >= pieceSize {
		go p.piece.cache.cleanPieces()
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (p *MemPiece) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.buffer != nil {
		p.piece.cache.putBuffer(p.buffer)
		p.buffer = nil
	}
	atomic.StoreInt64(&p.piece.Size, 0)
	p.piece.complete.Store(false)
}