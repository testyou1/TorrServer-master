package torrstor

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"server/log"
	"server/settings"
)

type DiskPiece struct {
	piece *Piece
	name  string
	mu    sync.RWMutex
}

func NewDiskPiece(p *Piece) *DiskPiece {
	name := filepath.Join(settings.BTsets.TorrentsSavePath, p.cache.hash.HexString(), strconv.Itoa(p.Id))
	ff, err := os.Stat(name)
	if err == nil {
		atomic.StoreInt64(&p.Size, ff.Size())
		p.complete.Store(ff.Size() == p.cache.pieceLength)
		atomic.StoreInt64(&p.Accessed, ff.ModTime().Unix())
	}
	return &DiskPiece{piece: p, name: name}
}

func (p *DiskPiece) WriteAt(b []byte, off int64) (n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ff, err := os.OpenFile(p.name, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		log.TLogln("Error open file:", err)
		return 0, err
	}
	defer ff.Close()
	n, err = ff.WriteAt(b, off)
	newSize := atomic.LoadInt64(&p.piece.Size) + int64(n)
	if newSize > p.piece.cache.pieceLength {
		newSize = p.piece.cache.pieceLength
	}
	atomic.StoreInt64(&p.piece.Size, newSize)
	atomic.StoreInt64(&p.piece.Accessed, time.Now().Unix())
	p.piece.cache.NotifyWrite(p.piece.Id, newSize)
	return
}

func (p *DiskPiece) ReadAt(b []byte, off int64) (n int, err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ff, err := os.OpenFile(p.name, os.O_RDONLY, 0o666)
	if os.IsNotExist(err) {
		return 0, io.EOF
	}
	if err != nil {
		log.TLogln("Error open file:", err)
		return 0, err
	}
	defer ff.Close()
	n, err = ff.ReadAt(b, off)
	pieceSize := atomic.LoadInt64(&p.piece.Size)
	atomic.StoreInt64(&p.piece.Accessed, time.Now().Unix())
	p.piece.cache.NotifyRead(p.piece.Id, pieceSize)
	if int64(len(b))+off >= pieceSize {
		go p.piece.cache.cleanPieces()
	}
	return n, err
}

func (p *DiskPiece) Release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt64(&p.piece.Size, 0)
	p.piece.complete.Store(false)
	os.Remove(p.name)
}