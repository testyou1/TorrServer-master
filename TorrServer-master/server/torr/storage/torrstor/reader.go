package torrstor

import (
	"bufio"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"server/log"
	"server/settings"
)

const (
	readerBufMin int64 = 128 * 1024
	readerBufMax int64 = 2 * 1024 * 1024
)

func calcReaderBufSize(pieceLength int64) int {
	if pieceLength <= 0 {
		return int(readerBufMin)
	}
	if pieceLength < readerBufMin {
		return int(readerBufMin)
	}
	if pieceLength > readerBufMax {
		return int(readerBufMax)
	}
	return int(pieceLength)
}

type rawReader struct {
	r *Reader
}

func (rr *rawReader) Read(p []byte) (int, error) {
	if rr.r.isClosed.Load() {
		return 0, io.EOF
	}
	if rr.r.file.Torrent() == nil || rr.r.file.Torrent().Info() == nil {
		return 0, io.EOF
	}
	n, err := rr.r.Reader.Read(p)
	rr.r.offset.Add(int64(n))
	rr.r.lastAccess.Store(time.Now().Unix())
	return n, err
}

type Reader struct {
	torrent.Reader
	offset     atomic.Int64
	readahead  atomic.Int64
	lastAccess atomic.Int64
	file       *torrent.File
	cache      *Cache
	isClosed   atomic.Bool
	isUse      atomic.Bool
	mu         sync.Mutex
	raw        rawReader
	bufReader  *bufio.Reader
}

func newReader(file *torrent.File, cache *Cache) *Reader {
	r := new(Reader)
	r.file = file
	r.Reader = file.NewReader()
	r.raw = rawReader{r: r}
	bufSize := calcReaderBufSize(cache.pieceLength)
	r.bufReader = bufio.NewReaderSize(&r.raw, bufSize)
	r.cache = cache
	r.isUse.Store(true)
	r.lastAccess.Store(time.Now().Unix())
	cache.activeReaderCount.Add(1)
	r.Reader.SetReadahead(0)
	cache.muReaders.Lock()
	cache.readers[r] = struct{}{}
	cache.muReaders.Unlock()
	return r
}

func (r *Reader) Seek(offset int64, whence int) (n int64, err error) {
	if r.isClosed.Load() {
		return 0, io.EOF
	}
	r.readerOn()
	n, err = r.Reader.Seek(offset, whence)
	r.offset.Store(n)
	r.lastAccess.Store(time.Now().Unix())
	r.bufReader.Reset(&r.raw)
	return
}

func (r *Reader) Read(p []byte) (n int, err error) {
	if r.isClosed.Load() {
		return 0, io.EOF
	}
	if r.file.Torrent() == nil || r.file.Torrent().Info() == nil {
		log.TLogln("Torrent closed and readed")
		return 0, io.EOF
	}
	r.readerOn()
	n, err = r.bufReader.Read(p)
	return
}

func (r *Reader) SetReadahead(length int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache != nil && length > r.cache.capacity {
		length = r.cache.capacity
	}
	r.readahead.Store(length)
	if r.isUse.Load() {
		r.Reader.SetReadahead(length)
	}
}

func (r *Reader) Offset() int64 {
	return r.offset.Load()
}

func (r *Reader) Readahead() int64 {
	return r.readahead.Load()
}

func (r *Reader) Close() {
	r.isClosed.Store(true)
	r.mu.Lock()
	wasUse := r.isUse.Load()
	r.isUse.Store(false)
	r.mu.Unlock()
	if wasUse {
		r.cache.activeReaderCount.Add(-1)
	}
	if len(r.file.Torrent().Files()) > 0 {
		r.Reader.Close()
	}
}

func (r *Reader) getPiecesRange() Range {
	startOff, endOff := r.getOffsetRange()
	return Range{r.getPieceNum(startOff), r.getPieceNum(endOff), r.file}
}

func (r *Reader) getReaderPiece() int {
	return r.getPieceNum(r.offset.Load())
}

func (r *Reader) getReaderRAHPiece() int {
	return r.getPieceNum(r.offset.Load() + r.readahead.Load())
}

func (r *Reader) getPieceNum(offset int64) int {
	pl := r.cache.pieceLength
	if pl <= 0 {
		return 0
	}
	return int((offset + r.file.Offset()) / pl)
}

func (r *Reader) getOffsetRange() (int64, int64) {
	prc := int64(settings.BTsets.ReaderReadAHead)
	readers := int64(r.cache.activeReaderCount.Load())
	if readers == 0 {
		readers = 1
	}
	off := r.offset.Load()
	capPerReader := r.cache.capacity / readers
	beginOffset := off - (capPerReader * (100 - prc) / 100)
	endOffset := off + (capPerReader * prc / 100)
	if beginOffset < 0 {
		beginOffset = 0
	}
	if endOffset > r.file.Length() {
		endOffset = r.file.Length()
	}
	return beginOffset, endOffset
}

func (r *Reader) checkReader() {
	lastAcc := r.lastAccess.Load()
	readerCount := int(r.cache.activeReaderCount.Load())
	if time.Now().Unix() > lastAcc+60 && readerCount > 1 {
		r.readerOff()
	} else {
		r.readerOn()
	}
}

func (r *Reader) readerOn() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.isUse.Load() {
		off := r.offset.Load()
		if pos, err := r.Reader.Seek(0, io.SeekCurrent); err == nil && pos == 0 {
			r.Reader.Seek(off, io.SeekStart)
		}
		ra := r.readahead.Load()
		if r.cache != nil && ra > r.cache.capacity {
			ra = r.cache.capacity
		}
		r.Reader.SetReadahead(ra)
		r.isUse.Store(true)
		r.cache.activeReaderCount.Add(1)
	}
}

func (r *Reader) readerOff() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isUse.Load() {
		r.Reader.SetReadahead(0)
		r.isUse.Store(false)
		r.cache.activeReaderCount.Add(-1)
		off := r.offset.Load()
		if off > 0 {
			r.Reader.Seek(0, io.SeekStart)
		}
	}
}

func (r *Reader) getUseReaders() int {
	return int(r.cache.activeReaderCount.Load())
}