package torrstor

import (
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"server/log"
	"server/settings"
	"server/torr/storage/state"
	"server/torr/utils"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

const (
	prioNone        = 0
	prioNow         = 1
	prioNext        = 2
	prioReadahead   = 3
	prioHigh        = 4
	prioNormal      = 5
	setPrioThrottle = 500 * time.Millisecond
)

type Cache struct {
	storage.TorrentImpl
	activeReaderCount atomic.Int32
	storage           *Storage
	capacity          int64
	filled            int64
	hash              metainfo.Hash
	pieceLength       int64
	pieceCount        int
	pieces            map[int]*Piece
	mu                sync.RWMutex
	readers           map[*Reader]struct{}
	muReaders         sync.Mutex
	closedOnce        sync.Once
	isClosed          atomic.Bool
	isRemove          atomic.Bool
	muRemove          sync.Mutex
	torrent           *torrent.Torrent
	muTorrent         sync.RWMutex
	bufPool           sync.Pool
	clearPrioSignal   chan struct{}
	clearPrioDone     chan struct{}
	lastSetPrio       time.Time
	muLastSetPrio     sync.Mutex
	lirs              *LIRSStore
}

func NewCache(capacity int64, storage *Storage) *Cache {
	ret := &Cache{
		capacity:        capacity,
		pieces:          make(map[int]*Piece),
		storage:         storage,
		readers:         make(map[*Reader]struct{}),
		clearPrioSignal: make(chan struct{}, 1),
		clearPrioDone:   make(chan struct{}),
	}
	go ret.priorityCleaner()
	return ret
}

func (c *Cache) Init(info *metainfo.Info, hash metainfo.Hash) {
	log.TLogln("Create cache for:", info.Name, hash.HexString())
	if c.capacity == 0 {
		c.capacity = info.PieceLength * 4
	}
	c.pieceLength = info.PieceLength
	c.pieceCount = info.NumPieces()
	c.hash = hash
	pieceLen := c.pieceLength
	c.bufPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, pieceLen)
			return buf
		},
	}
	if settings.BTsets.UseDisk {
		name := filepath.Join(settings.BTsets.TorrentsSavePath, hash.HexString())
		err := os.MkdirAll(name, 0o777)
		if err != nil {
			log.TLogln("Error create dir:", err)
		}
	}
	c.mu.Lock()
	for i := 0; i < c.pieceCount; i++ {
		c.pieces[i] = NewPiece(i, c)
	}
	c.mu.Unlock()
	c.lirs = NewLIRSStore(c.pieceCount, c.capacity)
}

func (c *Cache) getBuffer() []byte {
	buf := c.bufPool.Get().([]byte)
	if int64(len(buf)) != c.pieceLength {
		buf = make([]byte, c.pieceLength)
	}
	return buf
}

func (c *Cache) putBuffer(buf []byte) {
	if int64(len(buf)) != c.pieceLength {
		return
	}
	clear(buf)
	c.bufPool.Put(buf)
}

func (c *Cache) SetTorrent(torr *torrent.Torrent) {
	c.muTorrent.Lock()
	c.torrent = torr
	c.muTorrent.Unlock()
}

func (c *Cache) getTorrent() *torrent.Torrent {
	c.muTorrent.RLock()
	t := c.torrent
	c.muTorrent.RUnlock()
	return t
}

func (c *Cache) Piece(m metainfo.Piece) storage.PieceImpl {
	c.mu.RLock()
	val, ok := c.pieces[m.Index()]
	c.mu.RUnlock()
	if ok {
		return val
	}
	return &PieceFake{}
}

func (c *Cache) Close() error {
	c.closedOnce.Do(func() {
		c.isClosed.Store(true)
		close(c.clearPrioDone)

		c.storage.mu.Lock()
		delete(c.storage.caches, c.hash)
		c.storage.mu.Unlock()

		t := c.getTorrent()
		if t != nil {
			log.TLogln("Close cache for:", t.Name(), c.hash)
		} else {
			log.TLogln("Close cache for:", c.hash)
		}

		if settings.BTsets.RemoveCacheOnDrop {
			name := filepath.Join(settings.BTsets.TorrentsSavePath, c.hash.HexString())
			if name != "" && name != "/" {
				c.mu.RLock()
				toRemove := make([]*Piece, 0, len(c.pieces))
				for _, v := range c.pieces {
					toRemove = append(toRemove, v)
				}
				c.mu.RUnlock()
				for _, v := range toRemove {
					if v.dPiece != nil {
						os.Remove(v.dPiece.name)
					}
				}
				os.Remove(name)
			}
		}

		c.muReaders.Lock()
		c.readers = nil
		c.muReaders.Unlock()

		utils.FreeOSMemGC()
	})
	return nil
}

func (c *Cache) removePiece(piece *Piece) {
	if c.isClosed.Load() {
		return
	}
	if atomic.LoadInt64(&piece.Size) > 0 && c.lirs != nil {
		c.lirs.RecordEviction(piece.Id, atomic.LoadInt64(&piece.Size))
	}
	piece.Release()
}

func (c *Cache) NotifyRead(idx int, pieceSize int64) {
	if c.lirs == nil {
		return
	}
	c.lirs.RecordAccess(idx, pieceSize, lirsNowNs())
}

func (c *Cache) NotifyWrite(idx int, pieceSize int64) {
	if c.lirs == nil {
		return
	}
	c.lirs.RecordWrite(idx, pieceSize, lirsNowNs())
}

func (c *Cache) AdjustRA(readahead int64) {
	if settings.BTsets.CacheSize == 0 {
		c.capacity = readahead * 3
	}
	c.muReaders.Lock()
	for r := range c.readers {
		r.SetReadahead(readahead)
	}
	c.muReaders.Unlock()
}

func (c *Cache) GetState() *state.CacheState {
	cState := new(state.CacheState)
	piecesState := make(map[int]state.ItemState)
	var fill int64

	t := c.getTorrent()

	c.mu.RLock()
	for _, p := range c.pieces {
		sz := atomic.LoadInt64(&p.Size)
		if sz > 0 {
			fill += sz
			var prio int
			if t != nil {
				prio = int(t.PieceState(p.Id).Priority)
			}
			piecesState[p.Id] = state.ItemState{
				Id:        p.Id,
				Size:      sz,
				Length:    c.pieceLength,
				Completed: p.complete.Load(),
				Priority:  prio,
			}
		}
	}
	c.mu.RUnlock()

	readersState := make([]*state.ReaderState, 0)
	c.muReaders.Lock()
	for r := range c.readers {
		rng := r.getPiecesRange()
		pc := r.getReaderPiece()
		readersState = append(readersState, &state.ReaderState{
			Start:  rng.Start,
			End:    rng.End,
			Reader: pc,
		})
	}
	c.muReaders.Unlock()

	atomic.StoreInt64(&c.filled, fill)
	cState.Capacity = c.capacity
	cState.PiecesLength = c.pieceLength
	cState.PiecesCount = c.pieceCount
	cState.Hash = c.hash.HexString()
	cState.Filled = fill
	cState.Pieces = piecesState
	cState.Readers = readersState
	return cState
}

func (c *Cache) getQuickSnapshot() (filled int64, readerCount int) {
	var f int64
	c.mu.RLock()
	for _, p := range c.pieces {
		sz := atomic.LoadInt64(&p.Size)
		if sz > 0 {
			f += sz
		}
	}
	c.mu.RUnlock()
	rc := int(c.activeReaderCount.Load())
	return f, rc
}

func (c *Cache) cleanPieces() {
	if c.isClosed.Load() {
		return
	}
	if c.isRemove.Load() {
		return
	}
	if !c.muRemove.TryLock() {
		return
	}
	defer c.muRemove.Unlock()
	c.isRemove.Store(true)
	defer c.isRemove.Store(false)

	remPieces := c.getRemPieces()
	filled := atomic.LoadInt64(&c.filled)
	if filled > c.capacity {
		rems := (filled-c.capacity)/c.pieceLength + 1
		for _, p := range remPieces {
			c.removePiece(p)
			rems--
			if rems <= 0 {
				return
			}
		}
	}
}

type readerSnapshot struct {
	pos int
}

func (c *Cache) snapshotReadersLocked() []readerSnapshot {
	snaps := make([]readerSnapshot, 0, len(c.readers))
	for r := range c.readers {
		if r.isUse {
			snaps = append(snaps, readerSnapshot{pos: r.getReaderPiece()})
		}
	}
	return snaps
}

func evictionWeight(pieceID int, readers []readerSnapshot, backWindowPieces int) int {
	if len(readers) == 0 {
		return 0
	}
	bestWeight := math.MaxInt
	for _, r := range readers {
		readerPos := r.pos
		behindProtectStart := readerPos - backWindowPieces
		var w int
		if pieceID < behindProtectStart {
			w = (readerPos - pieceID) * 2
		} else if pieceID < readerPos {
			w = readerPos - pieceID
		} else {
			w = -(pieceID - readerPos)
		}
		if w < bestWeight {
			bestWeight = w
		}
	}
	return bestWeight
}

func (c *Cache) getRemPieces() []*Piece {
	c.muReaders.Lock()
	readers := make([]*Reader, 0, len(c.readers))
	for r := range c.readers {
		readers = append(readers, r)
	}
	activeSnaps := c.snapshotReadersLocked()
	c.muReaders.Unlock()

	ranges := make([]Range, 0, len(readers))
	for _, r := range readers {
		r.checkReader()
		if r.isUse {
			ranges = append(ranges, r.getPiecesRange())
		}
	}
	ranges = mergeRange(ranges)

	piecesRemove := make([]*Piece, 0)
	var fill int64

	c.mu.RLock()
	for id, p := range c.pieces {
		sz := atomic.LoadInt64(&p.Size)
		if sz > 0 {
			fill += sz
		}
		if len(ranges) > 0 {
			if !inRanges(ranges, id) {
				if sz > 0 && !c.isIdInFileBE(ranges, id) {
					piecesRemove = append(piecesRemove, p)
				}
			}
		} else {
			if sz > 0 && !c.isIdInFileBE(ranges, id) {
				piecesRemove = append(piecesRemove, p)
			}
		}
	}
	c.mu.RUnlock()

	atomic.StoreInt64(&c.filled, fill)

	c.requestClearPriority()

	now := time.Now()
	c.muLastSetPrio.Lock()
	shouldSetPrio := now.Sub(c.lastSetPrio) >= setPrioThrottle
	if shouldSetPrio {
		c.lastSetPrio = now
	}
	c.muLastSetPrio.Unlock()
	if shouldSetPrio {
		c.setLoadPriority(ranges)
	}

	backWindowPieces := 0
	if c.pieceLength > 0 {
		backWindowPieces = int(30 * 1024 * 1024 / c.pieceLength)
	}
	if backWindowPieces < 3 {
		backWindowPieces = 3
	}

	if c.lirs != nil && len(activeSnaps) > 0 {
		c.lirs.SortCandidates(piecesRemove, lirsNowNs())
	} else if len(activeSnaps) == 0 {
		sort.Slice(piecesRemove, func(i, j int) bool {
			return atomic.LoadInt64(&piecesRemove[i].Accessed) < atomic.LoadInt64(&piecesRemove[j].Accessed)
		})
	} else {
		sort.Slice(piecesRemove, func(i, j int) bool {
			wi := evictionWeight(piecesRemove[i].Id, activeSnaps, backWindowPieces)
			wj := evictionWeight(piecesRemove[j].Id, activeSnaps, backWindowPieces)
			if wi != wj {
				return wi > wj
			}
			return atomic.LoadInt64(&piecesRemove[i].Accessed) < atomic.LoadInt64(&piecesRemove[j].Accessed)
		})
	}
	return piecesRemove
}

type priorityZones struct {
	nowPiece  int
	nextPiece int
	rahEnd    int
	highEnd   int
	normalEnd int
}

func computePriorityZones(
	readerPos int,
	rahPos int,
	rangeEnd int,
	readahead int64,
	pieceLength int64,
) priorityZones {
	z := priorityZones{
		nowPiece:  readerPos,
		nextPiece: readerPos + 1,
		rahEnd:    rahPos,
	}
	rahPieces := int64(0)
	if pieceLength > 0 {
		rahPieces = readahead / pieceLength
	}
	highZoneLen := rahPieces / 4
	if highZoneLen < 1 {
		highZoneLen = 1
	}
	const maxHighZone = 32
	if highZoneLen > maxHighZone {
		highZoneLen = maxHighZone
	}
	z.highEnd = rahPos + int(highZoneLen)
	z.normalEnd = rangeEnd
	if z.nextPiece > rangeEnd {
		z.nextPiece = rangeEnd
	}
	if z.rahEnd > rangeEnd {
		z.rahEnd = rangeEnd
	}
	if z.highEnd > rangeEnd {
		z.highEnd = rangeEnd
	}
	return z
}

func getPieceTargetPriority(pieceIdx int, z priorityZones) int {
	switch {
	case pieceIdx == z.nowPiece:
		return prioNow
	case pieceIdx == z.nextPiece:
		return prioNext
	case pieceIdx > z.nextPiece && pieceIdx <= z.rahEnd:
		return prioReadahead
	case pieceIdx > z.rahEnd && pieceIdx <= z.highEnd:
		return prioHigh
	case pieceIdx > z.highEnd && pieceIdx <= z.normalEnd:
		return prioNormal
	default:
		return prioNone
	}
}

func (c *Cache) applyPiecePriority(idx int, targetPrio int) {
	t := c.getTorrent()
	if t == nil {
		return
	}
	switch targetPrio {
	case prioNone:
		if t.PieceState(idx).Priority != torrent.PiecePriorityNone {
			t.Piece(idx).SetPriority(torrent.PiecePriorityNone)
		}
	case prioNow:
		if t.PieceState(idx).Priority != torrent.PiecePriorityNow {
			t.Piece(idx).SetPriority(torrent.PiecePriorityNow)
		}
	case prioNext:
		if t.PieceState(idx).Priority != torrent.PiecePriorityNext {
			t.Piece(idx).SetPriority(torrent.PiecePriorityNext)
		}
	case prioReadahead:
		if t.PieceState(idx).Priority != torrent.PiecePriorityReadahead {
			t.Piece(idx).SetPriority(torrent.PiecePriorityReadahead)
		}
	case prioHigh:
		if t.PieceState(idx).Priority != torrent.PiecePriorityHigh {
			t.Piece(idx).SetPriority(torrent.PiecePriorityHigh)
		}
	case prioNormal:
		if t.PieceState(idx).Priority != torrent.PiecePriorityNormal {
			t.Piece(idx).SetPriority(torrent.PiecePriorityNormal)
		}
	}
}

func (c *Cache) setLoadPriority(ranges []Range) {
	t := c.getTorrent()
	if t == nil {
		return
	}
	c.mu.RLock()
	piecesNil := c.pieces == nil
	c.mu.RUnlock()
	if piecesNil {
		return
	}

	type readerInfo struct {
		readerPos   int
		rahPos      int
		rangeEnd    int
		readahead   int64
		pieceLength int64
	}

	c.muReaders.Lock()
	activeCount := int(c.activeReaderCount.Load())
	if activeCount == 0 {
		c.muReaders.Unlock()
		return
	}
	readerInfos := make([]readerInfo, 0, activeCount)
	for r := range c.readers {
		if !r.isUse {
			continue
		}
		if c.isIdInFileBE(ranges, r.getReaderPiece()) {
			continue
		}
		rng := r.getPiecesRange()
		readerInfos = append(readerInfos, readerInfo{
			readerPos:   r.getReaderPiece(),
			rahPos:      r.getReaderRAHPiece(),
			rangeEnd:    rng.End,
			readahead:   r.Readahead(),
			pieceLength: c.pieceLength,
		})
	}
	c.muReaders.Unlock()

	if len(readerInfos) == 0 {
		return
	}

	connLimit := settings.BTsets.ConnectionsLimit
	if connLimit <= 0 {
		connLimit = 25
	}
	countPerReader := connLimit / activeCount
	if countPerReader < 4 {
		countPerReader = 4
	}

	targetPriorities := make(map[int]int, countPerReader*len(readerInfos))
	for _, ri := range readerInfos {
		zones := computePriorityZones(
			ri.readerPos,
			ri.rahPos,
			ri.rangeEnd,
			ri.readahead,
			ri.pieceLength,
		)
		limit := 0
		for i := ri.readerPos; i <= ri.rangeEnd && limit < countPerReader; i++ {
			c.mu.RLock()
			p, ok := c.pieces[i]
			c.mu.RUnlock()
			if !ok {
				continue
			}
			if p.complete.Load() {
				if existing, alreadySet := targetPriorities[i]; !alreadySet || existing > prioNone {
					targetPriorities[i] = prioNone
				}
				continue
			}
			newPrio := getPieceTargetPriority(i, zones)
			if newPrio == prioNone {
				continue
			}
			if existing, alreadySet := targetPriorities[i]; !alreadySet || newPrio < existing {
				targetPriorities[i] = newPrio
			}
			limit++
		}
	}

	for idx, targetPrio := range targetPriorities {
		c.applyPiecePriority(idx, targetPrio)
	}
}

func (c *Cache) isIdInFileBE(ranges []Range, id int) bool {
	FileRangeNotDelete := int64(c.pieceLength)
	if FileRangeNotDelete < 8<<20 {
		FileRangeNotDelete = 8 << 20
	}
	for _, rng := range ranges {
		ss := int(rng.File.Offset() / c.pieceLength)
		se := int((rng.File.Offset() + FileRangeNotDelete) / c.pieceLength)
		es := int((rng.File.Offset() + rng.File.Length() - FileRangeNotDelete) / c.pieceLength)
		ee := int((rng.File.Offset() + rng.File.Length()) / c.pieceLength)
		if id >= ss && id < se || id > es && id <= ee {
			return true
		}
	}
	return false
}

func (c *Cache) NewReader(file *torrent.File) *Reader {
	return newReader(file, c)
}

func (c *Cache) GetUseReaders() int {
	if c == nil {
		return 0
	}
	return int(c.activeReaderCount.Load())
}

func (c *Cache) Readers() int {
	if c == nil {
		return 0
	}
	c.muReaders.Lock()
	defer c.muReaders.Unlock()
	if c.readers == nil {
		return 0
	}
	return len(c.readers)
}

func (c *Cache) CloseReader(r *Reader) {
	r.cache.muReaders.Lock()
	r.Close()
	delete(r.cache.readers, r)
	r.cache.muReaders.Unlock()
	c.requestClearPriority()
}

func (c *Cache) requestClearPriority() {
	select {
	case c.clearPrioSignal <- struct{}{}:
	default:
	}
}

func (c *Cache) priorityCleaner() {
	const debounceDelay = 200 * time.Millisecond
	var timer *time.Timer
	for {
		select {
		case <-c.clearPrioSignal:
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(debounceDelay, func() {
				c.doClearPriority()
			})
		case <-c.clearPrioDone:
			if timer != nil {
				timer.Stop()
			}
			return
		}
	}
}

func (c *Cache) doClearPriority() {
	t := c.getTorrent()
	if t == nil {
		return
	}

	ranges := make([]Range, 0)
	c.muReaders.Lock()
	for r := range c.readers {
		r.checkReader()
		if r.isUse {
			ranges = append(ranges, r.getPiecesRange())
		}
	}
	c.muReaders.Unlock()
	ranges = mergeRange(ranges)

	c.mu.RLock()
	ids := make([]int, 0, len(c.pieces))
	for id := range c.pieces {
		ids = append(ids, id)
	}
	c.mu.RUnlock()

	for _, id := range ids {
		if len(ranges) > 0 {
			if !inRanges(ranges, id) {
				if t.PieceState(id).Priority != torrent.PiecePriorityNone {
					t.Piece(id).SetPriority(torrent.PiecePriorityNone)
				}
			}
		} else {
			if t.PieceState(id).Priority != torrent.PiecePriorityNone {
				t.Piece(id).SetPriority(torrent.PiecePriorityNone)
			}
		}
	}
}

func (c *Cache) GetCapacity() int64 {
	if c == nil {
		return 0
	}
	return c.capacity
}