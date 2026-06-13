package torrstor

import (
	"encoding/json"
	"sync/atomic"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/storage"
	"server/settings"
)

type Piece struct {
	Size     int64
	Accessed int64
	complete atomic.Bool
	storage.PieceImpl `json:"-"`
	Id                int        `json:"-"`
	mPiece            *MemPiece  `json:"-"`
	dPiece            *DiskPiece `json:"-"`
	cache             *Cache     `json:"-"`
}

type pieceJSON struct {
	Id        int   `json:"id,omitempty"`
	Size      int64 `json:"size"`
	Complete  bool  `json:"complete"`
	Accessed  int64 `json:"accessed"`
}

func (p *Piece) MarshalJSON() ([]byte, error) {
	return json.Marshal(pieceJSON{
		Id:       p.Id,
		Size:     atomic.LoadInt64(&p.Size),
		Complete: p.complete.Load(),
		Accessed: atomic.LoadInt64(&p.Accessed),
	})
}

func NewPiece(id int, cache *Cache) *Piece {
	p := &Piece{
		Id:    id,
		cache: cache,
	}
	if !settings.BTsets.UseDisk {
		p.mPiece = NewMemPiece(p)
	} else {
		p.dPiece = NewDiskPiece(p)
	}
	return p
}

func (p *Piece) WriteAt(b []byte, off int64) (n int, err error) {
	if !settings.BTsets.UseDisk {
		return p.mPiece.WriteAt(b, off)
	}
	return p.dPiece.WriteAt(b, off)
}

func (p *Piece) ReadAt(b []byte, off int64) (n int, err error) {
	if !settings.BTsets.UseDisk {
		return p.mPiece.ReadAt(b, off)
	}
	return p.dPiece.ReadAt(b, off)
}

func (p *Piece) MarkComplete() error {
	p.complete.Store(true)
	return nil
}

func (p *Piece) MarkNotComplete() error {
	p.complete.Store(false)
	return nil
}

func (p *Piece) Completion() storage.Completion {
	return storage.Completion{
		Complete: p.complete.Load(),
		Ok:       true,
	}
}

func (p *Piece) Release() {
	if !settings.BTsets.UseDisk {
		p.mPiece.Release()
	} else {
		p.dPiece.Release()
	}
	t := p.cache.getTorrent()
	if t == nil {
		return
	}
	if p.cache.isClosed.Load() {
		return
	}
	t.Piece(p.Id).SetPriority(torrent.PiecePriorityNone)
	t.Piece(p.Id).UpdateCompletion()
}