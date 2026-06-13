package state

import (
	torrentState "server/torr/state"
)

type ItemState struct {
	Id        int
	Size      int64
	Length    int64
	Completed bool
	Priority  int
}

type ReaderState struct {
	Start  int
	End    int
	Reader int
}

type CacheState struct {
	Capacity     int64
	Filled       int64
	PiecesLength int64
	PiecesCount  int
	Hash         string
	Torrent      *torrentState.TorrentStatus
	Pieces       map[int]ItemState
	Readers      []*ReaderState
}

func (cs *CacheState) ActiveReaders() int {
	if cs == nil {
		return 0
	}
	return len(cs.Readers)
}