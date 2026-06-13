package torr

import (
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"server/settings"
	"server/torr/state"
	"server/torr/storage/torrstor"
)

func (t *Torrent) WaitInfo() bool {
	if t == nil || t.Torrent == nil {
		return false
	}
	tm := time.NewTimer(time.Minute + time.Second*time.Duration(settings.BTsets.TorrentDisconnectTimeout))
	defer tm.Stop()
	select {
	case <-t.Torrent.GotInfo():
		if t.bt != nil && t.bt.storage != nil {
			cache := t.bt.storage.GetCache(t.Hash())
			if cache != nil {
				t.muTorrent.Lock()
				t.cache = cache
				t.muTorrent.Unlock()
				cache.SetTorrent(t.Torrent)
			}
		}
		return true
	case <-t.closed:
		return false
	case <-tm.C:
		return false
	}
}

func (t *Torrent) GotInfo() bool {
	if t == nil {
		return false
	}
	t.muTorrent.Lock()
	stat := t.Stat
	t.muTorrent.Unlock()
	if stat == state.TorrentClosed {
		return false
	}
	if stat == state.TorrentPreload {
		return true
	}
	t.muTorrent.Lock()
	t.Stat = state.TorrentGettingInfo
	t.muTorrent.Unlock()

	if t.WaitInfo() {
		t.muTorrent.Lock()
		t.Stat = state.TorrentWorking
		t.muTorrent.Unlock()
		t.AddExpiredTime(time.Second * time.Duration(settings.BTsets.TorrentDisconnectTimeout))
		return true
	}
	t.Close()
	return false
}

func (t *Torrent) AddExpiredTime(duration time.Duration) {
	t.muTorrent.Lock()
	defer t.muTorrent.Unlock()
	newExpiredTime := time.Now().Add(duration)
	if t.expiredTime.Before(newExpiredTime) {
		t.expiredTime = newExpiredTime
	}
}

func (t *Torrent) Files() []*torrent.File {
	if t.Torrent != nil && t.Torrent.Info() != nil {
		return t.Torrent.Files()
	}
	return nil
}

func (t *Torrent) Hash() metainfo.Hash {
	if t.Torrent != nil {
		return t.Torrent.InfoHash()
	}
	if t.TorrentSpec != nil {
		return t.TorrentSpec.InfoHash
	}
	return [20]byte{}
}

func (t *Torrent) Length() int64 {
	if t.Info() == nil {
		return 0
	}
	return t.Torrent.Length()
}

func (t *Torrent) NewReader(file *torrent.File) *torrstor.Reader {
	t.muTorrent.Lock()
	stat := t.Stat
	cache := t.cache
	t.muTorrent.Unlock()
	if stat == state.TorrentClosed {
		return nil
	}
	if cache == nil {
		return nil
	}
	return cache.NewReader(file)
}

func (t *Torrent) CloseReader(reader *torrstor.Reader) {
	t.muTorrent.Lock()
	cache := t.cache
	t.muTorrent.Unlock()
	if cache != nil {
		cache.CloseReader(reader)
	}
	t.AddExpiredTime(time.Second * time.Duration(settings.BTsets.TorrentDisconnectTimeout))
}