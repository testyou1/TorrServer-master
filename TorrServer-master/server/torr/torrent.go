package torr

import (
	"errors"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"server/settings"
	"server/torr/state"
	"server/torr/storage/torrstor"
	"server/torr/utils"
)

type Torrent struct {
	Title    string
	Category string
	Poster   string
	Data     string
	*torrent.TorrentSpec
	Stat      state.TorrentStat
	Timestamp int64
	Size      int64
	*torrent.Torrent
	muTorrent sync.Mutex
	bt        *BTServer
	cache     *torrstor.Cache
	lastTimeSpeed   time.Time
	DownloadSpeed   float64
	UploadSpeed     float64
	BytesReadUsefulData int64
	BytesWrittenData    int64
	PreloadSize    int64
	PreloadedBytes int64
	DurationSeconds float64
	BitRate        string
	expiredTime time.Time
	closed      <-chan struct{}
	progressTicker  *time.Ticker
	progressRunning sync.Mutex
	raController   *RAController
	connController *ConnController
}

func NewTorrent(spec *torrent.TorrentSpec, bt *BTServer) (*Torrent, error) {
	if bt == nil || bt.client == nil {
		return nil, errors.New("BT client not connected")
	}
	switch settings.BTsets.RetrackersMode {
	case 1:
		spec.Trackers = append(spec.Trackers, [][]string{utils.GetDefTrackers()}...)
	case 2:
		spec.Trackers = nil
	case 3:
		spec.Trackers = [][]string{utils.GetDefTrackers()}
	}
	trackers := utils.GetTrackerFromFile()
	if len(trackers) > 0 {
		spec.Trackers = append(spec.Trackers, [][]string{trackers}...)
	}
	goTorrent, _, err := bt.client.AddTorrentSpec(spec)
	if err != nil {
		return nil, err
	}
	bt.mu.Lock()
	defer bt.mu.Unlock()
	if tor, ok := bt.torrents[spec.InfoHash]; ok {
		return tor, nil
	}
	timeout := time.Second * time.Duration(settings.BTsets.TorrentDisconnectTimeout)
	if timeout > time.Minute {
		timeout = time.Minute
	}
	torr := new(Torrent)
	torr.Torrent = goTorrent
	torr.Stat = state.TorrentAdded
	torr.lastTimeSpeed = time.Now()
	torr.bt = bt
	torr.closed = goTorrent.Closed()
	torr.TorrentSpec = spec
	torr.AddExpiredTime(timeout)
	torr.Timestamp = time.Now().Unix()
	go torr.watch()
	bt.torrents[spec.InfoHash] = torr
	return torr, nil
}

func (t *Torrent) watch() {
	ticker := time.NewTicker(time.Second)
	t.muTorrent.Lock()
	t.progressTicker = ticker
	t.muTorrent.Unlock()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if t.progressRunning.TryLock() {
				go func() {
					defer t.progressRunning.Unlock()
					t.progressEvent()
				}()
			}
		case <-t.closed:
			return
		}
	}
}

func (t *Torrent) drop() {
	t.muTorrent.Lock()
	defer t.muTorrent.Unlock()
	if t.Torrent != nil {
		t.Torrent.Drop()
		t.Torrent = nil
	}
}

func (t *Torrent) Close() bool {
	if t == nil {
		return false
	}
	t.muTorrent.Lock()
	if t.Stat == state.TorrentClosed {
		t.muTorrent.Unlock()
		return true
	}
	if settings.ReadOnly && t.cache != nil && t.cache.GetUseReaders() > 0 {
		t.muTorrent.Unlock()
		return false
	}
	t.Stat = state.TorrentClosed
	t.muTorrent.Unlock()

	if t.bt != nil {
		t.bt.mu.Lock()
		if _, ok := t.bt.torrents[t.Hash()]; ok {
			delete(t.bt.torrents, t.Hash())
		}
		t.bt.mu.Unlock()
	}
	t.drop()
	return true
}

func (t *Torrent) expired() bool {
	t.muTorrent.Lock()
	defer t.muTorrent.Unlock()
	if t.cache == nil {
		return false
	}
	stat := t.Stat
	expTime := t.expiredTime
	readers := t.cache.Readers()
	return readers == 0 && expTime.Before(time.Now()) && (stat == state.TorrentWorking || stat == state.TorrentClosed)
}

func (t *Torrent) GetCache() *torrstor.Cache {
	return t.cache
}