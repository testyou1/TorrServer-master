package torr

import (
	"errors"
	"sync"
	"sync/atomic"
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
	downloadSpeed   atomic.Int64
	uploadSpeed     atomic.Int64
	bytesReadUsefulData atomic.Int64
	bytesWrittenData    atomic.Int64
	preloadSize    atomic.Int64
	preloadedBytes atomic.Int64
	durationSeconds atomic.Int64
	bitRate        atomic.Pointer[string]
	expiredTime time.Time
	closed      <-chan struct{}
	progressTicker  *time.Ticker
	progressRunning sync.Mutex
	raController   *RAController
	connController *ConnController
	watchDone      chan struct{}
	closeMu        sync.Mutex
	isClosed       atomic.Bool
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
	if tor, ok := bt.torrents[spec.InfoHash]; ok {
		bt.mu.Unlock()
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
	br := ""
	torr.bitRate.Store(&br)
	torr.watchDone = make(chan struct{})
	go torr.watch()
	bt.torrents[spec.InfoHash] = torr
	bt.mu.Unlock()
	return torr, nil
}

func (t *Torrent) watch() {
	defer close(t.watchDone)
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
	t.closeMu.Lock()
	defer t.closeMu.Unlock()

	if t.isClosed.Load() {
		return true
	}

	t.muTorrent.Lock()
	if t.Stat == state.TorrentClosed {
		t.muTorrent.Unlock()
		if t.watchDone != nil {
			select {
			case <-t.watchDone:
			default:
			}
		}
		t.isClosed.Store(true)
		return true
	}
	cache := t.cache
	t.muTorrent.Unlock()

	if settings.ReadOnly && cache != nil && cache.GetUseReaders() > 0 {
		return false
	}

	t.muTorrent.Lock()
	t.Stat = state.TorrentClosed
	hashVal := t.Hash()
	btServer := t.bt
	t.muTorrent.Unlock()

	if btServer != nil {
		btServer.mu.Lock()
		if _, ok := btServer.torrents[hashVal]; ok {
			delete(btServer.torrents, hashVal)
		}
		btServer.mu.Unlock()
	}

	t.drop()

	if t.watchDone != nil {
		select {
		case <-t.watchDone:
		default:
		}
	}

	t.isClosed.Store(true)
	return true
}

func (t *Torrent) expired() bool {
	t.muTorrent.Lock()
	cache := t.cache
	stat := t.Stat
	expTime := t.expiredTime
	t.muTorrent.Unlock()

	if cache == nil {
		return false
	}
	readers := cache.Readers()
	return readers == 0 && expTime.Before(time.Now()) && (stat == state.TorrentWorking || stat == state.TorrentClosed)
}

func (t *Torrent) GetCache() *torrstor.Cache {
	t.muTorrent.Lock()
	defer t.muTorrent.Unlock()
	return t.cache
}

func (t *Torrent) SetDownloadSpeed(speed float64) {
	t.downloadSpeed.Store(int64(speed * 1e6))
}

func (t *Torrent) GetDownloadSpeed() float64 {
	return float64(t.downloadSpeed.Load()) / 1e6
}

func (t *Torrent) SetUploadSpeed(speed float64) {
	t.uploadSpeed.Store(int64(speed * 1e6))
}

func (t *Torrent) GetUploadSpeed() float64 {
	return float64(t.uploadSpeed.Load()) / 1e6
}

func (t *Torrent) SetBytesReadUsefulData(b int64) {
	t.bytesReadUsefulData.Store(b)
}

func (t *Torrent) GetBytesReadUsefulData() int64 {
	return t.bytesReadUsefulData.Load()
}

func (t *Torrent) SetBytesWrittenData(b int64) {
	t.bytesWrittenData.Store(b)
}

func (t *Torrent) GetBytesWrittenData() int64 {
	return t.bytesWrittenData.Load()
}

func (t *Torrent) SetPreloadSize(b int64) {
	t.preloadSize.Store(b)
}

func (t *Torrent) GetPreloadSize() int64 {
	return t.preloadSize.Load()
}

func (t *Torrent) SetPreloadedBytes(b int64) {
	t.preloadedBytes.Store(b)
}

func (t *Torrent) GetPreloadedBytes() int64 {
	return t.preloadedBytes.Load()
}

func (t *Torrent) SetDurationSeconds(d float64) {
	t.durationSeconds.Store(int64(d * 1e6))
}

func (t *Torrent) GetDurationSeconds() float64 {
	return float64(t.durationSeconds.Load()) / 1e6
}

func (t *Torrent) SetBitRate(s string) {
	t.bitRate.Store(&s)
}

func (t *Torrent) GetBitRate() string {
	br := t.bitRate.Load()
	if br == nil {
		return ""
	}
	return *br
}