package torr

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"server/ffprobe"
	"server/log"
	"server/settings"
	"server/torr/state"
	utils2 "server/utils"
	"github.com/anacrolix/torrent"
)

type preloadConfig struct {
	bufSize      int64
	readahead    int64
	startEnd     int64
	endStart     int64
	endEnd       int64
	earlyMinFrac float64
	safetyRatio  float64
}

var preloadBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1<<20)
	},
}

func computePreloadConfig(
	pieceLength int64,
	fileLength int64,
	preloadSize int64,
	cacheCapacity int64,
	mediaBitrateStr string,
) preloadConfig {
	const (
		minBuf int64 = 32 << 10
		maxBuf int64 = 1 << 20
	)
	bufSize := pieceLength
	if bufSize < minBuf {
		bufSize = minBuf
	}
	if bufSize > maxBuf {
		bufSize = maxBuf
	}

	startEnd := pieceLength
	if startEnd < 8<<20 {
		startEnd = 8 << 20
	}

	rahPreload := cacheCapacity / 2
	rahByPiece := pieceLength * 4
	if rahByPiece < rahPreload {
		rahPreload = rahByPiece
	}

	endStart := fileLength - startEnd
	if endStart < 0 {
		endStart = 0
	}
	endEnd := fileLength

	readerStartEnd := preloadSize - startEnd
	if readerStartEnd < 0 {
		readerStartEnd = preloadSize
	}
	if readerStartEnd > fileLength {
		readerStartEnd = fileLength
	}

	earlyMinFrac := 1.0
	safetyRatio := 1.2
	if mediaBitrateStr != "" {
		var bitsPerSec float64
		if _, err := fmt.Sscanf(mediaBitrateStr, "%f", &bitsPerSec); err == nil && bitsPerSec > 0 {
			earlyMinFrac = 0.8
		}
	}

	return preloadConfig{
		bufSize:      bufSize,
		readahead:    rahPreload,
		startEnd:     readerStartEnd,
		endStart:     endStart,
		endEnd:       endEnd,
		earlyMinFrac: earlyMinFrac,
		safetyRatio:  safetyRatio,
	}
}

func preloadShouldStop(
	t *Torrent,
	cfg preloadConfig,
	mediaBitrateBS float64,
) (stop bool, reason string) {
	t.muTorrent.Lock()
	stat := t.Stat
	preloadedBytes := t.GetPreloadedBytes()
	preloadSize := t.GetPreloadSize()
	downloadSpeed := t.GetDownloadSpeed()
	t.muTorrent.Unlock()

	if stat != state.TorrentPreload {
		return true, "stat changed"
	}

	if preloadSize > 0 && mediaBitrateBS > 0 && cfg.earlyMinFrac < 1.0 {
		fraction := float64(preloadedBytes) / float64(preloadSize)
		if fraction >= cfg.earlyMinFrac {
			if downloadSpeed >= mediaBitrateBS*cfg.safetyRatio {
				return true, fmt.Sprintf(
					"early termination: %.0f%% loaded, speed %.0f > bitrate*%.1f (%.0f)",
					fraction*100, downloadSpeed, cfg.safetyRatio,
					mediaBitrateBS*cfg.safetyRatio,
				)
			}
		}
	}

	return false, ""
}

func (t *Torrent) Preload(index int, size int64) {
	if size <= 0 {
		return
	}
	t.SetPreloadSize(size)

	if t.Stat == state.TorrentGettingInfo {
		if !t.WaitInfo() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.muTorrent.Lock()
	if t.Stat != state.TorrentWorking {
		t.muTorrent.Unlock()
		return
	}
	t.Stat = state.TorrentPreload
	t.muTorrent.Unlock()

	defer func() {
		t.muTorrent.Lock()
		if t.Stat == state.TorrentPreload {
			t.Stat = state.TorrentWorking
		}
		t.muTorrent.Unlock()
		t.SetBitRate("")
		t.SetDurationSeconds(0)
	}()

	file := t.findFileIndex(index)
	if file == nil {
		file = t.Files()[0]
	}

	if size > file.Length() {
		size = file.Length()
	}

	if t.Info() == nil {
		return
	}

	timeout := time.Second * time.Duration(settings.BTsets.TorrentDisconnectTimeout)
	if timeout > time.Minute {
		timeout = time.Minute
	}

	logStopChan := make(chan struct{})
	defer close(logStopChan)

	var atomicDownloaded int64

	go func(stopChan <-chan struct{}) {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				t.muTorrent.Lock()
				stat := t.Stat
				t.muTorrent.Unlock()

				if stat != state.TorrentPreload {
					return
				}

				statStr := fmt.Sprint(file.Torrent().InfoHash().HexString(), " ",
					utils2.Format(float64(atomic.LoadInt64(&atomicDownloaded))), "/",
					utils2.Format(float64(size)), " Speed:",
					utils2.Format(t.GetDownloadSpeed()), " Peers:",
					t.Torrent.Stats().ActivePeers, "/",
					t.Torrent.Stats().TotalPeers, " [Seeds:",
					t.Torrent.Stats().ConnectedSeeders, "]")
				log.TLogln("Preload:", statStr)
				t.AddExpiredTime(timeout)
			case <-stopChan:
				return
			}
		}
	}(logStopChan)

	if ffprobe.Exists() {
		link := "http://127.0.0.1:" + settings.Port + "/play/" + t.Hash().HexString() + "/" + strconv.Itoa(index)
		if settings.Ssl {
			link = "https://127.0.0.1:" + settings.SslPort + "/play/" + t.Hash().HexString() + "/" + strconv.Itoa(index)
		}
		if data, err := ffprobe.ProbeUrl(link); err == nil {
			t.SetBitRate(data.Format.BitRate)
			t.SetDurationSeconds(data.Format.DurationSeconds)
		}
	}

	t.muTorrent.Lock()
	isClosed := t.Stat == state.TorrentClosed
	bitRateStr := t.GetBitRate()
	t.muTorrent.Unlock()

	if isClosed {
		log.TLogln("End preload: torrent closed")
		return
	}

	cacheCapacity := int64(0)
	if t.cache != nil {
		cacheCapacity = t.cache.GetCapacity()
	}

	cfg := computePreloadConfig(
		t.Info().PieceLength,
		file.Length(),
		size,
		cacheCapacity,
		bitRateStr,
	)

	mediaBitrateBS := float64(0)
	if bitRateStr != "" {
		var bitsPerSec float64
		if _, err := fmt.Sscanf(bitRateStr, "%f", &bitsPerSec); err == nil {
			mediaBitrateBS = bitsPerSec / 8
		}
	}

	var wg sync.WaitGroup

	if cfg.endStart > cfg.startEnd {
		wg.Add(1)
		go func() {
			defer wg.Done()

			t.muTorrent.Lock()
			shouldPreload := t.Stat == state.TorrentPreload
			t.muTorrent.Unlock()
			if !shouldPreload {
				return
			}

			readerEnd := file.NewReader()
			if readerEnd == nil {
				log.TLogln("Err preload: null end reader")
				return
			}
			defer readerEnd.Close()

			readerEnd.SetResponsive()
			readerEnd.SetReadahead(0)

			if _, err := readerEnd.Seek(cfg.endStart, io.SeekStart); err != nil {
				log.TLogln("Err preload end seek:", err)
				return
			}

			endBuf := preloadBufPool.Get().([]byte)
			endBuf = endBuf[:0]
			if int64(cap(endBuf)) < cfg.bufSize {
				endBuf = make([]byte, 0, cfg.bufSize)
			}
			defer preloadBufPool.Put(endBuf)

			offset := cfg.endStart
			for offset < cfg.endEnd {
				toRead := cfg.endEnd - offset
				if toRead > cfg.bufSize {
					toRead = cfg.bufSize
				}

				readBuf := endBuf[:toRead]
				n, err := readerEnd.Read(readBuf)
				if err != nil {
					if err != io.EOF {
						log.TLogln("Err preload end read:", err)
					}
					break
				}
				atomic.AddInt64(&atomicDownloaded, int64(n))
				offset += int64(n)

				if stop, reason := preloadShouldStop(t, cfg, mediaBitrateBS); stop {
					if reason != "stat changed" {
						log.TLogln("Preload end goroutine:", reason)
					}
					break
				}
			}
		}()
	}

	readerStart := file.NewReader()
	if readerStart == nil {
		log.TLogln("End preload: null reader")
		wg.Wait()
		return
	}
	defer readerStart.Close()

	readerStart.SetResponsive()
	readerStart.SetReadahead(cfg.readahead)

	buf := preloadBufPool.Get().([]byte)
	buf = buf[:0]
	if int64(cap(buf)) < cfg.bufSize {
		buf = make([]byte, 0, cfg.bufSize)
	}
	defer preloadBufPool.Put(buf)

	offset := int64(0)
	for offset < cfg.startEnd {
		if stop, reason := preloadShouldStop(t, cfg, mediaBitrateBS); stop {
			if reason != "stat changed" {
				log.TLogln("Preload main:", reason)
			} else {
				log.TLogln("Preload cancelled")
			}
			break
		}

		toRead := cfg.startEnd - offset
		if toRead > cfg.bufSize {
			toRead = cfg.bufSize
		}

		readBuf := buf[:toRead]
		n, err := readerStart.Read(readBuf)
		if err != nil {
			if err != io.EOF {
				log.TLogln("Error preload:", err)
			}
			break
		}
		atomic.AddInt64(&atomicDownloaded, int64(n))
		offset += int64(n)

		remaining := cfg.startEnd - (offset + cfg.bufSize)
		if cfg.readahead > 0 && remaining < cfg.readahead {
			readerStart.SetReadahead(0)
			cfg.readahead = 0
		}
	}

	wg.Wait()

	t.muTorrent.Lock()
	finalStat := t.Stat
	t.muTorrent.Unlock()

	if finalStat == state.TorrentPreload {
		log.TLogln("End preload:", file.Torrent().InfoHash().HexString(),
			"Peers:", t.Torrent.Stats().ActivePeers, "/",
			t.Torrent.Stats().TotalPeers, "[ Seeds:",
			t.Torrent.Stats().ConnectedSeeders, "]")
	}
}

func (t *Torrent) findFileIndex(index int) *torrent.File {
	st := t.Status()
	var stFile *state.TorrentFileStat
	for _, f := range st.FileStats {
		if index == f.Id {
			stFile = f
			break
		}
	}
	if stFile == nil {
		return nil
	}
	for _, file := range t.Files() {
		if file.Path() == stFile.Path {
			return file
		}
	}
	return nil
}