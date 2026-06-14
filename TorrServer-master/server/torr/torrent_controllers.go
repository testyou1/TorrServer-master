package torr

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/anacrolix/torrent"

	"server/log"
	"server/settings"
	cacheSt "server/torr/storage/state"
)

const defaultMediaBitrateBS = 1_250_000

const ajbSampleCount = 60

type speedRingBuffer struct {
	samples [ajbSampleCount]float64
	head    int
	count   int
}

func (b *speedRingBuffer) push(v float64) {
	b.samples[b.head] = v
	b.head = (b.head + 1) % ajbSampleCount
	if b.count < ajbSampleCount {
		b.count++
	}
}

func (b *speedRingBuffer) copyTo(dst []float64) int {
	n := b.count
	if n == 0 {
		return 0
	}
	if b.count < ajbSampleCount {
		copy(dst[:n], b.samples[:n])
	} else {
		tail := ajbSampleCount - b.head
		copy(dst[:tail], b.samples[b.head:])
		copy(dst[tail:], b.samples[:b.head])
	}
	return n
}

type sortableFloat64 struct {
	data []float64
	n    int
}

func (s sortableFloat64) Len() int           { return s.n }
func (s sortableFloat64) Less(i, j int) bool { return s.data[i] < s.data[j] }
func (s sortableFloat64) Swap(i, j int)      { s.data[i], s.data[j] = s.data[j], s.data[i] }

type AJBStats struct {
	Q10    float64
	MuL    float64
	SigmaL float64
}

type AdaptiveJitterBuffer struct {
	ring    speedRingBuffer
	sortBuf [ajbSampleCount]float64

	lastBytes   int64
	lastTime    time.Time
	initialized bool

	prevBstar       float64
	lastAppliedTime time.Time

	mu sync.Mutex
}

func newAdaptiveJitterBuffer() *AdaptiveJitterBuffer {
	return &AdaptiveJitterBuffer{}
}

func (ajb *AdaptiveJitterBuffer) pushSample(currentBytes int64, now time.Time) {
	if !ajb.initialized {
		ajb.lastBytes = currentBytes
		ajb.lastTime = now
		ajb.initialized = true
		return
	}

	dt := now.Sub(ajb.lastTime).Seconds()
	if dt < 0.5 {
		return
	}

	delta := float64(currentBytes - ajb.lastBytes)
	if delta < 0 {
		delta = 0
	}
	speed := delta / dt

	ajb.lastBytes = currentBytes
	ajb.lastTime = now

	ajb.ring.push(speed)
}

func (ajb *AdaptiveJitterBuffer) computeStats() (AJBStats, bool) {
	n := ajb.ring.copyTo(ajb.sortBuf[:])
	if n < 5 {
		return AJBStats{}, false
	}

	sf := sortableFloat64{data: ajb.sortBuf[:], n: n}
	sort.Sort(sf)

	h := 0.10 * float64(n-1)
	lo := int(math.Floor(h))
	hi := lo + 1
	if hi >= n {
		hi = n - 1
	}
	frac := h - math.Floor(h)
	q10 := ajb.sortBuf[lo]*(1-frac) + ajb.sortBuf[hi]*frac

	var sumLn, sumLn2 float64
	for i := 0; i < n; i++ {
		v := math.Log(ajb.sortBuf[i] + 1)
		sumLn += v
		sumLn2 += v * v
	}
	muL := sumLn / float64(n)
	variance := sumLn2/float64(n) - muL*muL
	if variance < 0 {
		variance = 0
	}
	sigmaL := math.Sqrt(variance)

	return AJBStats{Q10: q10, MuL: muL, SigmaL: sigmaL}, true
}

func (ajb *AdaptiveJitterBuffer) ComputeRA(
	currentBytes int64,
	now time.Time,
	mediaBitrateBS float64,
	cacheCapacity int64,
	pieceLength int64,
) (raBytes int64, stats AJBStats, hasStats bool) {
	ajb.mu.Lock()
	defer ajb.mu.Unlock()

	ajb.pushSample(currentBytes, now)

	stats, hasStats = ajb.computeStats()

	vEff := mediaBitrateBS
	if vEff <= 0 {
		vEff = defaultMediaBitrateBS
	}

	pLen := float64(pieceLength)
	if pLen <= 0 {
		pLen = 256 * 1024
	}

	bMin := 3.0
	if pLen/vEff > bMin {
		bMin = pLen / vEff
	}

	bMax := 30.0
	if cacheCapacity > 0 {
		candidate := 0.75 * float64(cacheCapacity) / vEff
		if candidate < bMax {
			bMax = candidate
		}
	}

	if bMin > bMax {
		bMin = bMax
	}

	bStar := bMin
	if hasStats {
		q10 := stats.Q10
		ratio := (vEff - q10) / vEff
		if ratio < 0 {
			ratio = 0
		}
		if ratio > 1 {
			ratio = 1
		}
		bStar = bMin + (bMax-bMin)*ratio
	}

	const hysteresisFrac = 0.20
	const minApplyInterval = 2 * time.Second

	shouldApply := ajb.prevBstar <= 0
	if !shouldApply {
		relChange := math.Abs(bStar-ajb.prevBstar) / ajb.prevBstar
		if relChange > hysteresisFrac && now.Sub(ajb.lastAppliedTime) >= minApplyInterval {
			shouldApply = true
		}
	}

	if !shouldApply {
		raBytes = int64(ajb.prevBstar * vEff)
		if raBytes <= 0 {
			raBytes = pieceLength
		}
		return raBytes, stats, hasStats
	}

	ajb.prevBstar = bStar
	ajb.lastAppliedTime = now

	raBytes = int64(bStar * vEff)
	if raBytes <= 0 {
		raBytes = pieceLength
	}
	return raBytes, stats, hasStats
}

type RAController struct {
	ajb          *AdaptiveJitterBuffer
	minRA        int64
	mediaBitrate int64
	muBitrate    sync.RWMutex
}

func newRAController(pieceLength int64) *RAController {
	return &RAController{
		ajb:   newAdaptiveJitterBuffer(),
		minRA: pieceLength,
	}
}

func (rac *RAController) SetMediaBitrate(bitrateStr string) {
	if bitrateStr == "" {
		return
	}
	var bitsPerSec float64
	if _, err := fmt.Sscanf(bitrateStr, "%f", &bitsPerSec); err != nil || bitsPerSec <= 0 {
		return
	}
	rac.muBitrate.Lock()
	rac.mediaBitrate = int64(bitsPerSec / 8)
	rac.muBitrate.Unlock()
}

func (rac *RAController) Compute(
	cacheCapacity int64,
	activeReaders int,
	currentDownloadedBytes int64,
	pieceLength int64,
	now time.Time,
) int64 {
	rac.muBitrate.RLock()
	mediaBitrate := rac.mediaBitrate
	rac.muBitrate.RUnlock()

	ra, _, _ := rac.ajb.ComputeRA(
		currentDownloadedBytes,
		now,
		float64(mediaBitrate),
		cacheCapacity,
		pieceLength,
	)

	if ra < rac.minRA {
		ra = rac.minRA
	}

	maxRA := int64(float64(cacheCapacity) * 0.75)
	if activeReaders > 1 {
		maxRA = maxRA / int64(activeReaders)
	}
	if maxRA < rac.minRA {
		maxRA = rac.minRA
	}
	if ra > maxRA {
		ra = maxRA
	}

	return ra
}

const (
	peerPruneInterval       = 10 * time.Second
	peerWarmupDuration      = 45 * time.Second
	peerAbsoluteFloor       = 5 * 1024
	peerSafetyFactor        = 1.5
	peerPruneHysteresis     = 0.5
	peerRestoreHysteresis   = 2.0
	peerPruneMinConns       = 5
	pruneBlockFillThreshold = 0.75
)

type ConnController struct {
	nMin        int
	nMax        int
	lastApplied int
	hysteresis  int

	lastPruneTime time.Time
	pruneActive   bool
	startTime     time.Time
}

func newConnController(connLimit int) *ConnController {
	if connLimit <= 0 {
		connLimit = 25
	}
	nMin := connLimit / 5
	if nMin < 5 {
		nMin = 5
	}
	hysteresis := (connLimit - nMin) / 10
	if hysteresis < 2 {
		hysteresis = 2
	}
	return &ConnController{
		nMin:          nMin,
		nMax:          connLimit,
		lastApplied:   connLimit,
		hysteresis:    hysteresis,
		startTime:     time.Now(),
		lastPruneTime: time.Now(),
	}
}

func (cc *ConnController) Compute(
	downloadSpeed float64,
	mediaBitrateBS float64,
	cacheCapacity int64,
	cacheFilled int64,
) int {
	if mediaBitrateBS <= 0 {
		mediaBitrateBS = defaultMediaBitrateBS
	}

	fillRatio := float64(0)
	if cacheCapacity > 0 {
		fillRatio = float64(cacheFilled) / float64(cacheCapacity)
		if fillRatio > 1.0 {
			fillRatio = 1.0
		}
	}

	speedRatio := 1.0
	if downloadSpeed > 0 {
		speedRatio = mediaBitrateBS / downloadSpeed
		if speedRatio < 1.0 {
			speedRatio = 1.0
		}
	} else {
		speedRatio = 2.0
	}

	urgency := (1.0 - fillRatio) * speedRatio
	if urgency < 0.0 {
		urgency = 0.0
	}
	if urgency > 1.0 {
		urgency = 1.0
	}

	target := cc.nMin + int(float64(cc.nMax-cc.nMin)*urgency)

	if cc.pruneActive && target > cc.lastApplied {
		return cc.lastApplied
	}

	return target
}

func (cc *ConnController) ShouldApply(target int) (int, bool) {
	diff := target - cc.lastApplied
	if diff < 0 {
		diff = -diff
	}
	if diff < cc.hysteresis {
		return cc.lastApplied, false
	}
	cc.lastApplied = target
	return target, true
}

func (cc *ConnController) ComputePruneTarget(
	downloadSpeed float64,
	mediaBitrateBS float64,
	activePeers int,
	activeReaders int,
	cacheCapacity int64,
	cacheFilled int64,
) (int, bool) {
	if time.Since(cc.startTime) < peerWarmupDuration {
		return cc.lastApplied, false
	}

	if time.Since(cc.lastPruneTime) < peerPruneInterval {
		return cc.lastApplied, false
	}

	if activePeers <= peerPruneMinConns {
		if cc.pruneActive && cc.lastApplied <= peerPruneMinConns {
			cc.pruneActive = false
		}
		return cc.lastApplied, false
	}

	if cacheCapacity > 0 {
		fillRatio := float64(cacheFilled) / float64(cacheCapacity)
		if fillRatio >= pruneBlockFillThreshold {
			if cc.pruneActive {
				cc.pruneActive = false
			}
			return cc.lastApplied, false
		}
	}

	if mediaBitrateBS <= 0 {
		mediaBitrateBS = defaultMediaBitrateBS
	}
	if activeReaders <= 0 {
		activeReaders = 1
	}

	targetThroughput := mediaBitrateBS * peerSafetyFactor * float64(activeReaders)
	vMin := targetThroughput / float64(activePeers) * peerPruneHysteresis

	if vMin < peerAbsoluteFloor {
		vMin = peerAbsoluteFloor
	}

	avgSpeedPerPeer := float64(0)
	if activePeers > 0 {
		avgSpeedPerPeer = downloadSpeed / float64(activePeers)
	}

	if avgSpeedPerPeer >= vMin*peerRestoreHysteresis {
		if cc.pruneActive {
			cc.pruneActive = false
			if cc.lastApplied < cc.nMax {
				prev := cc.lastApplied
				restored := prev + cc.hysteresis*2
				if restored > cc.nMax {
					restored = cc.nMax
				}
				cc.lastApplied = restored
				cc.lastPruneTime = time.Now()
				log.TLogln(fmt.Sprintf(
					"PeerPrune: avgSpeed=%.0f B/s >= restoreThreshold=%.0f B/s, restoring conns %d->%d",
					avgSpeedPerPeer, vMin*peerRestoreHysteresis, prev, restored,
				))
				return restored, true
			}
		}
		return cc.lastApplied, false
	}

	if avgSpeedPerPeer < vMin {
		prev := cc.lastApplied
		pruned := prev - prev/4
		if pruned < peerPruneMinConns {
			pruned = peerPruneMinConns
		}
		if pruned == prev {
			if cc.pruneActive && pruned <= peerPruneMinConns {
				cc.pruneActive = false
			}
			return cc.lastApplied, false
		}
		cc.pruneActive = true
		cc.lastApplied = pruned
		cc.lastPruneTime = time.Now()
		log.TLogln(fmt.Sprintf(
			"PeerPrune: avgSpeed=%.0f B/s < vMin=%.0f B/s, reducing conns %d->%d",
			avgSpeedPerPeer, vMin, prev, pruned,
		))
		return pruned, true
	}

	return cc.lastApplied, false
}

func (t *Torrent) ensureControllersInit() {
	if t.raController != nil {
		return
	}
	if t.cache == nil || t.Info() == nil {
		return
	}
	pieceLen := t.Info().PieceLength
	t.raController = newRAController(pieceLen)
	t.connController = newConnController(settings.BTsets.ConnectionsLimit)
}

func (t *Torrent) progressEvent() {
	if t.expired() {
		if t.TorrentSpec != nil {
			log.TLogln("Torrent close by timeout", t.TorrentSpec.InfoHash.HexString())
		}
		t.bt.RemoveTorrent(t.Hash())
		return
	}

	t.muTorrent.Lock()

	var cacheSnapshot *cacheSt.CacheState
	var torrentStats torrent.TorrentStats

	if t.Torrent != nil && t.Torrent.Info() != nil {
		torrentStats = t.Torrent.Stats()

		deltaDlBytes := torrentStats.BytesRead.Int64() - t.GetBytesReadUsefulData()
		deltaUpBytes := torrentStats.BytesWritten.Int64() - t.GetBytesWrittenData()
		deltaTime := time.Since(t.lastTimeSpeed).Seconds()

		if deltaTime > 0 {
			t.SetDownloadSpeed(float64(deltaDlBytes) / deltaTime)
			t.SetUploadSpeed(float64(deltaUpBytes) / deltaTime)
		} else {
			t.SetDownloadSpeed(0)
			t.SetUploadSpeed(0)
		}

		t.SetBytesReadUsefulData(torrentStats.BytesRead.Int64())
		t.SetBytesWrittenData(torrentStats.BytesWritten.Int64())

		if t.cache != nil {
			cacheSnapshot = t.cache.GetState()
			t.SetPreloadedBytes(cacheSnapshot.Filled)
		}
	} else {
		t.SetDownloadSpeed(0)
		t.SetUploadSpeed(0)
	}

	t.muTorrent.Unlock()

	t.lastTimeSpeed = time.Now()
	t.updateRA(cacheSnapshot, torrentStats)
	t.updateConnections(cacheSnapshot, torrentStats)
}

func (t *Torrent) updateRA(cacheSnapshot *cacheSt.CacheState, torrentStats torrent.TorrentStats) {
	t.muTorrent.Lock()
	hasInfo := t.Torrent != nil && t.Info() != nil
	var (
		cacheCapacity     int64
		activeReaders     int
		currentDownloaded int64
		bitRate           string
		pieceLength       int64
	)
	if hasInfo && t.cache != nil && cacheSnapshot != nil {
		cacheCapacity = cacheSnapshot.Capacity
		activeReaders = cacheSnapshot.ActiveReaders()
		currentDownloaded = torrentStats.BytesRead.Int64()
		bitRate = t.GetBitRate()
		pieceLength = t.Info().PieceLength
	}
	t.muTorrent.Unlock()

	if !hasInfo || cacheCapacity == 0 {
		return
	}

	t.ensureControllersInit()

	if t.raController == nil {
		return
	}

	if bitRate != "" {
		t.raController.SetMediaBitrate(bitRate)
	}

	adj := t.raController.Compute(
		cacheCapacity,
		activeReaders,
		currentDownloaded,
		pieceLength,
		time.Now(),
	)

	go t.cache.AdjustRA(adj)
}

func (t *Torrent) updateConnections(cacheSnapshot *cacheSt.CacheState, torrentStats torrent.TorrentStats) {
	t.muTorrent.Lock()
	hasInfo := t.Torrent != nil && t.Info() != nil
	var (
		downloadSpeed float64
		bitRateStr    string
		cacheCapacity int64
		cacheFilled   int64
		activeReaders int
		activePeers   int
	)
	if hasInfo && t.cache != nil && cacheSnapshot != nil {
		downloadSpeed = t.GetDownloadSpeed()
		bitRateStr = t.GetBitRate()
		cacheCapacity = cacheSnapshot.Capacity
		cacheFilled = cacheSnapshot.Filled
		activeReaders = cacheSnapshot.ActiveReaders()
		activePeers = torrentStats.ActivePeers
	}
	torrentRef := t.Torrent
	t.muTorrent.Unlock()

	if !hasInfo || activeReaders == 0 || torrentRef == nil {
		return
	}

	t.ensureControllersInit()

	if t.connController == nil {
		return
	}

	mediaBitrateBS := float64(0)
	if bitRateStr != "" {
		var bitsPerSec float64
		if _, err := fmt.Sscanf(bitRateStr, "%f", &bitsPerSec); err == nil && bitsPerSec > 0 {
			mediaBitrateBS = bitsPerSec / 8
		}
	}

	if pruned, ok := t.connController.ComputePruneTarget(
		downloadSpeed,
		mediaBitrateBS,
		activePeers,
		activeReaders,
		cacheCapacity,
		cacheFilled,
	); ok {
		torrentRef.SetMaxEstablishedConns(pruned)
		return
	}

	target := t.connController.Compute(
		downloadSpeed,
		mediaBitrateBS,
		cacheCapacity,
		cacheFilled,
	)

	if applied, ok := t.connController.ShouldApply(target); ok {
		torrentRef.SetMaxEstablishedConns(applied)
	}
}