package torr

import (
	"context"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"net/url"
	"server/proxy"
	"sync"
	"time"

	"github.com/anacrolix/publicip"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/wlynxg/anet"
	"golang.org/x/time/rate"
	"server/settings"
	"server/torr/storage/torrstor"
	"server/torr/utils"
	"server/version"
)

type BTServer struct {
	config          *torrent.ClientConfig
	client          *torrent.Client
	storage         *torrstor.Storage
	torrents        map[metainfo.Hash]*Torrent
	mu              sync.Mutex
	uploadLimiter   *rate.Limiter
	uploadPacerStop chan struct{}
	uploadPacerWg   sync.WaitGroup
}

var privateIPBlocks []*net.IPNet

func init() {
	for _, cidr := range []string{
		"127.0.0.0/8",
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"169.254.0.0/16",
		"::1/128",
		"fe80::/10",
		"fc00::/7",
	} {
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Errorf("parse error on %q: %v", cidr, err))
		}
		privateIPBlocks = append(privateIPBlocks, block)
	}
}

func NewBTS() *BTServer {
	bts := new(BTServer)
	bts.torrents = make(map[metainfo.Hash]*Torrent)
	return bts
}

func (bt *BTServer) Connect() error {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	var err error
	bt.configure(context.TODO())
	bt.client, err = torrent.NewClient(bt.config)
	bt.torrents = make(map[metainfo.Hash]*Torrent)
	InitApiHelper(bt)
	proxy.Start()
	if !settings.BTsets.DisableUpload && settings.BTsets.UploadRateLimit >= 0 {
		bt.uploadPacerStop = make(chan struct{})
		bt.uploadPacerWg.Add(1)
		go bt.uploadPacer()
	}
	return err
}

func (bt *BTServer) Disconnect() {
	bt.mu.Lock()
	pacerStop := bt.uploadPacerStop
	bt.uploadPacerStop = nil
	bt.mu.Unlock()

	if pacerStop != nil {
		close(pacerStop)
		bt.uploadPacerWg.Wait()
	}

	bt.mu.Lock()
	if bt.client != nil {
		bt.client.Close()
		bt.client = nil
		utils.FreeOSMemGC()
	}
	bt.mu.Unlock()
	proxy.Stop()
}

func (bt *BTServer) uploadPacer() {
	defer bt.uploadPacerWg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	bt.mu.Lock()
	stopCh := bt.uploadPacerStop
	bt.mu.Unlock()
	var lastStreaming bool
	for {
		select {
		case <-stopCh:
			bt.mu.Lock()
			lim := bt.uploadLimiter
			bt.mu.Unlock()
			if lim != nil && lastStreaming {
				bt.restoreUploadLimit()
			}
			return
		case <-ticker.C:
			streaming := GetActiveStreams() > 0
			if streaming == lastStreaming {
				continue
			}
			lastStreaming = streaming
			if streaming {
				bt.throttleUpload()
			} else {
				bt.restoreUploadLimit()
			}
		}
	}
}

func (bt *BTServer) throttleUpload() {
	bt.mu.Lock()
	lim := bt.uploadLimiter
	configuredLimit := settings.BTsets.UploadRateLimit
	bt.mu.Unlock()
	if lim == nil {
		return
	}
	var throttleBytes float64
	if configuredLimit > 0 {
		configured := float64(configuredLimit) * 1024
		throttleBytes = configured * 0.1
	} else {
		throttleBytes = 100 * 1024
	}
	if throttleBytes < 10*1024 {
		throttleBytes = 10 * 1024
	}
	lim.SetLimit(rate.Limit(throttleBytes))
	lim.SetBurst(int(throttleBytes))
	log.Printf("Upload throttled to %.0f KB/s during streaming", throttleBytes/1024)
}

func (bt *BTServer) restoreUploadLimit() {
	bt.mu.Lock()
	lim := bt.uploadLimiter
	configuredLimit := settings.BTsets.UploadRateLimit
	bt.mu.Unlock()
	if lim == nil {
		return
	}
	if configuredLimit > 0 {
		limitBytes := float64(configuredLimit) * 1024
		lim.SetLimit(rate.Limit(limitBytes))
		burst := int(limitBytes)
		if burst < 16*1024 {
			burst = 16 * 1024
		}
		lim.SetBurst(burst)
		log.Printf("Upload restored to %d KB/s", configuredLimit)
	} else {
		lim.SetLimit(rate.Inf)
		lim.SetBurst(0)
		log.Println("Upload restored to unlimited")
	}
}

func (bt *BTServer) configure(ctx context.Context) {
	blocklist, _ := utils.ReadBlockedIP()
	bt.config = torrent.NewDefaultClientConfig()
	if settings.BTsets.EnableLPD {
		bt.config.LocalServiceDiscovery = &torrent.LocalServiceDiscoveryConfig{
			Ip6: settings.BTsets.LPDIPv6 && settings.BTsets.EnableIPv6,
		}
	} else {
		bt.config.LocalServiceDiscovery = nil
	}
	bt.storage = torrstor.NewStorage(settings.BTsets.CacheSize)
	bt.config.DefaultStorage = bt.storage
	userAgent := "qBittorrent/4.3.9"
	peerID := "-qB4390-"
	upnpID := "TorrServer/" + version.Version
	cliVers := userAgent
	bt.config.Debug = settings.BTsets.EnableDebug
	bt.config.DisableIPv6 = !settings.BTsets.EnableIPv6
	bt.config.DisableTCP = settings.BTsets.DisableTCP
	bt.config.DisableUTP = settings.BTsets.DisableUTP
	bt.config.NoDefaultPortForwarding = settings.BTsets.DisableUPNP
	bt.config.NoDHT = settings.BTsets.DisableDHT
	bt.config.DisablePEX = settings.BTsets.DisablePEX
	bt.config.NoUpload = settings.BTsets.DisableUpload
	bt.config.IPBlocklist = blocklist
	bt.config.Bep20 = peerID
	bt.config.PeerID = utils.PeerIDRandom(peerID)
	bt.config.UpnpID = upnpID
	bt.config.HTTPUserAgent = userAgent
	bt.config.ExtendedHandshakeClientVersion = cliVers
	bt.config.EstablishedConnsPerTorrent = settings.BTsets.ConnectionsLimit
	bt.config.TotalHalfOpenConns = 500
	bt.config.EncryptionPolicy = torrent.EncryptionPolicy{
		ForceEncryption: settings.BTsets.ForceEncrypt,
	}
	if settings.BTsets.DownloadRateLimit > 0 {
		bt.config.DownloadRateLimiter = utils.Limit(settings.BTsets.DownloadRateLimit * 1024)
	}
	bt.uploadLimiter = nil
	if settings.BTsets.UploadRateLimit > 0 {
		bt.config.Seed = true
		bt.uploadLimiter = utils.Limit(settings.BTsets.UploadRateLimit * 1024)
		bt.config.UploadRateLimiter = bt.uploadLimiter
	} else if !settings.BTsets.DisableUpload {
		bt.uploadLimiter = rate.NewLimiter(rate.Inf, 0)
		bt.config.UploadRateLimiter = bt.uploadLimiter
	}
	if settings.TorAddr != "" {
		log.Println("Set listen addr", settings.TorAddr)
		bt.config.SetListenAddr(settings.TorAddr)
	} else {
		if settings.BTsets.PeersListenPort > 0 {
			log.Println("Set listen port", settings.BTsets.PeersListenPort)
			bt.config.ListenPort = settings.BTsets.PeersListenPort
		} else {
			log.Println("Set listen port to random autoselect (0)")
			bt.config.ListenPort = 0
		}
	}
	if err := bt.configureProxy(); err != nil {
		log.Println("Proxy configuration error:", err)
	}
	log.Println("Client config:", settings.BTsets)
	var err error
	if settings.PubIPv4 != "" {
		if ip4 := net.ParseIP(settings.PubIPv4); ip4.To4() != nil && !isPrivateIP(ip4) {
			bt.config.PublicIp4 = ip4
		}
	}
	if bt.config.PublicIp4 == nil {
		bt.config.PublicIp4, err = publicip.Get4(ctx)
		if err != nil {
			log.Printf("error getting public ipv4 address: %v", err)
		}
	}
	if bt.config.PublicIp4.To4() == nil {
		bt.config.PublicIp4 = nil
	}
	if bt.config.PublicIp4 != nil {
		log.Println("PublicIp4:", bt.config.PublicIp4)
	}
	if settings.PubIPv6 != "" {
		if ip6 := net.ParseIP(settings.PubIPv6); ip6.To16() != nil && ip6.To4() == nil && !isPrivateIP(ip6) {
			bt.config.PublicIp6 = ip6
		}
	}
	if bt.config.PublicIp6 == nil && settings.BTsets.EnableIPv6 {
		bt.config.PublicIp6, err = publicip.Get6(ctx)
		if err != nil {
			log.Printf("error getting public ipv6 address: %v", err)
		}
	}
	if bt.config.PublicIp6.To16() == nil {
		bt.config.PublicIp6 = nil
	}
	if bt.config.PublicIp6 != nil {
		log.Println("PublicIp6:", bt.config.PublicIp6)
	}
}

func (bt *BTServer) configureProxy() error {
	proxyURL := settings.Args.ProxyURL
	if proxyURL == "" {
		return nil
	}
	proxyMode := settings.Args.ProxyMode
	if proxyMode == "" {
		proxyMode = "tracker"
	}
	parsedURL, err := url.Parse(proxyURL)
	if err != nil {
		return fmt.Errorf("invalid proxy URL: %w", err)
	}
	scheme := parsedURL.Scheme
	switch scheme {
	case "socks5", "socks5h", "socks4", "socks4a", "http", "https":
	default:
		return fmt.Errorf("unsupported proxy protocol: %s (supported: http, https, socks4, socks4a, socks5, socks5h)", scheme)
	}
	if proxyMode == "full" {
		log.Printf("Configuring proxy for all BitTorrent traffic: %s://%s", scheme, parsedURL.Host)
		bt.config.ProxyURL = proxyURL
		bt.config.HTTPProxy = func(req *http.Request) (*url.URL, error) {
			return parsedURL, nil
		}
		log.Println("Proxy configured successfully for all BitTorrent connections (tracker, DHT, peers)")
	} else if proxyMode == "peers" {
		log.Printf("Configuring proxy for peer connections only: %s://%s", scheme, parsedURL.Host)
		bt.config.ProxyURL = proxyURL
		log.Println("Proxy configured successfully for peer and DHT connections only")
	} else {
		log.Printf("Configuring proxy for HTTP tracker requests only: %s://%s", scheme, parsedURL.Host)
		bt.config.HTTPProxy = func(req *http.Request) (*url.URL, error) {
			return parsedURL, nil
		}
		log.Println("Proxy configured successfully for HTTP tracker connections only")
	}
	return nil
}

func (bt *BTServer) GetTorrent(hash torrent.InfoHash) *Torrent {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	if torr, ok := bt.torrents[hash]; ok {
		return torr
	}
	return nil
}

func (bt *BTServer) ListTorrents() map[metainfo.Hash]*Torrent {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	list := make(map[metainfo.Hash]*Torrent, len(bt.torrents))
	maps.Copy(list, bt.torrents)
	return list
}

func (bt *BTServer) RemoveTorrent(hash torrent.InfoHash) bool {
	bt.mu.Lock()
	torr, ok := bt.torrents[hash]
	bt.mu.Unlock()
	if ok {
		return torr.Close()
	}
	return false
}

func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func getPublicIp4() net.IP {
	ifaces, err := anet.Interfaces()
	if err != nil {
		log.Println("Error get public IPv4")
		return nil
	}
	for _, i := range ifaces {
		addrs, _ := anet.InterfaceAddrsByInterface(&i)
		if i.Flags&net.FlagUp == net.FlagUp {
			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				if !isPrivateIP(ip) && ip.To4() != nil {
					return ip
				}
			}
		}
	}
	return nil
}

func getPublicIp6() net.IP {
	ifaces, err := anet.Interfaces()
	if err != nil {
		log.Println("Error get public IPv6")
		return nil
	}
	for _, i := range ifaces {
		addrs, _ := anet.InterfaceAddrsByInterface(&i)
		if i.Flags&net.FlagUp == net.FlagUp {
			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				if !isPrivateIP(ip) && ip.To16() != nil && ip.To4() == nil {
					return ip
				}
			}
		}
	}
	return nil
}