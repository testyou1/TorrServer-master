package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"server/torr/utils"

	"github.com/alexflint/go-arg"
	"github.com/pkg/browser"

	"server"
	"server/docs"
	"server/log"
	"server/settings"
	"server/torr"
	"server/version"

	"github.com/wlynxg/anet"
)

type args struct {
	Port        string `arg:"-p" help:"web server port (default 8090)"`
	IP          string `arg:"-i" help:"web server addr (default empty)"`
	Ssl         bool   `help:"enables https"`
	SslPort     string `help:"web server ssl port, If not set, will be set to default 8091 or taken from db(if stored previously). Accepted if --ssl enabled."`
	SslCert     string `help:"path to ssl cert file. If not set, will be taken from db(if stored previously) or default self-signed certificate/key will be generated. Accepted if --ssl enabled."`
	SslKey      string `help:"path to ssl key file. If not set, will be taken from db(if stored previously) or default self-signed certificate/key will be generated. Accepted if --ssl enabled."`
	Path        string `arg:"-d" help:"database and config dir path"`
	LogPath     string `arg:"-l" help:"server log file path"`
	WebLogPath  string `arg:"-w" help:"web access log file path"`
	RDB         bool   `arg:"-r" help:"start in read-only DB mode"`
	HttpAuth    bool   `arg:"-a" help:"enable http auth on all requests"`
	DontKill    bool   `arg:"-k" help:"don't kill server on signal"`
	UI          bool   `arg:"-u" help:"open torrserver page in browser"`
	TorrentsDir string `arg:"-t" help:"autoload torrents from dir"`
	TorrentAddr string `help:"Torrent client address, like 127.0.0.1:1337 (default :PeersListenPort)"`
	PubIPv4     string `arg:"-4" help:"set public IPv4 addr"`
	PubIPv6     string `arg:"-6" help:"set public IPv6 addr"`
	SearchWA    bool   `arg:"-s" help:"search without auth"`
	MaxSize     string `arg:"-m" help:"max allowed stream size (in Bytes)"`
	TGToken     string `arg:"-T" help:"telegram bot token"`
	FusePath    string `arg:"-f" help:"fuse mount path"`
	WebDAV      bool   `help:"web dav enable"`
	ProxyURL    string `help:"proxy URL for BitTorrent traffic (http, socks4, socks5, socks5h), e.g. socks5://user:password@127.0.0.1:8080"`
	ProxyMode   string `help:"proxy mode: tracker (only HTTP trackers, default), peers (only peer connections), or full (all traffic)"`
	ForceHTTPS  bool   `arg:"--force-https" help:"redirect all HTTP requests to HTTPS (requires --ssl)"`
}

func (args) Version() string {
	return "TorrServer " + version.Version
}

var params args

var suspiciousBlocks []*net.IPNet

func init() {
	for _, cidr := range []string{
		"192.168.0.0/16",
		"169.254.0.0/16",
	} {
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Errorf("suspiciousBlocks parse error on %q: %v", cidr, err))
		}
		suspiciousBlocks = append(suspiciousBlocks, block)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	arg.MustParse(&params)

	if params.Path == "" {
		params.Path, _ = os.Getwd()
	}

	if params.Port == "" {
		params.Port = "8090"
	}

	settings.Path = params.Path
	settings.HttpAuth = params.HttpAuth
	log.Init(params.LogPath, params.WebLogPath)

	fmt.Println("=========== START ===========")
	fmt.Println("TorrServer", version.Version+",", runtime.Version()+",", "CPU Num:", runtime.NumCPU())
	if params.HttpAuth {
		log.TLogln("Use HTTP Auth file", settings.Path+"/accs.db")
	}
	if params.RDB {
		log.TLogln("Running in Read-only DB mode!")
	}
	docs.SwaggerInfo.Version = version.Version

	dnsStop := dnsResolve()

	Preconfig(params.DontKill)

	if params.UI {
		go func() {
			time.Sleep(time.Second)
			if params.Ssl {
				browser.OpenURL("https://127.0.0.1:" + params.SslPort)
			} else {
				browser.OpenURL("http://127.0.0.1:" + params.Port)
			}
		}()
	}

	if params.TorrentAddr != "" {
		settings.TorAddr = params.TorrentAddr
	}

	if params.PubIPv4 != "" {
		settings.PubIPv4 = params.PubIPv4
	}

	if params.PubIPv6 != "" {
		settings.PubIPv6 = params.PubIPv6
	}

	if params.TorrentsDir != "" {
		go watchTDir(params.TorrentsDir)
	}

	if params.MaxSize != "" {
		maxSize, err := strconv.ParseInt(params.MaxSize, 10, 64)
		if err == nil {
			settings.MaxSize = maxSize
		}
	}

	if params.ProxyURL != "" && params.ProxyMode == "" {
		params.ProxyMode = "tracker"
	}
	if params.ProxyMode != "" && params.ProxyMode != "tracker" && params.ProxyMode != "peers" && params.ProxyMode != "full" {
		log.TLogln("Invalid proxy mode, using default 'tracker'")
		params.ProxyMode = "tracker"
	}

	settings.Args = &settings.ExecArgs{
		Port:        params.Port,
		IP:          params.IP,
		Ssl:         params.Ssl,
		SslPort:     params.SslPort,
		SslCert:     params.SslCert,
		SslKey:      params.SslKey,
		Path:        params.Path,
		LogPath:     params.LogPath,
		WebLogPath:  params.WebLogPath,
		RDB:         params.RDB,
		HttpAuth:    params.HttpAuth,
		DontKill:    params.DontKill,
		UI:          params.UI,
		TorrentsDir: params.TorrentsDir,
		TorrentAddr: params.TorrentAddr,
		PubIPv4:     params.PubIPv4,
		PubIPv6:     params.PubIPv6,
		SearchWA:    params.SearchWA,
		MaxSize:     params.MaxSize,
		TGToken:     params.TGToken,
		FusePath:    params.FusePath,
		WebDAV:      params.WebDAV,
		ProxyURL:    params.ProxyURL,
		ProxyMode:   params.ProxyMode,
		ForceHTTPS:  params.ForceHTTPS,
	}

	if params.ProxyURL != "" {
		log.TLogln("Proxy configured from CLI:", params.ProxyURL, "mode:", settings.Args.ProxyMode)
	}

	if params.ForceHTTPS && !params.Ssl {
		log.TLogln("Error: --force-https requires --ssl")
		close(dnsStop)
		os.Exit(1)
	}

	server.Start()
	log.TLogln(server.WaitServer())

	close(dnsStop)

	log.Close()
	time.Sleep(time.Second * 3)
	os.Exit(0)
}

func watchTDir(dir string) {
	time.Sleep(5 * time.Second)
	path, err := filepath.Abs(dir)
	if err != nil {
		path = dir
	}
	for {
		files, err := os.ReadDir(path)
		if err == nil {
			for _, file := range files {
				filename := filepath.Join(path, file.Name())
				if strings.ToLower(filepath.Ext(file.Name())) == ".torrent" {
					sp, err := utils.OpenTorrentFile(filename)
					if err == nil {
						tor, err := torr.AddTorrent(sp, "", "", "", "")
						if err == nil {
							if tor.GotInfo() {
								if tor.Title == "" {
									tor.Title = tor.Name()
								}
								torr.SaveTorrentToDB(tor)
								tor.Drop()
								os.Remove(filename)
								time.Sleep(time.Second)
							} else {
								log.TLogln("Error get info from torrent")
							}
						} else {
							log.TLogln("Error parse torrent file:", err)
						}
					} else {
						log.TLogln("Error parse file name:", err)
					}
				}
			}
		} else {
			log.TLogln("Error read dir:", err)
		}
		time.Sleep(time.Second * 5)
	}
}

type DNSConfig struct {
	PrimaryServers       []string
	FallbackServers      []string
	Timeout              time.Duration
	CacheDuration        time.Duration
	NetworkCheckInterval time.Duration
	NetworkTestTimeout   time.Duration
}

func DefaultDNSConfig() DNSConfig {
	return DNSConfig{
		PrimaryServers: []string{
			"8.8.8.8:53",
			"1.1.1.1:53",
			"9.9.9.9:53",
		},
		FallbackServers: []string{
			"208.67.222.222:53",
			"64.6.64.6:53",
		},
		Timeout:              5 * time.Second,
		CacheDuration:        5 * time.Minute,
		NetworkCheckInterval: 3 * time.Second,
		NetworkTestTimeout:   1500 * time.Millisecond,
	}
}

type dnsEntry struct {
	addrs   []string
	expires time.Time
}

type atomicResolver struct {
	p unsafe.Pointer
}

func newAtomicResolver(r *net.Resolver) *atomicResolver {
	ar := &atomicResolver{}
	ar.store(r)
	return ar
}

func (ar *atomicResolver) store(r *net.Resolver) {
	atomic.StorePointer(&ar.p, unsafe.Pointer(r))
}

func (ar *atomicResolver) load() *net.Resolver {
	return (*net.Resolver)(atomic.LoadPointer(&ar.p))
}

type DNSChecker struct {
	config           DNSConfig
	activeResolver   *atomicResolver
	systemResolver   *net.Resolver
	fallbackResolver *net.Resolver
	cache            map[string]dnsEntry
	mu               sync.RWMutex
	prevIPs          map[string]struct{}
	netMu            sync.Mutex
}

func NewDNSChecker(config DNSConfig) *DNSChecker {
	if len(config.PrimaryServers) == 0 {
		config = DefaultDNSConfig()
	}

	d := &DNSChecker{
		config:  config,
		cache:   make(map[string]dnsEntry),
		prevIPs: make(map[string]struct{}),
	}

	d.systemResolver = &net.Resolver{
		PreferGo: true,
	}

	d.fallbackResolver = d.buildFallbackResolver()
	d.activeResolver = newAtomicResolver(d.systemResolver)

	return d
}

func (d *DNSChecker) buildFallbackResolver() *net.Resolver {
	primary := d.config.PrimaryServers
	fallback := d.config.FallbackServers
	timeout := d.config.Timeout

	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: timeout}

			for _, srv := range primary {
				conn, err := dialer.DialContext(ctx, "udp", srv)
				if err == nil {
					return conn, nil
				}
				log.TLogln("DNS: failed primary server", srv, ":", err)
			}

			for _, srv := range fallback {
				conn, err := dialer.DialContext(ctx, "udp", srv)
				if err == nil {
					log.TLogln("DNS: using fallback server", srv)
					return conn, nil
				}
				log.TLogln("DNS: failed fallback server", srv, ":", err)
			}

			return nil, fmt.Errorf("DNS: all servers unreachable")
		},
	}
}

func (d *DNSChecker) InitialCheck() {
	if d.testSystemDNS(d.config.Timeout) {
		log.TLogln("DNS: system resolver OK")
		d.activeResolver.store(d.systemResolver)
	} else {
		log.TLogln("DNS: system resolver failed, switching to fallback")
		d.activeResolver.store(d.fallbackResolver)
	}
	net.DefaultResolver = d.activeResolver.load()
}

func (d *DNSChecker) testSystemDNS(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	addrs, err := d.systemResolver.LookupHost(ctx, "8.8.8.8.nip.io")
	if err != nil {
		addrs, err = d.systemResolver.LookupHost(ctx, "dns.google")
		if err != nil {
			return false
		}
	}

	for _, addr := range addrs {
		if !isSuspiciousAddress(addr) {
			return true
		}
	}
	return false
}

func (d *DNSChecker) LookupHost(host string) ([]string, error) {
	if addrs, ok := d.getFromCache(host); ok {
		return addrs, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.config.Timeout)
	defer cancel()

	resolver := d.activeResolver.load()
	addrs, err := resolver.LookupHost(ctx, host)
	if err != nil {
		if resolver != d.fallbackResolver {
			log.TLogln("DNS: active resolver failed for", host, ", trying fallback")
			addrs, err = d.fallbackResolver.LookupHost(ctx, host)
		}
	}

	if err == nil && len(addrs) > 0 {
		d.addToCache(host, addrs)
	}

	return addrs, err
}

func (d *DNSChecker) getFromCache(host string) ([]string, bool) {
	d.mu.RLock()
	entry, ok := d.cache[host]
	d.mu.RUnlock()

	if !ok {
		return nil, false
	}

	if time.Now().Before(entry.expires) {
		return entry.addrs, true
	}

	d.mu.Lock()
	if e, still := d.cache[host]; still && !time.Now().Before(e.expires) {
		delete(d.cache, host)
	}
	d.mu.Unlock()

	return nil, false
}

func (d *DNSChecker) addToCache(host string, addrs []string) {
	d.mu.Lock()
	d.cache[host] = dnsEntry{
		addrs:   addrs,
		expires: time.Now().Add(d.config.CacheDuration),
	}
	d.mu.Unlock()
}

func (d *DNSChecker) invalidateCache() {
	d.mu.Lock()
	d.cache = make(map[string]dnsEntry)
	d.mu.Unlock()
}

func getLocalIPSet() map[string]struct{} {
	result := make(map[string]struct{})

	ifaces, anetErr := anet.Interfaces()
	if anetErr != nil {
		stdIfaces, err := net.Interfaces()
		if err != nil {
			return result
		}
		for _, iface := range stdIfaces {
			if iface.Flags&net.FlagUp == 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
					continue
				}
				result[ip.String()] = struct{}{}
			}
		}
		return result
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := anet.InterfaceAddrsByInterface(&iface)
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			result[ip.String()] = struct{}{}
		}
	}

	return result
}

func ipSetsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for ip := range a {
		if _, ok := b[ip]; !ok {
			return false
		}
	}
	return true
}

func (d *DNSChecker) watchNetwork(stopCh <-chan struct{}) {
	d.netMu.Lock()
	d.prevIPs = getLocalIPSet()
	d.netMu.Unlock()

	ticker := time.NewTicker(d.config.NetworkCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			log.TLogln("DNS: network watcher stopped")
			return

		case <-ticker.C:
			currIPs := getLocalIPSet()

			d.netMu.Lock()
			changed := !ipSetsEqual(d.prevIPs, currIPs)
			if changed {
				d.prevIPs = currIPs
			}
			d.netMu.Unlock()

			if !changed {
				continue
			}

			log.TLogln("DNS: network change detected, re-evaluating resolver")

			d.invalidateCache()

			if d.testSystemDNS(d.config.NetworkTestTimeout) {
				log.TLogln("DNS: system resolver OK after network change")
				d.activeResolver.store(d.systemResolver)
			} else {
				log.TLogln("DNS: system resolver failed after network change, using fallback")
				d.activeResolver.store(d.fallbackResolver)
			}

			net.DefaultResolver = d.activeResolver.load()
		}
	}
}

func isSuspiciousAddress(addr string) bool {
	ip := net.ParseIP(addr)
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() {
		return true
	}
	for _, block := range suspiciousBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func dnsResolve() chan struct{} {
	checker := NewDNSChecker(DefaultDNSConfig())
	checker.InitialCheck()

	stopCh := make(chan struct{})
	go checker.watchNetwork(stopCh)

	return stopCh
}