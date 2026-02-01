package search

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/keloran/distcache/internal/registry"
	pb "github.com/keloran/distcache/proto/cache"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerDiscoveredCallback is called when a new peer is discovered
// The callback receives the peer address and should return an error if the sync fails
type PeerDiscoveredCallback func(ctx context.Context, peerAddr string) error

type System struct {
	Config                *ConfigBuilder.Config
	Registry              *registry.Registry
	selfAddress           string
	stopChan              chan struct{}
	wg                    sync.WaitGroup
	onFirstPeerDiscovered PeerDiscoveredCallback
	hasSynced             bool
	syncMu                sync.Mutex
}

func NewSystem(cfg *ConfigBuilder.Config, reg *registry.Registry) *System {
	return &System{
		Config:   cfg,
		Registry: reg,
		stopChan: make(chan struct{}),
	}
}

// SetOnFirstPeerDiscovered sets a callback that will be called when the first peer is discovered
// This is typically used to sync the cache from an existing peer
func (s *System) SetOnFirstPeerDiscovered(callback PeerDiscoveredCallback) {
	s.onFirstPeerDiscovered = callback
}

// Helper methods to get config values from ProjectProperties
func (s *System) getServiceName() string {
	if v, ok := s.Config.ProjectProperties["search_service_name"].(string); ok {
		return v
	}
	return "distcache"
}

func (s *System) getDomains() []string {
	if v, ok := s.Config.ProjectProperties["search_domains"].([]string); ok {
		return v
	}
	return []string{".internal"}
}

func (s *System) getPort() int {
	if v, ok := s.Config.ProjectProperties["search_port"].(int); ok {
		return v
	}
	return 42069
}

func (s *System) getScanInterval() time.Duration {
	if v, ok := s.Config.ProjectProperties["search_scan_interval"].(time.Duration); ok {
		return v
	}
	return 30 * time.Second
}

func (s *System) getTimeout() time.Duration {
	if v, ok := s.Config.ProjectProperties["search_timeout"].(time.Duration); ok {
		return v
	}
	return 5 * time.Second
}

func (s *System) getMaxInstances() int {
	if v, ok := s.Config.ProjectProperties["search_max_instances"].(int); ok {
		return v
	}
	return 100
}

func (s *System) getPredefinedServers() []string {
	if v, ok := s.Config.ProjectProperties["search_predefined_servers"].([]string); ok {
		return v
	}
	return nil
}

func (s *System) getIPRanges() []string {
	if v, ok := s.Config.ProjectProperties["search_ip_ranges"].([]string); ok {
		return v
	}
	return nil
}

func (s *System) getScanOwnRange() bool {
	if v, ok := s.Config.ProjectProperties["search_scan_own_range"].(bool); ok {
		return v
	}
	return false
}

// getLocalIP returns the local non-loopback IPv4 address
func getLocalIP() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				return ip4
			}
		}
	}
	return nil
}

// getOwnIPRange returns the /24 CIDR range for the server's local IP address
func (s *System) getOwnIPRange() string {
	ip4 := getLocalIP()
	if ip4 == nil {
		logs.Infof("failed to get local IP address")
		return ""
	}

	// Only proceed if it's a private IP
	if !isPrivateIP(ip4) {
		logs.Infof("local IP %s is not a private address, skipping own range scan", ip4.String())
		return ""
	}

	// Create /24 range (e.g., 192.168.178.0/24)
	return fmt.Sprintf("%d.%d.%d.0/24", ip4[0], ip4[1], ip4[2])
}

func (s *System) getCallerName() string {
	if v, ok := s.Config.ProjectProperties["service_name"].(string); ok {
		return v
	}
	return "distcache"
}

func (s *System) getCallerVersion() string {
	if v, ok := s.Config.ProjectProperties["service_version"].(string); ok {
		return v
	}
	return "1.0.0"
}

func (s *System) SetSelfAddress(addr string) {
	s.selfAddress = addr
}

func (s *System) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.discoveryLoop(ctx)
}

func (s *System) Stop() {
	close(s.stopChan)
	s.wg.Wait()
}

func (s *System) discoveryLoop(ctx context.Context) {
	defer s.wg.Done()

	// Initial discovery
	s.FindOthers(ctx)

	ticker := time.NewTicker(s.getScanInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.FindOthers(ctx)
			s.healthCheck(ctx)
		}
	}
}

func (s *System) FindOthers(ctx context.Context) {
	var wg sync.WaitGroup

	serviceName := s.getServiceName()
	domains := s.getDomains()
	maxInstances := s.getMaxInstances()
	predefinedServers := s.getPredefinedServers()

	// Probe predefined servers first (useful for testing and known peers)
	for _, server := range predefinedServers {
		if server == "" {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			// Skip ourselves
			if addr == s.selfAddress {
				return
			}
			s.tryBroadcast(ctx, addr, addr)
		}(server)
	}

	// Probe IP ranges (must be private ranges only)
	ipRanges := s.getIPRanges()
	port := s.getPort()

	// If scan_own_range is enabled, add the server's own /24 range
	if s.getScanOwnRange() {
		ownRange := s.getOwnIPRange()
		if ownRange != "" {
			ipRanges = append(ipRanges, ownRange)
		}
	}
	for _, cidr := range ipRanges {
		if cidr == "" {
			continue
		}

		// Validate that it's a private range
		if !isPrivateRange(cidr) {
			logs.Warnf("skipping non-private IP range: %s (only private ranges are allowed)", cidr)
			continue
		}

		ips, err := expandCIDR(cidr)
		if err != nil {
			logs.Warnf("failed to expand IP range %s: %v", cidr, err)
			continue
		}

		for _, ip := range ips {
			wg.Add(1)
			go func(ipAddr net.IP) {
				defer wg.Done()
				address := fmt.Sprintf("%s:%d", ipAddr.String(), port)
				// Skip ourselves
				if address == s.selfAddress {
					return
				}
				s.tryBroadcast(ctx, ipAddr.String(), address)
			}(ip)
		}
	}

	// Probe domains for service discovery
	for _, domain := range domains {
		// Try base service name (e.g., cacheservice.internal)
		wg.Add(1)
		go func(d string) {
			defer wg.Done()
			s.probeService(ctx, fmt.Sprintf("%s%s", serviceName, d))
		}(domain)

		// Try numbered instances (e.g., cacheservice-1.internal, cacheservice-2.internal)
		for i := 0; i <= maxInstances; i++ {
			wg.Add(1)
			go func(d string, num int) {
				defer wg.Done()
				hostname := fmt.Sprintf("%s-%d%s", serviceName, num, d)
				s.probeService(ctx, hostname)
			}(domain, i)
		}
	}

	wg.Wait()
}

func (s *System) probeService(ctx context.Context, hostname string) {
	// First, try to resolve the hostname
	ips, err := net.LookupHost(hostname)
	if err != nil {
		// Host doesn't exist, skip silently
		return
	}

	port := s.getPort()
	for _, ip := range ips {
		address := fmt.Sprintf("%s:%d", ip, port)

		// Skip ourselves
		if address == s.selfAddress {
			continue
		}

		s.tryBroadcast(ctx, hostname, address)
	}
}

func (s *System) tryBroadcast(ctx context.Context, hostname, address string) {
	ctx, cancel := context.WithTimeout(ctx, s.getTimeout())
	defer cancel()

	if address == "192.168.178.79:42069" {
		logs.Infof("broadcasting %s to %s", address, hostname)
	}

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			logs.Warnf("Failed to close grpc connection: %v", err)
		}
	}()

	client := pb.NewCacheServiceClient(conn)
	broadcastResp, err := client.Broadcast(ctx, &pb.BroadcastRequest{
		CallerName:    s.getCallerName(),
		CallerAddress: s.selfAddress,
		CallerVersion: s.getCallerVersion(),
	})
	if err != nil {
		return
	}

	// Add to registry
	peerAddr := broadcastResp.Address
	if peerAddr == "" {
		peerAddr = address
	}

	name := broadcastResp.Name
	if name == "" {
		name = hostname
	}

	// Skip ourselves
	if peerAddr == s.selfAddress {
		return
	}

	isNewPeer := !s.Registry.Exists(peerAddr)
	if isNewPeer {
		logs.Infof("discovered peer: %s at %s", name, peerAddr)
	}
	s.Registry.Add(peerAddr, name)

	// If this is a new peer and we haven't synced yet, trigger sync
	if isNewPeer && s.onFirstPeerDiscovered != nil {
		s.syncMu.Lock()
		shouldSync := !s.hasSynced
		if shouldSync {
			s.hasSynced = true
		}
		s.syncMu.Unlock()

		if shouldSync {
			logs.Infof("first peer discovered, syncing cache from %s...", peerAddr)
			if err := s.onFirstPeerDiscovered(ctx, peerAddr); err != nil {
				logs.Warnf("failed to sync cache from peer %s: %v", peerAddr, err)
				// Reset so we can try again with another peer
				s.syncMu.Lock()
				s.hasSynced = false
				s.syncMu.Unlock()
			}
		}
	}
}

func (s *System) healthCheck(ctx context.Context) {
	peers := s.Registry.GetAll()

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p *registry.Peer) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(ctx, s.getTimeout())
			defer cancel()

			conn, err := grpc.NewClient(p.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				s.Registry.MarkUnhealthy(p.Address)
				logs.Infof("peer %s is unhealthy: %v", p.Address, err)
				return
			}
			defer func() {
				if err := conn.Close(); err != nil {
					logs.Warnf("failed to close connection to peer %s: %v", p.Address, err)
				}
			}()

			client := pb.NewCacheServiceClient(conn)
			_, err = client.Broadcast(ctx, &pb.BroadcastRequest{})
			if err != nil {
				s.Registry.MarkUnhealthy(p.Address)
				logs.Infof("peer %s is unhealthy: %v", p.Address, err)
				return
			}

			s.Registry.UpdateLastSeen(p.Address)
		}(peer)
	}

	wg.Wait()
}

// isPrivateIP checks if an IP address is in a private range (RFC 1918)
func isPrivateIP(ip net.IP) bool {
	if ip4 := ip.To4(); ip4 != nil {
		// 10.0.0.0/8
		if ip4[0] == 10 {
			return true
		}
		// 172.16.0.0/12
		if ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31 {
			return true
		}
		// 192.168.0.0/16
		if ip4[0] == 192 && ip4[1] == 168 {
			return true
		}
	}
	return false
}

// isPrivateRange checks if a CIDR range is entirely within private IP space
func isPrivateRange(cidr string) bool {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return false
	}

	// Check if the network address is private
	if !isPrivateIP(network.IP) {
		return false
	}

	// Check if the broadcast address is also private
	// Calculate the last IP in the range
	ones, bits := network.Mask.Size()
	if bits == 0 {
		return false
	}

	// For the last IP, we need to set all host bits to 1
	lastIP := make(net.IP, len(network.IP))
	copy(lastIP, network.IP)

	// Calculate number of host bits
	hostBits := bits - ones

	// Set all host bits to 1 to get the broadcast/last address
	for i := bits - 1; i >= ones; i-- {
		byteIdx := i / 8
		bitIdx := 7 - (i % 8)
		lastIP[byteIdx] |= 1 << bitIdx
		hostBits--
	}

	return isPrivateIP(lastIP)
}

// expandCIDR returns all IP addresses in a CIDR range
func expandCIDR(cidr string) ([]net.IP, error) {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("invalid CIDR: %w", err)
	}

	var ips []net.IP
	ip := network.IP.To4()
	if ip == nil {
		return nil, fmt.Errorf("only IPv4 ranges are supported")
	}

	// Convert to uint32 for easy iteration
	start := binary.BigEndian.Uint32(ip)

	ones, bits := network.Mask.Size()
	hostBits := bits - ones
	numHosts := uint32(1) << hostBits

	// Skip network address (first) and broadcast address (last) for /24 and smaller
	startOffset := uint32(1)
	endOffset := uint32(1)
	if hostBits <= 1 {
		// For /31 and /32, include all addresses
		startOffset = 0
		endOffset = 0
	}

	for i := startOffset; i < numHosts-endOffset; i++ {
		ipInt := start + i
		newIP := make(net.IP, 4)
		binary.BigEndian.PutUint32(newIP, ipInt)
		ips = append(ips, newIP)
	}

	return ips, nil
}

// ProbeAddress allows manual probing of a specific address
func (s *System) ProbeAddress(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, s.getTimeout())
	defer cancel()

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			logs.Warnf("failed to close connection to peer %s: %v", s.selfAddress, err)
		}
	}()

	client := pb.NewCacheServiceClient(conn)
	broadcastResp, err := client.Broadcast(ctx, &pb.BroadcastRequest{})
	if err != nil {
		return fmt.Errorf("broadcast failed: %w", err)
	}

	peerAddr := broadcastResp.Address
	if peerAddr == "" {
		peerAddr = address
	}

	s.Registry.Add(peerAddr, broadcastResp.Name)
	return nil
}
