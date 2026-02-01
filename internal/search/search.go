package search

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/keloran/distcache/internal/config"
	"github.com/keloran/distcache/internal/registry"
	pb "github.com/keloran/distcache/proto/cache"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type System struct {
	Config      *ConfigBuilder.Config
	Registry    *registry.Registry
	selfAddress string
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

func NewSystem(cfg *ConfigBuilder.Config, reg *registry.Registry) *System {
	return &System{
		Config:   cfg,
		Registry: reg,
		stopChan: make(chan struct{}),
	}
}

func (s *System) getSearchConfig() config.SearchConfig {
	return config.GetProjectConfig(s.Config).Search
}

func (s *System) getServiceConfig() config.ServiceConfig {
	return config.GetProjectConfig(s.Config).Service
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

	ticker := time.NewTicker(s.getSearchConfig().ScanInterval)
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

	searchCfg := s.getSearchConfig()

	// Probe predefined servers first (useful for testing and known peers)
	for _, server := range searchCfg.PredefinedServers {
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

	// Probe domains for service discovery
	for _, domain := range searchCfg.Domains {
		// Try base service name (e.g., cacheservice.internal)
		wg.Add(1)
		go func(d string) {
			defer wg.Done()
			s.probeService(ctx, fmt.Sprintf("%s%s", searchCfg.ServiceName, d))
		}(domain)

		// Try numbered instances (e.g., cacheservice-1.internal, cacheservice-2.internal)
		for i := 0; i <= searchCfg.MaxInstances; i++ {
			wg.Add(1)
			go func(d string, num int) {
				defer wg.Done()
				hostname := fmt.Sprintf("%s-%d%s", searchCfg.ServiceName, num, d)
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

	for _, ip := range ips {
		address := fmt.Sprintf("%s:%d", ip, s.getSearchConfig().Port)

		// Skip ourselves
		if address == s.selfAddress {
			continue
		}

		s.tryBroadcast(ctx, hostname, address)
	}
}

func (s *System) tryBroadcast(ctx context.Context, hostname, address string) {
	ctx, cancel := context.WithTimeout(ctx, s.getSearchConfig().Timeout)
	defer cancel()

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
	svcCfg := s.getServiceConfig()
	broadcastResp, err := client.Broadcast(ctx, &pb.BroadcastRequest{
		CallerName:    svcCfg.Name,
		CallerAddress: s.selfAddress,
		CallerVersion: svcCfg.Version,
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

	if !s.Registry.Exists(peerAddr) {
		logs.Infof("discovered peer: %s at %s", name, peerAddr)
	}
	s.Registry.Add(peerAddr, name)
}

func (s *System) healthCheck(ctx context.Context) {
	peers := s.Registry.GetAll()

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p *registry.Peer) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(ctx, s.getSearchConfig().Timeout)
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

// ProbeAddress allows manual probing of a specific address
func (s *System) ProbeAddress(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, s.getSearchConfig().Timeout)
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
