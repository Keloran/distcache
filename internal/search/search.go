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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type System struct {
	Config      config.SearchConfig
	Registry    *registry.Registry
	selfAddress string
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

func NewSystem(cfg config.SearchConfig, reg *registry.Registry) *System {
	return &System{
		Config:   cfg,
		Registry: reg,
		stopChan: make(chan struct{}),
	}
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

	ticker := time.NewTicker(s.Config.ScanInterval)
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

	for _, domain := range s.Config.Domains {
		// Try base service name (e.g., cacheservice.internal)
		wg.Add(1)
		go func(d string) {
			defer wg.Done()
			s.probeService(ctx, fmt.Sprintf("%s%s", s.Config.ServiceName, d))
		}(domain)

		// Try numbered instances (e.g., cacheservice-1.internal, cacheservice-2.internal)
		for i := 0; i <= s.Config.MaxInstances; i++ {
			wg.Add(1)
			go func(d string, num int) {
				defer wg.Done()
				hostname := fmt.Sprintf("%s-%d%s", s.Config.ServiceName, num, d)
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
		address := fmt.Sprintf("%s:%d", ip, s.Config.Port)

		// Skip ourselves
		if address == s.selfAddress {
			continue
		}

		s.tryBroadcast(ctx, hostname, address)
	}
}

func (s *System) tryBroadcast(ctx context.Context, hostname, address string) {
	ctx, cancel := context.WithTimeout(ctx, s.Config.Timeout)
	defer cancel()

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewCacheServiceClient(conn)
	broadcastResp, err := client.Broadcast(ctx, &pb.BroadcastRequest{})
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

			ctx, cancel := context.WithTimeout(ctx, s.Config.Timeout)
			defer cancel()

			conn, err := grpc.NewClient(p.Address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				s.Registry.MarkUnhealthy(p.Address)
				logs.Infof("peer %s is unhealthy: %v", p.Address, err)
				return
			}
			defer conn.Close()

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
	ctx, cancel := context.WithTimeout(ctx, s.Config.Timeout)
	defer cancel()

	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

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
