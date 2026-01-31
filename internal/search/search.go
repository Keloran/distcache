package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/keloran/distcache/internal/registry"
	ConfigBuilder "github.com/keloran/go-config"
)

type Config struct {
	ServiceName  string        // e.g., "cacheservice"
	Domains      []string      // e.g., [".internal", ".local"]
	Port         int           // port to check for broadcast endpoint
	ScanInterval time.Duration // how often to scan for new peers
	Timeout      time.Duration // HTTP timeout for broadcast requests
	MaxInstances int           // max instance number to try (e.g., cacheservice-1, cacheservice-2, etc.)
}

type System struct {
	AppConfig    ConfigBuilder.Config
	SearchConfig Config
	Registry     *registry.Registry
	selfAddress  string
	stopChan     chan struct{}
	wg           sync.WaitGroup
}

type BroadcastResponse struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Version string `json:"version,omitempty"`
}

func DefaultConfig() Config {
	return Config{
		ServiceName:  "cacheservice",
		Domains:      []string{".internal"},
		Port:         8080,
		ScanInterval: 30 * time.Second,
		Timeout:      5 * time.Second,
		MaxInstances: 100,
	}
}

func NewSystem(config ConfigBuilder.Config, searchConfig Config, reg *registry.Registry) *System {
	return &System{
		AppConfig:    config,
		SearchConfig: searchConfig,
		Registry:     reg,
		stopChan:     make(chan struct{}),
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

	ticker := time.NewTicker(s.SearchConfig.ScanInterval)
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

	for _, domain := range s.SearchConfig.Domains {
		// Try base service name (e.g., cacheservice.internal)
		wg.Add(1)
		go func(d string) {
			defer wg.Done()
			s.probeService(ctx, fmt.Sprintf("%s%s", s.SearchConfig.ServiceName, d))
		}(domain)

		// Try numbered instances (e.g., cacheservice-1.internal, cacheservice-2.internal)
		for i := 0; i <= s.SearchConfig.MaxInstances; i++ {
			wg.Add(1)
			go func(d string, num int) {
				defer wg.Done()
				hostname := fmt.Sprintf("%s-%d%s", s.SearchConfig.ServiceName, num, d)
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
		address := fmt.Sprintf("%s:%d", ip, s.SearchConfig.Port)

		// Skip ourselves
		if address == s.selfAddress {
			continue
		}

		s.tryBroadcast(ctx, hostname, address)
	}
}

func (s *System) tryBroadcast(ctx context.Context, hostname, address string) {
	url := fmt.Sprintf("http://%s/broadcast", address)

	ctx, cancel := context.WithTimeout(ctx, s.SearchConfig.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}

	client := &http.Client{
		Timeout: s.SearchConfig.Timeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	var broadcastResp BroadcastResponse
	if err := json.NewDecoder(resp.Body).Decode(&broadcastResp); err != nil {
		logs.Infof("failed to decode broadcast response from %s: %v", address, err)
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

			url := fmt.Sprintf("http://%s/broadcast", p.Address)

			ctx, cancel := context.WithTimeout(ctx, s.SearchConfig.Timeout)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				s.Registry.MarkUnhealthy(p.Address)
				return
			}

			client := &http.Client{
				Timeout: s.SearchConfig.Timeout,
			}

			resp, err := client.Do(req)
			if err != nil {
				s.Registry.MarkUnhealthy(p.Address)
				logs.Infof("peer %s is unhealthy: %v", p.Address, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				s.Registry.UpdateLastSeen(p.Address)
			} else {
				s.Registry.MarkUnhealthy(p.Address)
			}
		}(peer)
	}

	wg.Wait()
}

// ProbeAddress allows manual probing of a specific address
func (s *System) ProbeAddress(ctx context.Context, address string) error {
	url := fmt.Sprintf("http://%s/broadcast", address)

	ctx, cancel := context.WithTimeout(ctx, s.SearchConfig.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	client := &http.Client{
		Timeout: s.SearchConfig.Timeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var broadcastResp BroadcastResponse
	if err := json.NewDecoder(resp.Body).Decode(&broadcastResp); err != nil {
		return err
	}

	peerAddr := broadcastResp.Address
	if peerAddr == "" {
		peerAddr = address
	}

	s.Registry.Add(peerAddr, broadcastResp.Name)
	return nil
}
