package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/keloran/distcache/internal/registry"
	"github.com/keloran/distcache/internal/search"
	ConfigBuilder "github.com/keloran/go-config"
)

type ServiceConfig struct {
	Name    string
	Port    int
	Version string
}

type Service struct {
	Config        *ConfigBuilder.Config
	ServiceConfig ServiceConfig
	Registry      *registry.Registry
	Search        *search.System
	server        *http.Server
	selfAddress   string
}

func New(cfg *ConfigBuilder.Config) *Service {
	return &Service{
		Config: cfg,
	}
}

func NewWithConfig(cfg *ConfigBuilder.Config, svcConfig ServiceConfig, searchConfig search.Config) *Service {
	reg := registry.New()

	// Determine our own address
	selfAddress := fmt.Sprintf(":%d", svcConfig.Port)
	if hostname, err := os.Hostname(); err == nil {
		selfAddress = fmt.Sprintf("%s:%d", hostname, svcConfig.Port)
	}

	searchSystem := search.NewSystem(*cfg, searchConfig, reg)
	searchSystem.SetSelfAddress(selfAddress)

	return &Service{
		Config:        cfg,
		ServiceConfig: svcConfig,
		Registry:      reg,
		Search:        searchSystem,
		selfAddress:   selfAddress,
	}
}

func (s *Service) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("GET /broadcast", s.handleBroadcast)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/peers", s.handlePeers)

	s.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.ServiceConfig.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start the search/discovery system
	logs.Infof("starting service discovery...")
	s.Search.Start(ctx)

	// Start HTTP server in goroutine
	errChan := make(chan error, 1)
	go func() {
		logs.Infof("starting HTTP server on port %d", s.ServiceConfig.Port)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		return err
	case sig := <-sigChan:
		logs.Infof("received signal %v, shutting down...", sig)
		return s.shutdown(ctx)
	}
}

func (s *Service) shutdown(ctx context.Context) error {
	// Stop the search system
	s.Search.Stop()

	// Shutdown HTTP server
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return s.server.Shutdown(shutdownCtx)
}

func (s *Service) handleBroadcast(w http.ResponseWriter, r *http.Request) {
	response := search.BroadcastResponse{
		Name:    s.ServiceConfig.Name,
		Address: s.selfAddress,
		Version: s.ServiceConfig.Version,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		_ = logs.Errorf("error encoding broadcast response: %v", err)
	}
}

func (s *Service) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status":        "healthy",
		"peers":         s.Registry.Count(),
		"healthy_peers": len(s.Registry.GetHealthy()),
	}); err != nil {
		_ = logs.Errorf("error encoding health response: %v", err)
		return
	}
}

func (s *Service) handlePeers(w http.ResponseWriter, r *http.Request) {
	peers := s.Registry.GetAll()

	type peerInfo struct {
		Address  string    `json:"address"`
		Name     string    `json:"name"`
		LastSeen time.Time `json:"last_seen"`
		Healthy  bool      `json:"healthy"`
	}

	peerList := make([]peerInfo, 0, len(peers))
	for _, p := range peers {
		peerList = append(peerList, peerInfo{
			Address:  p.Address,
			Name:     p.Name,
			LastSeen: p.LastSeen,
			Healthy:  p.Healthy,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(peerList)
	if err != nil {
		_ = logs.Errorf("error encoding peers list: %v", err)
	}
}

// GetLocalIP attempts to determine the local IP address
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
