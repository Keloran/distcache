package internal

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/keloran/distcache/internal/cache"
	"github.com/keloran/distcache/internal/registry"
	"github.com/keloran/distcache/internal/search"
	pb "github.com/keloran/distcache/proto/cache"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware
)

type Service struct {
	pb.UnimplementedCacheServiceServer
	Config      *ConfigBuilder.Config
	Registry    *registry.Registry
	Search      *search.System
	Cache       *cache.Cache
	grpcServer  *grpc.Server
	selfAddress string
}

func New(cfg *ConfigBuilder.Config) *Service {
	reg := registry.New()
	searchSystem := search.NewSystem(cfg, reg)

	// Get cache TTL from config
	ttl := 10 * time.Second
	if v, ok := cfg.ProjectProperties["cache_ttl"].(time.Duration); ok {
		ttl = v
	}
	cacheSystem := cache.NewCache(ttl)

	svc := &Service{
		Config:   cfg,
		Registry: reg,
		Search:   searchSystem,
		Cache:    cacheSystem,
	}

	// Register callback to sync cache when first peer is discovered
	searchSystem.SetOnFirstPeerDiscovered(svc.SyncFromPeer)

	return svc
}

// updateSelfAddress updates the service's address and notifies the search system
func (s *Service) updateSelfAddress(port int) {
	s.selfAddress = fmt.Sprintf(":%d", port)
	if hostname, err := os.Hostname(); err == nil {
		s.selfAddress = fmt.Sprintf("%s:%d", hostname, port)
	}
	s.Search.SetSelfAddress(s.selfAddress)
}

// Helper methods to get config values from ProjectProperties
func (s *Service) getServiceName() string {
	if v, ok := s.Config.ProjectProperties["service_name"].(string); ok {
		return v
	}
	return "distcache"
}

func (s *Service) getServiceVersion() string {
	if v, ok := s.Config.ProjectProperties["service_version"].(string); ok {
		return v
	}
	return "1.0.0"
}

func (s *Service) getPort() int {
	if v, ok := s.Config.ProjectProperties["search_port"].(int); ok {
		return v
	}
	return 42069
}

func (s *Service) getMaxPortRetries() int {
	if v, ok := s.Config.ProjectProperties["search_max_port_retries"].(int); ok {
		return v
	}
	return 10
}

func (s *Service) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	basePort := s.getPort()
	maxRetries := s.getMaxPortRetries()
	if maxRetries <= 0 {
		maxRetries = 1
	}

	// Try to find an available port
	var lis net.Listener
	var actualPort int
	var err error

	for i := 0; i < maxRetries; i++ {
		tryPort := basePort + i
		lis, err = net.Listen("tcp", fmt.Sprintf(":%d", tryPort))
		if err == nil {
			actualPort = tryPort
			if i > 0 {
				logs.Infof("port %d was taken, using port %d instead", basePort, actualPort)
			}
			break
		}
		logs.Infof("port %d is not available, trying next...", tryPort)
	}

	if lis == nil {
		return fmt.Errorf("failed to find available port after %d attempts (tried %d-%d): %w",
			maxRetries, basePort, basePort+maxRetries-1, err)
	}

	// Update our address now that we know the actual port
	s.updateSelfAddress(actualPort)

	s.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer()),
	)
	reflection.RegisterV1(s.grpcServer)
	pb.RegisterCacheServiceServer(s.grpcServer, s)

	// Start the search/discovery system
	logs.Infof("starting service discovery...")
	s.Search.Start(ctx)

	// Start gRPC server in goroutine
	errChan := make(chan error, 1)
	go func() {
		logs.Infof("starting gRPC server on port %d", actualPort)
		if err := s.grpcServer.Serve(lis); err != nil {
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

func (s *Service) shutdown(_ context.Context) error {
	// Stop the search system
	s.Search.Stop()

	// Gracefully stop gRPC server
	s.grpcServer.GracefulStop()
	return nil
}

// Broadcast implements the gRPC Broadcast RPC
// When a peer calls Broadcast, we register them in our registry (mutual registration)
func (s *Service) Broadcast(_ context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	// Register the caller if they provided their info
	if req.CallerAddress != "" && req.CallerAddress != s.selfAddress {
		name := req.CallerName
		if name == "" {
			name = req.CallerAddress
		}

		if !s.Registry.Exists(req.CallerAddress) {
			logs.Infof("registered peer from broadcast: %s at %s", name, req.CallerAddress)
		}
		s.Registry.Add(req.CallerAddress, name)
	}

	return &pb.BroadcastResponse{
		Name:    s.getServiceName(),
		Address: s.selfAddress,
		Version: s.getServiceVersion(),
	}, nil
}

// HealthCheck implements the gRPC HealthCheck RPC
func (s *Service) HealthCheck(_ context.Context, _ *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Healthy:          true,
		PeerCount:        int32(s.Registry.Count()),
		HealthyPeerCount: int32(len(s.Registry.GetHealthy())),
	}, nil
}

// SetCache implements the gRPC SetCache RPC
// When a client sets a cache entry, we store it locally and replicate to peers
func (s *Service) SetCache(ctx context.Context, req *pb.SetCacheRequest) (*pb.SetCacheResponse, error) {
	// Convert timestamp
	timestamp := time.Unix(0, req.TimestampUnixNano)
	if req.TimestampUnixNano == 0 {
		timestamp = time.Now()
	}

	// Store locally
	cacheSystem := cache.New(s.Config, req.Key, s.Cache)
	cacheSystem.CreateEntryWithTimestamp(req.Value, timestamp)

	logs.Infof("cached key %q (from_replication=%v)", req.Key, req.FromReplication)

	// If this is not from replication, replicate to all peers
	if !req.FromReplication {
		s.replicateToPeers(ctx, req.Key, req.Value, timestamp)
	}

	return &pb.SetCacheResponse{
		Success: true,
	}, nil
}

// GetCache implements the gRPC GetCache RPC
func (s *Service) GetCache(_ context.Context, req *pb.GetCacheRequest) (*pb.GetCacheResponse, error) {
	cacheSystem := cache.New(s.Config, req.Key, s.Cache)
	entries := cacheSystem.GetEntries()

	if len(entries) == 0 {
		return &pb.GetCacheResponse{
			Found:   false,
			Entries: nil,
		}, nil
	}

	pbEntries := make([]*pb.CacheEntry, 0, len(entries))
	for _, entry := range entries {
		pbEntries = append(pbEntries, &pb.CacheEntry{
			Value:             entry.Value,
			TimestampUnixNano: entry.Timestamp.UnixNano(),
		})
	}

	return &pb.GetCacheResponse{
		Found:   true,
		Entries: pbEntries,
	}, nil
}

// replicateToPeers sends the cache entry to all healthy peers
func (s *Service) replicateToPeers(ctx context.Context, key string, value *pb.CacheValue, timestamp time.Time) {
	peers := s.Registry.GetHealthy()
	if len(peers) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, peer := range peers {
		// Skip ourselves
		if peer.Address == s.selfAddress {
			continue
		}

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			if err := s.replicateToPeer(ctx, addr, key, value, timestamp); err != nil {
				logs.Warnf("failed to replicate to peer %s: %v", addr, err)
				s.Registry.MarkUnhealthy(addr)
			}
		}(peer.Address)
	}

	wg.Wait()
}

// replicateToPeer sends a cache entry to a single peer
func (s *Service) replicateToPeer(ctx context.Context, addr, key string, value *pb.CacheValue, timestamp time.Time) error {
	// Use a timeout for replication
	timeout := 5 * time.Second
	if v, ok := s.Config.ProjectProperties["search_timeout"].(time.Duration); ok {
		timeout = v
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	client := pb.NewCacheServiceClient(conn)
	_, err = client.SetCache(ctx, &pb.SetCacheRequest{
		Key:               key,
		Value:             value,
		TimestampUnixNano: timestamp.UnixNano(),
		FromReplication:   true, // Mark as replication to prevent re-replication
	})
	if err != nil {
		return fmt.Errorf("SetCache failed: %w", err)
	}

	logs.Infof("replicated key %q to peer %s", key, addr)
	return nil
}

// SyncCache implements the gRPC SyncCache RPC
// Returns all cache entries for a new peer to synchronize
func (s *Service) SyncCache(_ context.Context, _ *pb.SyncCacheRequest) (*pb.SyncCacheResponse, error) {
	allEntries := s.Cache.GetAllEntries()

	syncEntries := make([]*pb.SyncCacheEntry, 0, len(allEntries))
	totalEntries := 0

	for key, entries := range allEntries {
		pbEntries := make([]*pb.CacheEntry, 0, len(entries))
		for _, entry := range entries {
			pbEntries = append(pbEntries, &pb.CacheEntry{
				Value:             entry.Value,
				TimestampUnixNano: entry.Timestamp.UnixNano(),
			})
		}
		syncEntries = append(syncEntries, &pb.SyncCacheEntry{
			Key:    key,
			Values: pbEntries,
		})
		totalEntries += len(entries)
	}

	return &pb.SyncCacheResponse{
		Entries:      syncEntries,
		TotalKeys:    int32(len(allEntries)),
		TotalEntries: int32(totalEntries),
	}, nil
}

// SyncFromPeer requests the full cache from a peer and imports it
func (s *Service) SyncFromPeer(ctx context.Context, peerAddr string) error {
	timeout := 30 * time.Second // Longer timeout for full sync
	if v, ok := s.Config.ProjectProperties["search_timeout"].(time.Duration); ok {
		timeout = v * 6 // 6x normal timeout for sync
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := grpc.NewClient(peerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s: %w", peerAddr, err)
	}
	defer conn.Close()

	client := pb.NewCacheServiceClient(conn)
	resp, err := client.SyncCache(ctx, &pb.SyncCacheRequest{})
	if err != nil {
		return fmt.Errorf("SyncCache failed: %w", err)
	}

	// Import all entries
	imported := 0
	for _, syncEntry := range resp.Entries {
		for _, entry := range syncEntry.Values {
			timestamp := time.Unix(0, entry.TimestampUnixNano)
			s.Cache.ImportEntry(syncEntry.Key, entry.Value, timestamp)
			imported++
		}
	}

	logs.Infof("synced cache from peer %s: %d keys, %d entries", peerAddr, resp.TotalKeys, imported)
	return nil
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
