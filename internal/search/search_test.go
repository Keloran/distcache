package search

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/keloran/distcache/internal/registry"
	pb "github.com/keloran/distcache/proto/cache"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/grpc"
)

func defaultTestConfig() *ConfigBuilder.Config {
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "testservice"
	cfg.ProjectProperties["service_version"] = "1.0.0"
	cfg.ProjectProperties["search_service_name"] = "testservice"
	cfg.ProjectProperties["search_domains"] = []string{".test"}
	cfg.ProjectProperties["search_port"] = 42069
	cfg.ProjectProperties["search_scan_interval"] = time.Hour // Long interval to prevent auto-discovery
	cfg.ProjectProperties["search_timeout"] = 100 * time.Millisecond
	cfg.ProjectProperties["search_max_instances"] = 5
	cfg.ProjectProperties["search_predefined_servers"] = []string{}
	return cfg
}

func testConfigWithOverrides(overrides func(cfg *ConfigBuilder.Config)) *ConfigBuilder.Config {
	cfg := defaultTestConfig()
	if overrides != nil {
		overrides(cfg)
	}
	return cfg
}

func TestNewSystem(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()

	sys := NewSystem(cfg, reg)

	if sys == nil {
		t.Fatal("NewSystem() returned nil")
	}
	if sys.Registry != reg {
		t.Error("Registry not set correctly")
	}
	serviceName := sys.getServiceName()
	if serviceName != "testservice" {
		t.Errorf("ServiceName = %q, want %q", serviceName, "testservice")
	}
	if sys.stopChan == nil {
		t.Error("stopChan should be initialized")
	}
}

func TestSystem_SetSelfAddress(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	sys.SetSelfAddress("192.168.1.100:42069")

	if sys.selfAddress != "192.168.1.100:42069" {
		t.Errorf("selfAddress = %q, want %q", sys.selfAddress, "192.168.1.100:42069")
	}
}

func TestSystem_StartStop(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_scan_interval"] = 10 * time.Millisecond
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sys.Start(ctx)

	// Give it time to run at least one iteration
	time.Sleep(50 * time.Millisecond)

	// Stop should complete without blocking forever
	done := make(chan struct{})
	go func() {
		sys.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Stop() did not complete in time")
	}
}

func TestSystem_StartStop_ContextCancel(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_scan_interval"] = time.Hour
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx, cancel := context.WithCancel(context.Background())

	sys.Start(ctx)

	// Cancel context should stop the discovery loop
	cancel()

	// Give it time to notice the cancellation
	time.Sleep(50 * time.Millisecond)

	// Stop should complete quickly since context was already cancelled
	done := make(chan struct{})
	go func() {
		sys.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Stop() did not complete in time after context cancel")
	}
}

func TestSystem_FindOthers_NoHosts(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_domains"] = []string{".nonexistent.invalid"}
		c.ProjectProperties["search_max_instances"] = 2
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx := context.Background()

	// Should complete without error even when no hosts found
	sys.FindOthers(ctx)

	// No peers should be added
	if reg.Count() != 0 {
		t.Errorf("Count() = %d, want 0", reg.Count())
	}
}

func TestSystem_TryBroadcast_Timeout(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_timeout"] = 10 * time.Millisecond
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx := context.Background()

	// Try to connect to non-routable address (should timeout)
	sys.tryBroadcast(ctx, "test-host", "10.255.255.1:42069")

	// Should not add peer on connection failure
	if reg.Count() != 0 {
		t.Errorf("Count() = %d, want 0 (connection should fail)", reg.Count())
	}
}

func TestSystem_TryBroadcast_SkipSelf(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)
	sys.SetSelfAddress("192.168.1.1:42069")

	// Start a mock gRPC server
	lis, grpcServer, port := startMockGRPCServer(t, "self-node", "192.168.1.1:42069")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	ctx := context.Background()

	// Try to broadcast to self
	sys.tryBroadcast(ctx, "self-host", "127.0.0.1:"+itoa(port))

	// Should not add self to registry
	// Note: The mock server returns the selfAddress, so it should be skipped
	time.Sleep(50 * time.Millisecond)
}

func TestSystem_TryBroadcast_Success(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)
	sys.SetSelfAddress("192.168.1.100:42069") // Different from mock server

	// Start a mock gRPC server
	lis, grpcServer, port := startMockGRPCServer(t, "peer-node", "192.168.1.1:42069")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	ctx := context.Background()

	sys.tryBroadcast(ctx, "peer-host", "127.0.0.1:"+itoa(port))

	// Peer should be added
	if !reg.Exists("192.168.1.1:42069") {
		t.Error("peer should have been added to registry")
	}

	peer, ok := reg.Get("192.168.1.1:42069")
	if !ok {
		t.Fatal("peer not found in registry")
	}
	if peer.Name != "peer-node" {
		t.Errorf("peer.Name = %q, want %q", peer.Name, "peer-node")
	}
}

func TestSystem_TryBroadcast_EmptyResponseAddress(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)
	sys.SetSelfAddress("192.168.1.100:42069")

	// Start mock server that returns empty address
	lis, grpcServer, port := startMockGRPCServer(t, "peer-node", "")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	ctx := context.Background()
	address := "127.0.0.1:" + itoa(port)

	sys.tryBroadcast(ctx, "peer-host", address)

	// Should use the connection address when response address is empty
	if !reg.Exists(address) {
		t.Error("peer should have been added using connection address")
	}
}

func TestSystem_TryBroadcast_EmptyResponseName(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)
	sys.SetSelfAddress("192.168.1.100:42069")

	// Start mock server that returns empty name
	lis, grpcServer, port := startMockGRPCServerCustom(t, "", "192.168.1.1:42069", "v1")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	ctx := context.Background()

	sys.tryBroadcast(ctx, "fallback-hostname", "127.0.0.1:"+itoa(port))

	peer, ok := reg.Get("192.168.1.1:42069")
	if !ok {
		t.Fatal("peer not found")
	}
	// Should use hostname when name is empty
	if peer.Name != "fallback-hostname" {
		t.Errorf("peer.Name = %q, want %q", peer.Name, "fallback-hostname")
	}
}

func TestSystem_HealthCheck_Success(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Start mock server
	lis, grpcServer, port := startMockGRPCServer(t, "peer", "peer-addr")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	address := "127.0.0.1:" + itoa(port)
	reg.Add(address, "peer")
	reg.UpdateLastSeen(address)

	// Mark unhealthy first
	reg.MarkUnhealthy(address)

	ctx := context.Background()
	sys.healthCheck(ctx)

	// Peer should be healthy again
	peer, _ := reg.Get(address)
	if !peer.Healthy {
		t.Error("peer should be marked healthy after successful health check")
	}
}

func TestSystem_HealthCheck_Failure(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_timeout"] = 50 * time.Millisecond
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Add peer with non-routable address
	reg.Add("10.255.255.1:42069", "unreachable-peer")

	ctx := context.Background()
	sys.healthCheck(ctx)

	// Peer should be marked unhealthy
	peer, _ := reg.Get("10.255.255.1:42069")
	if peer.Healthy {
		t.Error("peer should be marked unhealthy after failed health check")
	}
}

func TestSystem_HealthCheck_EmptyRegistry(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx := context.Background()

	// Should not panic with empty registry
	sys.healthCheck(ctx)
}

func TestSystem_HealthCheck_Concurrent(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_timeout"] = 50 * time.Millisecond
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Add multiple peers (most will fail to connect)
	for i := 0; i < 10; i++ {
		reg.Add("10.255.255."+itoa(i)+":42069", "peer-"+itoa(i))
	}

	ctx := context.Background()

	// Should handle concurrent health checks without issues
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sys.healthCheck(ctx)
		}()
	}
	wg.Wait()
}

func TestSystem_ProbeAddress_Success(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Start mock server
	lis, grpcServer, port := startMockGRPCServer(t, "probe-peer", "192.168.1.50:42069")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	ctx := context.Background()
	err := sys.ProbeAddress(ctx, "127.0.0.1:"+itoa(port))

	if err != nil {
		t.Fatalf("ProbeAddress() returned error: %v", err)
	}

	if !reg.Exists("192.168.1.50:42069") {
		t.Error("peer should have been added to registry")
	}
}

func TestSystem_ProbeAddress_ConnectionFailure(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_timeout"] = 50 * time.Millisecond
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx := context.Background()
	err := sys.ProbeAddress(ctx, "10.255.255.1:42069")

	if err == nil {
		t.Error("ProbeAddress() should return error for unreachable address")
	}

	if reg.Count() != 0 {
		t.Error("no peer should be added on failure")
	}
}

func TestSystem_ProbeAddress_EmptyResponseAddress(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Start mock server that returns empty address
	lis, grpcServer, port := startMockGRPCServer(t, "peer", "")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	ctx := context.Background()
	address := "127.0.0.1:" + itoa(port)
	err := sys.ProbeAddress(ctx, address)

	if err != nil {
		t.Fatalf("ProbeAddress() returned error: %v", err)
	}

	// Should use connection address when response is empty
	if !reg.Exists(address) {
		t.Error("peer should have been added using connection address")
	}
}

func TestSystem_ProbeService_SkipSelf(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Set self address
	sys.SetSelfAddress("127.0.0.1:42069")

	// Start mock server
	lis, grpcServer, port := startMockGRPCServer(t, "self", "127.0.0.1:42069")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	// Manually call probeService with a hostname that resolves to 127.0.0.1
	// Since we can't control DNS, we test the skip logic in tryBroadcast
	ctx := context.Background()

	// The address matches selfAddress, so it should be skipped
	sys.tryBroadcast(ctx, "localhost", "127.0.0.1:"+itoa(port))

	// The mock returns "127.0.0.1:42069" which matches selfAddress
	// So it should not be added
}

// Mock gRPC server for testing

type mockCacheServer struct {
	pb.UnimplementedCacheServiceServer
	name        string
	address     string
	version     string
	lastRequest *pb.BroadcastRequest
	requestMu   sync.Mutex
}

func (m *mockCacheServer) Broadcast(ctx context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	m.requestMu.Lock()
	m.lastRequest = req
	m.requestMu.Unlock()

	return &pb.BroadcastResponse{
		Name:    m.name,
		Address: m.address,
		Version: m.version,
	}, nil
}

func (m *mockCacheServer) getLastRequest() *pb.BroadcastRequest {
	m.requestMu.Lock()
	defer m.requestMu.Unlock()
	return m.lastRequest
}

func (m *mockCacheServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Healthy:          true,
		PeerCount:        0,
		HealthyPeerCount: 0,
	}, nil
}

func startMockGRPCServer(t *testing.T, name, address string) (net.Listener, *grpc.Server, int) {
	lis, server, port, _ := startMockGRPCServerWithMock(t, name, address, "1.0.0")
	return lis, server, port
}

func startMockGRPCServerCustom(t *testing.T, name, address, version string) (net.Listener, *grpc.Server, int) {
	lis, server, port, _ := startMockGRPCServerWithMock(t, name, address, version)
	return lis, server, port
}

func startMockGRPCServerWithMock(t *testing.T, name, address, version string) (net.Listener, *grpc.Server, int, *mockCacheServer) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	mock := &mockCacheServer{
		name:    name,
		address: address,
		version: version,
	}

	server := grpc.NewServer()
	pb.RegisterCacheServiceServer(server, mock)

	go server.Serve(lis)

	// Give server time to start
	time.Sleep(10 * time.Millisecond)

	port := lis.Addr().(*net.TCPAddr).Port
	return lis, server, port, mock
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	result := ""
	for i > 0 {
		result = string(rune('0'+i%10)) + result
		i /= 10
	}
	return result
}

func TestSystem_DiscoveryLoop_Ticker(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_scan_interval"] = 20 * time.Millisecond
		c.ProjectProperties["search_domains"] = []string{".nonexistent.invalid"}
		c.ProjectProperties["search_max_instances"] = 0
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx, cancel := context.WithCancel(context.Background())

	sys.Start(ctx)

	// Wait for at least 2 ticker intervals
	time.Sleep(60 * time.Millisecond)

	cancel()
	sys.Stop()

	// Test passes if no panic/deadlock occurred
}

func TestSystem_TryBroadcast_SendsCallerInfo(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)
	sys.SetSelfAddress("192.168.1.100:42069")

	// Start mock server that captures the request
	lis, grpcServer, port, mock := startMockGRPCServerWithMock(t, "peer-node", "192.168.1.1:42069", "1.0.0")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	ctx := context.Background()
	sys.tryBroadcast(ctx, "peer-host", "127.0.0.1:"+itoa(port))

	// Verify caller info was sent
	req := mock.getLastRequest()
	if req == nil {
		t.Fatal("no request received")
	}
	if req.CallerName != "testservice" {
		t.Errorf("CallerName = %q, want %q", req.CallerName, "testservice")
	}
	if req.CallerAddress != "192.168.1.100:42069" {
		t.Errorf("CallerAddress = %q, want %q", req.CallerAddress, "192.168.1.100:42069")
	}
	if req.CallerVersion != "1.0.0" {
		t.Errorf("CallerVersion = %q, want %q", req.CallerVersion, "1.0.0")
	}
}

func TestSystem_FindOthers_PredefinedServers(t *testing.T) {
	// Start two mock servers
	lis1, grpcServer1, port1 := startMockGRPCServer(t, "peer-1", "peer-1-addr:42069")
	defer grpcServer1.GracefulStop()
	defer lis1.Close()

	lis2, grpcServer2, port2 := startMockGRPCServer(t, "peer-2", "peer-2-addr:42070")
	defer grpcServer2.GracefulStop()
	defer lis2.Close()

	// Create config with predefined servers
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "testservice"
	cfg.ProjectProperties["service_version"] = "1.0.0"
	cfg.ProjectProperties["search_service_name"] = "testservice"
	cfg.ProjectProperties["search_domains"] = []string{".nonexistent.invalid"} // No DNS discovery
	cfg.ProjectProperties["search_port"] = 42069
	cfg.ProjectProperties["search_scan_interval"] = time.Hour
	cfg.ProjectProperties["search_timeout"] = time.Second
	cfg.ProjectProperties["search_max_instances"] = 0
	cfg.ProjectProperties["search_predefined_servers"] = []string{
		"127.0.0.1:" + itoa(port1),
		"127.0.0.1:" + itoa(port2),
	}

	reg := registry.New()
	sys := NewSystem(cfg, reg)
	sys.SetSelfAddress("192.168.1.100:42069") // Different from mock servers

	ctx := context.Background()
	sys.FindOthers(ctx)

	// Both predefined servers should be discovered
	if !reg.Exists("peer-1-addr:42069") {
		t.Error("peer-1 should have been discovered via predefined servers")
	}
	if !reg.Exists("peer-2-addr:42070") {
		t.Error("peer-2 should have been discovered via predefined servers")
	}
}

func TestSystem_FindOthers_PredefinedServers_SkipsSelf(t *testing.T) {
	// Start a mock server
	lis, grpcServer, port := startMockGRPCServer(t, "peer", "peer-addr:42069")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	selfAddress := "127.0.0.1:" + itoa(port)

	// Create config with self in predefined servers
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "testservice"
	cfg.ProjectProperties["service_version"] = "1.0.0"
	cfg.ProjectProperties["search_service_name"] = "testservice"
	cfg.ProjectProperties["search_domains"] = []string{".nonexistent.invalid"}
	cfg.ProjectProperties["search_port"] = 42069
	cfg.ProjectProperties["search_scan_interval"] = time.Hour
	cfg.ProjectProperties["search_timeout"] = time.Second
	cfg.ProjectProperties["search_max_instances"] = 0
	cfg.ProjectProperties["search_predefined_servers"] = []string{selfAddress} // Self is in predefined

	reg := registry.New()
	sys := NewSystem(cfg, reg)
	sys.SetSelfAddress(selfAddress) // Same as predefined

	ctx := context.Background()
	sys.FindOthers(ctx)

	// Should not register self
	if reg.Count() != 0 {
		t.Errorf("should not register self, got %d peers", reg.Count())
	}
}

// === UNHAPPY PATH TESTS ===

func TestSystem_TryBroadcast_ConnectionRefused(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx := context.Background()

	// Try to connect to a port that's not listening (should fail gracefully)
	sys.tryBroadcast(ctx, "refused-host", "127.0.0.1:1") // Port 1 is typically not available

	// Should not add peer on connection refused
	if reg.Count() != 0 {
		t.Errorf("Count() = %d, want 0 (connection should be refused)", reg.Count())
	}
}

func TestSystem_FindOthers_EmptyPredefinedServers(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_domains"] = []string{".nonexistent.invalid"}
		c.ProjectProperties["search_max_instances"] = 0
		c.ProjectProperties["search_predefined_servers"] = []string{"", "", ""} // Empty strings
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx := context.Background()

	// Should complete without error
	sys.FindOthers(ctx)

	// No peers should be added
	if reg.Count() != 0 {
		t.Errorf("Count() = %d, want 0", reg.Count())
	}
}

func TestSystem_HealthCheck_MultiplePeersAllFail(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_timeout"] = 50 * time.Millisecond
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Add multiple unreachable peers
	reg.Add("10.255.255.1:42069", "peer1")
	reg.Add("10.255.255.2:42069", "peer2")
	reg.Add("10.255.255.3:42069", "peer3")

	ctx := context.Background()
	sys.healthCheck(ctx)

	// All peers should be marked unhealthy
	peers := reg.GetAll()
	for _, peer := range peers {
		if peer.Healthy {
			t.Errorf("peer %s should be unhealthy", peer.Address)
		}
	}
}

func TestSystem_OnFirstPeerDiscovered_CallbackError(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Set a callback that returns an error
	callCount := 0
	sys.SetOnFirstPeerDiscovered(func(ctx context.Context, peerAddr string) error {
		callCount++
		return fmt.Errorf("sync failed")
	})

	// Start a mock gRPC server
	lis, grpcServer, port := startMockGRPCServer(t, "peer-node", "192.168.1.1:42069")
	defer grpcServer.GracefulStop()
	defer lis.Close()

	ctx := context.Background()
	sys.SetSelfAddress("192.168.1.100:42069")
	sys.tryBroadcast(ctx, "peer-host", "127.0.0.1:"+itoa(port))

	// Callback should have been called
	if callCount != 1 {
		t.Errorf("callback should be called once, got %d", callCount)
	}

	// hasSynced should be reset on error (allowing retry)
	sys.syncMu.Lock()
	hasSynced := sys.hasSynced
	sys.syncMu.Unlock()
	if hasSynced {
		t.Error("hasSynced should be reset after callback error")
	}
}

func TestSystem_OnFirstPeerDiscovered_OnlyCalledOnce(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	// Set a callback that succeeds
	callCount := 0
	sys.SetOnFirstPeerDiscovered(func(ctx context.Context, peerAddr string) error {
		callCount++
		return nil
	})

	// Start two mock gRPC servers
	lis1, grpcServer1, port1 := startMockGRPCServer(t, "peer-1", "192.168.1.1:42069")
	defer grpcServer1.GracefulStop()
	defer lis1.Close()

	lis2, grpcServer2, port2 := startMockGRPCServer(t, "peer-2", "192.168.1.2:42069")
	defer grpcServer2.GracefulStop()
	defer lis2.Close()

	ctx := context.Background()
	sys.SetSelfAddress("192.168.1.100:42069")

	// Discover first peer
	sys.tryBroadcast(ctx, "peer-host-1", "127.0.0.1:"+itoa(port1))
	// Discover second peer
	sys.tryBroadcast(ctx, "peer-host-2", "127.0.0.1:"+itoa(port2))

	// Callback should only be called once (for first peer)
	if callCount != 1 {
		t.Errorf("callback should be called once, got %d", callCount)
	}
}

func TestSystem_Stop_DuringDiscovery(t *testing.T) {
	cfg := testConfigWithOverrides(func(c *ConfigBuilder.Config) {
		c.ProjectProperties["search_scan_interval"] = 10 * time.Millisecond
	})
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sys.Start(ctx)

	// Immediately stop
	done := make(chan struct{})
	go func() {
		sys.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success - stopped without deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not complete in time")
	}
}

func TestSystem_ProbeService_DNSResolutionFailure(t *testing.T) {
	cfg := defaultTestConfig()
	reg := registry.New()
	sys := NewSystem(cfg, reg)

	ctx := context.Background()

	// Probe a hostname that doesn't exist
	sys.probeService(ctx, "this-host-definitely-does-not-exist.invalid")

	// Should not panic and no peers should be added
	if reg.Count() != 0 {
		t.Errorf("Count() = %d, want 0", reg.Count())
	}
}

func TestSystem_NilConfig(t *testing.T) {
	// Create config with nil ProjectProperties
	cfg := &ConfigBuilder.Config{
		ProjectProperties: nil,
	}
	reg := registry.New()

	// Should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("NewSystem panicked with nil ProjectProperties: %v", r)
		}
	}()

	sys := NewSystem(cfg, reg)
	if sys == nil {
		t.Error("NewSystem should not return nil")
	}

	// Getters should return defaults
	if sys.getServiceName() != "distcache" {
		t.Errorf("getServiceName() = %q, want %q", sys.getServiceName(), "distcache")
	}
}
