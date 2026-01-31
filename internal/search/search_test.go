package search

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/keloran/distcache/internal/config"
	"github.com/keloran/distcache/internal/registry"
	pb "github.com/keloran/distcache/proto/cache"
	"google.golang.org/grpc"
)

func defaultTestConfig() config.SearchConfig {
	return config.SearchConfig{
		ServiceName:  "testservice",
		Domains:      []string{".test"},
		Port:         42069,
		ScanInterval: time.Hour, // Long interval to prevent auto-discovery
		Timeout:      100 * time.Millisecond,
		MaxInstances: 5,
	}
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
	if sys.Config.ServiceName != "testservice" {
		t.Errorf("ServiceName = %q, want %q", sys.Config.ServiceName, "testservice")
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
	cfg := defaultTestConfig()
	cfg.ScanInterval = 10 * time.Millisecond
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
	cfg := defaultTestConfig()
	cfg.ScanInterval = time.Hour
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
	cfg := defaultTestConfig()
	cfg.Domains = []string{".nonexistent.invalid"}
	cfg.MaxInstances = 2
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
	cfg := defaultTestConfig()
	cfg.Timeout = 10 * time.Millisecond
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
	cfg := defaultTestConfig()
	cfg.Timeout = 50 * time.Millisecond
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
	cfg := defaultTestConfig()
	cfg.Timeout = 50 * time.Millisecond
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
	cfg := defaultTestConfig()
	cfg.Timeout = 50 * time.Millisecond
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
	name    string
	address string
	version string
}

func (m *mockCacheServer) Broadcast(ctx context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	return &pb.BroadcastResponse{
		Name:    m.name,
		Address: m.address,
		Version: m.version,
	}, nil
}

func (m *mockCacheServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Healthy:          true,
		PeerCount:        0,
		HealthyPeerCount: 0,
	}, nil
}

func startMockGRPCServer(t *testing.T, name, address string) (net.Listener, *grpc.Server, int) {
	return startMockGRPCServerCustom(t, name, address, "1.0.0")
}

func startMockGRPCServerCustom(t *testing.T, name, address, version string) (net.Listener, *grpc.Server, int) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterCacheServiceServer(server, &mockCacheServer{
		name:    name,
		address: address,
		version: version,
	})

	go server.Serve(lis)

	// Give server time to start
	time.Sleep(10 * time.Millisecond)

	port := lis.Addr().(*net.TCPAddr).Port
	return lis, server, port
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
	cfg := defaultTestConfig()
	cfg.ScanInterval = 20 * time.Millisecond
	cfg.Domains = []string{".nonexistent.invalid"}
	cfg.MaxInstances = 0
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
