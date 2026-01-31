package internal

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/keloran/distcache/internal/config"
	"github.com/keloran/distcache/internal/registry"
	"github.com/keloran/distcache/internal/search"
	pb "github.com/keloran/distcache/proto/cache"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupTestConfig() *ConfigBuilder.Config {
	// Clear env vars
	os.Unsetenv("SERVICE_NAME")
	os.Unsetenv("SERVICE_VERSION")
	os.Unsetenv("GRPC_PORT")

	cfg := &ConfigBuilder.Config{}
	c := config.Configurator{}
	c.Build(cfg)
	return cfg
}

func TestNew(t *testing.T) {
	cfg := setupTestConfig()

	svc := New(cfg)

	if svc == nil {
		t.Fatal("New() returned nil")
	}
	if svc.Config != cfg {
		t.Error("Config not set correctly")
	}
	if svc.Registry == nil {
		t.Error("Registry should be initialized")
	}
	if svc.Search == nil {
		t.Error("Search should be initialized")
	}
	if svc.selfAddress == "" {
		t.Error("selfAddress should be set")
	}
}

func TestNew_SelfAddress(t *testing.T) {
	cfg := setupTestConfig()

	svc := New(cfg)

	// selfAddress should contain the port
	pc := config.GetProjectConfig(cfg)
	expectedPort := pc.Search.Port

	// Check that selfAddress ends with the correct port
	if svc.selfAddress == "" {
		t.Fatal("selfAddress is empty")
	}

	// The address should contain the port
	_, port, err := net.SplitHostPort(svc.selfAddress)
	if err != nil {
		t.Fatalf("selfAddress %q is not a valid host:port: %v", svc.selfAddress, err)
	}
	if port != "42069" {
		t.Errorf("port = %q, want %q (from default port %d)", port, "42069", expectedPort)
	}
}

func TestService_Broadcast(t *testing.T) {
	// Set up custom config values
	os.Setenv("SERVICE_NAME", "test-cache")
	os.Setenv("SERVICE_VERSION", "1.2.3")
	defer func() {
		os.Unsetenv("SERVICE_NAME")
		os.Unsetenv("SERVICE_VERSION")
	}()

	cfg := &ConfigBuilder.Config{}
	config.Configurator{}.Build(cfg)

	svc := New(cfg)
	svc.selfAddress = "testhost:42069"

	resp, err := svc.Broadcast(context.Background(), &pb.BroadcastRequest{})

	if err != nil {
		t.Fatalf("Broadcast() returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("Broadcast() returned nil response")
	}
	if resp.Name != "test-cache" {
		t.Errorf("Name = %q, want %q", resp.Name, "test-cache")
	}
	if resp.Address != "testhost:42069" {
		t.Errorf("Address = %q, want %q", resp.Address, "testhost:42069")
	}
	if resp.Version != "1.2.3" {
		t.Errorf("Version = %q, want %q", resp.Version, "1.2.3")
	}
}

func TestService_Broadcast_DefaultValues(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	resp, err := svc.Broadcast(context.Background(), &pb.BroadcastRequest{})

	if err != nil {
		t.Fatalf("Broadcast() returned error: %v", err)
	}
	if resp.Name != config.DefaultServiceName {
		t.Errorf("Name = %q, want %q", resp.Name, config.DefaultServiceName)
	}
	if resp.Version != "1.0.0" {
		t.Errorf("Version = %q, want %q", resp.Version, "1.0.0")
	}
}

func TestService_HealthCheck(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	resp, err := svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})

	if err != nil {
		t.Fatalf("HealthCheck() returned error: %v", err)
	}
	if resp == nil {
		t.Fatal("HealthCheck() returned nil response")
	}
	if !resp.Healthy {
		t.Error("Healthy should be true")
	}
	if resp.PeerCount != 0 {
		t.Errorf("PeerCount = %d, want 0", resp.PeerCount)
	}
	if resp.HealthyPeerCount != 0 {
		t.Errorf("HealthyPeerCount = %d, want 0", resp.HealthyPeerCount)
	}
}

func TestService_HealthCheck_WithPeers(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	// Add some peers
	svc.Registry.Add("192.168.1.1:42069", "peer-1")
	svc.Registry.Add("192.168.1.2:42069", "peer-2")
	svc.Registry.Add("192.168.1.3:42069", "peer-3")

	// Mark one as unhealthy
	svc.Registry.MarkUnhealthy("192.168.1.2:42069")

	resp, err := svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})

	if err != nil {
		t.Fatalf("HealthCheck() returned error: %v", err)
	}
	if resp.PeerCount != 3 {
		t.Errorf("PeerCount = %d, want 3", resp.PeerCount)
	}
	if resp.HealthyPeerCount != 2 {
		t.Errorf("HealthyPeerCount = %d, want 2", resp.HealthyPeerCount)
	}
}

func TestService_HealthCheck_AllUnhealthy(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	svc.Registry.Add("192.168.1.1:42069", "peer-1")
	svc.Registry.Add("192.168.1.2:42069", "peer-2")
	svc.Registry.MarkUnhealthy("192.168.1.1:42069")
	svc.Registry.MarkUnhealthy("192.168.1.2:42069")

	resp, err := svc.HealthCheck(context.Background(), &pb.HealthCheckRequest{})

	if err != nil {
		t.Fatalf("HealthCheck() returned error: %v", err)
	}
	if resp.PeerCount != 2 {
		t.Errorf("PeerCount = %d, want 2", resp.PeerCount)
	}
	if resp.HealthyPeerCount != 0 {
		t.Errorf("HealthyPeerCount = %d, want 0", resp.HealthyPeerCount)
	}
}

func TestGetLocalIP(t *testing.T) {
	ip := GetLocalIP()

	// GetLocalIP can return empty string on some systems
	if ip != "" {
		// Verify it's a valid IPv4 address
		parsed := net.ParseIP(ip)
		if parsed == nil {
			t.Errorf("GetLocalIP() returned invalid IP: %q", ip)
		}
		if parsed.To4() == nil {
			t.Errorf("GetLocalIP() should return IPv4, got: %q", ip)
		}
		if parsed.IsLoopback() {
			t.Errorf("GetLocalIP() should not return loopback, got: %q", ip)
		}
	}
}

func TestService_GRPCServer_Integration(t *testing.T) {
	// Find an available port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()

	// Set up config with the available port
	os.Setenv("GRPC_PORT", string(rune(port)))
	os.Setenv("SERVICE_NAME", "integration-test")
	os.Setenv("SERVICE_VERSION", "test-version")
	defer func() {
		os.Unsetenv("GRPC_PORT")
		os.Unsetenv("SERVICE_NAME")
		os.Unsetenv("SERVICE_VERSION")
	}()

	// Create service with custom port
	cfg := &ConfigBuilder.Config{}
	config.Configurator{}.Build(cfg)

	// Manually create service components to avoid port conflicts
	reg := registry.New()
	searchCfg := config.SearchConfig{
		Port:         port,
		ServiceName:  "integration-test",
		Domains:      []string{".test"},
		ScanInterval: time.Hour, // Long interval to avoid discovery during test
		Timeout:      time.Second,
		MaxInstances: 1,
	}
	searchSystem := search.NewSystem(searchCfg, reg)

	svc := &Service{
		Config: cfg,
		ProjectConfig: config.ProjectConfig{
			Service: config.ServiceConfig{
				Name:    "integration-test",
				Version: "test-version",
			},
			Search: searchCfg,
		},
		Registry:    reg,
		Search:      searchSystem,
		selfAddress: "localhost:" + string(rune(port)),
	}

	// Start gRPC server
	lis, err = net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	actualPort := lis.Addr().(*net.TCPAddr).Port

	grpcServer := grpc.NewServer()
	pb.RegisterCacheServiceServer(grpcServer, svc)

	go func() {
		grpcServer.Serve(lis)
	}()
	defer grpcServer.GracefulStop()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Connect as client
	conn, err := grpc.NewClient(
		"localhost:"+itoa(actualPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewCacheServiceClient(conn)

	// Test Broadcast RPC
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	broadcastResp, err := client.Broadcast(ctx, &pb.BroadcastRequest{})
	if err != nil {
		t.Fatalf("Broadcast RPC failed: %v", err)
	}
	if broadcastResp.Name != "integration-test" {
		t.Errorf("Broadcast Name = %q, want %q", broadcastResp.Name, "integration-test")
	}

	// Test HealthCheck RPC
	healthResp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck RPC failed: %v", err)
	}
	if !healthResp.Healthy {
		t.Error("HealthCheck should return healthy=true")
	}
}

func itoa(i int) string {
	return string([]byte{
		byte('0' + i/10000%10),
		byte('0' + i/1000%10),
		byte('0' + i/100%10),
		byte('0' + i/10%10),
		byte('0' + i%10),
	})
}

func TestService_Shutdown(t *testing.T) {
	cfg := setupTestConfig()

	// Create a minimal service for shutdown test
	reg := registry.New()
	searchCfg := config.GetProjectConfig(cfg).Search
	searchCfg.ScanInterval = time.Hour // Long interval
	searchSystem := search.NewSystem(searchCfg, reg)

	svc := &Service{
		Config:        cfg,
		ProjectConfig: config.GetProjectConfig(cfg),
		Registry:      reg,
		Search:        searchSystem,
		grpcServer:    grpc.NewServer(),
	}

	// Start search system
	ctx := context.Background()
	svc.Search.Start(ctx)

	// Shutdown should not panic or error
	err := svc.shutdown(ctx)
	if err != nil {
		t.Errorf("shutdown() returned error: %v", err)
	}
}
