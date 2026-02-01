package internal

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/keloran/distcache/internal/cache"
	"github.com/keloran/distcache/internal/registry"
	"github.com/keloran/distcache/internal/search"
	pb "github.com/keloran/distcache/proto/cache"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

func stringValue(s string) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewStringValue(s)}
}

func intValue(i int64) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewNumberValue(float64(i))}
}

func floatValue(f float64) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewNumberValue(f)}
}

func boolValue(b bool) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewBoolValue(b)}
}

func bytesValue(b []byte) *pb.CacheValue {
	return &pb.CacheValue{Value: structpb.NewStringValue(string(b))}
}

func setupTestConfig() *ConfigBuilder.Config {
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "distcache"
	cfg.ProjectProperties["service_version"] = "1.0.0"
	cfg.ProjectProperties["search_service_name"] = "distcache"
	cfg.ProjectProperties["search_domains"] = []string{".internal"}
	cfg.ProjectProperties["search_port"] = 42069
	cfg.ProjectProperties["search_scan_interval"] = time.Hour
	cfg.ProjectProperties["search_timeout"] = 5 * time.Second
	cfg.ProjectProperties["search_max_instances"] = 100
	cfg.ProjectProperties["search_max_port_retries"] = 10
	cfg.ProjectProperties["search_predefined_servers"] = []string{}
	cfg.ProjectProperties["cache_ttl"] = 10 * time.Second
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
	if svc.Cache == nil {
		t.Error("Cache should be initialized")
	}
	// selfAddress is set during Start(), not New()
}

func TestService_UpdateSelfAddress(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	// Initially selfAddress should be empty
	if svc.selfAddress != "" {
		t.Errorf("selfAddress should be empty before Start(), got %q", svc.selfAddress)
	}

	// Manually call updateSelfAddress
	svc.updateSelfAddress(42069)

	// Now it should be set
	if svc.selfAddress == "" {
		t.Fatal("selfAddress is empty after updateSelfAddress")
	}

	// The address should contain the port
	_, port, err := net.SplitHostPort(svc.selfAddress)
	if err != nil {
		t.Fatalf("selfAddress %q is not a valid host:port: %v", svc.selfAddress, err)
	}
	if port != "42069" {
		t.Errorf("port = %q, want %q", port, "42069")
	}
}

func TestService_Broadcast(t *testing.T) {
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "test-cache"
	cfg.ProjectProperties["service_version"] = "1.2.3"
	cfg.ProjectProperties["search_service_name"] = "test-cache"
	cfg.ProjectProperties["search_domains"] = []string{".internal"}
	cfg.ProjectProperties["search_port"] = 42069
	cfg.ProjectProperties["search_scan_interval"] = time.Hour
	cfg.ProjectProperties["search_timeout"] = 5 * time.Second
	cfg.ProjectProperties["search_max_instances"] = 100
	cfg.ProjectProperties["search_max_port_retries"] = 10
	cfg.ProjectProperties["search_predefined_servers"] = []string{}

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
	if resp.Name != "distcache" {
		t.Errorf("Name = %q, want %q", resp.Name, "distcache")
	}
	if resp.Version != "1.0.0" {
		t.Errorf("Version = %q, want %q", resp.Version, "1.0.0")
	}
}

func TestService_Broadcast_RegistersCaller(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)
	svc.selfAddress = "myhost:42069"

	// Call Broadcast with caller info
	_, err := svc.Broadcast(context.Background(), &pb.BroadcastRequest{
		CallerName:    "remote-cache",
		CallerAddress: "192.168.1.50:42069",
		CallerVersion: "2.0.0",
	})

	if err != nil {
		t.Fatalf("Broadcast() returned error: %v", err)
	}

	// Caller should be registered
	if !svc.Registry.Exists("192.168.1.50:42069") {
		t.Error("caller should have been registered")
	}

	peer, ok := svc.Registry.Get("192.168.1.50:42069")
	if !ok {
		t.Fatal("peer not found in registry")
	}
	if peer.Name != "remote-cache" {
		t.Errorf("peer.Name = %q, want %q", peer.Name, "remote-cache")
	}
}

func TestService_Broadcast_SkipsSelf(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)
	svc.selfAddress = "myhost:42069"

	// Call Broadcast with our own address
	_, err := svc.Broadcast(context.Background(), &pb.BroadcastRequest{
		CallerName:    "self",
		CallerAddress: "myhost:42069",
		CallerVersion: "1.0.0",
	})

	if err != nil {
		t.Fatalf("Broadcast() returned error: %v", err)
	}

	// Should not register ourselves
	if svc.Registry.Exists("myhost:42069") {
		t.Error("should not register self")
	}
}

func TestService_Broadcast_EmptyCallerAddress(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	initialCount := svc.Registry.Count()

	// Call Broadcast without caller info (empty request)
	_, err := svc.Broadcast(context.Background(), &pb.BroadcastRequest{})

	if err != nil {
		t.Fatalf("Broadcast() returned error: %v", err)
	}

	// No peer should be added
	if svc.Registry.Count() != initialCount {
		t.Error("should not register peer with empty address")
	}
}

func TestService_Broadcast_UsesAddressAsNameFallback(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)
	svc.selfAddress = "myhost:42069"

	// Call Broadcast with address but no name
	_, err := svc.Broadcast(context.Background(), &pb.BroadcastRequest{
		CallerAddress: "192.168.1.100:42069",
	})

	if err != nil {
		t.Fatalf("Broadcast() returned error: %v", err)
	}

	peer, ok := svc.Registry.Get("192.168.1.100:42069")
	if !ok {
		t.Fatal("peer not found in registry")
	}
	// Should use address as name when name is empty
	if peer.Name != "192.168.1.100:42069" {
		t.Errorf("peer.Name = %q, want %q", peer.Name, "192.168.1.100:42069")
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

	// Create config with test settings
	testCfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	testCfg.ProjectProperties["service_name"] = "integration-test"
	testCfg.ProjectProperties["service_version"] = "test-version"
	testCfg.ProjectProperties["search_service_name"] = "integration-test"
	testCfg.ProjectProperties["search_domains"] = []string{".test"}
	testCfg.ProjectProperties["search_port"] = port
	testCfg.ProjectProperties["search_scan_interval"] = time.Hour // Long interval to avoid discovery during test
	testCfg.ProjectProperties["search_timeout"] = time.Second
	testCfg.ProjectProperties["search_max_instances"] = 1
	testCfg.ProjectProperties["search_max_port_retries"] = 1
	testCfg.ProjectProperties["search_predefined_servers"] = []string{}

	reg := registry.New()
	searchSystem := search.NewSystem(testCfg, reg)
	cacheSystem := cache.NewCache(10 * time.Second)

	svc := &Service{
		Config:      testCfg,
		Registry:    reg,
		Search:      searchSystem,
		Cache:       cacheSystem,
		selfAddress: "localhost:" + itoa(port),
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
	searchSystem := search.NewSystem(cfg, reg)
	cacheSystem := cache.NewCache(10 * time.Second)

	svc := &Service{
		Config:     cfg,
		Registry:   reg,
		Search:     searchSystem,
		Cache:      cacheSystem,
		grpcServer: grpc.NewServer(),
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

func TestService_PortCycling(t *testing.T) {
	// Occupy a port
	basePort := 43210
	lis, err := net.Listen("tcp", ":"+itoa(basePort))
	if err != nil {
		t.Skipf("could not occupy port %d for test: %v", basePort, err)
	}
	defer lis.Close()

	// Create config with the occupied port
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "distcache"
	cfg.ProjectProperties["service_version"] = "1.0.0"
	cfg.ProjectProperties["search_service_name"] = "distcache"
	cfg.ProjectProperties["search_domains"] = []string{".internal"}
	cfg.ProjectProperties["search_port"] = basePort
	cfg.ProjectProperties["search_scan_interval"] = time.Hour
	cfg.ProjectProperties["search_timeout"] = 5 * time.Second
	cfg.ProjectProperties["search_max_instances"] = 100
	cfg.ProjectProperties["search_max_port_retries"] = 5
	cfg.ProjectProperties["search_predefined_servers"] = []string{}

	svc := New(cfg)

	// Start in goroutine since Start() blocks
	errChan := make(chan error, 1)
	go func() {
		errChan <- svc.Start()
	}()

	// Give service time to start and find a port
	time.Sleep(100 * time.Millisecond)

	// Service should be running on a different port
	if svc.selfAddress == "" {
		t.Fatal("selfAddress should be set after Start()")
	}

	_, port, err := net.SplitHostPort(svc.selfAddress)
	if err != nil {
		t.Fatalf("invalid selfAddress: %v", err)
	}

	// Port should be basePort + 1 since basePort is occupied
	expectedPort := itoa(basePort + 1)
	if port != expectedPort {
		t.Errorf("port = %q, want %q (should have cycled from occupied port)", port, expectedPort)
	}

	// Cleanup
	svc.grpcServer.GracefulStop()
}

func TestService_PortCycling_AllPortsTaken(t *testing.T) {
	// Occupy multiple ports
	basePort := 43220
	maxRetries := 3
	listeners := make([]net.Listener, maxRetries)

	for i := 0; i < maxRetries; i++ {
		lis, err := net.Listen("tcp", ":"+itoa(basePort+i))
		if err != nil {
			// Clean up already-opened listeners
			for j := 0; j < i; j++ {
				listeners[j].Close()
			}
			t.Skipf("could not occupy port %d for test: %v", basePort+i, err)
		}
		listeners[i] = lis
	}
	defer func() {
		for _, lis := range listeners {
			if lis != nil {
				lis.Close()
			}
		}
	}()

	// Create config with the occupied port and limited retries
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "distcache"
	cfg.ProjectProperties["service_version"] = "1.0.0"
	cfg.ProjectProperties["search_service_name"] = "distcache"
	cfg.ProjectProperties["search_domains"] = []string{".internal"}
	cfg.ProjectProperties["search_port"] = basePort
	cfg.ProjectProperties["search_scan_interval"] = time.Hour
	cfg.ProjectProperties["search_timeout"] = 5 * time.Second
	cfg.ProjectProperties["search_max_instances"] = 100
	cfg.ProjectProperties["search_max_port_retries"] = maxRetries
	cfg.ProjectProperties["search_predefined_servers"] = []string{}

	svc := New(cfg)

	// Start should fail since all ports are taken
	err := svc.Start()
	if err == nil {
		svc.grpcServer.GracefulStop()
		t.Fatal("Start() should return error when all ports are taken")
	}
}

func TestService_SetCache(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()
	resp, err := svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "test-key",
		Value: stringValue("test-value"),
	})

	if err != nil {
		t.Fatalf("SetCache() returned error: %v", err)
	}
	if !resp.Success {
		t.Error("SetCache should return success=true")
	}

	// Verify the entry was stored
	getResp, err := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "test-key"})
	if err != nil {
		t.Fatalf("GetCache() returned error: %v", err)
	}
	if !getResp.Found {
		t.Error("GetCache should find the entry")
	}
	if len(getResp.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(getResp.Entries))
	}
	if getResp.Entries[0].Value.GetValue().GetStringValue() != "test-value" {
		t.Errorf("value = %q, want %q", getResp.Entries[0].Value.GetValue().GetStringValue(), "test-value")
	}
}

func TestService_SetCache_WithTimestamp(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	// Use a recent timestamp that won't be expired by TTL
	customTime := time.Now().Add(-1 * time.Second)
	ctx := context.Background()
	resp, err := svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:               "test-key",
		Value:             stringValue("test-value"),
		TimestampUnixNano: customTime.UnixNano(),
	})

	if err != nil {
		t.Fatalf("SetCache() returned error: %v", err)
	}
	if !resp.Success {
		t.Error("SetCache should return success=true")
	}

	// Verify the timestamp was preserved
	getResp, err := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "test-key"})
	if err != nil {
		t.Fatalf("GetCache() returned error: %v", err)
	}
	if len(getResp.Entries) == 0 {
		t.Fatal("expected at least 1 entry")
	}
	if getResp.Entries[0].TimestampUnixNano != customTime.UnixNano() {
		t.Errorf("timestamp = %d, want %d", getResp.Entries[0].TimestampUnixNano, customTime.UnixNano())
	}
}

func TestService_GetCache_NotFound(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()
	resp, err := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "missing-key"})

	if err != nil {
		t.Fatalf("GetCache() returned error: %v", err)
	}
	if resp.Found {
		t.Error("GetCache should return found=false for missing key")
	}
	if len(resp.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(resp.Entries))
	}
}

func TestService_SetCache_MultipleEntries(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()

	// Add multiple entries for the same key
	svc.SetCache(ctx, &pb.SetCacheRequest{Key: "test-key", Value: stringValue("first")})
	svc.SetCache(ctx, &pb.SetCacheRequest{Key: "test-key", Value: stringValue("second")})
	svc.SetCache(ctx, &pb.SetCacheRequest{Key: "test-key", Value: stringValue("third")})

	getResp, err := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "test-key"})
	if err != nil {
		t.Fatalf("GetCache() returned error: %v", err)
	}
	if len(getResp.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(getResp.Entries))
	}
}

func TestService_SetCache_FromReplication(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)
	svc.selfAddress = "localhost:42069"

	// Add a mock peer
	svc.Registry.Add("192.168.1.100:42069", "peer")

	ctx := context.Background()
	resp, err := svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:             "test-key",
		Value:           stringValue("replicated-value"),
		FromReplication: true, // Should not trigger re-replication
	})

	if err != nil {
		t.Fatalf("SetCache() returned error: %v", err)
	}
	if !resp.Success {
		t.Error("SetCache should return success=true")
	}

	// Entry should be stored
	getResp, _ := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "test-key"})
	if !getResp.Found {
		t.Error("entry should be stored")
	}
}

func TestService_SetCache_DifferentTypes(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()

	// Test string
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "string-key",
		Value: stringValue("hello"),
	})

	// Test int
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "int-key",
		Value: intValue(42),
	})

	// Test float
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "float-key",
		Value: floatValue(3.14),
	})

	// Test bool
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "bool-key",
		Value: boolValue(true),
	})

	// Test bytes (JSON)
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "json-key",
		Value: bytesValue([]byte(`{"name":"test"}`)),
	})

	// Verify each type
	stringResp, _ := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "string-key"})
	if stringResp.Entries[0].Value.GetValue().GetStringValue() != "hello" {
		t.Errorf("string value = %q, want %q", stringResp.Entries[0].Value.GetValue().GetStringValue(), "hello")
	}

	intResp, _ := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "int-key"})
	if intResp.Entries[0].Value.GetValue().GetNumberValue() != 42 {
		t.Errorf("int value = %v, want 42", intResp.Entries[0].Value.GetValue().GetNumberValue())
	}

	floatResp, _ := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "float-key"})
	if floatResp.Entries[0].Value.GetValue().GetNumberValue() != 3.14 {
		t.Errorf("float value = %f, want 3.14", floatResp.Entries[0].Value.GetValue().GetNumberValue())
	}

	boolResp, _ := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "bool-key"})
	if boolResp.Entries[0].Value.GetValue().GetBoolValue() != true {
		t.Errorf("bool value = %v, want true", boolResp.Entries[0].Value.GetValue().GetBoolValue())
	}

	jsonResp, _ := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "json-key"})
	if jsonResp.Entries[0].Value.GetValue().GetStringValue() != `{"name":"test"}` {
		t.Errorf("bytes value = %q, want %q", jsonResp.Entries[0].Value.GetValue().GetStringValue(), `{"name":"test"}`)
	}
}

func TestService_SyncCache_Empty(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()
	resp, err := svc.SyncCache(ctx, &pb.SyncCacheRequest{})

	if err != nil {
		t.Fatalf("SyncCache() returned error: %v", err)
	}
	if resp.TotalKeys != 0 {
		t.Errorf("TotalKeys = %d, want 0", resp.TotalKeys)
	}
	if resp.TotalEntries != 0 {
		t.Errorf("TotalEntries = %d, want 0", resp.TotalEntries)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("len(Entries) = %d, want 0", len(resp.Entries))
	}
}

func TestService_SyncCache_WithEntries(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()

	// Add some cache entries
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "key1",
		Value: stringValue("value1"),
	})
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "key2",
		Value: intValue(42),
	})
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "key1", // Second entry for key1
		Value: stringValue("value1b"),
	})

	resp, err := svc.SyncCache(ctx, &pb.SyncCacheRequest{})

	if err != nil {
		t.Fatalf("SyncCache() returned error: %v", err)
	}
	if resp.TotalKeys != 2 {
		t.Errorf("TotalKeys = %d, want 2", resp.TotalKeys)
	}
	if resp.TotalEntries != 3 {
		t.Errorf("TotalEntries = %d, want 3", resp.TotalEntries)
	}

	// Verify entries are present
	foundKey1 := false
	foundKey2 := false
	for _, entry := range resp.Entries {
		switch entry.Key {
		case "key1":
			foundKey1 = true
			if len(entry.Values) != 2 {
				t.Errorf("key1 should have 2 values, got %d", len(entry.Values))
			}
		case "key2":
			foundKey2 = true
			if len(entry.Values) != 1 {
				t.Errorf("key2 should have 1 value, got %d", len(entry.Values))
			}
		}
	}
	if !foundKey1 {
		t.Error("key1 not found in sync response")
	}
	if !foundKey2 {
		t.Error("key2 not found in sync response")
	}
}

func TestCache_ImportEntry(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	// Directly import an entry (simulating sync)
	timestamp := time.Now().Add(-5 * time.Second)
	svc.Cache.ImportEntry("imported-key", stringValue("imported-value"), timestamp)

	// Verify it was imported
	ctx := context.Background()
	resp, err := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "imported-key"})

	if err != nil {
		t.Fatalf("GetCache() returned error: %v", err)
	}
	if !resp.Found {
		t.Error("imported key should be found")
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(resp.Entries))
	}
	if resp.Entries[0].Value.GetValue().GetStringValue() != "imported-value" {
		t.Errorf("value = %q, want %q", resp.Entries[0].Value.GetValue().GetStringValue(), "imported-value")
	}
}

// === UNHAPPY PATH TESTS ===

func TestService_SyncFromPeer_ConnectionFailure(t *testing.T) {
	cfg := setupTestConfig()
	cfg.ProjectProperties["search_timeout"] = 100 * time.Millisecond // Short timeout for faster test
	svc := New(cfg)

	ctx := context.Background()
	// Try to sync from a non-existent peer
	err := svc.SyncFromPeer(ctx, "10.255.255.1:42069")

	if err == nil {
		t.Error("SyncFromPeer should return error for unreachable peer")
	}
}

func TestService_SetCache_NilValue(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()
	// SetCache with nil value should still work (stores nil)
	resp, err := svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "nil-key",
		Value: nil,
	})

	if err != nil {
		t.Fatalf("SetCache() returned error: %v", err)
	}
	if !resp.Success {
		t.Error("SetCache should succeed even with nil value")
	}
}

func TestService_SetCache_EmptyKey(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()
	// SetCache with empty key should still work
	resp, err := svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "",
		Value: stringValue("value"),
	})

	if err != nil {
		t.Fatalf("SetCache() returned error: %v", err)
	}
	if !resp.Success {
		t.Error("SetCache should succeed with empty key")
	}

	// Should be retrievable
	getResp, _ := svc.GetCache(ctx, &pb.GetCacheRequest{Key: ""})
	if !getResp.Found {
		t.Error("empty key should be retrievable")
	}
}

func TestService_GetCache_AfterTTLExpiry(t *testing.T) {
	// Create config with very short TTL
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "distcache"
	cfg.ProjectProperties["service_version"] = "1.0.0"
	cfg.ProjectProperties["cache_ttl"] = 50 * time.Millisecond

	svc := New(cfg)

	ctx := context.Background()
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "expiring-key",
		Value: stringValue("will-expire"),
	})

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// Entry should not be found
	resp, err := svc.GetCache(ctx, &pb.GetCacheRequest{Key: "expiring-key"})

	if err != nil {
		t.Fatalf("GetCache() returned error: %v", err)
	}
	if resp.Found {
		t.Error("expired entry should not be found")
	}
}

func TestService_SyncCache_AfterTTLExpiry(t *testing.T) {
	// Create config with very short TTL
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}
	cfg.ProjectProperties["service_name"] = "distcache"
	cfg.ProjectProperties["service_version"] = "1.0.0"
	cfg.ProjectProperties["cache_ttl"] = 50 * time.Millisecond

	svc := New(cfg)

	ctx := context.Background()
	svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "expiring-key",
		Value: stringValue("will-expire"),
	})

	// Wait for TTL to expire
	time.Sleep(60 * time.Millisecond)

	// SyncCache should not include expired entries
	resp, err := svc.SyncCache(ctx, &pb.SyncCacheRequest{})

	if err != nil {
		t.Fatalf("SyncCache() returned error: %v", err)
	}
	if resp.TotalKeys != 0 {
		t.Errorf("expired entries should not be in sync response, got %d keys", resp.TotalKeys)
	}
}

func TestService_ReplicateToPeers_NoHealthyPeers(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)
	svc.selfAddress = "localhost:42069"

	// Add peers but mark them unhealthy
	svc.Registry.Add("192.168.1.100:42069", "peer1")
	svc.Registry.Add("192.168.1.101:42069", "peer2")
	svc.Registry.MarkUnhealthy("192.168.1.100:42069")
	svc.Registry.MarkUnhealthy("192.168.1.101:42069")

	ctx := context.Background()
	// SetCache should succeed even with no healthy peers (just won't replicate)
	resp, err := svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "no-replicate-key",
		Value: stringValue("value"),
	})

	if err != nil {
		t.Fatalf("SetCache() returned error: %v", err)
	}
	if !resp.Success {
		t.Error("SetCache should succeed even without healthy peers")
	}
}

func TestService_ReplicateToPeers_UnreachablePeer(t *testing.T) {
	cfg := setupTestConfig()
	cfg.ProjectProperties["search_timeout"] = 100 * time.Millisecond // Short timeout
	svc := New(cfg)
	svc.selfAddress = "localhost:42069"

	// Add an unreachable peer
	svc.Registry.Add("10.255.255.1:42069", "unreachable-peer")

	ctx := context.Background()
	// SetCache should succeed locally even if replication fails
	resp, err := svc.SetCache(ctx, &pb.SetCacheRequest{
		Key:   "replicate-fail-key",
		Value: stringValue("value"),
	})

	if err != nil {
		t.Fatalf("SetCache() returned error: %v", err)
	}
	if !resp.Success {
		t.Error("SetCache should succeed locally even if replication fails")
	}

	// Peer should be marked unhealthy after failed replication
	time.Sleep(150 * time.Millisecond) // Wait for replication attempt
	peer, _ := svc.Registry.Get("10.255.255.1:42069")
	if peer.Healthy {
		t.Error("unreachable peer should be marked unhealthy after failed replication")
	}
}

func TestService_Broadcast_InvalidCallerAddress(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)
	svc.selfAddress = "localhost:42069"

	ctx := context.Background()
	// Broadcast with invalid address format should not crash
	resp, err := svc.Broadcast(ctx, &pb.BroadcastRequest{
		CallerName:    "peer",
		CallerAddress: "not-a-valid-address",
	})

	if err != nil {
		t.Fatalf("Broadcast() returned error: %v", err)
	}
	if resp == nil {
		t.Error("Broadcast should return response")
	}

	// The peer should be registered (we don't validate address format)
	if !svc.Registry.Exists("not-a-valid-address") {
		t.Error("peer should be registered even with unusual address")
	}
}

func TestService_HealthCheck_NoPeers(t *testing.T) {
	cfg := setupTestConfig()
	svc := New(cfg)

	ctx := context.Background()
	resp, err := svc.HealthCheck(ctx, &pb.HealthCheckRequest{})

	if err != nil {
		t.Fatalf("HealthCheck() returned error: %v", err)
	}
	if !resp.Healthy {
		t.Error("service should be healthy even with no peers")
	}
	if resp.PeerCount != 0 {
		t.Errorf("PeerCount = %d, want 0", resp.PeerCount)
	}
	if resp.HealthyPeerCount != 0 {
		t.Errorf("HealthyPeerCount = %d, want 0", resp.HealthyPeerCount)
	}
}

func TestService_New_DefaultTTL(t *testing.T) {
	// Config without cache_ttl should use default
	cfg := &ConfigBuilder.Config{
		ProjectProperties: make(ConfigBuilder.ProjectProperties),
	}

	svc := New(cfg)

	if svc.Cache == nil {
		t.Fatal("Cache should be initialized")
	}
	if svc.Cache.GetTTL() != 5*time.Minute {
		t.Errorf("Default TTL = %v, want 5m", svc.Cache.GetTTL())
	}
}
