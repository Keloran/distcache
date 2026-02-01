package internal

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/keloran/distcache/internal/config"
	"github.com/keloran/distcache/internal/registry"
	"github.com/keloran/distcache/internal/search"
	pb "github.com/keloran/distcache/proto/cache"
	ConfigBuilder "github.com/keloran/go-config"
	"google.golang.org/grpc"
)

type Service struct {
	pb.UnimplementedCacheServiceServer
	Config        *ConfigBuilder.Config
	ProjectConfig config.ProjectConfig
	Registry      *registry.Registry
	Search        *search.System
	grpcServer    *grpc.Server
	selfAddress   string
}

func New(cfg *ConfigBuilder.Config) *Service {
	pc := config.GetProjectConfig(cfg)
	reg := registry.New()
	searchSystem := search.NewSystem(cfg, reg)

	return &Service{
		Config:        cfg,
		ProjectConfig: pc,
		Registry:      reg,
		Search:        searchSystem,
	}
}

// updateSelfAddress updates the service's address and notifies the search system
func (s *Service) updateSelfAddress(port int) {
	s.selfAddress = fmt.Sprintf(":%d", port)
	if hostname, err := os.Hostname(); err == nil {
		s.selfAddress = fmt.Sprintf("%s:%d", hostname, port)
	}
	s.Search.SetSelfAddress(s.selfAddress)
}

func (s *Service) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	basePort := s.ProjectConfig.Search.Port
	maxRetries := s.ProjectConfig.Search.MaxPortRetries
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

	s.grpcServer = grpc.NewServer()
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
		Name:    s.ProjectConfig.Service.Name,
		Address: s.selfAddress,
		Version: s.ProjectConfig.Service.Version,
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
