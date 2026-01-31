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

	// Determine our own address using the gRPC port
	selfAddress := fmt.Sprintf(":%d", pc.Search.Port)
	if hostname, err := os.Hostname(); err == nil {
		selfAddress = fmt.Sprintf("%s:%d", hostname, pc.Search.Port)
	}

	searchSystem := search.NewSystem(pc.Search, reg)
	searchSystem.SetSelfAddress(selfAddress)

	return &Service{
		Config:        cfg,
		ProjectConfig: pc,
		Registry:      reg,
		Search:        searchSystem,
		selfAddress:   selfAddress,
	}
}

func (s *Service) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := s.ProjectConfig.Search.Port

	// Setup gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterCacheServiceServer(s.grpcServer, s)

	// Start the search/discovery system
	logs.Infof("starting service discovery...")
	s.Search.Start(ctx)

	// Start gRPC server in goroutine
	errChan := make(chan error, 1)
	go func() {
		logs.Infof("starting gRPC server on port %d", port)
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
func (s *Service) Broadcast(_ context.Context, _ *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
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
