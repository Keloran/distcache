package config

import (
	"testing"
	"time"

	ConfigBuilder "github.com/keloran/go-config"
	"github.com/keloran/go-config/local"
)

func TestConfigurator_Build_Defaults(t *testing.T) {
	cfg := &ConfigBuilder.Config{}
	c := Configurator{}

	err := c.Build(cfg)
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	pc := GetProjectConfig(cfg)

	// Check service defaults
	if pc.Service.Name != DefaultServiceName {
		t.Errorf("Service.Name = %q, want %q", pc.Service.Name, DefaultServiceName)
	}
	if pc.Service.Version != "1.0.0" {
		t.Errorf("Service.Version = %q, want %q", pc.Service.Version, "1.0.0")
	}

	// Check search defaults
	if pc.Search.ServiceName != DefaultServiceName {
		t.Errorf("Search.ServiceName = %q, want %q", pc.Search.ServiceName, DefaultServiceName)
	}
	// Port comes from cfg.Local.GRPCPort, defaults to 3000 in go-config
	expectedPort := 3000
	if cfg.Local.GRPCPort != 0 {
		expectedPort = cfg.Local.GRPCPort
	}
	if pc.Search.Port != expectedPort {
		t.Errorf("Search.Port = %d, want %d", pc.Search.Port, expectedPort)
	}
	if pc.Search.ScanInterval != DefaultScanInterval {
		t.Errorf("Search.ScanInterval = %v, want %v", pc.Search.ScanInterval, DefaultScanInterval)
	}
	if pc.Search.Timeout != DefaultTimeout {
		t.Errorf("Search.Timeout = %v, want %v", pc.Search.Timeout, DefaultTimeout)
	}
	if pc.Search.MaxInstances != DefaultMaxInstances {
		t.Errorf("Search.MaxInstances = %d, want %d", pc.Search.MaxInstances, DefaultMaxInstances)
	}
	if pc.Search.MaxPortRetries != DefaultMaxPortRetries {
		t.Errorf("Search.MaxPortRetries = %d, want %d", pc.Search.MaxPortRetries, DefaultMaxPortRetries)
	}
	if len(pc.Search.Domains) != 1 || pc.Search.Domains[0] != DefaultDomains {
		t.Errorf("Search.Domains = %v, want %v", pc.Search.Domains, []string{DefaultDomains})
	}
	if len(pc.Search.PredefinedServers) != 0 {
		t.Errorf("Search.PredefinedServers = %v, want empty", pc.Search.PredefinedServers)
	}
}

func TestConfigurator_Build_WithProjectProperties(t *testing.T) {
	cfg := &ConfigBuilder.Config{
		Local: local.System{
			GRPCPort: 50051,
		},
		ProjectProperties: ConfigBuilder.ProjectProperties{
			"SERVICE_NAME":          "my-cache",
			"SERVICE_VERSION":       "2.0.0",
			"SEARCH_SERVICE_NAME":   "cache-search",
			"SEARCH_DOMAINS":        ".local,.internal,.cluster",
			"SEARCH_SCAN_INTERVAL":  "1m",
			"SEARCH_TIMEOUT":        "10s",
			"SEARCH_MAX_INSTANCES":  50,
			"MAX_PORT_RETRIES":      5,
			"PREDEFINED_SERVERS":    "localhost:42069,localhost:42070",
		},
	}
	c := Configurator{}

	err := c.Build(cfg)
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	pc := GetProjectConfig(cfg)

	if pc.Service.Name != "my-cache" {
		t.Errorf("Service.Name = %q, want %q", pc.Service.Name, "my-cache")
	}
	if pc.Service.Version != "2.0.0" {
		t.Errorf("Service.Version = %q, want %q", pc.Service.Version, "2.0.0")
	}
	if pc.Search.ServiceName != "cache-search" {
		t.Errorf("Search.ServiceName = %q, want %q", pc.Search.ServiceName, "cache-search")
	}
	if pc.Search.Port != 50051 {
		t.Errorf("Search.Port = %d, want %d", pc.Search.Port, 50051)
	}
	if pc.Search.ScanInterval != time.Minute {
		t.Errorf("Search.ScanInterval = %v, want %v", pc.Search.ScanInterval, time.Minute)
	}
	if pc.Search.Timeout != 10*time.Second {
		t.Errorf("Search.Timeout = %v, want %v", pc.Search.Timeout, 10*time.Second)
	}
	if pc.Search.MaxInstances != 50 {
		t.Errorf("Search.MaxInstances = %d, want %d", pc.Search.MaxInstances, 50)
	}
	if pc.Search.MaxPortRetries != 5 {
		t.Errorf("Search.MaxPortRetries = %d, want %d", pc.Search.MaxPortRetries, 5)
	}

	expectedDomains := []string{".local", ".internal", ".cluster"}
	if len(pc.Search.Domains) != len(expectedDomains) {
		t.Errorf("Search.Domains = %v, want %v", pc.Search.Domains, expectedDomains)
	} else {
		for i, d := range expectedDomains {
			if pc.Search.Domains[i] != d {
				t.Errorf("Search.Domains[%d] = %q, want %q", i, pc.Search.Domains[i], d)
			}
		}
	}

	expectedServers := []string{"localhost:42069", "localhost:42070"}
	if len(pc.Search.PredefinedServers) != len(expectedServers) {
		t.Errorf("Search.PredefinedServers = %v, want %v", pc.Search.PredefinedServers, expectedServers)
	}
}

func TestConfigurator_Build_InvalidDurationFallsBackToDefault(t *testing.T) {
	cfg := &ConfigBuilder.Config{
		ProjectProperties: ConfigBuilder.ProjectProperties{
			"SEARCH_SCAN_INTERVAL": "invalid-duration",
			"SEARCH_TIMEOUT":       "bad",
		},
	}
	c := Configurator{}

	err := c.Build(cfg)
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	pc := GetProjectConfig(cfg)

	// Should use defaults when parsing fails
	if pc.Search.ScanInterval != DefaultScanInterval {
		t.Errorf("Search.ScanInterval = %v, want %v (default)", pc.Search.ScanInterval, DefaultScanInterval)
	}
	if pc.Search.Timeout != DefaultTimeout {
		t.Errorf("Search.Timeout = %v, want %v (default)", pc.Search.Timeout, DefaultTimeout)
	}
}

func TestConfigurator_Build_SearchServiceNameFallsBackToServiceName(t *testing.T) {
	cfg := &ConfigBuilder.Config{
		ProjectProperties: ConfigBuilder.ProjectProperties{
			"SERVICE_NAME": "my-service",
			// SEARCH_SERVICE_NAME not set - should fall back to SERVICE_NAME
		},
	}
	c := Configurator{}

	err := c.Build(cfg)
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	pc := GetProjectConfig(cfg)

	if pc.Search.ServiceName != "my-service" {
		t.Errorf("Search.ServiceName = %q, want %q (should fall back to SERVICE_NAME)", pc.Search.ServiceName, "my-service")
	}
}

func TestGetProjectConfig_NilProjectProperties(t *testing.T) {
	cfg := &ConfigBuilder.Config{
		ProjectProperties: nil,
	}

	pc := GetProjectConfig(cfg)

	// Should return empty ProjectConfig
	if pc.Service.Name != "" {
		t.Errorf("Service.Name = %q, want empty string", pc.Service.Name)
	}
	if pc.Search.Port != 0 {
		t.Errorf("Search.Port = %d, want 0", pc.Search.Port)
	}
}

func TestGetProjectConfig_WrongType(t *testing.T) {
	cfg := &ConfigBuilder.Config{
		ProjectProperties: ConfigBuilder.ProjectProperties{
			"project": "not-a-project-config",
		},
	}

	pc := GetProjectConfig(cfg)

	// Should return empty ProjectConfig when type assertion fails
	if pc.Service.Name != "" {
		t.Errorf("Service.Name = %q, want empty string", pc.Service.Name)
	}
}

func TestGetProjectConfig_MissingKey(t *testing.T) {
	cfg := &ConfigBuilder.Config{
		ProjectProperties: ConfigBuilder.ProjectProperties{
			"other-key": "some-value",
		},
	}

	pc := GetProjectConfig(cfg)

	// Should return empty ProjectConfig when key doesn't exist
	if pc.Service.Name != "" {
		t.Errorf("Service.Name = %q, want empty string", pc.Service.Name)
	}
}

func TestConfigurator_Build_ZeroPortUsesDefault(t *testing.T) {
	cfg := &ConfigBuilder.Config{
		Local: local.System{
			GRPCPort: 0, // Zero should trigger default
		},
	}
	c := Configurator{}

	err := c.Build(cfg)
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	pc := GetProjectConfig(cfg)

	// Should use 3000 when GRPCPort is 0
	if pc.Search.Port != 3000 {
		t.Errorf("Search.Port = %d, want 3000", pc.Search.Port)
	}
}
