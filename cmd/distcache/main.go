package main

import (
	"time"

	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/caarlos0/env/v8"
	"github.com/keloran/distcache/internal"
	ConfigBuilder "github.com/keloran/go-config"
)

var (
	BuildVersion = "0.0.1"
	BuildHash    = "unknown"
	ServiceName  = "distcache"
)

type ProjectConfig struct{}

func (pc ProjectConfig) Build(cfg *ConfigBuilder.Config) error {
	type ServiceConfig struct {
		Name    string `env:"SERVICE_NAME" envDefault:"distcache"`
		Version string `env:"SERVICE_VERSION" envDefault:"1.0.0"`
	}

	type SearchConfig struct {
		ServiceName             string        `env:"SERVICE_NAME" envDefault:"distcache"`
		Domains                 []string      `env:"SERVICE_DOMAINS" envSeparator:"," envDefault:".internal"`
		Port                    int           `env:"SERVICE_PORT" envDefault:"42069"`
		PortRangeEnd            int           `env:"SERVICE_PORT_RANGE_END" envDefault:"0"` // 0 means use Port + 10
		ScanInterval            time.Duration `env:"SERVICE_SCAN_INTERVAL" envDefault:"30s"`
		Timeout                 time.Duration `env:"SERVICE_TIMEOUT" envDefault:"5s"`
		MaxInstances            int           `env:"SERVICE_MAX_INSTANCES" envDefault:"100"`
		MaxPortRetries          int           `env:"PORT_RETRIES" envDefault:"10"`
		PredefinedServers       []string      `env:"PREDEFINED_SERVERS" envSeparator:","`
		IPRanges                []string      `env:"SEARCH_IP_RANGES" envSeparator:","`
		ScanOwnRange            bool          `env:"SEARCH_SCAN_OWN_RANGE" envDefault:"false"`
		ActiveDiscoveryDuration time.Duration `env:"SEARCH_ACTIVE_DISCOVERY_DURATION" envDefault:"1m"`
	}

	type CacheConfig struct {
		TTL time.Duration `env:"CACHE_TTL" envDefault:"5m"`
	}

	var service ServiceConfig
	if err := env.Parse(&service); err != nil {
		return logs.Errorf("failed to parse service config: %v", err)
	}

	var search SearchConfig
	if err := env.Parse(&search); err != nil {
		return logs.Errorf("failed to parse search config: %v", err)
	}

	var cacheConf CacheConfig
	if err := env.Parse(&cacheConf); err != nil {
		return logs.Errorf("failed to parse cache config: %v", err)
	}

	if cfg.ProjectProperties == nil {
		cfg.ProjectProperties = make(ConfigBuilder.ProjectProperties)
	}

	// Service properties
	cfg.ProjectProperties["service_name"] = service.Name
	cfg.ProjectProperties["service_version"] = service.Version

	// Search properties
	cfg.ProjectProperties["search_service_name"] = search.ServiceName
	cfg.ProjectProperties["search_domains"] = search.Domains
	cfg.ProjectProperties["search_port"] = search.Port
	cfg.ProjectProperties["search_scan_interval"] = search.ScanInterval
	cfg.ProjectProperties["search_timeout"] = search.Timeout
	cfg.ProjectProperties["search_max_instances"] = search.MaxInstances
	cfg.ProjectProperties["search_max_port_retries"] = search.MaxPortRetries
	cfg.ProjectProperties["search_predefined_servers"] = search.PredefinedServers
	cfg.ProjectProperties["search_ip_ranges"] = search.IPRanges
	cfg.ProjectProperties["search_scan_own_range"] = search.ScanOwnRange
	cfg.ProjectProperties["search_active_discovery_duration"] = search.ActiveDiscoveryDuration
	if search.PortRangeEnd > 0 {
		cfg.ProjectProperties["search_port_range_end"] = search.PortRangeEnd
	}

	// Cache properties
	cfg.ProjectProperties["cache_ttl"] = cacheConf.TTL

	return nil
}

func main() {
	logs.Logf("Starting %s version %s (build %s)", ServiceName, BuildVersion, BuildHash)

	c := ConfigBuilder.NewConfigNoVault()
	if err := c.Build(
		ConfigBuilder.Local,
		ConfigBuilder.WithProjectConfigurator(ProjectConfig{}),
	); err != nil {
		logs.Fatalf("failed to build config: %v", err)
	}

	if err := internal.New(c).Start(); err != nil {
		logs.Fatalf("failed to start server: %v", err)
	}
}
