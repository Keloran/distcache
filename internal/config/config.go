package config

import (
	"strings"
	"time"

	ConfigBuilder "github.com/keloran/go-config"
)

const (
	DefaultServiceName    = "distcache"
	DefaultScanInterval   = 30 * time.Second
	DefaultTimeout        = 5 * time.Second
	DefaultMaxInstances   = 100
	DefaultDomains        = ".internal"
	DefaultMaxPortRetries = 10
)

type ProjectConfig struct {
	Service ServiceConfig
	Search  SearchConfig
}

type ServiceConfig struct {
	Name    string
	Version string
}

type SearchConfig struct {
	ServiceName       string
	Domains           []string
	Port              int
	ScanInterval      time.Duration
	Timeout           time.Duration
	MaxInstances      int
	MaxPortRetries    int      // How many ports to try if default is taken
	PredefinedServers []string // Specific addresses to probe (e.g., "localhost:42069,localhost:42070")
}

type Configurator struct{}

func (c Configurator) Build(cfg *ConfigBuilder.Config) error {
	if cfg.ProjectProperties == nil {
		cfg.ProjectProperties = make(ConfigBuilder.ProjectProperties)
	}

	// Get GRPC port from go-config's Local (defaults to 3000)
	grpcPort := cfg.Local.GRPCPort
	if grpcPort == 0 {
		grpcPort = 3000
	}

	// Service config
	serviceName := cfg.ProjectProperties.GetString("SERVICE_NAME")
	if serviceName == "" {
		serviceName = DefaultServiceName
	}

	serviceVersion := cfg.ProjectProperties.GetString("SERVICE_VERSION")
	if serviceVersion == "" {
		serviceVersion = "1.0.0"
	}

	// Search config
	searchServiceName := cfg.ProjectProperties.GetString("SEARCH_SERVICE_NAME")
	if searchServiceName == "" {
		searchServiceName = serviceName
	}

	domainsStr := cfg.ProjectProperties.GetString("SEARCH_DOMAINS")
	var domains []string
	if domainsStr != "" {
		domains = strings.Split(domainsStr, ",")
	} else {
		domains = []string{DefaultDomains}
	}

	scanInterval := DefaultScanInterval
	if intervalStr := cfg.ProjectProperties.GetString("SEARCH_SCAN_INTERVAL"); intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil {
			scanInterval = d
		}
	}

	timeout := DefaultTimeout
	if timeoutStr := cfg.ProjectProperties.GetString("SEARCH_TIMEOUT"); timeoutStr != "" {
		if d, err := time.ParseDuration(timeoutStr); err == nil {
			timeout = d
		}
	}

	maxInstances := DefaultMaxInstances
	if v, ok := cfg.ProjectProperties.GetValue("SEARCH_MAX_INSTANCES").(int); ok && v != 0 {
		maxInstances = v
	}

	maxPortRetries := DefaultMaxPortRetries
	if v, ok := cfg.ProjectProperties.GetValue("MAX_PORT_RETRIES").(int); ok && v != 0 {
		maxPortRetries = v
	}

	predefinedStr := cfg.ProjectProperties.GetString("PREDEFINED_SERVERS")
	var predefinedServers []string
	if predefinedStr != "" {
		predefinedServers = strings.Split(predefinedStr, ",")
	}

	pc := ProjectConfig{
		Service: ServiceConfig{
			Name:    serviceName,
			Version: serviceVersion,
		},
		Search: SearchConfig{
			ServiceName:       searchServiceName,
			Domains:           domains,
			Port:              grpcPort,
			ScanInterval:      scanInterval,
			Timeout:           timeout,
			MaxInstances:      maxInstances,
			MaxPortRetries:    maxPortRetries,
			PredefinedServers: predefinedServers,
		},
	}

	cfg.ProjectProperties.Set("project", pc)

	return nil
}

func GetProjectConfig(cfg *ConfigBuilder.Config) ProjectConfig {
	if cfg.ProjectProperties == nil {
		return ProjectConfig{}
	}

	if pc, ok := cfg.ProjectProperties["project"].(ProjectConfig); ok {
		return pc
	}

	return ProjectConfig{}
}
