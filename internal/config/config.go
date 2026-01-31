package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	ConfigBuilder "github.com/keloran/go-config"
)

const (
	DefaultGRPCPort      = 42069
	DefaultServiceName   = "distcache"
	DefaultScanInterval  = 30 * time.Second
	DefaultTimeout       = 5 * time.Second
	DefaultMaxInstances  = 100
	DefaultDomains       = ".internal"
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
	ServiceName  string
	Domains      []string
	Port         int
	ScanInterval time.Duration
	Timeout      time.Duration
	MaxInstances int
}

type Configurator struct{}

func (c Configurator) Build(cfg *ConfigBuilder.Config) error {
	pc := ProjectConfig{
		Service: ServiceConfig{
			Name:    getEnv("SERVICE_NAME", DefaultServiceName),
			Version: getEnv("SERVICE_VERSION", "1.0.0"),
		},
		Search: SearchConfig{
			ServiceName:  getEnv("SEARCH_SERVICE_NAME", DefaultServiceName),
			Domains:      getEnvSlice("SEARCH_DOMAINS", []string{DefaultDomains}),
			Port:         getEnvInt("GRPC_PORT", DefaultGRPCPort),
			ScanInterval: getEnvDuration("SEARCH_SCAN_INTERVAL", DefaultScanInterval),
			Timeout:      getEnvDuration("SEARCH_TIMEOUT", DefaultTimeout),
			MaxInstances: getEnvInt("SEARCH_MAX_INSTANCES", DefaultMaxInstances),
		},
	}

	if cfg.ProjectProperties == nil {
		cfg.ProjectProperties = make(ConfigBuilder.ProjectProperties)
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

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}

func getEnvSlice(key string, defaultVal []string) []string {
	if val := os.Getenv(key); val != "" {
		return strings.Split(val, ",")
	}
	return defaultVal
}
