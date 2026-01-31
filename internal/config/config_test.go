package config

import (
	"os"
	"testing"
	"time"

	ConfigBuilder "github.com/keloran/go-config"
)

func TestConfigurator_Build_Defaults(t *testing.T) {
	// Clear any env vars that might interfere
	envVars := []string{
		"SERVICE_NAME", "SERVICE_VERSION", "SEARCH_SERVICE_NAME",
		"SEARCH_DOMAINS", "GRPC_PORT", "SEARCH_SCAN_INTERVAL",
		"SEARCH_TIMEOUT", "SEARCH_MAX_INSTANCES",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

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
	if pc.Search.Port != DefaultGRPCPort {
		t.Errorf("Search.Port = %d, want %d", pc.Search.Port, DefaultGRPCPort)
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
	if len(pc.Search.Domains) != 1 || pc.Search.Domains[0] != DefaultDomains {
		t.Errorf("Search.Domains = %v, want %v", pc.Search.Domains, []string{DefaultDomains})
	}
}

func TestConfigurator_Build_WithEnvVars(t *testing.T) {
	// Set custom env vars
	os.Setenv("SERVICE_NAME", "my-cache")
	os.Setenv("SERVICE_VERSION", "2.0.0")
	os.Setenv("SEARCH_SERVICE_NAME", "cache-search")
	os.Setenv("SEARCH_DOMAINS", ".local,.internal,.cluster")
	os.Setenv("GRPC_PORT", "50051")
	os.Setenv("SEARCH_SCAN_INTERVAL", "1m")
	os.Setenv("SEARCH_TIMEOUT", "10s")
	os.Setenv("SEARCH_MAX_INSTANCES", "50")

	defer func() {
		os.Unsetenv("SERVICE_NAME")
		os.Unsetenv("SERVICE_VERSION")
		os.Unsetenv("SEARCH_SERVICE_NAME")
		os.Unsetenv("SEARCH_DOMAINS")
		os.Unsetenv("GRPC_PORT")
		os.Unsetenv("SEARCH_SCAN_INTERVAL")
		os.Unsetenv("SEARCH_TIMEOUT")
		os.Unsetenv("SEARCH_MAX_INSTANCES")
	}()

	cfg := &ConfigBuilder.Config{}
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
}

func TestConfigurator_Build_InvalidEnvVars(t *testing.T) {
	// Set invalid values - should fall back to defaults
	os.Setenv("GRPC_PORT", "not-a-number")
	os.Setenv("SEARCH_SCAN_INTERVAL", "invalid-duration")
	os.Setenv("SEARCH_TIMEOUT", "bad")
	os.Setenv("SEARCH_MAX_INSTANCES", "abc")

	defer func() {
		os.Unsetenv("GRPC_PORT")
		os.Unsetenv("SEARCH_SCAN_INTERVAL")
		os.Unsetenv("SEARCH_TIMEOUT")
		os.Unsetenv("SEARCH_MAX_INSTANCES")
	}()

	cfg := &ConfigBuilder.Config{}
	c := Configurator{}

	err := c.Build(cfg)
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	pc := GetProjectConfig(cfg)

	// Should use defaults when parsing fails
	if pc.Search.Port != DefaultGRPCPort {
		t.Errorf("Search.Port = %d, want %d (default)", pc.Search.Port, DefaultGRPCPort)
	}
	if pc.Search.ScanInterval != DefaultScanInterval {
		t.Errorf("Search.ScanInterval = %v, want %v (default)", pc.Search.ScanInterval, DefaultScanInterval)
	}
	if pc.Search.Timeout != DefaultTimeout {
		t.Errorf("Search.Timeout = %v, want %v (default)", pc.Search.Timeout, DefaultTimeout)
	}
	if pc.Search.MaxInstances != DefaultMaxInstances {
		t.Errorf("Search.MaxInstances = %d, want %d (default)", pc.Search.MaxInstances, DefaultMaxInstances)
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

func TestGetEnv(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		envValue   string
		defaultVal string
		want       string
	}{
		{
			name:       "env var set",
			key:        "TEST_VAR_1",
			envValue:   "custom-value",
			defaultVal: "default",
			want:       "custom-value",
		},
		{
			name:       "env var not set",
			key:        "TEST_VAR_2",
			envValue:   "",
			defaultVal: "default",
			want:       "default",
		},
		{
			name:       "env var empty string",
			key:        "TEST_VAR_3",
			envValue:   "",
			defaultVal: "fallback",
			want:       "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Unsetenv(tt.key)
			if tt.envValue != "" {
				os.Setenv(tt.key, tt.envValue)
				defer os.Unsetenv(tt.key)
			}

			got := getEnv(tt.key, tt.defaultVal)
			if got != tt.want {
				t.Errorf("getEnv(%q, %q) = %q, want %q", tt.key, tt.defaultVal, got, tt.want)
			}
		})
	}
}

func TestGetEnvInt(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		envValue   string
		defaultVal int
		want       int
	}{
		{
			name:       "valid int",
			key:        "TEST_INT_1",
			envValue:   "12345",
			defaultVal: 0,
			want:       12345,
		},
		{
			name:       "invalid int",
			key:        "TEST_INT_2",
			envValue:   "not-a-number",
			defaultVal: 100,
			want:       100,
		},
		{
			name:       "not set",
			key:        "TEST_INT_3",
			envValue:   "",
			defaultVal: 42,
			want:       42,
		},
		{
			name:       "negative int",
			key:        "TEST_INT_4",
			envValue:   "-500",
			defaultVal: 0,
			want:       -500,
		},
		{
			name:       "zero",
			key:        "TEST_INT_5",
			envValue:   "0",
			defaultVal: 999,
			want:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Unsetenv(tt.key)
			if tt.envValue != "" {
				os.Setenv(tt.key, tt.envValue)
				defer os.Unsetenv(tt.key)
			}

			got := getEnvInt(tt.key, tt.defaultVal)
			if got != tt.want {
				t.Errorf("getEnvInt(%q, %d) = %d, want %d", tt.key, tt.defaultVal, got, tt.want)
			}
		})
	}
}

func TestGetEnvDuration(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		envValue   string
		defaultVal time.Duration
		want       time.Duration
	}{
		{
			name:       "valid duration seconds",
			key:        "TEST_DUR_1",
			envValue:   "30s",
			defaultVal: time.Minute,
			want:       30 * time.Second,
		},
		{
			name:       "valid duration minutes",
			key:        "TEST_DUR_2",
			envValue:   "5m",
			defaultVal: time.Second,
			want:       5 * time.Minute,
		},
		{
			name:       "invalid duration",
			key:        "TEST_DUR_3",
			envValue:   "invalid",
			defaultVal: 10 * time.Second,
			want:       10 * time.Second,
		},
		{
			name:       "not set",
			key:        "TEST_DUR_4",
			envValue:   "",
			defaultVal: time.Hour,
			want:       time.Hour,
		},
		{
			name:       "complex duration",
			key:        "TEST_DUR_5",
			envValue:   "1h30m45s",
			defaultVal: time.Second,
			want:       1*time.Hour + 30*time.Minute + 45*time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Unsetenv(tt.key)
			if tt.envValue != "" {
				os.Setenv(tt.key, tt.envValue)
				defer os.Unsetenv(tt.key)
			}

			got := getEnvDuration(tt.key, tt.defaultVal)
			if got != tt.want {
				t.Errorf("getEnvDuration(%q, %v) = %v, want %v", tt.key, tt.defaultVal, got, tt.want)
			}
		})
	}
}

func TestGetEnvSlice(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		envValue   string
		defaultVal []string
		want       []string
	}{
		{
			name:       "single value",
			key:        "TEST_SLICE_1",
			envValue:   ".internal",
			defaultVal: []string{".default"},
			want:       []string{".internal"},
		},
		{
			name:       "multiple values",
			key:        "TEST_SLICE_2",
			envValue:   ".local,.internal,.cluster",
			defaultVal: []string{".default"},
			want:       []string{".local", ".internal", ".cluster"},
		},
		{
			name:       "not set",
			key:        "TEST_SLICE_3",
			envValue:   "",
			defaultVal: []string{".fallback1", ".fallback2"},
			want:       []string{".fallback1", ".fallback2"},
		},
		{
			name:       "empty segments",
			key:        "TEST_SLICE_4",
			envValue:   "a,,b",
			defaultVal: []string{"default"},
			want:       []string{"a", "", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Unsetenv(tt.key)
			if tt.envValue != "" {
				os.Setenv(tt.key, tt.envValue)
				defer os.Unsetenv(tt.key)
			}

			got := getEnvSlice(tt.key, tt.defaultVal)
			if len(got) != len(tt.want) {
				t.Errorf("getEnvSlice(%q, %v) = %v, want %v", tt.key, tt.defaultVal, got, tt.want)
				return
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("getEnvSlice(%q, %v)[%d] = %q, want %q", tt.key, tt.defaultVal, i, got[i], tt.want[i])
				}
			}
		})
	}
}
