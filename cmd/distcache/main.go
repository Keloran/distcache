package main

import (
	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/keloran/distcache/internal"
	ConfigBuilder "github.com/keloran/go-config"
)

type ProjectConfig struct{}

func (pc ProjectConfig) Build(cfg *ConfigBuilder.Config) error {
	type PC struct {
	}
	p := PC{}

	if cfg.ProjectProperties == nil {
		cfg.ProjectProperties = make(ConfigBuilder.ProjectProperties)
	}
	cfg.ProjectProperties.Set("cacheConfig", p)
	cfg.ProjectProperties.Set("searchConfig", p)

	return nil
}

func main() {
	c := ConfigBuilder.NewConfigNoVault()
	if err := c.Build(
		ConfigBuilder.Local,
	); err != nil {
		logs.Fatalf("failed to build config: %v", err)
	}

	if err := internal.New(c).Start(); err != nil {
		logs.Fatalf("failed to start server: %v", err)
	}
}
