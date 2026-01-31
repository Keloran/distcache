package main

import (
	"github.com/bugfixes/go-bugfixes/logs"
	"github.com/keloran/distcache/internal"
	"github.com/keloran/distcache/internal/config"
	ConfigBuilder "github.com/keloran/go-config"
)

func main() {
	c := ConfigBuilder.NewConfigNoVault()
	if err := c.Build(
		ConfigBuilder.Local,
		ConfigBuilder.WithProjectConfigurator(config.Configurator{}),
	); err != nil {
		logs.Fatalf("failed to build config: %v", err)
	}

	if err := internal.New(c).Start(); err != nil {
		logs.Fatalf("failed to start server: %v", err)
	}
}
