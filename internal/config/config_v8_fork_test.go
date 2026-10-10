package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestV8MigrationKeepsForkRoots(t *testing.T) {
	legacy := []byte(`port: 8317
usage-statistics-enabled: true
usage-statistics-persistence:
  enabled: true
  postgres-dsn: "postgres://user:pass@127.0.0.1:5432/cliproxy"
model-prices:
  claude-opus-5:
    prompt: 5
    completion: 25
    cache: 0.5
claude-1m-context:
  enabled: true
  models:
    - claude-opus-4-6
`)
	var doc yaml.Node
	if err := yaml.Unmarshal(legacy, &doc); err != nil {
		t.Fatal(err)
	}
	if IsV8ConfigLayout(doc.Content[0]) {
		t.Fatal("fork roots alone marked a legacy document as v8")
	}

	migrated, _, err := NormalizeConfigLayout(legacy, true)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err = ValidateV8Config(migrated); err != nil {
		t.Fatalf("v8 validation rejected fork roots: %v", err)
	}
	cfg, err := ParseConfigBytes(migrated)
	if err != nil {
		t.Fatalf("parse migrated config: %v", err)
	}
	if !cfg.UsageStatisticsEnabled || !cfg.UsageStatisticsPersistence.Enabled || cfg.UsageStatisticsPersistence.PostgresDSN == "" {
		t.Fatalf("usage persistence lost during v8 migration: %+v", cfg.UsageStatisticsPersistence)
	}
	if got := cfg.ModelPrices["claude-opus-5"]; got.Prompt != 5 || got.Completion != 25 || got.Cache != 0.5 {
		t.Fatalf("model price lost during v8 migration: %+v", got)
	}
	if !cfg.Claude1MContext.Enabled || len(cfg.Claude1MContext.Models) != 1 {
		t.Fatalf("claude-1m-context lost during v8 migration: %+v", cfg.Claude1MContext)
	}
}
