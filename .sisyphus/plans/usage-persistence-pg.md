# Usage Statistics PostgreSQL Persistence — Implementation Plan

**Created**: 2026-03-26
**Status**: Draft v2 — Revised per momus review
**Branch**: `feat/usage-pg-persistence`
**Scope**: Fork-only feature (upstream rejected)

---

## Executive Summary

CLIProxyAPI's usage statistics are 100% in-memory (`RequestStatistics` in `internal/usage/logger_plugin.go`). On restart, all data is lost. This plan adds PostgreSQL persistence as an async write-behind layer while keeping the existing in-memory stats as the live dashboard cache.

**Architecture**: Hybrid — in-memory `RequestStatistics` remains the source of truth for the dashboard. A new `PersistencePlugin` (implementing `coreusage.Plugin`) receives the same `Record` events via the existing plugin dispatch system and asynchronously writes them to PostgreSQL. On startup, the in-memory cache is restored from a PG snapshot + replay delta.

**Key files created**: 7 new files in `internal/usage/`
**Key files modified**: 4 existing files (`logger_plugin.go`, `logger_plugin_test.go`, `config.go`, `config.example.yaml`, `service.go`)
**Total estimated effort**: ~12-18 hours

---

## Phase 0: Ring Buffer Fix (Prerequisite)

**Goal**: Fix the unbounded `modelStats.Details` memory leak before adding persistence.
**Effort**: S (30 min)
**Dependencies**: None
**Commit**: `fix(usage): cap in-memory request details per model to prevent unbounded growth`

### Background

`internal/usage/logger_plugin.go:225`:
```go
modelStatsValue.Details = append(modelStatsValue.Details, detail)
```
This is append-only with no eviction. Over time, memory grows without bound.

### Step 0.1: Write test first

**File**: `internal/usage/logger_plugin_test.go` (modify — append after line 96)
**Package**: `usage` (white-box, same package)

Add test `TestModelDetailsRingBufferCap`:
```go
func TestModelDetailsRingBufferCap(t *testing.T) {
	stats := NewRequestStatistics()
	// Insert 250 records (exceeding default cap of 200)
	for i := 0; i < 250; i++ {
		stats.Record(context.Background(), coreusage.Record{
			APIKey:      "test-key",
			Model:       "gpt-4",
			RequestedAt: time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC),
			Detail:      coreusage.Detail{InputTokens: int64(i), TotalTokens: int64(i)},
		})
	}
	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-4"].Details
	if len(details) > DefaultMaxDetailsPerModel {
		t.Fatalf("details len = %d, want <= %d", len(details), DefaultMaxDetailsPerModel)
	}
	// Verify we kept the MOST RECENT entries (highest InputTokens)
	if details[0].Tokens.InputTokens != 50 {
		t.Fatalf("oldest retained detail InputTokens = %d, want 50", details[0].Tokens.InputTokens)
	}
	if details[len(details)-1].Tokens.InputTokens != 249 {
		t.Fatalf("newest detail InputTokens = %d, want 249", details[len(details)-1].Tokens.InputTokens)
	}
}
```

### Step 0.2: Add ring buffer constant and eviction logic

**File**: `internal/usage/logger_plugin.go` (modify)

1. Add constant after line 17 (before `var statisticsEnabled`):
   ```go
   // DefaultMaxDetailsPerModel is the maximum number of RequestDetail entries
   // retained per stats_key+model combination in memory.
   const DefaultMaxDetailsPerModel = 200
   ```

2. Modify `updateAPIStats` (line 215-226) — add eviction after the append:
   ```go
   func (s *RequestStatistics) updateAPIStats(stats *apiStats, model string, detail RequestDetail) {
       stats.TotalRequests++
       stats.TotalTokens += detail.Tokens.TotalTokens
       modelStatsValue, ok := stats.Models[model]
       if !ok {
           modelStatsValue = &modelStats{}
           stats.Models[model] = modelStatsValue
       }
       modelStatsValue.TotalRequests++
       modelStatsValue.TotalTokens += detail.Tokens.TotalTokens
       modelStatsValue.Details = append(modelStatsValue.Details, detail)
       // Evict oldest entries when cap is exceeded
       if len(modelStatsValue.Details) > DefaultMaxDetailsPerModel {
           excess := len(modelStatsValue.Details) - DefaultMaxDetailsPerModel
           copy(modelStatsValue.Details, modelStatsValue.Details[excess:])
           modelStatsValue.Details = modelStatsValue.Details[:DefaultMaxDetailsPerModel]
       }
   }
   ```

### Step 0.3: Verify

**MUST NOT**:
- Change `StatisticsSnapshot` or `MergeSnapshot` or `Snapshot()` structure
- Change the `RequestDetail` struct
- Add any config field for cap size yet (use constant)
- Touch any file outside `internal/usage/`

**Verification**:
```bash
go test -v ./internal/usage/ -run TestModelDetailsRingBufferCap
go test -v ./internal/usage/     # all existing tests still pass
go vet ./internal/usage/
```

---

## Phase 1: Config Struct Addition

**Goal**: Add `UsageStatisticsPersistence` config block.
**Effort**: S (30 min)
**Dependencies**: None (can run in parallel with Phase 0)
**Commit**: `feat(config): add usage-statistics-persistence config block for PG persistence`

### Step 1.1: Add config struct

**File**: `internal/config/config.go` (modify)

Add new struct definition (insert near other config structs like `PprofConfig`, `TLSConfig`):

```go
// UsagePersistenceConfig controls PostgreSQL-backed usage statistics persistence.
type UsagePersistenceConfig struct {
	// Enabled toggles whether usage data is persisted to PostgreSQL.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// PostgresDSN is the connection string for the usage database.
	PostgresDSN string `yaml:"postgres-dsn" json:"-"`
	// Schema is the PostgreSQL schema name. Empty means default (public).
	Schema string `yaml:"schema,omitempty" json:"schema,omitempty"`
	// RecentDetailsPerModel caps in-memory detail entries per stats_key+model.
	RecentDetailsPerModel int `yaml:"recent-details-per-model" json:"recent-details-per-model"`
	// RetentionDays controls how long detail rows are kept. 0 = forever.
	RetentionDays int `yaml:"retention-days" json:"retention-days"`
	// BatchSize is the number of detail rows to batch-insert at once.
	BatchSize int `yaml:"batch-size" json:"batch-size"`
	// SnapshotFlushIntervalSeconds controls periodic snapshot flush frequency.
	SnapshotFlushIntervalSeconds int `yaml:"snapshot-flush-interval-seconds" json:"snapshot-flush-interval-seconds"`
	// BootstrapOnStart controls whether to restore in-memory stats from PG on startup.
	BootstrapOnStart bool `yaml:"bootstrap-on-start" json:"bootstrap-on-start"`
}
```

Add field to `Config` struct (insert after `UsageStatisticsEnabled` at line 66):

```go
	// UsageStatisticsPersistence controls PostgreSQL-backed persistence for usage data.
	UsageStatisticsPersistence UsagePersistenceConfig `yaml:"usage-statistics-persistence" json:"usage-statistics-persistence"`
```

### Step 1.2: Add defaults

**File**: `internal/config/config.go` (modify — in `LoadConfigOptional`, after line 582)

Add after `cfg.UsageStatisticsEnabled = false`:
```go
	cfg.UsageStatisticsPersistence.RecentDetailsPerModel = 200
	cfg.UsageStatisticsPersistence.RetentionDays = 30
	cfg.UsageStatisticsPersistence.BatchSize = 100
	cfg.UsageStatisticsPersistence.SnapshotFlushIntervalSeconds = 15
	cfg.UsageStatisticsPersistence.BootstrapOnStart = true
```

### Step 1.3: Update config.example.yaml

**File**: `config.example.yaml` (modify — insert after `usage-statistics-enabled: false` at line 63)

```yaml
# PostgreSQL persistence for usage statistics (fork-only feature).
# Requires usage-statistics-enabled: true to have any effect.
# usage-statistics-persistence:
#   enabled: false
#   postgres-dsn: "postgres://user:pass@127.0.0.1:5432/cliproxy?sslmode=disable"
#   schema: ""
#   recent-details-per-model: 200
#   retention-days: 30
#   batch-size: 100
#   snapshot-flush-interval-seconds: 15
#   bootstrap-on-start: true
```

### Step 1.4: Verify

**MUST NOT**:
- Add any `json:"-"` to fields other than `PostgresDSN` (only DSN is sensitive)
- Add sanitize logic yet (keep simple for this phase)
- Add any env var detection
- Change any existing field

**Verification**:
```bash
go build ./...                   # compiles
go vet ./...                     # no warnings
go test ./internal/config/...    # existing config tests pass
```

---

## Phase 2: PG Store Layer (Database Operations)

**Goal**: Create the database access layer — connect, create schema, insert details, upsert snapshots, query for bootstrap, delete for retention. All methods are on a concrete `PGStore` struct that satisfies the `usageStore` interface (defined in Phase 3).
**Effort**: L (3-4 hours)
**Dependencies**: Phase 1 (needs config struct)
**Commit**: `feat(usage): add PostgreSQL store layer for usage statistics persistence`

### Step 2.1: Write store test file first (TDD)

**File**: `internal/usage/pg_store_test.go` (NEW)
**Package**: `usage`

This file tests SQL generation and helper logic without requiring a live PG instance.

```go
package usage

import (
	"testing"
	"time"
)

func TestBuildBatchInsertSQL(t *testing.T) {
	details := []pgDetail{
		{RecordedAt: time.Now(), StatsKey: "k1", Model: "m1", Failed: false, LatencyMs: 100},
		{RecordedAt: time.Now(), StatsKey: "k2", Model: "m2", Failed: true, LatencyMs: 200},
	}
	query, args := buildBatchInsertSQL("\"usage_request_details\"", details)
	// Verify query contains 2 value groups
	// Verify args length = 2 * 15 (15 columns per row)
	if len(args) != 30 {
		t.Fatalf("args len = %d, want 30", len(args))
	}
	if query == "" {
		t.Fatal("query is empty")
	}
}

func TestBuildBatchInsertSQLEmpty(t *testing.T) {
	query, args := buildBatchInsertSQL("\"usage_request_details\"", nil)
	if query != "" || args != nil {
		t.Fatal("expected empty query and nil args for empty input")
	}
}

func TestPGQuoteIdentifier(t *testing.T) {
	tests := []struct{ in, want string }{
		{"simple", `"simple"`},
		{`has"quote`, `"has""quote"`},
		{"cliproxy", `"cliproxy"`},
	}
	for _, tt := range tests {
		got := pgQuoteIdentifier(tt.in)
		if got != tt.want {
			t.Fatalf("pgQuoteIdentifier(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestPGFullTableName(t *testing.T) {
	// With schema
	got := pgFullTableName("myschema", "mytable")
	if got != `"myschema"."mytable"` {
		t.Fatalf("got %q, want %q", got, `"myschema"."mytable"`)
	}
	// Without schema
	got = pgFullTableName("", "mytable")
	if got != `"mytable"` {
		t.Fatalf("got %q, want %q", got, `"mytable"`)
	}
}
```

### Step 2.2: Create PG store implementation

**File**: `internal/usage/pg_store.go` (NEW)
**Package**: `usage`

Key types and functions:

```go
package usage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	log "github.com/sirupsen/logrus"
)

const (
	defaultDetailsTable = "usage_request_details"
	defaultStateTable   = "usage_state"
)

// pgDetail represents a single request detail row for PG insertion.
// This is the PG-facing struct, distinct from RequestDetail (the in-memory struct).
type pgDetail struct {
	RecordedAt      time.Time
	StatsKey        string
	APIKey          string
	Provider        string
	Model           string
	Source          string
	AuthID          string
	AuthIndex       string
	Failed          bool
	LatencyMs       int64
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
	CachedTokens    int64
	TotalTokens     int64
}

// PGStore handles all PostgreSQL operations for usage persistence.
// It implements the usageStore interface defined in persistence_plugin.go.
type PGStore struct {
	db           *sql.DB
	schema       string
	detailsTable string // fully qualified table name
	stateTable   string // fully qualified table name
}

// NewPGStore opens a connection to PostgreSQL and verifies reachability.
// Follows the pattern from internal/store/postgresstore.go:49-100.
func NewPGStore(ctx context.Context, dsn, schema string) (*PGStore, error) {
	trimmedDSN := strings.TrimSpace(dsn)
	if trimmedDSN == "" {
		return nil, fmt.Errorf("usage pg store: DSN is required")
	}
	db, err := sql.Open("pgx", trimmedDSN)
	if err != nil {
		return nil, fmt.Errorf("usage pg store: open database connection: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("usage pg store: ping database: %w", err)
	}
	return &PGStore{
		db:           db,
		schema:       schema,
		detailsTable: pgFullTableName(schema, defaultDetailsTable),
		stateTable:   pgFullTableName(schema, defaultStateTable),
	}, nil
}

// ensureSchema creates schema + tables if they don't exist.
func (s *PGStore) ensureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("usage pg store: not initialized")
	}
	if schema := strings.TrimSpace(s.schema); schema != "" {
		query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pgQuoteIdentifier(schema))
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("usage pg store: create schema: %w", err)
		}
	}
	// Create details table
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id              BIGSERIAL PRIMARY KEY,
			recorded_at     TIMESTAMPTZ NOT NULL,
			stats_key       TEXT NOT NULL,
			api_key         TEXT NOT NULL DEFAULT '',
			provider        TEXT NOT NULL DEFAULT '',
			model           TEXT NOT NULL,
			source          TEXT NOT NULL DEFAULT '',
			auth_id         TEXT NOT NULL DEFAULT '',
			auth_index      TEXT NOT NULL DEFAULT '',
			failed          BOOLEAN NOT NULL,
			latency_ms      BIGINT NOT NULL DEFAULT 0,
			input_tokens    BIGINT NOT NULL DEFAULT 0,
			output_tokens   BIGINT NOT NULL DEFAULT 0,
			reasoning_tokens BIGINT NOT NULL DEFAULT 0,
			cached_tokens   BIGINT NOT NULL DEFAULT 0,
			total_tokens    BIGINT NOT NULL DEFAULT 0,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, s.detailsTable)); err != nil {
		return fmt.Errorf("usage pg store: create details table: %w", err)
	}
	// Create indexes
	// Use a schema-safe index name: replace dots and quotes
	safePrefix := strings.NewReplacer("\"", "", ".", "_").Replace(s.detailsTable)
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS idx_%s_recorded ON %s(recorded_at)`,
		safePrefix, s.detailsTable,
	)); err != nil {
		return fmt.Errorf("usage pg store: create recorded_at index: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS idx_%s_key_model ON %s(stats_key, model, recorded_at DESC)`,
		safePrefix, s.detailsTable,
	)); err != nil {
		return fmt.Errorf("usage pg store: create key_model index: %w", err)
	}
	// Create state table
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name            TEXT PRIMARY KEY,
			snapshot        JSONB NOT NULL,
			last_detail_id  BIGINT NOT NULL DEFAULT 0,
			updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, s.stateTable)); err != nil {
		return fmt.Errorf("usage pg store: create state table: %w", err)
	}
	return nil
}

// insertDetails batch-inserts detail rows using a multi-value INSERT.
func (s *PGStore) insertDetails(ctx context.Context, details []pgDetail) error {
	if len(details) == 0 {
		return nil
	}
	query, args := buildBatchInsertSQL(s.detailsTable, details)
	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("usage pg store: batch insert: %w", err)
	}
	return nil
}

// upsertSnapshot saves the aggregated snapshot to the state table.
func (s *PGStore) upsertSnapshot(ctx context.Context, name string, snapshot StatisticsSnapshot, lastDetailID int64) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("usage pg store: marshal snapshot: %w", err)
	}
	query := fmt.Sprintf(`
		INSERT INTO %s (name, snapshot, last_detail_id, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (name)
		DO UPDATE SET snapshot = EXCLUDED.snapshot, last_detail_id = EXCLUDED.last_detail_id, updated_at = NOW()
	`, s.stateTable)
	_, err = s.db.ExecContext(ctx, query, name, data, lastDetailID)
	if err != nil {
		return fmt.Errorf("usage pg store: upsert snapshot: %w", err)
	}
	return nil
}

// loadSnapshot retrieves the latest snapshot from the state table.
// Returns zero-value snapshot and 0 if no row exists (NOT an error).
func (s *PGStore) loadSnapshot(ctx context.Context, name string) (StatisticsSnapshot, int64, error) {
	query := fmt.Sprintf(
		`SELECT snapshot, last_detail_id FROM %s WHERE name = $1`, s.stateTable,
	)
	var data []byte
	var lastID int64
	err := s.db.QueryRowContext(ctx, query, name).Scan(&data, &lastID)
	if err != nil {
		if err == sql.ErrNoRows {
			return StatisticsSnapshot{}, 0, nil
		}
		return StatisticsSnapshot{}, 0, fmt.Errorf("usage pg store: load snapshot: %w", err)
	}
	var snapshot StatisticsSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return StatisticsSnapshot{}, 0, fmt.Errorf("usage pg store: unmarshal snapshot: %w", err)
	}
	return snapshot, lastID, nil
}

// loadDetailsSince retrieves detail rows with id > afterID, ordered by id ASC.
func (s *PGStore) loadDetailsSince(ctx context.Context, afterID int64, limit int) ([]pgDetail, int64, error) {
	query := fmt.Sprintf(
		`SELECT id, recorded_at, stats_key, api_key, provider, model, source, auth_id, auth_index, failed, latency_ms, input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens FROM %s WHERE id > $1 ORDER BY id ASC LIMIT $2`,
		s.detailsTable,
	)
	rows, err := s.db.QueryContext(ctx, query, afterID, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("usage pg store: load details since: %w", err)
	}
	defer rows.Close()

	var details []pgDetail
	var maxID int64
	for rows.Next() {
		var d pgDetail
		var id int64
		if err := rows.Scan(&id, &d.RecordedAt, &d.StatsKey, &d.APIKey, &d.Provider, &d.Model, &d.Source, &d.AuthID, &d.AuthIndex, &d.Failed, &d.LatencyMs, &d.InputTokens, &d.OutputTokens, &d.ReasoningTokens, &d.CachedTokens, &d.TotalTokens); err != nil {
			return nil, 0, fmt.Errorf("usage pg store: scan detail row: %w", err)
		}
		details = append(details, d)
		if id > maxID {
			maxID = id
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("usage pg store: iterate detail rows: %w", err)
	}
	return details, maxID, nil
}

// deleteOlderThan removes detail rows older than the given duration.
func (s *PGStore) deleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE recorded_at < $1`, s.detailsTable,
	)
	result, err := s.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("usage pg store: delete older than: %w", err)
	}
	return result.RowsAffected()
}

// lastDetailID returns the maximum id from the details table.
func (s *PGStore) lastDetailID(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(
		`SELECT COALESCE(MAX(id), 0) FROM %s`, s.detailsTable,
	)
	var id int64
	if err := s.db.QueryRowContext(ctx, query).Scan(&id); err != nil {
		return 0, fmt.Errorf("usage pg store: last detail id: %w", err)
	}
	return id, nil
}

// close releases the database connection.
func (s *PGStore) close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// --- SQL helpers ---

func pgQuoteIdentifier(id string) string {
	return "\"" + strings.ReplaceAll(id, "\"", "\"\"") + "\""
}

func pgFullTableName(schema, table string) string {
	if strings.TrimSpace(schema) == "" {
		return pgQuoteIdentifier(table)
	}
	return pgQuoteIdentifier(schema) + "." + pgQuoteIdentifier(table)
}

// buildBatchInsertSQL generates a multi-row INSERT statement.
// Returns ("", nil) for empty input.
func buildBatchInsertSQL(tableName string, details []pgDetail) (string, []any) {
	if len(details) == 0 {
		return "", nil
	}
	const cols = 15
	var b strings.Builder
	b.WriteString(fmt.Sprintf(
		`INSERT INTO %s (recorded_at, stats_key, api_key, provider, model, source, auth_id, auth_index, failed, latency_ms, input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens) VALUES `,
		tableName,
	))
	args := make([]any, 0, len(details)*cols)
	for i, d := range details {
		if i > 0 {
			b.WriteByte(',')
		}
		base := i * cols
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8,
			base+9, base+10, base+11, base+12, base+13, base+14, base+15)
		args = append(args, d.RecordedAt, d.StatsKey, d.APIKey, d.Provider, d.Model,
			d.Source, d.AuthID, d.AuthIndex, d.Failed, d.LatencyMs,
			d.InputTokens, d.OutputTokens, d.ReasoningTokens, d.CachedTokens, d.TotalTokens)
	}
	return b.String(), args
}
```

### Step 2.3: Verify

**MUST NOT**:
- Import from `internal/store/` (separate concern, separate lifecycle)
- Use `PGSTORE_DSN` env var
- Add retry logic (caller handles graceful degradation)
- Use `pgxpool` (stay consistent with existing `database/sql` + `pgx/stdlib` pattern)
- Use `COPY` protocol (stay with standard SQL for simplicity and `database/sql` compatibility)

**Verification**:
```bash
go test -v ./internal/usage/ -run TestBuildBatchInsert
go test -v ./internal/usage/ -run TestPGQuoteIdentifier
go test -v ./internal/usage/ -run TestPGFullTableName
go build ./internal/usage/
go vet ./internal/usage/
```

---

## Phase 3: PersistencePlugin (The Core Plugin)

**Goal**: Implement `PersistencePlugin` that receives `Record` events and async-writes to PG. Define the `usageStore` interface that decouples the plugin from the concrete `PGStore`, enabling mock-based unit tests.
**Effort**: L (3-4 hours)
**Dependencies**: Phase 2 (needs pgStore + pgDetail type)
**Commit**: `feat(usage): add PersistencePlugin for async PG write-behind`

### Step 3.1: Write plugin test file first (TDD)

**File**: `internal/usage/persistence_plugin_test.go` (NEW)
**Package**: `usage`

Test the plugin's core logic using a `mockUsageStore` that implements the `usageStore` interface:

```go
package usage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// mockUsageStore implements usageStore for testing without a real PG connection.
type mockUsageStore struct {
	mu             sync.Mutex
	insertCount    int64
	insertedRows   []pgDetail
	snapshotCount  int64
	lastSnapshot   StatisticsSnapshot
	lastSnapshotID int64
	deletedCount   int64
	closed         atomic.Bool
}

func (m *mockUsageStore) insertDetails(_ context.Context, details []pgDetail) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insertCount++
	m.insertedRows = append(m.insertedRows, details...)
	return nil
}

func (m *mockUsageStore) upsertSnapshot(_ context.Context, _ string, snap StatisticsSnapshot, lastID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshotCount++
	m.lastSnapshot = snap
	m.lastSnapshotID = lastID
	return nil
}

func (m *mockUsageStore) lastDetailID(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.insertedRows)), nil
}

func (m *mockUsageStore) deleteOlderThan(_ context.Context, _ time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedCount++
	return 0, nil
}

func (m *mockUsageStore) loadSnapshot(_ context.Context, _ string) (StatisticsSnapshot, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSnapshot, m.lastSnapshotID, nil
}

func (m *mockUsageStore) loadDetailsSince(_ context.Context, _ int64, _ int) ([]pgDetail, int64, error) {
	return nil, 0, nil
}

func (m *mockUsageStore) close() error {
	m.closed.Store(true)
	return nil
}

func TestPersistencePluginConvertRecord(t *testing.T) {
	record := coreusage.Record{
		Provider:    "claude",
		Model:       "claude-sonnet-4",
		APIKey:      "key1",
		AuthID:      "auth1",
		AuthIndex:   "0",
		Source:      "user@example.com",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Latency:     1500 * time.Millisecond,
		Failed:      false,
		Detail: coreusage.Detail{
			InputTokens:  100,
			OutputTokens: 200,
			TotalTokens:  300,
		},
	}
	detail := recordToPGDetail(record, "POST /v1/chat/completions")
	if detail.StatsKey != "POST /v1/chat/completions" {
		t.Fatalf("StatsKey = %q, want %q", detail.StatsKey, "POST /v1/chat/completions")
	}
	if detail.LatencyMs != 1500 {
		t.Fatalf("LatencyMs = %d, want 1500", detail.LatencyMs)
	}
	if detail.Provider != "claude" {
		t.Fatalf("Provider = %q, want %q", detail.Provider, "claude")
	}
	if detail.TotalTokens != 300 {
		t.Fatalf("TotalTokens = %d, want 300", detail.TotalTokens)
	}
}

func TestPersistencePluginStopDrainsChannel(t *testing.T) {
	mock := &mockUsageStore{}
	plugin := NewPersistencePlugin(PersistencePluginConfig{
		Store:                mock,
		Stats:                NewRequestStatistics(),
		BatchSize:            10,
		FlushIntervalSeconds: 60, // long interval — we test drain, not timer
	})
	plugin.Start(context.Background())

	// Push 25 records via HandleUsage
	for i := 0; i < 25; i++ {
		plugin.HandleUsage(context.Background(), coreusage.Record{
			APIKey:      "test",
			Model:       "m1",
			RequestedAt: time.Now(),
			Detail:      coreusage.Detail{TotalTokens: 1},
		})
	}

	// Stop should drain all 25 records
	plugin.Stop()

	mock.mu.Lock()
	totalRows := len(mock.insertedRows)
	mock.mu.Unlock()

	if totalRows != 25 {
		t.Fatalf("drained rows = %d, want 25", totalRows)
	}
	if !mock.closed.Load() {
		t.Fatal("store was not closed")
	}
}

func TestPersistencePluginBatchFlush(t *testing.T) {
	mock := &mockUsageStore{}
	plugin := NewPersistencePlugin(PersistencePluginConfig{
		Store:                mock,
		Stats:                NewRequestStatistics(),
		BatchSize:            5,
		FlushIntervalSeconds: 60,
	})
	plugin.Start(context.Background())

	// Push exactly 5 records (= batchSize) — should trigger one batch insert
	for i := 0; i < 5; i++ {
		plugin.HandleUsage(context.Background(), coreusage.Record{
			APIKey:      "test",
			Model:       "m1",
			RequestedAt: time.Now(),
			Detail:      coreusage.Detail{TotalTokens: 1},
		})
	}

	// Give the batch worker time to process
	time.Sleep(100 * time.Millisecond)

	mock.mu.Lock()
	count := mock.insertCount
	mock.mu.Unlock()

	if count < 1 {
		t.Fatalf("insertCount = %d, want >= 1 (batch should have flushed)", count)
	}

	plugin.Stop()
}
```

### Step 3.2: Create PersistencePlugin with usageStore interface

**File**: `internal/usage/persistence_plugin.go` (NEW)
**Package**: `usage`

```go
package usage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const (
	defaultChannelBuffer = 4096
	snapshotStateName    = "default"
	flushTimeout         = 10 * time.Second
)

// usageStore abstracts PostgreSQL operations so PersistencePlugin can be tested
// with a mock. The concrete implementation is *PGStore (pg_store.go).
type usageStore interface {
	insertDetails(ctx context.Context, details []pgDetail) error
	upsertSnapshot(ctx context.Context, name string, snapshot StatisticsSnapshot, lastDetailID int64) error
	lastDetailID(ctx context.Context) (int64, error)
	deleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
	loadSnapshot(ctx context.Context, name string) (StatisticsSnapshot, int64, error)
	loadDetailsSince(ctx context.Context, afterID int64, limit int) ([]pgDetail, int64, error)
	close() error
}

// PersistencePlugin implements coreusage.Plugin and asynchronously persists
// usage records to PostgreSQL. It maintains a buffered channel as a write-ahead
// queue and a background goroutine that batch-inserts to PG.
type PersistencePlugin struct {
	store         usageStore
	stats         *RequestStatistics // reference to in-memory stats for snapshot export
	batchSize     int
	flushSecs     int
	retentionDays int
	ch            chan pgDetail
	stopOnce      sync.Once
	stopped       chan struct{} // closed when batchWorker exits
	cancel        context.CancelFunc
	running       atomic.Bool
}

// PersistencePluginConfig holds runtime parameters for the plugin.
type PersistencePluginConfig struct {
	Store                usageStore
	Stats                *RequestStatistics
	BatchSize            int
	FlushIntervalSeconds int
	RetentionDays        int
	ChannelBuffer        int
}

// NewPersistencePlugin creates a new persistence plugin. Call Start() to begin
// the background workers.
func NewPersistencePlugin(cfg PersistencePluginConfig) *PersistencePlugin {
	buf := cfg.ChannelBuffer
	if buf <= 0 {
		buf = defaultChannelBuffer
	}
	bs := cfg.BatchSize
	if bs <= 0 {
		bs = 100
	}
	fs := cfg.FlushIntervalSeconds
	if fs <= 0 {
		fs = 15
	}
	return &PersistencePlugin{
		store:         cfg.Store,
		stats:         cfg.Stats,
		batchSize:     bs,
		flushSecs:     fs,
		retentionDays: cfg.RetentionDays,
		ch:            make(chan pgDetail, buf),
		stopped:       make(chan struct{}),
	}
}

// HandleUsage implements coreusage.Plugin. It converts the record and enqueues
// it for async PG insertion. If the channel is full, the record is dropped with
// a warning log (graceful degradation — dashboard still works via in-memory stats).
func (p *PersistencePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if p == nil || !p.running.Load() {
		return
	}
	statsKey := record.APIKey
	if statsKey == "" {
		statsKey = resolveAPIIdentifier(ctx, record)
	}
	detail := recordToPGDetail(record, statsKey)
	select {
	case p.ch <- detail:
	default:
		log.Warn("usage persistence: channel full, dropping record")
	}
}

// Start launches the background batch-insert, snapshot-flush, and retention goroutines.
func (p *PersistencePlugin) Start(ctx context.Context) {
	if p == nil {
		return
	}
	workerCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.running.Store(true)
	go p.batchWorker(workerCtx)
	go p.snapshotWorker(workerCtx)
	if p.retentionDays > 0 {
		go p.retentionWorker(workerCtx)
	}
}

// Stop signals the background workers to drain and exit, then blocks until complete.
func (p *PersistencePlugin) Stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		p.running.Store(false)
		if p.cancel != nil {
			p.cancel()
		}
		<-p.stopped // wait for batchWorker to drain and exit

		// Final snapshot flush with a fresh context — the worker context is
		// already cancelled, so we must NOT reuse it for DB operations.
		flushCtx, flushCancel := context.WithTimeout(context.Background(), flushTimeout)
		defer flushCancel()
		p.flushSnapshot(flushCtx)

		if p.store != nil {
			_ = p.store.close()
		}
	})
}

// batchWorker reads from the channel and batch-inserts to PG.
func (p *PersistencePlugin) batchWorker(ctx context.Context) {
	defer close(p.stopped)
	batch := make([]pgDetail, 0, p.batchSize)
	ticker := time.NewTicker(time.Duration(p.flushSecs) * time.Second)
	defer ticker.Stop()

	// flush uses context.Background with a timeout instead of the worker ctx.
	// This is critical: during shutdown drain, the worker ctx is cancelled but
	// we still need to write the final batch(es) to PG.
	flush := func() {
		if len(batch) == 0 {
			return
		}
		flushCtx, flushCancel := context.WithTimeout(context.Background(), flushTimeout)
		defer flushCancel()
		if err := p.store.insertDetails(flushCtx, batch); err != nil {
			log.Warnf("usage persistence: batch insert failed: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case detail, ok := <-p.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, detail)
			if len(batch) >= p.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			// Drain remaining items from channel after context cancellation.
			for {
				select {
				case detail, ok := <-p.ch:
					if !ok {
						flush()
						return
					}
					batch = append(batch, detail)
					if len(batch) >= p.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// snapshotWorker periodically flushes the aggregated in-memory snapshot to PG.
func (p *PersistencePlugin) snapshotWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.flushSecs) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Use a fresh context with timeout for the DB operation.
			opCtx, opCancel := context.WithTimeout(context.Background(), flushTimeout)
			p.flushSnapshot(opCtx)
			opCancel()
		case <-ctx.Done():
			return
		}
	}
}

// flushSnapshot exports the current in-memory snapshot and upserts it to PG.
func (p *PersistencePlugin) flushSnapshot(ctx context.Context) {
	if p.stats == nil || p.store == nil {
		return
	}
	snapshot := p.stats.Snapshot()
	lastID, err := p.store.lastDetailID(ctx)
	if err != nil {
		log.Warnf("usage persistence: get last detail ID: %v", err)
		return
	}
	if err := p.store.upsertSnapshot(ctx, snapshotStateName, snapshot, lastID); err != nil {
		log.Warnf("usage persistence: snapshot flush: %v", err)
	}
}

// retentionWorker periodically deletes old detail rows.
func (p *PersistencePlugin) retentionWorker(ctx context.Context) {
	// Run once immediately on startup, then every 6 hours.
	p.runRetention()
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.runRetention()
		case <-ctx.Done():
			return
		}
	}
}

func (p *PersistencePlugin) runRetention() {
	dur := retentionDuration(p.retentionDays)
	if dur == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deleted, err := p.store.deleteOlderThan(ctx, dur)
	if err != nil {
		log.Warnf("usage persistence: retention cleanup failed: %v", err)
		return
	}
	if deleted > 0 {
		log.Infof("usage persistence: retention cleanup deleted %d old records", deleted)
	}
}

func retentionDuration(days int) time.Duration {
	if days <= 0 {
		return 0
	}
	return time.Duration(days) * 24 * time.Hour
}

// recordToPGDetail converts a coreusage.Record to a pgDetail.
func recordToPGDetail(record coreusage.Record, statsKey string) pgDetail {
	ts := record.RequestedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	tokens := normaliseDetail(record.Detail)
	return pgDetail{
		RecordedAt:      ts,
		StatsKey:        statsKey,
		APIKey:          record.APIKey,
		Provider:        record.Provider,
		Model:           record.Model,
		Source:          record.Source,
		AuthID:          record.AuthID,
		AuthIndex:       record.AuthIndex,
		Failed:          record.Failed,
		LatencyMs:       normaliseLatency(record.Latency),
		InputTokens:     tokens.InputTokens,
		OutputTokens:    tokens.OutputTokens,
		ReasoningTokens: tokens.ReasoningTokens,
		CachedTokens:    tokens.CachedTokens,
		TotalTokens:     tokens.TotalTokens,
	}
}
```

### Step 3.3: Verify

**MUST NOT**:
- Block in `HandleUsage()` — must be non-blocking (select with default)
- Panic on nil store — graceful no-op
- Use the worker ctx for DB operations in `flush()` — must use `context.Background()` with timeout
- Use the cancelled ctx for final snapshot in `Stop()` — must use `context.Background()` with timeout
- Close `p.ch` from the producer side (only the stop path controls lifecycle)
- Add any registration in `init()` — that happens in Phase 4
- Use concrete `*PGStore` type for the store field — must use `usageStore` interface

**Verification**:
```bash
go test -v ./internal/usage/ -run TestPersistencePlugin
go build ./internal/usage/
go vet ./internal/usage/
```

---

## Phase 4: Service Lifecycle Wiring

**Goal**: Wire PersistencePlugin creation, start, and stop into the Service lifecycle.
**Effort**: M (1-2 hours)
**Dependencies**: Phase 1 + Phase 3
**Commit**: `feat(usage): wire PersistencePlugin into service startup and shutdown`

### Step 4.1: Add persistence field to Service

**File**: `sdk/cliproxy/service.go` (modify)

Add field to `Service` struct (find struct definition, add near other plugin/gateway fields):
```go
	usagePersistence *usage.PersistencePlugin
```

Add import:
```go
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
```

### Step 4.2: Add initialization in Run()

**File**: `sdk/cliproxy/service.go` (modify — insert between line 483 and line 493)

Insert after `usage.StartDefault(ctx)` (line 483) and before `ensureAuthDir()` (line 493):

```go
	// Initialize usage persistence if configured.
	if s.cfg.UsageStatisticsEnabled && s.cfg.UsageStatisticsPersistence.Enabled {
		if err := s.initUsagePersistence(ctx); err != nil {
			log.Warnf("usage persistence unavailable, continuing with in-memory only: %v", err)
			// Graceful degradation — do NOT return error
		}
	}
```

### Step 4.3: Add initUsagePersistence method

**File**: `sdk/cliproxy/service.go` (modify — add new method after Shutdown())

```go
func (s *Service) initUsagePersistence(ctx context.Context) error {
	cfg := s.cfg.UsageStatisticsPersistence
	store, err := usage.NewPGStore(ctx, cfg.PostgresDSN, cfg.Schema)
	if err != nil {
		return fmt.Errorf("connect to usage database: %w", err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		_ = store.Close()
		return fmt.Errorf("ensure usage schema: %w", err)
	}

	stats := usage.GetRequestStatistics()

	// Bootstrap: restore in-memory stats from PG if configured
	if cfg.BootstrapOnStart {
		if err := usage.BootstrapFromPG(store, stats); err != nil {
			log.Warnf("usage persistence: bootstrap failed, starting fresh: %v", err)
		}
	}

	plugin := usage.NewPersistencePlugin(usage.PersistencePluginConfig{
		Store:                store,
		Stats:                stats,
		BatchSize:            cfg.BatchSize,
		FlushIntervalSeconds: cfg.SnapshotFlushIntervalSeconds,
		RetentionDays:        cfg.RetentionDays,
	})
	plugin.Start(ctx)

	// Register with the usage manager — NOTE: no unregister, so this
	// only works correctly on first startup (not hot-reload).
	coreusage.RegisterPlugin(plugin)

	s.usagePersistence = plugin
	log.Info("usage persistence: PostgreSQL backend initialized")
	return nil
}
```

Note: `store.EnsureSchema` and `store.Close` must be exported methods on `PGStore`. In `pg_store.go`, export them:
- `func (s *PGStore) EnsureSchema(ctx context.Context) error` (capitalize)
- `func (s *PGStore) Close() error` (capitalize)

The `usageStore` interface uses lowercase method names (unexported interface methods). The `PGStore` struct has both the exported public API (`EnsureSchema`, `Close`) for use from `service.go`, and the unexported interface-satisfying methods (`ensureSchema`, `insertDetails`, etc.) called through the `usageStore` interface. To satisfy both needs: implement the exported methods and have the unexported interface methods delegate to them, OR simply make all methods exported and have the interface use exported names too.

**Simplest approach**: Make the `usageStore` interface methods exported (capital first letter). This is the cleanest Go convention when the interface is used cross-package conceptually (even though it's in the same package here). Update the interface:

```go
type usageStore interface {
	InsertDetails(ctx context.Context, details []pgDetail) error
	UpsertSnapshot(ctx context.Context, name string, snapshot StatisticsSnapshot, lastDetailID int64) error
	LastDetailID(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
	LoadSnapshot(ctx context.Context, name string) (StatisticsSnapshot, int64, error)
	LoadDetailsSince(ctx context.Context, afterID int64, limit int) ([]pgDetail, int64, error)
	Close() error
}
```

And capitalize all method names on `PGStore` and `mockUsageStore` accordingly. This ensures `PGStore` methods are callable from `service.go` AND satisfy the interface.

### Step 4.4: Add stop in Shutdown()

**File**: `sdk/cliproxy/service.go` (modify — insert at line 764, between server.Stop and usage.StopDefault)

Insert after `s.server.Stop()` block (line 764) and before `usage.StopDefault()` (line 766):

```go
		// Flush and close usage persistence before stopping the dispatcher.
		// Must happen BEFORE usage.StopDefault() so the dispatcher is still
		// running to drain any final records.
		if s.usagePersistence != nil {
			s.usagePersistence.Stop()
		}
```

### Step 4.5: Verify

**MUST NOT**:
- Return error from `initUsagePersistence` failure — must log.Warn and continue (graceful degradation)
- Call `initUsagePersistence` during hot reload (Manager.Register has no unregister)
- Stop `usagePersistence` AFTER `usage.StopDefault()` — the dispatcher must still be running to drain
- Add any env var detection (config-file only)

**Verification**:
```bash
go build ./...                   # full project compiles
go vet ./...
go test ./...                    # all existing tests pass
# Manual: start server with config that has persistence disabled → no errors
# Manual: start server with persistence enabled but unreachable PG → warning log, server runs normally
```

---

## Phase 5: Bootstrap / Restore on Startup

**Goal**: Implement the bootstrap logic that restores in-memory stats from PG on startup, including a new `ReplaceFromSnapshot` method that correctly restores all aggregated totals.
**Effort**: M (1-2 hours)
**Dependencies**: Phase 2 + Phase 3
**Commit**: `feat(usage): add bootstrap restore from PostgreSQL on startup`

### Background: Why MergeSnapshot is insufficient for bootstrap

`MergeSnapshot()` (logger_plugin.go:294-356) iterates detail rows and calls `recordImported()` for each one. `recordImported()` (line 358-381) increments counters one-by-one:
```go
s.totalRequests++    // always adds 1
if detail.Failed { s.failureCount++ } else { s.successCount++ }
s.totalTokens += totalTokens
```

So a snapshot with `TotalRequests=42` but 1 detail row would only produce `totalRequests=1` after merge. The top-level aggregates (`TotalRequests`, `SuccessCount`, `FailureCount`, `TotalTokens`), time-bucket maps (`RequestsByDay`, `RequestsByHour`, `TokensByDay`, `TokensByHour`), and per-API/model aggregates (`apiStats.TotalRequests`, `modelStats.TotalRequests`) are all lost.

Furthermore, `StatisticsSnapshot.RequestsByHour` uses `map[string]int64` (keys "00"-"23") but the internal `requestsByHour` uses `map[int]int64` — `ReplaceFromSnapshot` must convert string keys back to ints.

### Step 5.0: Write ReplaceFromSnapshot test

**File**: `internal/usage/logger_plugin_test.go` (modify — append)

```go
func TestReplaceFromSnapshotRestoresAggregates(t *testing.T) {
	stats := NewRequestStatistics()

	// Pre-populate with some data that should be overwritten
	stats.Record(context.Background(), coreusage.Record{
		APIKey:  "old-key",
		Model:   "old-model",
		Detail:  coreusage.Detail{TotalTokens: 999},
	})

	snapshot := StatisticsSnapshot{
		TotalRequests: 42,
		SuccessCount:  38,
		FailureCount:  4,
		TotalTokens:   50000,
		APIs: map[string]APISnapshot{
			"test-key": {
				TotalRequests: 42,
				TotalTokens:   50000,
				Models: map[string]ModelSnapshot{
					"gpt-4": {
						TotalRequests: 42,
						TotalTokens:   50000,
						Details: []RequestDetail{
							{
								Timestamp: time.Date(2026, 3, 20, 14, 0, 0, 0, time.UTC),
								LatencyMs: 1500,
								Source:    "user@example.com",
								AuthIndex: "0",
								Tokens:    TokenStats{InputTokens: 100, OutputTokens: 200, TotalTokens: 300},
							},
						},
					},
				},
			},
		},
		RequestsByDay:  map[string]int64{"2026-03-20": 42},
		RequestsByHour: map[string]int64{"14": 42},
		TokensByDay:    map[string]int64{"2026-03-20": 50000},
		TokensByHour:   map[string]int64{"14": 50000},
	}

	stats.ReplaceFromSnapshot(snapshot)
	s := stats.Snapshot()

	// Verify top-level aggregates are restored (not re-counted from details)
	if s.TotalRequests != 42 {
		t.Fatalf("TotalRequests = %d, want 42", s.TotalRequests)
	}
	if s.SuccessCount != 38 {
		t.Fatalf("SuccessCount = %d, want 38", s.SuccessCount)
	}
	if s.FailureCount != 4 {
		t.Fatalf("FailureCount = %d, want 4", s.FailureCount)
	}
	if s.TotalTokens != 50000 {
		t.Fatalf("TotalTokens = %d, want 50000", s.TotalTokens)
	}

	// Verify time-bucket maps
	if s.RequestsByDay["2026-03-20"] != 42 {
		t.Fatalf("RequestsByDay[2026-03-20] = %d, want 42", s.RequestsByDay["2026-03-20"])
	}
	if s.RequestsByHour["14"] != 42 {
		t.Fatalf("RequestsByHour[14] = %d, want 42", s.RequestsByHour["14"])
	}
	if s.TokensByDay["2026-03-20"] != 50000 {
		t.Fatalf("TokensByDay[2026-03-20] = %d, want 50000", s.TokensByDay["2026-03-20"])
	}
	if s.TokensByHour["14"] != 50000 {
		t.Fatalf("TokensByHour[14] = %d, want 50000", s.TokensByHour["14"])
	}

	// Verify API/model hierarchy
	apiSnap, ok := s.APIs["test-key"]
	if !ok {
		t.Fatal("APIs[test-key] not found")
	}
	if apiSnap.TotalRequests != 42 {
		t.Fatalf("API TotalRequests = %d, want 42", apiSnap.TotalRequests)
	}
	modelSnap := apiSnap.Models["gpt-4"]
	if modelSnap.TotalRequests != 42 {
		t.Fatalf("Model TotalRequests = %d, want 42", modelSnap.TotalRequests)
	}
	if len(modelSnap.Details) != 1 {
		t.Fatalf("Details len = %d, want 1", len(modelSnap.Details))
	}

	// Verify pre-existing data was overwritten (old-key should be gone)
	if _, exists := s.APIs["old-key"]; exists {
		t.Fatal("old-key should have been overwritten by ReplaceFromSnapshot")
	}
}

func TestReplaceFromSnapshotHandlesNilAndEmpty(t *testing.T) {
	stats := NewRequestStatistics()
	// Empty snapshot should reset to zero
	stats.ReplaceFromSnapshot(StatisticsSnapshot{})
	s := stats.Snapshot()
	if s.TotalRequests != 0 {
		t.Fatalf("TotalRequests = %d, want 0", s.TotalRequests)
	}

	// Nil receiver should not panic
	var nilStats *RequestStatistics
	nilStats.ReplaceFromSnapshot(StatisticsSnapshot{TotalRequests: 1})
	// If we get here without panic, test passes
}

func TestReplaceFromSnapshotHourKeyConversion(t *testing.T) {
	stats := NewRequestStatistics()
	snapshot := StatisticsSnapshot{
		RequestsByHour: map[string]int64{"00": 5, "09": 10, "23": 15},
		TokensByHour:   map[string]int64{"00": 500, "09": 1000, "23": 1500},
	}
	stats.ReplaceFromSnapshot(snapshot)
	s := stats.Snapshot()

	// Verify round-trip: string "09" → int 9 → string "09"
	if s.RequestsByHour["09"] != 10 {
		t.Fatalf("RequestsByHour[09] = %d, want 10", s.RequestsByHour["09"])
	}
	if s.RequestsByHour["00"] != 5 {
		t.Fatalf("RequestsByHour[00] = %d, want 5", s.RequestsByHour["00"])
	}
	if s.TokensByHour["23"] != 1500 {
		t.Fatalf("TokensByHour[23] = %d, want 1500", s.TokensByHour["23"])
	}
}
```

### Step 5.1: Implement ReplaceFromSnapshot

**File**: `internal/usage/logger_plugin.go` (modify — add new method after `MergeSnapshot`, around line 356)

```go
// ReplaceFromSnapshot overwrites all in-memory statistics with the given snapshot.
// Unlike MergeSnapshot (which imports individual detail rows and recounts),
// this method restores the exact aggregated totals, time-bucket maps, and
// API→Model→Details hierarchy from the snapshot. Pre-existing data is discarded.
//
// This is used for bootstrap restore from PostgreSQL where the snapshot already
// contains correct aggregate values that should NOT be recounted from details.
func (s *RequestStatistics) ReplaceFromSnapshot(snapshot StatisticsSnapshot) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Reset all state
	s.totalRequests = snapshot.TotalRequests
	s.successCount = snapshot.SuccessCount
	s.failureCount = snapshot.FailureCount
	s.totalTokens = snapshot.TotalTokens

	// Rebuild APIs hierarchy
	s.apis = make(map[string]*apiStats, len(snapshot.APIs))
	for apiName, apiSnap := range snapshot.APIs {
		stats := &apiStats{
			TotalRequests: apiSnap.TotalRequests,
			TotalTokens:   apiSnap.TotalTokens,
			Models:        make(map[string]*modelStats, len(apiSnap.Models)),
		}
		for modelName, modelSnap := range apiSnap.Models {
			details := make([]RequestDetail, len(modelSnap.Details))
			copy(details, modelSnap.Details)
			stats.Models[modelName] = &modelStats{
				TotalRequests: modelSnap.TotalRequests,
				TotalTokens:   modelSnap.TotalTokens,
				Details:       details,
			}
		}
		s.apis[apiName] = stats
	}

	// Restore day buckets (string keys, direct copy)
	s.requestsByDay = make(map[string]int64, len(snapshot.RequestsByDay))
	for k, v := range snapshot.RequestsByDay {
		s.requestsByDay[k] = v
	}
	s.tokensByDay = make(map[string]int64, len(snapshot.TokensByDay))
	for k, v := range snapshot.TokensByDay {
		s.tokensByDay[k] = v
	}

	// Restore hour buckets: snapshot uses map[string]int64 ("00"-"23"),
	// internal uses map[int]int64 — must parse string keys back to ints.
	s.requestsByHour = make(map[int]int64, len(snapshot.RequestsByHour))
	for k, v := range snapshot.RequestsByHour {
		hour := parseHourKey(k)
		if hour >= 0 {
			s.requestsByHour[hour] = v
		}
	}
	s.tokensByHour = make(map[int]int64, len(snapshot.TokensByHour))
	for k, v := range snapshot.TokensByHour {
		hour := parseHourKey(k)
		if hour >= 0 {
			s.tokensByHour[hour] = v
		}
	}
}

// parseHourKey converts a string hour key ("00"-"23") to an int (0-23).
// Returns -1 for invalid input.
func parseHourKey(key string) int {
	if len(key) == 0 {
		return -1
	}
	hour := 0
	for _, c := range key {
		if c < '0' || c > '9' {
			return -1
		}
		hour = hour*10 + int(c-'0')
	}
	if hour < 0 || hour > 23 {
		return -1
	}
	return hour
}
```

### Step 5.2: Write bootstrap test

**File**: `internal/usage/bootstrap_test.go` (NEW)
**Package**: `usage`

```go
package usage

import (
	"testing"
	"time"
)

func TestBootstrapRestoresAggregatesFromSnapshot(t *testing.T) {
	// This test verifies that bootstrap uses ReplaceFromSnapshot (not MergeSnapshot)
	// so that aggregated totals like TotalRequests=42 are preserved exactly,
	// rather than being recounted as 1 (the number of detail rows).
	mock := &mockUsageStore{}

	// Simulate a stored snapshot with TotalRequests=42 but only 1 detail row
	mock.lastSnapshot = StatisticsSnapshot{
		TotalRequests: 42,
		SuccessCount:  40,
		FailureCount:  2,
		TotalTokens:   50000,
		APIs: map[string]APISnapshot{
			"test-key": {
				TotalRequests: 42,
				TotalTokens:   50000,
				Models: map[string]ModelSnapshot{
					"gpt-4": {
						TotalRequests: 42,
						TotalTokens:   50000,
						Details: []RequestDetail{{
							Timestamp: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
							LatencyMs: 1500,
							Tokens:    TokenStats{InputTokens: 100, OutputTokens: 200, TotalTokens: 300},
						}},
					},
				},
			},
		},
		RequestsByDay:  map[string]int64{"2026-03-20": 42},
		RequestsByHour: map[string]int64{"12": 42},
		TokensByDay:    map[string]int64{"2026-03-20": 50000},
		TokensByHour:   map[string]int64{"12": 50000},
	}
	mock.lastSnapshotID = 100

	stats := NewRequestStatistics()
	if err := BootstrapFromPG(mock, stats); err != nil {
		t.Fatalf("BootstrapFromPG: %v", err)
	}

	s := stats.Snapshot()

	// Key assertion: TotalRequests must be 42, NOT 1
	if s.TotalRequests != 42 {
		t.Fatalf("TotalRequests = %d, want 42 (ReplaceFromSnapshot should preserve aggregates)", s.TotalRequests)
	}
	if s.SuccessCount != 40 {
		t.Fatalf("SuccessCount = %d, want 40", s.SuccessCount)
	}
	if s.FailureCount != 2 {
		t.Fatalf("FailureCount = %d, want 2", s.FailureCount)
	}
	if s.RequestsByDay["2026-03-20"] != 42 {
		t.Fatalf("RequestsByDay[2026-03-20] = %d, want 42", s.RequestsByDay["2026-03-20"])
	}
}

func TestBootstrapReplaysDeltaAfterSnapshot(t *testing.T) {
	mock := &mockUsageStore{
		lastSnapshot: StatisticsSnapshot{
			TotalRequests: 10,
			SuccessCount:  10,
			TotalTokens:   1000,
			APIs: map[string]APISnapshot{
				"key1": {
					TotalRequests: 10,
					TotalTokens:   1000,
					Models: map[string]ModelSnapshot{
						"gpt-4": {
							TotalRequests: 10,
							TotalTokens:   1000,
							Details: []RequestDetail{{
								Timestamp: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
								Tokens:    TokenStats{TotalTokens: 100},
							}},
						},
					},
				},
			},
		},
		lastSnapshotID: 50,
	}

	// Simulate delta rows that were added AFTER the snapshot
	// Override loadDetailsSince to return delta
	mock.deltaDetails = []pgDetail{{
		RecordedAt:  time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		StatsKey:    "key1",
		Model:       "gpt-4",
		Source:      "user@example.com",
		InputTokens: 50,
		TotalTokens: 150,
	}}
	mock.deltaMaxID = 51

	stats := NewRequestStatistics()
	if err := BootstrapFromPG(mock, stats); err != nil {
		t.Fatalf("BootstrapFromPG: %v", err)
	}

	s := stats.Snapshot()
	// After ReplaceFromSnapshot: TotalRequests=10, 1 detail
	// After delta replay via MergeSnapshot: TotalRequests=11, 2 details
	if s.TotalRequests != 11 {
		t.Fatalf("TotalRequests = %d, want 11 (10 from snapshot + 1 from delta)", s.TotalRequests)
	}
	details := s.APIs["key1"].Models["gpt-4"].Details
	if len(details) != 2 {
		t.Fatalf("Details len = %d, want 2", len(details))
	}
}

func TestDetailsToSnapshot(t *testing.T) {
	details := []pgDetail{
		{
			RecordedAt:   time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
			StatsKey:     "test-key",
			Model:        "gpt-4",
			Source:       "user@example.com",
			AuthIndex:    "0",
			Failed:       false,
			LatencyMs:    500,
			InputTokens:  50,
			OutputTokens: 100,
			TotalTokens:  150,
		},
	}
	snapshot := detailsToSnapshot(details)
	if len(snapshot.APIs) != 1 {
		t.Fatalf("APIs count = %d, want 1", len(snapshot.APIs))
	}
	modelSnap := snapshot.APIs["test-key"].Models["gpt-4"]
	if len(modelSnap.Details) != 1 {
		t.Fatalf("Details count = %d, want 1", len(modelSnap.Details))
	}
	if modelSnap.Details[0].Tokens.TotalTokens != 150 {
		t.Fatalf("TotalTokens = %d, want 150", modelSnap.Details[0].Tokens.TotalTokens)
	}
}
```

Note: The `mockUsageStore` in `persistence_plugin_test.go` must be extended with `deltaDetails` and `deltaMaxID` fields, and `loadDetailsSince` must return them when `afterID` matches. Update the mock:

```go
// Add to mockUsageStore struct:
deltaDetails []pgDetail
deltaMaxID   int64

// Update loadDetailsSince:
func (m *mockUsageStore) LoadDetailsSince(_ context.Context, afterID int64, _ int) ([]pgDetail, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if afterID < m.deltaMaxID && len(m.deltaDetails) > 0 {
		return m.deltaDetails, m.deltaMaxID, nil
	}
	return nil, 0, nil
}
```

### Step 5.3: Create bootstrap implementation

**File**: `internal/usage/bootstrap.go` (NEW)
**Package**: `usage`

```go
package usage

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	bootstrapTimeout      = 30 * time.Second
	replayBatchSize       = 1000
	maxReplayRows         = 100000
)

// BootstrapFromPG restores in-memory RequestStatistics from the PostgreSQL backend.
// It loads the most recent snapshot via ReplaceFromSnapshot (preserving exact
// aggregate totals), then replays any detail rows added since that snapshot
// via MergeSnapshot (which correctly adds individual details with dedup).
func BootstrapFromPG(store usageStore, stats *RequestStatistics) error {
	if store == nil || stats == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), bootstrapTimeout)
	defer cancel()

	// Step 1: Load saved snapshot
	snapshot, lastDetailID, err := store.LoadSnapshot(ctx, snapshotStateName)
	if err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}

	// Step 2: Restore snapshot using ReplaceFromSnapshot — this preserves
	// exact aggregate totals (TotalRequests, SuccessCount, FailureCount,
	// RequestsByDay/Hour, TokensByDay/Hour, per-API/model totals).
	// DO NOT use MergeSnapshot here — it would recount from details only.
	if len(snapshot.APIs) > 0 || snapshot.TotalRequests > 0 {
		stats.ReplaceFromSnapshot(snapshot)
		log.Infof("usage persistence: bootstrap restored snapshot (total_requests=%d, total_tokens=%d)",
			snapshot.TotalRequests, snapshot.TotalTokens)
	}

	// Step 3: Replay delta — detail rows added AFTER the snapshot.
	// These are merged via MergeSnapshot which correctly adds individual
	// details and increments counters per-row, with dedup protection.
	if lastDetailID > 0 {
		totalReplayed := int64(0)
		totalLoaded := int64(0)
		cursor := lastDetailID
		for {
			details, maxID, err := store.LoadDetailsSince(ctx, cursor, replayBatchSize)
			if err != nil {
				return fmt.Errorf("replay delta: %w", err)
			}
			if len(details) == 0 {
				break
			}
			deltaSnapshot := detailsToSnapshot(details)
			result := stats.MergeSnapshot(deltaSnapshot)
			totalReplayed += result.Added
			totalLoaded += int64(len(details))
			cursor = maxID
			if len(details) < replayBatchSize {
				break
			}
			if totalLoaded >= maxReplayRows {
				log.Warnf("usage persistence: bootstrap delta replay truncated at %d rows", totalLoaded)
				break
			}
		}
		if totalReplayed > 0 {
			log.Infof("usage persistence: bootstrap replayed %d delta records", totalReplayed)
		}
	}

	return nil
}

// detailsToSnapshot converts pgDetail rows into a StatisticsSnapshot suitable
// for MergeSnapshot(). This reconstructs the API→Model→Details hierarchy.
// Note: Top-level aggregates are NOT set — this is for delta replay only,
// where MergeSnapshot increments counters per-detail.
func detailsToSnapshot(details []pgDetail) StatisticsSnapshot {
	snapshot := StatisticsSnapshot{
		APIs: make(map[string]APISnapshot),
	}
	for _, d := range details {
		apiSnap, ok := snapshot.APIs[d.StatsKey]
		if !ok {
			apiSnap = APISnapshot{Models: make(map[string]ModelSnapshot)}
		}
		modelSnap := apiSnap.Models[d.Model]
		modelSnap.Details = append(modelSnap.Details, RequestDetail{
			Timestamp: d.RecordedAt,
			LatencyMs: d.LatencyMs,
			Source:    d.Source,
			AuthIndex: d.AuthIndex,
			Tokens: TokenStats{
				InputTokens:     d.InputTokens,
				OutputTokens:    d.OutputTokens,
				ReasoningTokens: d.ReasoningTokens,
				CachedTokens:    d.CachedTokens,
				TotalTokens:     d.TotalTokens,
			},
			Failed: d.Failed,
		})
		apiSnap.Models[d.Model] = modelSnap
		snapshot.APIs[d.StatsKey] = apiSnap
	}
	return snapshot
}
```

### Step 5.4: Verify

**MUST NOT**:
- Use `MergeSnapshot` for the initial bootstrap snapshot restore — use `ReplaceFromSnapshot` instead
- Block startup if PG is slow — use 30s context timeout
- Panic on empty/missing snapshot (`sql.ErrNoRows` → return zero snapshot)
- Load ALL historical details if snapshot is missing — cap at 100k rows with warning
- Modify `MergeSnapshot()` — reuse existing dedup logic as-is for delta replay only

**Verification**:
```bash
go test -v ./internal/usage/ -run TestReplaceFromSnapshot
go test -v ./internal/usage/ -run TestBootstrap
go test -v ./internal/usage/ -run TestDetailsToSnapshot
go build ./...
go vet ./...
```

---

## Phase 6: Retention Cleanup

**Goal**: Retention is already integrated into `PersistencePlugin` in Phase 3 (Step 3.2). This phase adds the specific test.
**Effort**: S (15 min)
**Dependencies**: Phase 3
**Commit**: Included in Phase 3 commit (retention is part of the plugin)

### Step 6.1: Write retention test

**File**: `internal/usage/persistence_plugin_test.go` (modify — append)

```go
func TestRetentionDurationCalculation(t *testing.T) {
	d := retentionDuration(30)
	if d != 30*24*time.Hour {
		t.Fatalf("got %v, want %v", d, 30*24*time.Hour)
	}
	d = retentionDuration(0)
	if d != 0 {
		t.Fatalf("got %v, want 0 (disabled)", d)
	}
	d = retentionDuration(-1)
	if d != 0 {
		t.Fatalf("got %v, want 0 (negative = disabled)", d)
	}
}
```

### Step 6.2: Verify

**MUST NOT**:
- Run retention more frequently than every hour (6h is correct)
- Block the batch worker or snapshot worker

**Verification**:
```bash
go test -v ./internal/usage/ -run TestRetention
go build ./internal/usage/
go vet ./internal/usage/
```

---

## Phase 7: Integration Tests

**Goal**: End-to-end tests with a real PostgreSQL instance (skipped when PG unavailable).
**Effort**: M (1-2 hours)
**Dependencies**: All previous phases
**Commit**: `test(usage): add PostgreSQL integration tests for usage persistence`

### Step 7.1: Create integration test file

**File**: `internal/usage/pg_integration_test.go` (NEW)
**Package**: `usage`

```go
package usage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func skipIfNoPG(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("USAGE_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("USAGE_TEST_PG_DSN not set, skipping PG integration test")
	}
	return dsn
}

func cleanupSchema(t *testing.T, dsn, schema string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return
	}
	defer db.Close()
	_, _ = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pgQuoteIdentifier(schema)))
}

func TestPGStoreRoundTrip(t *testing.T) {
	dsn := skipIfNoPG(t)
	schema := fmt.Sprintf("usage_test_%d", time.Now().UnixNano())
	defer cleanupSchema(t, dsn, schema)

	ctx := context.Background()
	store, err := NewPGStore(ctx, dsn, schema)
	if err != nil {
		t.Fatalf("NewPGStore: %v", err)
	}
	defer store.Close()

	// Test 1: EnsureSchema creates tables
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Test 2: InsertDetails + LoadDetailsSince round-trip
	details := []pgDetail{{
		RecordedAt: time.Now(), StatsKey: "k1", Provider: "claude",
		Model: "sonnet", LatencyMs: 500, InputTokens: 100, TotalTokens: 300,
	}}
	if err := store.InsertDetails(ctx, details); err != nil {
		t.Fatalf("InsertDetails: %v", err)
	}
	loaded, maxID, err := store.LoadDetailsSince(ctx, 0, 100)
	if err != nil {
		t.Fatalf("LoadDetailsSince: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded %d rows, want 1", len(loaded))
	}
	if maxID <= 0 {
		t.Fatalf("maxID = %d, want > 0", maxID)
	}

	// Test 3: UpsertSnapshot + LoadSnapshot round-trip
	snapshot := StatisticsSnapshot{TotalRequests: 42, TotalTokens: 5000}
	if err := store.UpsertSnapshot(ctx, "default", snapshot, maxID); err != nil {
		t.Fatalf("UpsertSnapshot: %v", err)
	}
	loadedSnap, loadedID, err := store.LoadSnapshot(ctx, "default")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if loadedSnap.TotalRequests != 42 {
		t.Fatalf("TotalRequests = %d, want 42", loadedSnap.TotalRequests)
	}
	if loadedID != maxID {
		t.Fatalf("lastDetailID = %d, want %d", loadedID, maxID)
	}

	// Test 4: DeleteOlderThan
	// Insert a row with old timestamp
	oldDetails := []pgDetail{{
		RecordedAt: time.Now().Add(-48 * time.Hour), StatsKey: "old", Model: "m",
	}}
	store.InsertDetails(ctx, oldDetails)
	deleted, err := store.DeleteOlderThan(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
}

func TestPGStoreBootstrapFullCycle(t *testing.T) {
	dsn := skipIfNoPG(t)
	schema := fmt.Sprintf("usage_test_%d", time.Now().UnixNano())
	defer cleanupSchema(t, dsn, schema)

	ctx := context.Background()
	store, err := NewPGStore(ctx, dsn, schema)
	if err != nil {
		t.Fatalf("NewPGStore: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Simulate: insert some details, save a snapshot, insert more details
	batch1 := []pgDetail{{
		RecordedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		StatsKey: "key1", Model: "gpt-4", Provider: "openai",
		InputTokens: 100, TotalTokens: 300,
	}}
	store.InsertDetails(ctx, batch1)
	_, maxID1, _ := store.LoadDetailsSince(ctx, 0, 100)
	snap1 := StatisticsSnapshot{
		TotalRequests: 10,
		SuccessCount:  10,
		TotalTokens:   3000,
		APIs: map[string]APISnapshot{
			"key1": {
				TotalRequests: 10,
				TotalTokens:   3000,
				Models: map[string]ModelSnapshot{
					"gpt-4": {
						TotalRequests: 10,
						TotalTokens:   3000,
						Details: []RequestDetail{{
							Timestamp: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
							Tokens:    TokenStats{InputTokens: 100, TotalTokens: 300},
						}},
					},
				},
			},
		},
		RequestsByDay:  map[string]int64{"2026-03-20": 10},
		RequestsByHour: map[string]int64{"12": 10},
		TokensByDay:    map[string]int64{"2026-03-20": 3000},
		TokensByHour:   map[string]int64{"12": 3000},
	}
	store.UpsertSnapshot(ctx, "default", snap1, maxID1)

	// More details after snapshot
	batch2 := []pgDetail{{
		RecordedAt: time.Date(2026, 3, 21, 14, 0, 0, 0, time.UTC),
		StatsKey: "key1", Model: "gpt-4", Provider: "openai",
		InputTokens: 200, TotalTokens: 600,
	}}
	store.InsertDetails(ctx, batch2)

	// Bootstrap into fresh stats
	stats := NewRequestStatistics()
	if err := BootstrapFromPG(store, stats); err != nil {
		t.Fatalf("BootstrapFromPG: %v", err)
	}

	s := stats.Snapshot()

	// TotalRequests should be 11: 10 from snapshot + 1 from delta replay
	if s.TotalRequests != 11 {
		t.Fatalf("TotalRequests = %d, want 11", s.TotalRequests)
	}

	// Should have 2 detail entries for key1/gpt-4
	details := s.APIs["key1"].Models["gpt-4"].Details
	if len(details) != 2 {
		t.Fatalf("details = %d, want 2", len(details))
	}

	// Day buckets should include both days
	if s.RequestsByDay["2026-03-20"] != 10 {
		t.Fatalf("RequestsByDay[2026-03-20] = %d, want 10", s.RequestsByDay["2026-03-20"])
	}
	if s.RequestsByDay["2026-03-21"] != 1 {
		t.Fatalf("RequestsByDay[2026-03-21] = %d, want 1 (from delta)", s.RequestsByDay["2026-03-21"])
	}
}
```

### Step 7.2: Verify

**Verification**:
```bash
# Without PG (CI default — tests skip gracefully):
go test -v ./internal/usage/ -run TestPGStore
# → SKIP: USAGE_TEST_PG_DSN not set

# With PG (local development):
USAGE_TEST_PG_DSN="postgres://user:pass@127.0.0.1:5432/cliproxy_test?sslmode=disable" \
  go test -v ./internal/usage/ -run TestPGStore

# Full test suite still passes:
go test ./...
```

---

## Appendix A: Complete File Inventory

### New Files (7)

| File | Package | Lines (est.) | Purpose |
|------|---------|-------------|---------|
| `internal/usage/pg_store.go` | `usage` | ~280 | PG database operations (connect, schema, batch insert, query, delete) |
| `internal/usage/pg_store_test.go` | `usage` | ~80 | Unit tests for SQL generation and helpers |
| `internal/usage/persistence_plugin.go` | `usage` | ~260 | `PersistencePlugin` + `usageStore` interface + retention worker |
| `internal/usage/persistence_plugin_test.go` | `usage` | ~180 | Unit tests with `mockUsageStore` — drain, batch, convert, retention |
| `internal/usage/bootstrap.go` | `usage` | ~100 | Bootstrap restore logic (ReplaceFromSnapshot + delta replay) |
| `internal/usage/bootstrap_test.go` | `usage` | ~120 | Unit tests for bootstrap + detailsToSnapshot conversion |
| `internal/usage/pg_integration_test.go` | `usage` | ~150 | Integration tests (skip when no PG) |

### Modified Files (5)

| File | Changes | Lines Affected |
|------|---------|---------------|
| `internal/usage/logger_plugin.go` | Add `DefaultMaxDetailsPerModel` constant, ring buffer eviction in `updateAPIStats`, `ReplaceFromSnapshot()` method, `parseHourKey()` helper | Lines 17, 215-226, insert after line 356 |
| `internal/usage/logger_plugin_test.go` | Add `TestModelDetailsRingBufferCap`, `TestReplaceFromSnapshotRestoresAggregates`, `TestReplaceFromSnapshotHandlesNilAndEmpty`, `TestReplaceFromSnapshotHourKeyConversion` | Append after line 96 |
| `internal/config/config.go` | Add `UsagePersistenceConfig` struct + field on `Config` + defaults | Lines 66-67, ~200 area, 582 |
| `config.example.yaml` | Add commented `usage-statistics-persistence` block | After line 63 |
| `sdk/cliproxy/service.go` | Add `usagePersistence` field, `initUsagePersistence()`, startup/shutdown wiring | Lines 483-493, 764-766, new method |

---

## Appendix B: Risk Register

| # | Risk | Severity | Likelihood | Mitigation |
|---|------|----------|-----------|------------|
| R1 | **Manager.Register() has no unregister** — hot reload calls Register again, duplicating the plugin | High | Medium | Document as restart-required. Do NOT call initUsagePersistence on hot reload. Check `s.usagePersistence != nil` before re-init. |
| R2 | **Channel overflow under high load** — 4096 buffer fills up, records dropped | Medium | Low | Log warning on drop. In-memory stats unaffected (LoggerPlugin receives same Record independently). Dashboard always works. |
| R3 | **PG connection lost at runtime** — batch insert fails | Medium | Medium | Log warning, skip batch, retry on next tick. In-memory stats unaffected. No circuit-breaker needed for v1. |
| R4 | **Bootstrap loads stale snapshot** — snapshot was saved before PG details were flushed | Low | Low | Delta replay catches up. `LoadDetailsSince(lastDetailID)` covers the gap. |
| R5 | **statsKey mismatch between LoggerPlugin and PersistencePlugin** — they resolve statsKey differently | High | Medium | Both use `record.APIKey` with fallback to `resolveAPIIdentifier(ctx, record)`. Keep logic identical. Extract to shared helper if needed. |
| R6 | **`resolveAPIIdentifier` depends on gin.Context** — PersistencePlugin receives same ctx but gin context may not be available | Medium | Low | `resolveAPIIdentifier` already handles nil/missing gin context gracefully (falls back to Provider or "unknown"). |
| R7 | **Large bootstrap on first start** — no snapshot exists, delta replays ALL detail rows | Medium | Low | Add limit (100k rows max via `maxReplayRows`). Log warning if truncated. |
| R8 | **Batch INSERT SQL injection via statsKey** — statsKey contains user-controlled HTTP path | High | Low | All column values use parameterized queries (`$1, $2`). Column values are NEVER interpolated into SQL string. Table/schema names use `pgQuoteIdentifier()`. |
| R9 | **Shutdown drain uses cancelled context** — `batchWorker.flush()` would fail with `context.Canceled` | High | High | `flush()` creates its own `context.WithTimeout(context.Background(), 10s)` for DB operations. `Stop()` also uses fresh `context.Background()` for the final snapshot. |
| R10 | **`atomic.Value.Store(nil)` panic** | High | None | We don't use `atomic.Value` — we use `atomic.Bool` and channels. No risk. |
| R11 | **MergeSnapshot used for bootstrap loses aggregates** — TotalRequests=42 becomes 1 | High | None (mitigated) | `BootstrapFromPG` uses `ReplaceFromSnapshot` for the snapshot (preserves exact aggregates), then `MergeSnapshot` only for delta replay (correctly adds individual details). |

---

## Appendix C: Verification Checklist

### Per-Phase Commands

| Phase | Command | Expected |
|-------|---------|----------|
| 0 | `go test -v ./internal/usage/ -run TestModelDetailsRingBuffer` | PASS |
| 0 | `go test -v ./internal/usage/` | All tests PASS (existing + new) |
| 1 | `go build ./...` | Compiles |
| 1 | `go vet ./...` | No warnings |
| 2 | `go test -v ./internal/usage/ -run TestBuildBatchInsert` | PASS |
| 2 | `go test -v ./internal/usage/ -run TestPGQuoteIdentifier` | PASS |
| 2 | `go test -v ./internal/usage/ -run TestPGFullTableName` | PASS |
| 3 | `go test -v ./internal/usage/ -run TestPersistencePlugin` | PASS |
| 3 | `go test -v ./internal/usage/ -run TestRetention` | PASS |
| 4 | `go build ./...` | Full project compiles |
| 4 | `go test ./...` | All existing tests pass |
| 5 | `go test -v ./internal/usage/ -run TestReplaceFromSnapshot` | PASS |
| 5 | `go test -v ./internal/usage/ -run TestBootstrap` | PASS |
| 5 | `go test -v ./internal/usage/ -run TestDetailsToSnapshot` | PASS |
| 7 | `go test -v ./internal/usage/ -run TestPGStore` | SKIP (no PG) or PASS |
| ALL | `go test ./...` | All tests pass |
| ALL | `go build -o /dev/null ./cmd/server/` | Binary builds |
| ALL | `CGO_ENABLED=0 go build -o /dev/null ./cmd/server/` | Static binary builds |

### Manual Smoke Tests

1. **No config change** → server starts normally, no PG warnings
2. **Persistence enabled, PG unreachable** → server starts with warning log, dashboard works
3. **Persistence enabled, PG available** → server starts, usage data appears in PG after requests
4. **Server restart** → bootstrap restores previous stats with correct aggregates, dashboard shows `TotalRequests=N` (not recounted from details)
5. **Export/Import API** → unchanged behavior, returns in-memory snapshot as before
6. **TUI dashboard** → unchanged behavior, shows live stats

### Invariants to Verify

- [ ] `internal/usage/logger_plugin.go` `init()` unchanged (LoggerPlugin still registered)
- [ ] `internal/usage/logger_plugin.go` `HandleUsage` unchanged (in-memory path unaffected)
- [ ] `internal/usage/logger_plugin.go` `Snapshot()` unchanged (dashboard unaffected)
- [ ] `internal/usage/logger_plugin.go` `MergeSnapshot()` unchanged (import API unaffected)
- [ ] Management API endpoints unchanged (GET/POST usage, export, import)
- [ ] No new env vars introduced
- [ ] `PGSTORE_DSN` not reused
- [ ] No files in `internal/store/` modified
- [ ] No files in `internal/translator/` modified
- [ ] `CGO_ENABLED=0` build succeeds

---

## Appendix D: Dependency Graph

```
Phase 0 (ring buffer) ──────────────────────────────────┐
                                                         │
Phase 1 (config) ──────────┬─────────────────────────────┤
                           │                             │
                           ▼                             │
Phase 2 (pg store) ────────┤                             │
                           │                             │
                           ▼                             │
Phase 3 (plugin +   ───────┤                             │
 retention + interface)     │                             │
                           │                             │
Phase 5 (bootstrap + ◄─────┘                             │
 ReplaceFromSnapshot)                                    │
         │                                               │
         ▼                                               │
Phase 4 (wiring) ◄──────────────────────────────────────-┘
         │
         ▼
Phase 7 (integration tests)
```

**Critical path**: Phase 1 → Phase 2 → Phase 3 → Phase 5 → Phase 4
**Parallel tracks**: Phase 0 can run in parallel with Phases 1-3
**Note**: Phase 6 (retention) is folded into Phase 3

---

## Appendix E: Global MUST NOT Rules

These apply to ALL phases:

1. **MUST NOT** modify any file in `internal/translator/` (CI path guard blocks translator PRs)
2. **MUST NOT** use `encoding/json` Unmarshal for request/response translation (OK in usage package)
3. **MUST NOT** use testify or any external test framework
4. **MUST NOT** add `//go:build cgo` or require CGO
5. **MUST NOT** change the `Plugin` interface in `sdk/cliproxy/usage/manager.go`
6. **MUST NOT** change the `Record` or `Detail` types in `sdk/cliproxy/usage/manager.go`
7. **MUST NOT** change the `StatisticsSnapshot` JSON structure (breaks export/import API compatibility)
8. **MUST NOT** add retry/backoff to PG operations in v1 (keep simple, log and continue)
9. **MUST NOT** use `sync.WaitGroup` where channel signaling suffices
10. **MUST NOT** store DSN in JSON output (`json:"-"` on PostgresDSN field)
11. **MUST NOT** call `initUsagePersistence` during hot reload (Manager.Register has no unregister)
12. **MUST NOT** close `p.ch` from producer side (only stop/drain path manages lifecycle)
13. **MUST NOT** use `MergeSnapshot` for bootstrap snapshot restore — use `ReplaceFromSnapshot` instead (MergeSnapshot only imports individual details and recounts, losing aggregate totals)
14. **MUST NOT** use the worker context for DB operations in `flush()` or `Stop()` — it may be cancelled during shutdown drain; use `context.Background()` with timeout instead
15. **MUST NOT** use concrete `*PGStore` type for `PersistencePlugin.store` field — use `usageStore` interface to enable mock-based testing
