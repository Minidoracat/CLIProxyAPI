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

// EnsureSchema creates schema + tables if they don't exist.
func (s *PGStore) EnsureSchema(ctx context.Context) error {
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

// InsertDetails batch-inserts detail rows using a multi-value INSERT.
func (s *PGStore) InsertDetails(ctx context.Context, details []pgDetail) error {
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

// UpsertSnapshot saves the aggregated snapshot to the state table.
func (s *PGStore) UpsertSnapshot(ctx context.Context, name string, snapshot StatisticsSnapshot, lastDetailID int64) error {
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

// LoadSnapshot retrieves the latest snapshot from the state table.
// Returns zero-value snapshot and 0 if no row exists (NOT an error).
func (s *PGStore) LoadSnapshot(ctx context.Context, name string) (StatisticsSnapshot, int64, error) {
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

// LoadDetailsSince retrieves detail rows with id > afterID, ordered by id ASC.
func (s *PGStore) LoadDetailsSince(ctx context.Context, afterID int64, limit int) ([]pgDetail, int64, error) {
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

// DeleteOlderThan removes detail rows older than the given duration.
func (s *PGStore) DeleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error) {
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

// LastDetailID returns the maximum id from the details table.
func (s *PGStore) LastDetailID(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(
		`SELECT COALESCE(MAX(id), 0) FROM %s`, s.detailsTable,
	)
	var id int64
	if err := s.db.QueryRowContext(ctx, query).Scan(&id); err != nil {
		return 0, fmt.Errorf("usage pg store: last detail id: %w", err)
	}
	return id, nil
}

// Close releases the database connection.
func (s *PGStore) Close() error {
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

// Ensure PGStore satisfies usageStore at compile time.
var _ usageStore = (*PGStore)(nil)

// Suppress unused import warning for log in builds where no log call is reachable.
var _ = log.Infof
