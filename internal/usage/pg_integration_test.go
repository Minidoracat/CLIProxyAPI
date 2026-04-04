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

func TestPGStoreRoundTrip(t *testing.T) {
	dsn := skipIfNoPG(t)
	schema := fmt.Sprintf("usage_test_%d", time.Now().UnixNano())
	ctx := context.Background()

	store, err := NewPGStore(ctx, dsn, schema)
	if err != nil {
		t.Fatalf("NewPGStore: %v", err)
	}
	defer func() {
		_ = store.Close()
		db, _ := sql.Open("pgx", dsn)
		if db != nil {
			db.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", pgQuoteIdentifier(schema)))
			_ = db.Close()
		}
	}()

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Test insertDetails + LoadDetailsSince
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

	// Test UpsertSnapshot + LoadSnapshot
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

	// Test DeleteOlderThan
	deleted, err := store.DeleteOlderThan(ctx, 0)
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
	ctx := context.Background()

	store, err := NewPGStore(ctx, dsn, schema)
	if err != nil {
		t.Fatalf("NewPGStore: %v", err)
	}
	defer func() {
		_ = store.Close()
		db, _ := sql.Open("pgx", dsn)
		if db != nil {
			db.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", pgQuoteIdentifier(schema)))
			_ = db.Close()
		}
	}()

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// Insert batch1, snapshot, batch2
	batch1 := []pgDetail{{
		RecordedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		StatsKey:   "key1", Model: "gpt-4", Provider: "openai",
		InputTokens: 100, TotalTokens: 300,
	}}
	store.InsertDetails(ctx, batch1)
	_, maxID1, _ := store.LoadDetailsSince(ctx, 0, 100)
	snap1 := StatisticsSnapshot{
		TotalRequests: 1, TotalTokens: 300,
		APIs: map[string]APISnapshot{
			"key1": {TotalRequests: 1, TotalTokens: 300, Models: map[string]ModelSnapshot{
				"gpt-4": {TotalRequests: 1, TotalTokens: 300, Details: []RequestDetail{{
					Timestamp: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
					Tokens:    TokenStats{InputTokens: 100, TotalTokens: 300},
				}}},
			}},
		},
	}
	store.UpsertSnapshot(ctx, "default", snap1, maxID1)

	batch2 := []pgDetail{{
		RecordedAt: time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		StatsKey:   "key1", Model: "gpt-4", Provider: "openai",
		InputTokens: 200, TotalTokens: 600,
	}}
	store.InsertDetails(ctx, batch2)

	// Bootstrap
	stats := NewRequestStatistics()
	if err := BootstrapFromPG(store, stats); err != nil {
		t.Fatalf("BootstrapFromPG: %v", err)
	}

	s := stats.Snapshot()
	if s.TotalRequests != 2 {
		t.Fatalf("TotalRequests = %d, want 2", s.TotalRequests)
	}
	details := s.APIs["key1"].Models["gpt-4"].Details
	if len(details) != 2 {
		t.Fatalf("details = %d, want 2", len(details))
	}
}
