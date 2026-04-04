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
	deltaDetails   []pgDetail
	deltaMaxID     int64
}

func (m *mockUsageStore) InsertDetails(_ context.Context, details []pgDetail) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insertCount++
	m.insertedRows = append(m.insertedRows, details...)
	return nil
}

func (m *mockUsageStore) UpsertSnapshot(_ context.Context, _ string, snap StatisticsSnapshot, lastID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshotCount++
	m.lastSnapshot = snap
	m.lastSnapshotID = lastID
	return nil
}

func (m *mockUsageStore) LastDetailID(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.insertedRows)), nil
}

func (m *mockUsageStore) DeleteOlderThan(_ context.Context, _ time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedCount++
	return 0, nil
}

func (m *mockUsageStore) LoadSnapshot(_ context.Context, _ string) (StatisticsSnapshot, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSnapshot, m.lastSnapshotID, nil
}

func (m *mockUsageStore) LoadDetailsSince(_ context.Context, afterID int64, _ int) ([]pgDetail, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if afterID < m.deltaMaxID && len(m.deltaDetails) > 0 {
		return m.deltaDetails, m.deltaMaxID, nil
	}
	return nil, 0, nil
}

func (m *mockUsageStore) Close() error {
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
		t.Fatalf("got %v, want 0 (negative)", d)
	}
}
