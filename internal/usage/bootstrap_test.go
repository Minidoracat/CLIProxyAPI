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
