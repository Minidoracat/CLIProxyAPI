package usage

import (
	"context"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestRequestStatisticsRecordIncludesLatency(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Latency:     1500 * time.Millisecond,
		Detail: coreusage.Detail{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		},
	})

	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 {
		t.Fatalf("details len = %d, want 1", len(details))
	}
	if details[0].LatencyMs != 1500 {
		t.Fatalf("latency_ms = %d, want 1500", details[0].LatencyMs)
	}
}

func TestRequestStatisticsMergeSnapshotDedupIgnoresLatency(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	first := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							LatencyMs: 0,
							Source:    "user@example.com",
							AuthIndex: "0",
							Tokens: TokenStats{
								InputTokens:  10,
								OutputTokens: 20,
								TotalTokens:  30,
							},
						}},
					},
				},
			},
		},
	}
	second := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							LatencyMs: 2500,
							Source:    "user@example.com",
							AuthIndex: "0",
							Tokens: TokenStats{
								InputTokens:  10,
								OutputTokens: 20,
								TotalTokens:  30,
							},
						}},
					},
				},
			},
		},
	}

	result := stats.MergeSnapshot(first)
	if result.Added != 1 || result.Skipped != 0 {
		t.Fatalf("first merge = %+v, want added=1 skipped=0", result)
	}

	result = stats.MergeSnapshot(second)
	if result.Added != 0 || result.Skipped != 1 {
		t.Fatalf("second merge = %+v, want added=0 skipped=1", result)
	}

	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 {
		t.Fatalf("details len = %d, want 1", len(details))
	}
}

func TestModelDetailsRingBufferCap(t *testing.T) {
	stats := NewRequestStatistics()
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
	if details[0].Tokens.InputTokens != 50 {
		t.Fatalf("oldest retained detail InputTokens = %d, want 50", details[0].Tokens.InputTokens)
	}
	if details[len(details)-1].Tokens.InputTokens != 249 {
		t.Fatalf("newest detail InputTokens = %d, want 249", details[len(details)-1].Tokens.InputTokens)
	}
}

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
