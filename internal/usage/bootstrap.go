package usage

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	bootstrapTimeout = 30 * time.Second
	replayBatchSize  = 1000
	maxReplayRows    = 100000
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
