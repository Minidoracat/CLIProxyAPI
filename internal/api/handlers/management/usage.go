package management

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/redisqueue"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/usage"
)

type usageExportPayload struct {
	Version    int                      `json:"version"`
	ExportedAt time.Time                `json:"exported_at"`
	Usage      usage.StatisticsSnapshot `json:"usage"`
}

type usageImportPayload struct {
	Version int                      `json:"version"`
	Usage   usage.StatisticsSnapshot `json:"usage"`
}

type usageQueueRecord []byte

func (r usageQueueRecord) MarshalJSON() ([]byte, error) {
	if json.Valid(r) {
		return append([]byte(nil), r...), nil
	}
	return json.Marshal(string(r))
}

// GetUsageStatistics returns the in-memory request statistics snapshot.
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot
	if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	if h != nil && h.cfg != nil {
		enrichSnapshotWithCosts(&snapshot, h.snapshotModelPrices())
	}
	c.JSON(http.StatusOK, gin.H{
		"usage":           snapshot,
		"failed_requests": snapshot.FailureCount,
	})
}

// ExportUsageStatistics returns a complete usage snapshot for backup/migration.
func (h *Handler) ExportUsageStatistics(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot
	if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	if h != nil && h.cfg != nil {
		enrichSnapshotWithCosts(&snapshot, h.snapshotModelPrices())
	}
	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	})
}

// ImportUsageStatistics merges a previously exported usage snapshot into memory.
func (h *Handler) ImportUsageStatistics(c *gin.Context) {
	if h == nil || h.usageStats == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "usage statistics unavailable"})
		return
	}

	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	var payload usageImportPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if payload.Version != 0 && payload.Version != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported version"})
		return
	}

	result := h.usageStats.MergeSnapshot(payload.Usage)
	snapshot := h.usageStats.Snapshot()
	c.JSON(http.StatusOK, gin.H{
		"added":           result.Added,
		"skipped":         result.Skipped,
		"total_requests":  snapshot.TotalRequests,
		"failed_requests": snapshot.FailureCount,
	})
}

// GetUsageQueue pops queued usage records from the usage queue.
func (h *Handler) GetUsageQueue(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler unavailable"})
		return
	}

	count, errCount := parseUsageQueueCount(c.Query("count"))
	if errCount != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errCount.Error()})
		return
	}

	items := redisqueue.PopOldest(count)
	records := make([]usageQueueRecord, 0, len(items))
	for _, item := range items {
		records = append(records, usageQueueRecord(append([]byte(nil), item...)))
	}

	c.JSON(http.StatusOK, records)
}

func parseUsageQueueCount(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 1, nil
	}
	count, errCount := strconv.Atoi(value)
	if errCount != nil || count <= 0 {
		return 0, errors.New("count must be a positive integer")
	}
	return count, nil
}

// enrichSnapshotWithCosts populates Cost/TotalCost fields in the snapshot
// using the configured model prices. When no price is configured for a model
// the cost remains zero.
func enrichSnapshotWithCosts(snapshot *usage.StatisticsSnapshot, prices map[string]config.ModelPrice) {
	if snapshot == nil || len(prices) == 0 {
		return
	}
	var grandTotal float64
	for apiName, apiSnap := range snapshot.APIs {
		var apiTotal float64
		for modelName, modelSnap := range apiSnap.Models {
			price, ok := prices[modelName]
			if !ok {
				continue
			}
			var modelTotal float64
			for i := range modelSnap.Details {
				d := &modelSnap.Details[i]
				cost := tokenCost(modelName, d.Tokens, price)
				d.Cost = cost
				modelTotal += cost
			}
			if modelSnap.TokenStatsComplete || hasTokenStats(modelSnap.Tokens) {
				modelTotal = tokenCost(modelName, modelSnap.Tokens, price)
			}
			modelSnap.TotalCost = modelTotal
			apiSnap.Models[modelName] = modelSnap
			apiTotal += modelTotal
		}
		apiSnap.TotalCost = apiTotal
		snapshot.APIs[apiName] = apiSnap
		grandTotal += apiTotal
	}
	snapshot.TotalCost = grandTotal
}

func tokenCost(model string, tokens usage.TokenStats, price config.ModelPrice) float64 {
	promptTokens := tokens.InputTokens
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "claude-") {
		promptTokens -= tokens.CachedTokens
		if promptTokens < 0 {
			promptTokens = 0
		}
	}
	return float64(promptTokens)/1_000_000*price.Prompt +
		float64(tokens.CachedTokens)/1_000_000*price.Cache +
		float64(tokens.OutputTokens)/1_000_000*price.Completion
}

func hasTokenStats(tokens usage.TokenStats) bool {
	return tokens.InputTokens != 0 ||
		tokens.OutputTokens != 0 ||
		tokens.ReasoningTokens != 0 ||
		tokens.CachedTokens != 0 ||
		tokens.TotalTokens != 0
}
