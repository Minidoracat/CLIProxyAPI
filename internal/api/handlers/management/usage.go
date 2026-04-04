package management

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
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

// GetUsageStatistics returns the in-memory request statistics snapshot.
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot
	if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	if h != nil && h.cfg != nil {
		enrichSnapshotWithCosts(&snapshot, h.cfg.ModelPrices)
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
		enrichSnapshotWithCosts(&snapshot, h.cfg.ModelPrices)
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
				promptTokens := d.Tokens.InputTokens - d.Tokens.CachedTokens
				if promptTokens < 0 {
					promptTokens = 0
				}
				cost := float64(promptTokens)/1_000_000*price.Prompt +
					float64(d.Tokens.CachedTokens)/1_000_000*price.Cache +
					float64(d.Tokens.OutputTokens)/1_000_000*price.Completion
				d.Cost = cost
				modelTotal += cost
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
