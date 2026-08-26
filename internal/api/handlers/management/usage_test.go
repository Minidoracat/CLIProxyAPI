package management

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/redisqueue"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/usage"
)

func TestGetUsageQueuePopsRequestedRecords(t *testing.T) {
	withManagementUsageQueue(t, func() {
		redisqueue.Enqueue([]byte(`{"id":1}`))
		redisqueue.Enqueue([]byte(`{"id":2}`))
		redisqueue.Enqueue([]byte(`{"id":3}`))

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage-queue?count=2", nil)

		h := &Handler{}
		h.GetUsageQueue(ginCtx)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload []json.RawMessage
		if errUnmarshal := json.Unmarshal(rec.Body.Bytes(), &payload); errUnmarshal != nil {
			t.Fatalf("unmarshal response: %v", errUnmarshal)
		}
		if len(payload) != 2 {
			t.Fatalf("response records = %d, want 2", len(payload))
		}
		requireRecordID(t, payload[0], 1)
		requireRecordID(t, payload[1], 2)

		remaining := redisqueue.PopOldest(10)
		if len(remaining) != 1 || string(remaining[0]) != `{"id":3}` {
			t.Fatalf("remaining queue = %q, want third item only", remaining)
		}
	})
}

func TestGetUsageQueueInvalidCountDoesNotPop(t *testing.T) {
	withManagementUsageQueue(t, func() {
		redisqueue.Enqueue([]byte(`{"id":1}`))

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/usage-queue?count=0", nil)

		h := &Handler{}
		h.GetUsageQueue(ginCtx)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
		}

		remaining := redisqueue.PopOldest(10)
		if len(remaining) != 1 || string(remaining[0]) != `{"id":1}` {
			t.Fatalf("remaining queue = %q, want original item", remaining)
		}
	})
}

func withManagementUsageQueue(t *testing.T, fn func()) {
	t.Helper()

	prevQueueEnabled := redisqueue.Enabled()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)

	defer func() {
		redisqueue.SetEnabled(false)
		redisqueue.SetEnabled(prevQueueEnabled)
	}()

	fn()
}

func requireRecordID(t *testing.T, raw json.RawMessage, want int) {
	t.Helper()

	var payload struct {
		ID int `json:"id"`
	}
	if errUnmarshal := json.Unmarshal(raw, &payload); errUnmarshal != nil {
		t.Fatalf("unmarshal record: %v", errUnmarshal)
	}
	if payload.ID != want {
		t.Fatalf("record id = %d, want %d", payload.ID, want)
	}
}

func TestEnrichSnapshotWithCostsUsesCompleteAggregateTokens(t *testing.T) {
	snapshot := usage.StatisticsSnapshot{
		APIs: map[string]usage.APISnapshot{
			"test-key": {
				Models: map[string]usage.ModelSnapshot{
					"gpt-5.6-sol": {
						TotalRequests:      250,
						TotalTokens:        30000,
						Tokens:             usage.TokenStats{InputTokens: 25000, OutputTokens: 5000, CachedTokens: 10000, TotalTokens: 30000},
						TokenStatsComplete: true,
						Details: []usage.RequestDetail{{
							Tokens: usage.TokenStats{InputTokens: 100, OutputTokens: 20, CachedTokens: 40, TotalTokens: 120},
						}},
					},
				},
			},
		},
	}
	prices := map[string]config.ModelPrice{
		"gpt-5.6-sol": {Prompt: 5, Completion: 30, Cache: 0.5},
	}

	enrichSnapshotWithCosts(&snapshot, prices)

	model := snapshot.APIs["test-key"].Models["gpt-5.6-sol"]
	want := float64(15000)/1_000_000*5 + float64(10000)/1_000_000*0.5 + float64(5000)/1_000_000*30
	if math.Abs(model.TotalCost-want) > 1e-12 {
		t.Fatalf("model TotalCost = %.12f, want %.12f", model.TotalCost, want)
	}
	if math.Abs(snapshot.TotalCost-want) > 1e-12 {
		t.Fatalf("snapshot TotalCost = %.12f, want %.12f", snapshot.TotalCost, want)
	}
	if model.Details[0].Cost == 0 {
		t.Fatal("retained detail cost was not populated")
	}
}

func TestEnrichSnapshotWithCostsUsesKnownPartialAggregateTokens(t *testing.T) {
	snapshot := usage.StatisticsSnapshot{
		APIs: map[string]usage.APISnapshot{
			"test-key": {
				Models: map[string]usage.ModelSnapshot{
					"gpt-5.5": {
						TotalRequests: 3,
						TotalTokens:   360,
						Tokens:        usage.TokenStats{InputTokens: 200, OutputTokens: 40, CachedTokens: 80, TotalTokens: 240},
						Details: []usage.RequestDetail{{
							Tokens: usage.TokenStats{InputTokens: 100, OutputTokens: 20, CachedTokens: 40, TotalTokens: 120},
						}},
					},
				},
			},
		},
	}
	prices := map[string]config.ModelPrice{
		"gpt-5.5": {Prompt: 5, Completion: 30, Cache: 0.5},
	}

	enrichSnapshotWithCosts(&snapshot, prices)

	model := snapshot.APIs["test-key"].Models["gpt-5.5"]
	want := tokenCost("gpt-5.5", model.Tokens, prices["gpt-5.5"])
	if math.Abs(model.TotalCost-want) > 1e-12 {
		t.Fatalf("model TotalCost = %.12f, want known aggregate %.12f", model.TotalCost, want)
	}
	if model.TotalCost <= model.Details[0].Cost {
		t.Fatalf("model TotalCost = %.12f, want more than retained detail %.12f", model.TotalCost, model.Details[0].Cost)
	}
}

func TestTokenCostTreatsClaudeInputAsUncached(t *testing.T) {
	tokens := usage.TokenStats{
		InputTokens:  10,
		OutputTokens: 20,
		CachedTokens: 100,
	}
	price := config.ModelPrice{Prompt: 3, Completion: 15, Cache: 0.3}

	want := float64(10)/1_000_000*3 + float64(100)/1_000_000*0.3 + float64(20)/1_000_000*15
	if got := tokenCost("claude-sonnet-4-6", tokens, price); math.Abs(got-want) > 1e-12 {
		t.Fatalf("tokenCost() = %.12f, want %.12f", got, want)
	}
}
