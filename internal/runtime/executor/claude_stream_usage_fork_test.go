package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

type captureClaudeForkUsagePlugin struct {
	authID  string
	records chan usage.Record
}

func (p *captureClaudeForkUsagePlugin) HandleUsage(_ context.Context, record usage.Record) {
	if record.AuthID != p.authID {
		return
	}
	select {
	case p.records <- record:
	default:
	}
}

// The fork estimates Claude thinking tokens from thinking deltas and records a
// stream that ends without message_stop as a failed usage record while keeping
// the client stream successful.
func TestClaudeExecutorStreamUsageForkSemantics(t *testing.T) {
	thinking := strings.Repeat("t", 40)
	start := "event: message_start\n" +
		`data: {"type":"message_start","message":{"id":"msg_fork","type":"message","role":"assistant","content":[],"model":"claude-opus-5","usage":{"input_tokens":100,"cache_read_input_tokens":50,"output_tokens":1}}}` + "\n\n" +
		"event: content_block_delta\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"` + thinking + `"}}` + "\n\n" +
		"event: message_delta\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":30}}` + "\n\n"
	stop := "event: message_stop\n" + `data: {"type":"message_stop"}` + "\n\n"

	for _, tc := range []struct {
		name           string
		responseFormat sdktranslator.Format
		body           string
		wantFailed     bool
	}{
		{name: "claude-complete", responseFormat: sdktranslator.FormatClaude, body: start + stop},
		{name: "claude-missing-stop", responseFormat: sdktranslator.FormatClaude, body: start, wantFailed: true},
		{name: "openai-complete", responseFormat: sdktranslator.FormatOpenAI, body: start + stop},
		{name: "openai-missing-stop", responseFormat: sdktranslator.FormatOpenAI, body: start, wantFailed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			authID := "claude-fork-usage-" + tc.name
			plugin := &captureClaudeForkUsagePlugin{authID: authID, records: make(chan usage.Record, 4)}
			usage.RegisterPlugin(plugin)

			auth := &cliproxyauth.Auth{ID: authID, Provider: "claude", Attributes: map[string]string{
				"api_key":  "key-123",
				"base_url": server.URL,
			}}
			result, err := NewClaudeExecutor(&config.Config{}).ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
				Model:   "claude-opus-5",
				Payload: []byte(`{"model":"claude-opus-5","max_tokens":64,"messages":[{"role":"user","content":"hi"}],"stream":true}`),
			}, cliproxyexecutor.Options{SourceFormat: tc.responseFormat, ResponseFormat: tc.responseFormat})
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					t.Fatalf("client stream error = %v, want success", chunk.Err)
				}
			}

			var record usage.Record
			select {
			case record = <-plugin.records:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for usage record")
			}
			if record.Failed != tc.wantFailed {
				t.Fatalf("Failed = %v, want %v", record.Failed, tc.wantFailed)
			}
			got := record.Detail
			if got.InputTokens != 100 || got.CacheReadTokens != 50 || got.OutputTokens != 30 {
				t.Fatalf("usage = %+v, want input 100, cache read 50, output 30", got)
			}
			if got.ReasoningTokens != int64(len(thinking))/4 {
				t.Fatalf("reasoning tokens = %d, want %d", got.ReasoningTokens, len(thinking)/4)
			}
		})
	}
}
