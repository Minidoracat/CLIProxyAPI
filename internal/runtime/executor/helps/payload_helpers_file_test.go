package helps

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/tidwall/gjson"
)

func TestApplyPayloadConfigWithRoot_OverrideFileSetsFileContents(t *testing.T) {
	dir := t.TempDir()
	promptPath := filepath.Join(dir, "system-prompt.md")
	if err := os.WriteFile(promptPath, []byte("file prompt\nsecond line"), 0o600); err != nil {
		t.Fatalf("write prompt file: %v", err)
	}

	cfg := &config.Config{
		Payload: config.PayloadConfig{
			OverrideFile: []config.PayloadRule{
				{
					Models: []config.PayloadModelRule{{Name: "gpt-*", Protocol: "codex"}},
					Params: map[string]any{"instructions": promptPath},
				},
			},
		},
	}

	payload := []byte(`{"model":"gpt-5.5","instructions":"old prompt","input":"hello"}`)
	out := ApplyPayloadConfigWithRoot(cfg, "gpt-5.5", "codex", "", payload, nil, "", "")

	if got := gjson.GetBytes(out, "instructions").String(); got != "file prompt\nsecond line" {
		t.Fatalf("instructions = %q, want file contents", got)
	}
}

func TestApplyPayloadConfigWithRoot_DefaultFileKeepsExistingInstructions(t *testing.T) {
	dir := t.TempDir()
	promptPath := filepath.Join(dir, "system-prompt.md")
	if err := os.WriteFile(promptPath, []byte("file prompt"), 0o600); err != nil {
		t.Fatalf("write prompt file: %v", err)
	}

	cfg := &config.Config{
		Payload: config.PayloadConfig{
			DefaultFile: []config.PayloadRule{
				{
					Models: []config.PayloadModelRule{{Name: "gpt-*", Protocol: "codex"}},
					Params: map[string]any{"instructions": promptPath},
				},
			},
		},
	}

	payload := []byte(`{"model":"gpt-5.5","instructions":"client prompt","input":"hello"}`)
	out := ApplyPayloadConfigWithRoot(cfg, "gpt-5.5", "codex", "", payload, nil, "", "")

	if got := gjson.GetBytes(out, "instructions").String(); got != "client prompt" {
		t.Fatalf("instructions = %q, want original client prompt", got)
	}
}

func TestPayloadFileRulesResolveQueryPaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keep.txt")
	if errWrite := os.WriteFile(path, []byte("all"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	rules := []config.PayloadRule{{
		Models: []config.PayloadModelRule{{Name: "gpt-*", Protocol: "codex"}},
		Params: map[string]any{`context_management.edits.#(type=="clear_thinking_20251015")#.keep`: path},
	}}
	for _, mode := range []string{"default-file", "override-file"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{}
			if mode == "default-file" {
				cfg.Payload.DefaultFile = rules
			} else {
				cfg.Payload.OverrideFile = rules
			}
			payload := []byte(`{"context_management":{"edits":[{"type":"clear_thinking_20251015","keep":"client"},{"type":"clear_thinking_20251015"}]}}`)
			out, touched := ApplyPayloadConfigWithRequestTracked(cfg, "gpt-5.5", "codex", "", "", payload, nil, "", "", nil, "context_management")
			wantFirst := "all"
			if mode == "default-file" {
				wantFirst = "client"
			}
			if got := gjson.GetBytes(out, "context_management.edits.0.keep").String(); got != wantFirst {
				t.Fatalf("first keep = %q, want %q", got, wantFirst)
			}
			if got := gjson.GetBytes(out, "context_management.edits.1.keep").String(); got != "all" {
				t.Fatalf("missing keep = %q, want file contents", got)
			}
			if !touched {
				t.Fatal("applied file rule did not claim context management")
			}

			unmatched := []byte(`{"context_management":{"edits":[{"type":"other"}]}}`)
			out, touched = ApplyPayloadConfigWithRequestTracked(cfg, "gpt-5.5", "codex", "", "", unmatched, nil, "", "", nil, "context_management")
			if touched || !bytes.Equal(out, unmatched) {
				t.Fatalf("unmatched query changed payload ownership: touched=%v, payload=%s", touched, out)
			}
		})
	}
}
