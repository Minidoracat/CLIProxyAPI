package helps

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
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
