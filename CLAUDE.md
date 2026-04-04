# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go proxy server providing OpenAI/Gemini/Claude/Codex compatible API interfaces for CLI tools. Supports multi-account OAuth load balancing, cross-provider format translation, and an embeddable SDK.

Module: `github.com/router-for-me/CLIProxyAPI/v6` | Go 1.26 | CGO always disabled

## Commands

```bash
# Build
go build -o cli-proxy-api ./cmd/server/
CGO_ENABLED=0 go build -ldflags "-X main.Version=dev -X main.Commit=$(git rev-parse --short HEAD) -X main.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o cli-proxy-api ./cmd/server/

# Run
go run ./cmd/server/                     # Start proxy server
go run ./cmd/server/ -tui               # TUI management mode
go run ./cmd/server/ -tui -standalone   # TUI with embedded server

# Test
go test ./...                           # All tests
go test -v ./test/...                   # Integration tests only
go test -run TestFoo ./path/to/pkg/     # Single test
go test -bench=. ./sdk/cliproxy/auth/   # Benchmarks

# Docker
docker build -t cli-proxy-api .
docker compose up -d
```

## Architecture

### Request Flow

Client → Gin HTTP Server (`internal/api/`) → Access Auth → Conductor (`sdk/cliproxy/auth/conductor.go`) selects credential → Translator converts request format → Provider Executor sends to upstream → Translator converts response → Client

### Key Components

| Component | Location | Role |
|-----------|----------|------|
| Service | `sdk/cliproxy/service.go` | SDK lifecycle: Run/Shutdown, auth queue, watcher |
| Builder | `sdk/cliproxy/builder.go` | Fluent config: `NewBuilder().WithConfig().Build()` |
| Conductor | `sdk/cliproxy/auth/conductor.go` | Core auth: credential selection, retry, cooldown |
| Server | `internal/api/server.go` | Gin HTTP server, all routes in `setupRoutes()` |
| Config | `internal/config/config.go` | 92-field struct with YAML tags, inline embedding |
| ModelRegistry | `internal/registry/model_registry.go` | Global model tracking with reference counting |
| Translators | `internal/translator/` | Cross-provider format conversion matrix |
| Executors | `internal/runtime/executor/` | Per-provider HTTP execution |

### Three Auth Managers

- `authManager` — OAuth flows (token refresh, login)
- `coreManager` — Request execution (credential selection, retry, cooldown)
- `accessManager` — HTTP request authentication (API keys)

### Translator Architecture

Directory structure: `internal/translator/{target_provider}/{source_provider}/{endpoint_type}/` (target first, counterintuitive).

Registration uses Go `init()` + blank imports in `internal/translator/init.go`. Uses `gjson`/`sjson` for JSON manipulation — **never use `encoding/json` unmarshal in translators**.

### Config System

YAML-based (`config.yaml`), hot-reloadable via fsnotify with 150ms debounce. YAML keys use **kebab-case** (`auth-dir`, `commercial-mode`). `config.example.yaml` is the source of truth for available options.

## Code Conventions

- **Logging**: Always `import log "github.com/sirupsen/logrus"` — aliased as `log` everywhere
- **JSON ops in translators**: `gjson` (read) + `sjson` (write), never `encoding/json`
- **Dot imports**: `internal/translator/init.go` uses `. "internal/constant"` for provider constants
- **Concurrency**: `sync.RWMutex` for registries, `atomic.Value` for hot-reload, buffered channels for auth updates
- **Errors**: Structured `auth.Error` with `Code`, `Message`, `Retryable`, `HTTPStatus` fields
- **Testing**: Standard library only (`t.Fatal`/`t.Error`), no testify. White-box tests (same package)
- **Config changes**: Must update both `internal/config/config.go` and `config.example.yaml`

## Anti-Patterns to Avoid

- `os.RemoveAll` on watched directories — triggers fsnotify race; operate file-by-file
- Injecting fake thinking blocks in Antigravity translator — API validates signatures
- Top-level schema placeholders — Gemini rejects them
- Calling `Apply()` without `ValidateConfig()` first in thinking providers
- Updating model registry before coreManager — coreManager must update first
- `atomic.Value.Store(nil)` — will panic; always initialize with non-nil
- Registering routes outside `setupRoutes()` — breaks middleware chain
- Missing blank import in `internal/translator/init.go` — translator silently fails to register

## CI & PR Notes

- **PR build**: Compile only (`go build`), no tests
- **Path guard**: `pr-path-guard.yml` blocks `internal/translator/**` changes in PRs
- **Model catalog**: `internal/registry/models/models.json` auto-refreshes from `router-for-me/models` repo in CI
- **Upstream**: `router-for-me/CLIProxyAPI` (fork: `Minidoracat/CLIProxyAPI`)

### Commit Format

```
feat(scope): description
fix(scope): description
docs: description
refactor(scope): description
```

### PR Title & Branch

- Title: `feat(scope): description` / `fix(scope): description`
- Branch: `{user}:{type}/{short-description}` (e.g. `Minidoracat:feat/claude-1m-context`)
- Body: Summary (bilingual OK), Changes, Test plan (checkbox format)

## Ports

| Port | Purpose |
|------|---------|
| 8317 | Main API server |
| 8085 | Gemini OAuth callback |
| 1455 | Codex OAuth callback |
| 54545 | Claude OAuth callback |

## Adding a New Provider

Requires three pieces:
1. **Auth**: `internal/auth/{provider}/` — OAuth or API key flow
2. **Executor**: `internal/runtime/executor/` — implements `ProviderExecutor` interface
3. **Translators**: `internal/translator/{target}/{source}/` — request/response format conversion + blank import in `init.go`

## Provider Quirks

- **Claude**: Uses `utls` custom TLS fingerprint (Chrome) to bypass Cloudflare
- **iFlow**: Dual auth modes (OAuth2 + cookie) — unique among providers
- **Kimi**: Returns HTTP 200 for `authorization_pending` — must check JSON body, not status code
- **Antigravity**: API validates thinking block signatures — cannot inject fake blocks
