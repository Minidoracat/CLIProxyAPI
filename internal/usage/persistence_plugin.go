package usage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const (
	defaultChannelBuffer = 4096
	snapshotStateName    = "default"
	flushTimeout         = 10 * time.Second
)

// usageStore abstracts PostgreSQL operations so PersistencePlugin can be tested
// with a mock. The concrete implementation is *PGStore (pg_store.go).
type usageStore interface {
	InsertDetails(ctx context.Context, details []pgDetail) error
	UpsertSnapshot(ctx context.Context, name string, snapshot StatisticsSnapshot, lastDetailID int64) error
	LastDetailID(ctx context.Context) (int64, error)
	DeleteOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
	LoadSnapshot(ctx context.Context, name string) (StatisticsSnapshot, int64, error)
	LoadDetailsSince(ctx context.Context, afterID int64, limit int) ([]pgDetail, int64, error)
	Close() error
}

// PersistencePlugin implements coreusage.Plugin and asynchronously persists
// usage records to PostgreSQL. It maintains a buffered channel as a write-ahead
// queue and a background goroutine that batch-inserts to PG.
type PersistencePlugin struct {
	store         usageStore
	stats         *RequestStatistics // reference to in-memory stats for snapshot export
	batchSize     int
	flushSecs     int
	retentionDays int
	ch            chan pgDetail
	stopOnce      sync.Once
	stopped       chan struct{} // closed when batchWorker exits
	cancel        context.CancelFunc
	running       atomic.Bool
}

// PersistencePluginConfig holds runtime parameters for the plugin.
type PersistencePluginConfig struct {
	Store                usageStore
	Stats                *RequestStatistics
	BatchSize            int
	FlushIntervalSeconds int
	RetentionDays        int
	ChannelBuffer        int
}

// NewPersistencePlugin creates a new persistence plugin. Call Start() to begin
// the background workers.
func NewPersistencePlugin(cfg PersistencePluginConfig) *PersistencePlugin {
	buf := cfg.ChannelBuffer
	if buf <= 0 {
		buf = defaultChannelBuffer
	}
	bs := cfg.BatchSize
	if bs <= 0 {
		bs = 100
	}
	fs := cfg.FlushIntervalSeconds
	if fs <= 0 {
		fs = 15
	}
	return &PersistencePlugin{
		store:         cfg.Store,
		stats:         cfg.Stats,
		batchSize:     bs,
		flushSecs:     fs,
		retentionDays: cfg.RetentionDays,
		ch:            make(chan pgDetail, buf),
		stopped:       make(chan struct{}),
	}
}

// HandleUsage implements coreusage.Plugin. It converts the record and enqueues
// it for async PG insertion. If the channel is full, the record is dropped with
// a warning log (graceful degradation — dashboard still works via in-memory stats).
func (p *PersistencePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if p == nil || !p.running.Load() {
		return
	}
	statsKey := record.APIKey
	if statsKey == "" {
		statsKey = resolveAPIIdentifier(ctx, record)
	}
	detail := recordToPGDetail(record, statsKey)
	select {
	case p.ch <- detail:
	default:
		log.Warn("usage persistence: channel full, dropping record")
	}
}

// Start launches the background batch-insert, snapshot-flush, and retention goroutines.
func (p *PersistencePlugin) Start(ctx context.Context) {
	if p == nil {
		return
	}
	workerCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.running.Store(true)
	go p.batchWorker(workerCtx)
	go p.snapshotWorker(workerCtx)
	if p.retentionDays > 0 {
		go p.retentionWorker(workerCtx)
	}
}

// Stop signals the background workers to drain and exit, then blocks until complete.
func (p *PersistencePlugin) Stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		p.running.Store(false)
		if p.cancel != nil {
			p.cancel()
		}
		<-p.stopped // wait for batchWorker to drain and exit

		// Final snapshot flush with a fresh context — the worker context is
		// already cancelled, so we must NOT reuse it for DB operations.
		flushCtx, flushCancel := context.WithTimeout(context.Background(), flushTimeout)
		defer flushCancel()
		p.flushSnapshot(flushCtx)

		if p.store != nil {
			_ = p.store.Close()
		}
	})
}

// batchWorker reads from the channel and batch-inserts to PG.
func (p *PersistencePlugin) batchWorker(ctx context.Context) {
	defer close(p.stopped)
	batch := make([]pgDetail, 0, p.batchSize)
	ticker := time.NewTicker(time.Duration(p.flushSecs) * time.Second)
	defer ticker.Stop()

	// flush uses context.Background with a timeout instead of the worker ctx.
	// This is critical: during shutdown drain, the worker ctx is cancelled but
	// we still need to write the final batch(es) to PG.
	flush := func() {
		if len(batch) == 0 {
			return
		}
		flushCtx, flushCancel := context.WithTimeout(context.Background(), flushTimeout)
		defer flushCancel()
		if err := p.store.InsertDetails(flushCtx, batch); err != nil {
			log.Warnf("usage persistence: batch insert failed: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case detail, ok := <-p.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, detail)
			if len(batch) >= p.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			// Drain remaining items from channel after context cancellation.
			for {
				select {
				case detail, ok := <-p.ch:
					if !ok {
						flush()
						return
					}
					batch = append(batch, detail)
					if len(batch) >= p.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// snapshotWorker periodically flushes the aggregated in-memory snapshot to PG.
func (p *PersistencePlugin) snapshotWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(p.flushSecs) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Use a fresh context with timeout for the DB operation.
			opCtx, opCancel := context.WithTimeout(context.Background(), flushTimeout)
			p.flushSnapshot(opCtx)
			opCancel()
		case <-ctx.Done():
			return
		}
	}
}

// flushSnapshot exports the current in-memory snapshot and upserts it to PG.
func (p *PersistencePlugin) flushSnapshot(ctx context.Context) {
	if p.stats == nil || p.store == nil {
		return
	}
	snapshot := p.stats.Snapshot()
	lastID, err := p.store.LastDetailID(ctx)
	if err != nil {
		log.Warnf("usage persistence: get last detail ID: %v", err)
		return
	}
	if err := p.store.UpsertSnapshot(ctx, snapshotStateName, snapshot, lastID); err != nil {
		log.Warnf("usage persistence: snapshot flush: %v", err)
	}
}

// retentionWorker periodically deletes old detail rows.
func (p *PersistencePlugin) retentionWorker(ctx context.Context) {
	// Run once immediately on startup, then every 6 hours.
	p.runRetention()
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.runRetention()
		case <-ctx.Done():
			return
		}
	}
}

func (p *PersistencePlugin) runRetention() {
	dur := retentionDuration(p.retentionDays)
	if dur == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deleted, err := p.store.DeleteOlderThan(ctx, dur)
	if err != nil {
		log.Warnf("usage persistence: retention cleanup failed: %v", err)
		return
	}
	if deleted > 0 {
		log.Infof("usage persistence: retention cleanup deleted %d old records", deleted)
	}
}

func retentionDuration(days int) time.Duration {
	if days <= 0 {
		return 0
	}
	return time.Duration(days) * 24 * time.Hour
}

// recordToPGDetail converts a coreusage.Record to a pgDetail.
func recordToPGDetail(record coreusage.Record, statsKey string) pgDetail {
	ts := record.RequestedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	tokens := normaliseDetail(record.Detail)
	return pgDetail{
		RecordedAt:      ts,
		StatsKey:        statsKey,
		APIKey:          record.APIKey,
		Provider:        record.Provider,
		Model:           record.Model,
		Source:          record.Source,
		AuthID:          record.AuthID,
		AuthIndex:       record.AuthIndex,
		Failed:          record.Failed,
		LatencyMs:       normaliseLatency(record.Latency),
		InputTokens:     tokens.InputTokens,
		OutputTokens:    tokens.OutputTokens,
		ReasoningTokens: tokens.ReasoningTokens,
		CachedTokens:    tokens.CachedTokens,
		TotalTokens:     tokens.TotalTokens,
	}
}
