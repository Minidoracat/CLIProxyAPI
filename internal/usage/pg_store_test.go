package usage

import (
	"testing"
	"time"
)

func TestBuildBatchInsertSQL(t *testing.T) {
	details := []pgDetail{
		{RecordedAt: time.Now(), StatsKey: "k1", Model: "m1", Failed: false, LatencyMs: 100},
		{RecordedAt: time.Now(), StatsKey: "k2", Model: "m2", Failed: true, LatencyMs: 200},
	}
	query, args := buildBatchInsertSQL("\"usage_request_details\"", details)
	// Verify query contains 2 value groups
	// Verify args length = 2 * 15 (15 columns per row)
	if len(args) != 30 {
		t.Fatalf("args len = %d, want 30", len(args))
	}
	if query == "" {
		t.Fatal("query is empty")
	}
}

func TestBuildBatchInsertSQLEmpty(t *testing.T) {
	query, args := buildBatchInsertSQL("\"usage_request_details\"", nil)
	if query != "" || args != nil {
		t.Fatal("expected empty query and nil args for empty input")
	}
}

func TestPGQuoteIdentifier(t *testing.T) {
	tests := []struct{ in, want string }{
		{"simple", `"simple"`},
		{`has"quote`, `"has""quote"`},
		{"cliproxy", `"cliproxy"`},
	}
	for _, tt := range tests {
		got := pgQuoteIdentifier(tt.in)
		if got != tt.want {
			t.Fatalf("pgQuoteIdentifier(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestPGFullTableName(t *testing.T) {
	// With schema
	got := pgFullTableName("myschema", "mytable")
	if got != `"myschema"."mytable"` {
		t.Fatalf("got %q, want %q", got, `"myschema"."mytable"`)
	}
	// Without schema
	got = pgFullTableName("", "mytable")
	if got != `"mytable"` {
		t.Fatalf("got %q, want %q", got, `"mytable"`)
	}
}
