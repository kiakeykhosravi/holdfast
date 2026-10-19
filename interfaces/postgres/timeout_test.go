package postgres

import (
	"context"
	"testing"
	"time"
)

// TestWithOptionalTimeoutZeroIsUnbounded guards dump_timeout: 0. The
// physical path used context.WithTimeout directly, which turned "no limit"
// into "already expired" and failed every pg_basebackup on such a host.
func TestWithOptionalTimeoutZeroIsUnbounded(t *testing.T) {
	ctx, cancel := withOptionalTimeout(context.Background(), 0)
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("a zero duration must not set a deadline")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("context already done: %v", err)
	}

	ctx2, cancel2 := withOptionalTimeout(context.Background(), time.Hour)
	defer cancel2()
	if _, ok := ctx2.Deadline(); !ok {
		t.Fatal("a positive duration must set a deadline")
	}
}
