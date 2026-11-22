package core_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/zhuweizz/powersync_golang/internal/core"
)

// TestFileLockPreventsDoubleOpen 验证同一 DB 文件被第二个实例打开会失败，
// Close 之后可以重新打开。
func TestFileLockPreventsDoubleOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.db")
	s, err := core.ParseSchema([]byte(schemaV1))
	if err != nil {
		t.Fatal(err)
	}
	first, err := core.Open(context.Background(), core.Options{Path: path, Schema: s})
	if err != nil {
		t.Fatalf("第一次 Open: %v", err)
	}

	if _, err := core.Open(context.Background(), core.Options{Path: path, Schema: s}); !errors.Is(err, core.ErrDatabaseLocked) {
		t.Fatalf("第二次 Open err = %v, want ErrDatabaseLocked", err)
	}

	// 显式关掉文件锁的实例可以打开（测试/特殊场景用）。
	unlocked, err := core.Open(context.Background(), core.Options{Path: path, Schema: s, NoFileLock: true})
	if err != nil {
		t.Fatalf("NoFileLock Open: %v", err)
	}
	unlocked.Close()

	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again, err := core.Open(context.Background(), core.Options{Path: path, Schema: s})
	if err != nil {
		t.Fatalf("Close 之后应能重新打开: %v", err)
	}
	again.Close()
}

// TestFileLockSkippedForMemoryDB 验证 :memory: 不加锁。
func TestFileLockSkippedForMemoryDB(t *testing.T) {
	for i := 0; i < 2; i++ {
		db, err := core.Open(context.Background(), core.Options{Path: ":memory:"})
		if err != nil {
			t.Fatalf("第 %d 次打开内存库: %v", i+1, err)
		}
		db.Close()
	}
}
