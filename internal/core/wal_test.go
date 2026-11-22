package core_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zhuewizz/powersync_golang/internal/core"
)

// TestCloseCheckpointsWAL 验证关闭时会把 WAL 截断回主库，使 .db 自包含
// （图形工具/复制场景不会再因为 -wal 缺失或 stale -shm 报 "malformed"）。
func TestCloseCheckpointsWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.db")
	s, err := core.ParseSchema([]byte(schemaV1))
	if err != nil {
		t.Fatal(err)
	}
	db, err := core.Open(context.Background(), core.Options{Path: path, Schema: s})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.SQL().Exec(`INSERT INTO todos(id,title) VALUES('a','x')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if fi, err := os.Stat(path + "-wal"); err == nil && fi.Size() > 0 {
		t.Errorf("关闭后 -wal 仍有 %d 字节（应被截断为 0）", fi.Size())
	}

	// 重新打开（不依赖 -wal）应能看到数据
	db2, err := core.Open(context.Background(), core.Options{Path: path, Schema: s})
	if err != nil {
		t.Fatalf("重新 Open: %v", err)
	}
	defer db2.Close()
	var n int
	if err := db2.SQL().QueryRow(`SELECT count(*) FROM todos`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("重新打开后行数 = %d, %v", n, err)
	}
	var ok string
	if err := db2.SQL().QueryRow(`PRAGMA integrity_check`).Scan(&ok); err != nil || ok != "ok" {
		t.Errorf("integrity_check = %q, %v", ok, err)
	}
}
