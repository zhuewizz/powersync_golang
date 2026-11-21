//go:build !windows

package core

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// errLocked 表示同一数据库文件已被另一个实例（进程或同进程的另一个 *DB）占用。
var errLocked = errors.New("core: database is locked by another instance")

// fileLock 是跨进程的排它锁（flock）。用 Sidecar 文件 <db>.lock 实现，
// 避免和 SQLite 自己的锁语义混在一起。
type fileLock struct {
	f *os.File
}

func acquireFileLock(path string) (*fileLock, error) {
	lockPath := path + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("core: 打开锁文件 %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", errLocked, path)
		}
		return nil, fmt.Errorf("core: 加锁 %s: %w", lockPath, err)
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
	l.f = nil
}
