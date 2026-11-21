//go:build windows

package core

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// errLocked 表示同一数据库文件已被另一个实例（进程或同进程的另一个 *DB）占用。
var errLocked = errors.New("core: database is locked by another instance")

// fileLock 是跨进程的排它锁（LockFileEx）。用 Sidecar 文件 <db>.lock 实现。
type fileLock struct {
	f *os.File
}

func acquireFileLock(path string) (*fileLock, error) {
	lockPath := path + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("core: 打开锁文件 %s: %w", lockPath, err)
	}
	// 锁住文件头 1 个字节；失败立即返回（不阻塞）。
	var ol windows.Overlapped
	err = windows.LockFileEx(windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ol)
	if err != nil {
		f.Close()
		errno := windows.ERROR_LOCK_VIOLATION
		if errors.Is(err, errno) || errors.Is(err, windows.ERROR_IO_PENDING) || errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
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
	var ol windows.Overlapped
	_ = windows.UnlockFileEx(windows.Handle(l.f.Fd()), 0, 1, 0, &ol)
	_ = l.f.Close()
	l.f = nil
}
