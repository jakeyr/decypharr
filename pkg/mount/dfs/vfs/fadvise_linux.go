//go:build linux

package vfs

import (
	"os"

	"golang.org/x/sys/unix"
)

// dropFileCache advises the kernel to evict the given file range from the page cache.
func dropFileCache(f *os.File, offset, length int64) {
	if f == nil || length <= 0 {
		return
	}
	fd := int(f.Fd())
	if fd < 0 {
		return
	}

	page := int64(os.Getpagesize())
	start := offset - (offset % page)
	end := offset + length
	if rem := end % page; rem != 0 {
		end += page - rem
	}
	if end <= start {
		end = start + page
	}
	_ = unix.Fadvise(fd, start, end-start, unix.FADV_DONTNEED)
}
