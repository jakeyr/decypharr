//go:build linux

package lightning

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// checkKernelVersion checks if Linux kernel version is >= required
func checkKernelVersion(major, minor int) bool {
	var uname syscall.Utsname
	if err := syscall.Uname(&uname); err != nil {
		return false
	}

	// Convert release string to version numbers
	release := string(uname.Release[:])
	parts := strings.Split(release, ".")
	if len(parts) < 2 {
		return false
	}

	kMajor, err1 := strconv.Atoi(parts[0])
	kMinor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}

	if kMajor > major {
		return true
	}
	if kMajor == major && kMinor >= minor {
		return true
	}

	return false
}

// hasSpliceSupport checks if splice is available
func hasSpliceSupport() bool {
	// splice is available since kernel 2.6.17
	return checkKernelVersion(2, 6)
}

// HasSendfileSupport checks if sendfile is available
func HasSendfileSupport() bool {
	// sendfile is available since kernel 2.2
	return true
}

// GetPageSize returns system page size
func GetPageSize() int {
	return syscall.Getpagesize()
}

// SetNonBlocking sets file descriptor to non-blocking mode
func SetNonBlocking(fd int) error {
	return syscall.SetNonblock(fd, true)
}

// GetFD gets file descriptor from file
func GetFD(f *os.File) int {
	return int(f.Fd())
}

// Splice performs zero-copy data transfer using splice system call
func Splice(rfd int, roff *int64, wfd int, woff *int64, len int, flags int) (n int, err error) {
	r, _, errno := syscall.Syscall6(
		syscall.SYS_SPLICE,
		uintptr(rfd),
		uintptr(0), // roff not used for sockets
		uintptr(wfd),
		uintptr(0), // woff not used for pipes
		uintptr(len),
		uintptr(flags),
	)
	if errno != 0 {
		return 0, errno
	}
	return int(r), nil
}

// Constants for splice
const (
	SPLICE_F_MOVE     = 0x01 // Move pages instead of copying
	SPLICE_F_NONBLOCK = 0x02 // Non-blocking operation
	SPLICE_F_MORE     = 0x04 // More data coming
)

// CreatePipe creates a pipe for splice operations
func CreatePipe() (r, w int, err error) {
	var p [2]int
	err = syscall.Pipe(p[:])
	if err != nil {
		return 0, 0, err
	}
	return p[0], p[1], nil
}

// SetPipeSize sets the pipe buffer size (Linux 2.6.35+)
func SetPipeSize(fd int, size int) error {
	const F_SETPIPE_SZ = 1031 // From fcntl.h
	_, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		uintptr(fd),
		uintptr(F_SETPIPE_SZ),
		uintptr(size),
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// GetPipeSize gets the pipe buffer size
func GetPipeSize(fd int) (int, error) {
	const F_GETPIPE_SZ = 1032 // From fcntl.h
	size, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		uintptr(fd),
		uintptr(F_GETPIPE_SZ),
		0,
	)
	if errno != 0 {
		return 0, errno
	}
	return int(size), nil
}

// Sendfile performs zero-copy data transfer using sendfile system call
func Sendfile(outfd int, infd int, offset *int64, count int) (written int, err error) {
	n, err := syscall.Sendfile(outfd, infd, offset, count)
	return int(n), err
}

// Tee duplicates pipe contents without consuming (Linux 2.6.17+)
func Tee(rfd int, wfd int, len int, flags uint) (n int, err error) {
	r, _, errno := syscall.Syscall6(
		syscall.SYS_TEE,
		uintptr(rfd),
		uintptr(wfd),
		uintptr(len),
		uintptr(flags),
		0,
		0,
	)
	if errno != 0 {
		return 0, errno
	}
	return int(r), nil
}

// Vmsplice maps user memory into pipe (Linux 2.6.17+)
func Vmsplice(fd int, iov []syscall.Iovec, flags uint) (n int, err error) {
	r, _, errno := syscall.Syscall6(
		syscall.SYS_VMSPLICE,
		uintptr(fd),
		uintptr(unsafe.Pointer(&iov[0])),
		uintptr(len(iov)),
		uintptr(flags),
		0,
		0,
	)
	if errno != 0 {
		return 0, errno
	}
	return int(r), nil
}

// SetTCPNoDelay disables Nagle's algorithm for low latency
func SetTCPNoDelay(fd int) error {
	return syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
}

// SetReceiveBuffer sets SO_RCVBUF for large receive buffer
func SetReceiveBuffer(fd int, size int) error {
	return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, size)
}

// SetSendBuffer sets SO_SNDBUF for large send buffer
func SetSendBuffer(fd int, size int) error {
	return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, size)
}

// SetKeepalive enables TCP keepalive
func SetKeepalive(fd int) error {
	return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1)
}

// SetKeepaliveParams sets TCP keepalive parameters
func SetKeepaliveParams(fd int, idleSec, intervalSec, count int) error {
	// TCP_KEEPIDLE: seconds before sending keepalive probes
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, idleSec); err != nil {
		return fmt.Errorf("set TCP_KEEPIDLE: %w", err)
	}

	// TCP_KEEPINTVL: seconds between keepalive probes
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, intervalSec); err != nil {
		return fmt.Errorf("set TCP_KEEPINTVL: %w", err)
	}

	// TCP_KEEPCNT: number of keepalive probes
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, count); err != nil {
		return fmt.Errorf("set TCP_KEEPCNT: %w", err)
	}

	return nil
}
