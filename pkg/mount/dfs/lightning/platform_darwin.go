//go:build darwin

package lightning

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// checkKernelVersion on macOS (Darwin kernel)
func checkKernelVersion(major, minor int) bool {
	// macOS doesn't use Linux kernel versioning
	// This function is for Linux compatibility
	return false
}

// HasSendfileSupport checks if sendfile is available
func HasSendfileSupport() bool {
	// sendfile available since Darwin 9.0 (OS X 10.5)
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

// Sendfile performs zero-copy data transfer using sendfile system call
// macOS sendfile has different signature than Linux
func Sendfile(outfd int, infd int, offset *int64, count int) (written int, err error) {
	// macOS sendfile signature:
	// int sendfile(int fd, int s, off_t offset, off_t *len, struct sf_hdtr *hdtr, int flags);
	//
	// Where:
	// - fd: file descriptor (input file)
	// - s: socket descriptor (output socket)
	// - offset: starting offset in file
	// - len: pointer to length (value-result parameter)
	// - hdtr: headers/trailers (we use nil)
	// - flags: flags (we use 0)

	length := int64(count)

	// Call sendfile
	_, _, errno := syscall.Syscall6(
		syscall.SYS_SENDFILE,
		uintptr(infd),      // fd (file)
		uintptr(outfd),     // s (socket)
		uintptr(*offset),   // offset
		uintptr(unsafe.Pointer(&length)), // len (value-result)
		0,                  // hdtr (NULL)
		0,                  // flags
	)

	if errno != 0 {
		// On macOS, EAGAIN means some bytes were sent
		if errno == syscall.EAGAIN {
			return int(length), nil
		}
		return int(length), errno
	}

	return int(length), nil
}

// CreatePipe creates a pipe
func CreatePipe() (r, w int, err error) {
	var p [2]int
	err = syscall.Pipe(p[:])
	if err != nil {
		return 0, 0, err
	}
	return p[0], p[1], nil
}

// SetPipeSize - macOS doesn't support F_SETPIPE_SZ
func SetPipeSize(fd int, size int) error {
	// Not supported on macOS, return nil (no-op)
	return nil
}

// GetPipeSize - macOS doesn't support F_GETPIPE_SZ
func GetPipeSize(fd int) (int, error) {
	// Return default pipe size on macOS (16KB)
	return 16 * 1024, nil
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

// SetKeepaliveParams sets TCP keepalive parameters (macOS specific)
func SetKeepaliveParams(fd int, idleSec, intervalSec, count int) error {
	// On macOS, use TCP_KEEPALIVE for idle time (similar to TCP_KEEPIDLE)
	const TCP_KEEPALIVE = 0x10 // From netinet/tcp.h

	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, TCP_KEEPALIVE, idleSec); err != nil {
		return fmt.Errorf("set TCP_KEEPALIVE: %w", err)
	}

	// macOS doesn't have TCP_KEEPINTVL and TCP_KEEPCNT
	// The system uses default values
	return nil
}

// Kqueue creates a kqueue for event notification
func Kqueue() (int, error) {
	kq, err := syscall.Kqueue()
	return kq, err
}

// Kevent registers events with kqueue
func Kevent(kq int, changes, events []syscall.Kevent_t, timeout *syscall.Timespec) (n int, err error) {
	return syscall.Kevent(kq, changes, events, timeout)
}

// KqueueEvent represents a kqueue event
type KqueueEvent struct {
	Ident  uint64 // Identifier (usually fd)
	Filter int16  // Event filter
	Flags  uint16 // Action flags
	Fflags uint32 // Filter flags
	Data   int64  // Filter-specific data
	Udata  unsafe.Pointer // User data
}

// Kqueue filter types
const (
	EVFILT_READ  = syscall.EVFILT_READ  // File descriptor readable
	EVFILT_WRITE = syscall.EVFILT_WRITE // File descriptor writable
)

// Kqueue flags
const (
	EV_ADD     = syscall.EV_ADD     // Add event to kqueue
	EV_DELETE  = syscall.EV_DELETE  // Delete event from kqueue
	EV_ENABLE  = syscall.EV_ENABLE  // Enable event
	EV_DISABLE = syscall.EV_DISABLE // Disable event
	EV_ONESHOT = syscall.EV_ONESHOT // Only trigger once
	EV_CLEAR   = syscall.EV_CLEAR   // Clear event after retrieval
	EV_EOF     = syscall.EV_EOF     // EOF detected
	EV_ERROR   = syscall.EV_ERROR   // Error occurred
)

// RegisterReadEvent registers a fd for read events with kqueue
func RegisterReadEvent(kq int, fd int) error {
	event := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: EVFILT_READ,
		Flags:  EV_ADD | EV_ENABLE | EV_CLEAR,
		Fflags: 0,
		Data:   0,
		Udata:  nil,
	}

	changes := []syscall.Kevent_t{event}
	_, err := Kevent(kq, changes, nil, nil)
	return err
}

// UnregisterEvent unregisters a fd from kqueue
func UnregisterEvent(kq int, fd int, filter int16) error {
	event := syscall.Kevent_t{
		Ident:  uint64(fd),
		Filter: filter,
		Flags:  EV_DELETE,
		Fflags: 0,
		Data:   0,
		Udata:  nil,
	}

	changes := []syscall.Kevent_t{event}
	_, err := Kevent(kq, changes, nil, nil)
	return err
}

// WaitEvents waits for events on kqueue
func WaitEvents(kq int, events []syscall.Kevent_t, timeoutMs int) (int, error) {
	var timeout *syscall.Timespec
	if timeoutMs >= 0 {
		ts := syscall.NsecToTimespec(int64(timeoutMs) * 1000000)
		timeout = &ts
	}

	return Kevent(kq, nil, events, timeout)
}
