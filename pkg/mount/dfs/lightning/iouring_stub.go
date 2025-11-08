//go:build !linux

package lightning

import (
	"context"
	"errors"
	"io"
)

// NewIOUringReader stub for non-Linux platforms
// This always returns an error, causing the caller to fall back to RingBufferReader
func NewIOUringReader(ctx context.Context, source io.ReadCloser) (ZeroCopyReader, error) {
	return nil, errors.New("io_uring not available on this platform")
}
