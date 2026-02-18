package manager

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/melbahja/got"
)

const (
	smallFile  = 1 << 20  // 1 MB
	mediumFile = 10 << 20 // 10 MB
	largeFile  = 50 << 20 // 50 MB
)

func newTestServer(size int) *httptest.Server {
	data := make([]byte, size)
	rand.Read(data)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "testfile.bin", time.Now(), NewByteReadSeeker(data))
	}))
}

type ByteReadSeeker struct {
	data   []byte
	offset int64
}

func NewByteReadSeeker(data []byte) *ByteReadSeeker {
	return &ByteReadSeeker{data: data}
}

func (b *ByteReadSeeker) Read(p []byte) (int, error) {
	if b.offset >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.offset:])
	b.offset += int64(n)
	return n, nil
}

func (b *ByteReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.offset = offset
	case io.SeekCurrent:
		b.offset += offset
	case io.SeekEnd:
		b.offset = int64(len(b.data)) + offset
	}
	if b.offset < 0 {
		b.offset = 0
	}
	return b.offset, nil
}

// Shared transport to avoid port exhaustion in benchmarks
var sharedTransport = &http.Transport{
	MaxIdleConns:        100,
	MaxIdleConnsPerHost: 100,
	IdleConnTimeout:     90 * time.Second,
	DisableKeepAlives:   false,
}

// =============================================================================
// Method 1: grab (current implementation)
// =============================================================================

func downloadWithGrab(client *grab.Client, url, dest string) error {
	req, err := grab.NewRequest(dest, url)
	if err != nil {
		return err
	}
	resp := client.Do(req)
	return resp.Err()
}

// =============================================================================
// Method 2: net/http stdlib with io.Copy (simple)
// =============================================================================

func downloadWithStdlib(client *http.Client, ctx context.Context, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	return err
}

// =============================================================================
// Method 3: net/http stdlib with custom buffer + progress tracking
// =============================================================================

func downloadWithStdlibBuffered(client *http.Client, ctx context.Context, url, dest string, bufSize int) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, bufSize)
	var downloaded atomic.Int64

	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			_, writeErr := f.Write(buf[:n])
			if writeErr != nil {
				return writeErr
			}
			downloaded.Add(int64(n))
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return readErr
		}
	}

	return nil
}

// =============================================================================
// Method 4: melbahja/got (parallel chunk downloader)
// =============================================================================

func downloadWithGot(url, dest string, concurrency uint) error {
	d := got.NewDownload(context.Background(), url, dest)
	d.Concurrency = concurrency

	if err := d.Init(); err != nil {
		return err
	}

	return d.Start()
}

// =============================================================================
// Method 5: aria2c (external binary)
// =============================================================================

func downloadWithAria2(url, dest string, connections int) error {
	dir := filepath.Dir(dest)
	name := filepath.Base(dest)
	cmd := exec.Command("aria2c",
		"--dir="+dir,
		"--out="+name,
		fmt.Sprintf("--split=%d", connections),
		fmt.Sprintf("--max-connection-per-server=%d", connections),
		"--min-split-size=1M",
		"--file-allocation=none",
		"--console-log-level=error",
		"--summary-interval=0",
		url,
	)
	return cmd.Run()
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkDownload(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1MB", smallFile},
		{"10MB", mediumFile},
		{"50MB", largeFile},
	}

	// Create shared clients once
	grabClient := &grab.Client{
		UserAgent:  "BenchmarkTest",
		HTTPClient: &http.Client{Transport: sharedTransport},
	}
	stdlibClient := &http.Client{Transport: sharedTransport}

	for _, sz := range sizes {
		server := newTestServer(sz.size)

		tmpDir := b.TempDir()

		b.Run(fmt.Sprintf("Grab/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("grab_%s_%d", sz.name, i))
				if err := downloadWithGrab(grabClient, server.URL, dest); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		b.Run(fmt.Sprintf("Stdlib/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("stdlib_%s_%d", sz.name, i))
				if err := downloadWithStdlib(stdlibClient, context.Background(), server.URL, dest); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		b.Run(fmt.Sprintf("StdlibBuf32K/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("stdlib32k_%s_%d", sz.name, i))
				if err := downloadWithStdlibBuffered(stdlibClient, context.Background(), server.URL, dest, 32*1024); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		b.Run(fmt.Sprintf("StdlibBuf256K/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("stdlib256k_%s_%d", sz.name, i))
				if err := downloadWithStdlibBuffered(stdlibClient, context.Background(), server.URL, dest, 256*1024); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		b.Run(fmt.Sprintf("StdlibBuf1M/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("stdlib1m_%s_%d", sz.name, i))
				if err := downloadWithStdlibBuffered(stdlibClient, context.Background(), server.URL, dest, 1024*1024); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		b.Run(fmt.Sprintf("Got_1conn/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("got1_%s_%d", sz.name, i))
				if err := downloadWithGot(server.URL, dest, 1); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		b.Run(fmt.Sprintf("Got_4conn/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("got4_%s_%d", sz.name, i))
				if err := downloadWithGot(server.URL, dest, 4); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		b.Run(fmt.Sprintf("Got_8conn/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("got8_%s_%d", sz.name, i))
				if err := downloadWithGot(server.URL, dest, 8); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		// --- aria2c (1 connection) ---
		b.Run(fmt.Sprintf("Aria2_1conn/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("aria2_1_%s_%d", sz.name, i))
				if err := downloadWithAria2(server.URL+"/testfile.bin", dest, 1); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		// --- aria2c (4 connections) ---
		b.Run(fmt.Sprintf("Aria2_4conn/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("aria2_4_%s_%d", sz.name, i))
				if err := downloadWithAria2(server.URL+"/testfile.bin", dest, 4); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		// --- aria2c (8 connections) ---
		b.Run(fmt.Sprintf("Aria2_8conn/%s", sz.name), func(b *testing.B) {
			b.SetBytes(int64(sz.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dest := filepath.Join(tmpDir, fmt.Sprintf("aria2_8_%s_%d", sz.name, i))
				if err := downloadWithAria2(server.URL+"/testfile.bin", dest, 8); err != nil {
					b.Fatal(err)
				}
				os.Remove(dest)
			}
		})

		server.Close()
	}
}
