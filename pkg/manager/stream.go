package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

var (
	ErrUnAuthorized        = errors.New("unauthorized access to download link")
	ErrLinkNotFound        = errors.New("download link not found")
	ErrBandwidthExceeded   = errors.New("bandwidth limit exceeded")
	ErrInvalidDownloadCode = errors.New("invalid download code")
	ErrLinkExpired         = errors.New("download link expired")
	ErrFileNotAvailable    = errors.New("file not available for download")
	Err404                 = errors.New("HTTP 404 Not Found")
	Err429                 = errors.New("HTTP 429 Too Many Requests")
	Err503                 = errors.New("HTTP 503 Service Unavailable")
)

func errorCodeToError(code string) error {
	switch code {
	case "link_not_found":
		return ErrLinkNotFound
	case "bandwidth_exceeded":
		return ErrBandwidthExceeded
	case "link_expired":
		return ErrLinkExpired
	case "file_not_available":
		return ErrFileNotAvailable
	case "invalid_download_code":
		return ErrInvalidDownloadCode
	case "401", "unauthorized":
		return ErrUnAuthorized
	case "404":
		return Err404
	case "429":
		return Err429
	case "503":
		return Err503
	default:
		return fmt.Errorf("unknown error code: %s", code)

	}
}

const (
	MaxNetworkRetries = 5
)

type StreamError struct {
	Err       error
	Retryable bool
	LinkError bool // true if we should try a new link
}

func (e StreamError) Error() string {
	return e.Err.Error()
}

// isConnectionError checks if the error is related to connection issues
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// Check for common connection errors
	if strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused") {
		return true
	}

	// Check for net.Error types
	var netErr net.Error
	return errors.As(err, &netErr)
}

// handleBadDL marks a download link as invalid
func (m *Manager) handleBadDL(torrent *storage.Torrent, filename string, downloadLink types.DownloadLink, err error) {
	client := m.DebridClient(downloadLink.Debrid)
	if client == nil {
		return
	}
	accountManager := client.AccountManager()

	if errors.Is(err, ErrBandwidthExceeded) {
		// Disable the account for bandwidth exceeded or unauthorized errors
		account, err := accountManager.GetAccount(downloadLink.Token)
		if err != nil {
			m.logger.Error().Err(err).Str("token", utils.Mask(downloadLink.Token)).Msg("Failed to get account to disable")
			return
		}
		if account == nil {
			m.logger.Error().Str("token", utils.Mask(downloadLink.Token)).Msg("Account not found to disable")
			return
		}
		accountManager.Disable(account)
	} else if errors.Is(err, ErrInvalidDownloadCode) {
		// Mark the file has deleted and never to be tried again
		file, ok := torrent.Files[filename]
		if !ok {
			m.logger.Error().Str("torrent", torrent.Name).Str("file", filename).Msg("File not found to mark as deleted")
			return
		}
		file.Deleted = true
		torrent.UpdatedAt = time.Now()
		_ = m.storage.AddOrUpdate(torrent)
	}
}

func (m *Manager) LinkIsValid(ctx context.Context, downloadLink types.DownloadLink) error {
	err, exists := m.touchedLinks.Load(downloadLink.DownloadLink)
	if exists {
		return err
	}
	err = m.TouchFile(ctx, downloadLink)
	m.touchedLinks.Store(downloadLink.DownloadLink, err)
	// First time touching the link, log any error
	if err != nil {
		m.logger.Error().
			Str("debrid", downloadLink.Debrid).
			Str("token", utils.Mask(downloadLink.Token)).
			Str("link", downloadLink.DownloadLink).
			Err(err).
			Msg("Download link validation failed")
	}
	return err
}

func (m *Manager) TouchFile(ctx context.Context, downloadLink types.DownloadLink) error {
	// Make a HEAD request to touch the file
	req, err := http.NewRequestWithContext(ctx, "HEAD", downloadLink.DownloadLink, nil)
	if err != nil {
		return fmt.Errorf("failed to create HEAD request: %w", err)
	}
	resp, err := m.streamClient.Do(req)
	if err != nil {
		return fmt.Errorf("HEAD request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil // Link is valid
	}

	errorCode := resp.Header.Get("X-Error")
	// Realdebrid adds an X-Error header for invalid links
	if errorCode == "" {
		errorCode = strconv.Itoa(resp.StatusCode) // Fallback to status code
	}
	return errorCodeToError(errorCode)
}

func (m *Manager) Stream(ctx context.Context, torrentName, filename string, start, end int64) (*http.Response, error) {
	var lastErr error
	torrent, err := m.GetTorrentByFileName(torrentName, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get torrent: %w", err)
	}

	// First, get the download link
	downloadLink, err := m.GetDownloadLink(torrent, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get download link: %w", err)
	}
	// Validate the link
	if err := m.LinkIsValid(ctx, downloadLink); err != nil {
		// Handle the error here
		m.handleBadDL(torrent, filename, downloadLink, err)
		return nil, fmt.Errorf("download link is invalid: %w", err)
	}

	// Outer loop: Link retries
	for retry := 0; retry < m.config.Retries; retry++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := m.doRequest(ctx, downloadLink.DownloadLink, start, end)
		if err != nil {
			// Network/connection error
			lastErr = err

			// Backoff and continue network retry
			if retry < m.config.Retries {
				backoff := time.Duration(retry+1) * time.Second
				jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
				select {
				case <-time.After(backoff + jitter):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			} else {
				return nil, fmt.Errorf("network request failed after retries: %w", lastErr)
			}
		}

		// Got response - check status
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			// Reset error counter on success
			return resp, nil
		} else {
			return nil, StreamError{
				Err:       fmt.Errorf("unexpected HTTP status: %d", resp.StatusCode),
				Retryable: false,
				LinkError: false,
			}
		}
	}

	return nil, fmt.Errorf("error streaming %s/%s failed after %d link retries: %w", torrentName, filename, m.config.Retries, lastErr)
}

func (m *Manager) StreamReader(ctx context.Context, torrentName, filename string, start, end int64) (io.ReadCloser, error) {
	resp, err := m.Stream(ctx, torrentName, filename, start, end)
	if err != nil {
		return nil, err
	}

	// Validate we got the expected content
	if resp.ContentLength == 0 {
		resp.Body.Close()
		return nil, fmt.Errorf("received empty response")
	}

	return resp.Body, nil
}

func (m *Manager) doRequest(ctx context.Context, url string, start, end int64) (*http.Response, error) {
	var lastErr error
	// Retry loop specifically for connection-level failures (EOF, reset, etc.)
	for connRetry := 0; connRetry < 3; connRetry++ {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, StreamError{Err: err, Retryable: false}
		}

		// Set range header
		if start > 0 || end > 0 {
			rangeHeader := fmt.Sprintf("bytes=%d-", start)
			if end > 0 {
				rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
			}
			req.Header.Set("Range", rangeHeader)
		}

		// Set optimized headers for streaming
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Accept-Encoding", "identity") // Disable compression for streaming
		req.Header.Set("Cache-Control", "no-cache")

		resp, err := m.streamClient.Do(req)
		if err != nil {
			lastErr = err

			// Check if it's a connection error that we should retry
			if isConnectionError(err) && connRetry < 2 {
				// Brief backoff before retrying with fresh connection
				time.Sleep(time.Duration(connRetry+1) * 100 * time.Millisecond)
				continue
			}

			return nil, StreamError{Err: err, Retryable: true}
		}
		return resp, nil
	}

	return nil, StreamError{Err: fmt.Errorf("connection retry exhausted: %w", lastErr), Retryable: true}
}
