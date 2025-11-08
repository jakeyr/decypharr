package lightning

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

// ConnectionPool manages multiple persistent HTTP/2 connections
// for parallel downloading and connection reuse
type ConnectionPool struct {
	// Configuration
	poolSize   int
	serverURL  string
	httpClient *http.Client

	// Connection management
	connections []*HTTP2Connection
	healthMon   *HealthMonitor
	router      *ConnectionRouter

	// State
	mu       sync.RWMutex
	closed   atomic.Bool
	stats    *PoolStats
	initOnce sync.Once
	initErr  error
}

// HTTP2Connection represents a single HTTP/2 connection
type HTTP2Connection struct {
	id     int
	client *http.Client
	url    string
	pool   *ConnectionPool // Reference to pool for error handling

	// Health tracking
	mu           sync.RWMutex
	healthy      atomic.Bool
	lastUsed     atomic.Int64 // Unix timestamp
	errorCount   atomic.Int64
	successCount atomic.Int64
}

// PoolStats tracks minimal pool statistics
type PoolStats struct {
	TotalRequests      atomic.Int64
	FailedRequests     atomic.Int64
	HealthyConnections atomic.Int32
}

// HealthMonitor continuously monitors connection health
type HealthMonitor struct {
	pool          *ConnectionPool
	checkInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// ConnectionRouter routes requests to optimal connections
type ConnectionRouter struct {
	pool *ConnectionPool
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(ctx context.Context, serverURL string, poolSize int) (*ConnectionPool, error) {
	if poolSize <= 0 {
		poolSize = 8 // Default
	}

	pool := &ConnectionPool{
		poolSize:    poolSize,
		serverURL:   serverURL,
		connections: make([]*HTTP2Connection, poolSize),
		stats:       &PoolStats{},
	}

	// Initialize pool once
	pool.initOnce.Do(func() {
		pool.initErr = pool.initialize(ctx)
	})

	if pool.initErr != nil {
		return nil, pool.initErr
	}

	// Create router
	pool.router = &ConnectionRouter{pool: pool}

	// Start health monitor
	pool.healthMon = newHealthMonitor(ctx, pool)
	pool.healthMon.start()

	return pool, nil
}

// initialize sets up all connections
func (cp *ConnectionPool) initialize(ctx context.Context) error {
	// Create HTTP/2 transport
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			// Use plain TCP for HTTP
			return net.Dial(network, addr)
		},
		DisableCompression: true, // We want raw data
		ReadIdleTimeout:    30 * time.Second,
		PingTimeout:        15 * time.Second,
	}

	// Create base HTTP client
	baseClient := &http.Client{
		Transport: transport,
		Timeout:   0, // No timeout for streaming
	}

	// Initialize connections
	for i := 0; i < cp.poolSize; i++ {
		conn := &HTTP2Connection{
			id:     i,
			client: baseClient,
			url:    cp.serverURL,
			pool:   cp,
		}
		conn.healthy.Store(true)
		conn.lastUsed.Store(time.Now().Unix())

		cp.connections[i] = conn
	}

	cp.stats.HealthyConnections.Store(int32(cp.poolSize))

	return nil
}

// GetConnection returns the best available connection for a request
func (cp *ConnectionPool) GetConnection(ctx context.Context) (*HTTP2Connection, error) {
	if cp.closed.Load() {
		return nil, errors.New("connection pool is closed")
	}

	return cp.router.selectConnection(ctx)
}

// Download downloads a byte range using the pool
func (cp *ConnectionPool) Download(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
	conn, err := cp.GetConnection(ctx)
	if err != nil {
		return nil, err
	}

	return conn.downloadRange(ctx, offset, size)
}

// downloadRange performs a range request on this connection with retries
func (c *HTTP2Connection) downloadRange(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
	if !c.healthy.Load() {
		return nil, errors.New("connection is unhealthy")
	}

	var lastErr error
	// Retry loop for connection-level failures (EOF, reset, etc.)
	for connRetry := 0; connRetry < 3; connRetry++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Create request
		req, err := http.NewRequestWithContext(ctx, "GET", c.url, nil)
		if err != nil {
			c.recordError()
			return nil, fmt.Errorf("create request: %w", err)
		}

		// Set range header
		rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
		req.Header.Set("Range", rangeHeader)
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Accept-Encoding", "identity") // Disable compression for streaming
		req.Header.Set("Cache-Control", "no-cache")

		// Perform request
		c.pool.stats.TotalRequests.Add(1)
		resp, err := c.client.Do(req)

		if err != nil {
			lastErr = err
			c.recordError()

			// Check if it's a connection error that we should retry
			if isConnectionError(err) && connRetry < 2 {
				// Brief backoff before retrying with fresh connection
				time.Sleep(time.Duration(connRetry+1) * 100 * time.Millisecond)
				continue
			}

			return nil, fmt.Errorf("http request failed after %d retries: %w", connRetry+1, err)
		}

		// Check status code
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			// Success
			c.recordSuccess()
			c.lastUsed.Store(time.Now().Unix())
			return resp.Body, nil
		}

		// Handle HTTP errors
		resp.Body.Close()
		c.recordError()

		switch resp.StatusCode {
		case http.StatusNotFound:
			// Link not found - fatal error
			return nil, fmt.Errorf("download link not found (404)")

		case http.StatusServiceUnavailable, http.StatusTooManyRequests:
			// Rate limited or service unavailable - retryable
			lastErr = fmt.Errorf("HTTP %d: rate limited or service unavailable", resp.StatusCode)
			if connRetry < 2 {
				backoff := time.Duration(connRetry+1) * time.Second
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				continue
			}

		default:
			// Other errors - check if retryable
			if resp.StatusCode >= 500 && connRetry < 2 {
				lastErr = fmt.Errorf("HTTP %d: server error", resp.StatusCode)
				time.Sleep(time.Duration(connRetry+1) * 500 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}
	}

	return nil, fmt.Errorf("connection retry exhausted: %w", lastErr)
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

// recordError increments error count and may mark connection unhealthy
func (c *HTTP2Connection) recordError() {
	c.pool.stats.FailedRequests.Add(1)
	errorCount := c.errorCount.Add(1)

	// If error rate is too high, mark as unhealthy
	successCount := c.successCount.Load()
	totalRequests := errorCount + successCount

	if totalRequests > 10 {
		errorRate := float64(errorCount) / float64(totalRequests)
		if errorRate > 0.2 { // More than 20% error rate
			c.healthy.Store(false)
		}
	}
}

// recordSuccess increments success count and marks healthy
func (c *HTTP2Connection) recordSuccess() {
	c.successCount.Add(1)
	c.errorCount.Store(0) // Reset error count on success
	c.healthy.Store(true)
}

// selectConnection chooses the best connection for a request
func (cr *ConnectionRouter) selectConnection(ctx context.Context) (*HTTP2Connection, error) {
	cp := cr.pool
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	var best *HTTP2Connection
	var lowestErrors int64 = -1

	// Find the healthiest connection with lowest error count
	for _, conn := range cp.connections {
		if !conn.healthy.Load() {
			continue
		}

		errorCount := conn.errorCount.Load()
		if best == nil || errorCount < lowestErrors {
			best = conn
			lowestErrors = errorCount
		}
	}

	if best == nil {
		// No healthy connections, try to find least bad one
		for _, conn := range cp.connections {
			if best == nil || conn.errorCount.Load() < best.errorCount.Load() {
				best = conn
			}
		}
	}

	if best == nil {
		return nil, errors.New("no available connections")
	}

	return best, nil
}

// newHealthMonitor creates a health monitor
func newHealthMonitor(ctx context.Context, pool *ConnectionPool) *HealthMonitor {
	ctx, cancel := context.WithCancel(ctx)
	return &HealthMonitor{
		pool:          pool,
		checkInterval: 30 * time.Second,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// start begins health monitoring
func (hm *HealthMonitor) start() {
	hm.wg.Add(1)
	go hm.monitorLoop()
}

// monitorLoop periodically checks connection health
func (hm *HealthMonitor) monitorLoop() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.checkHealth()
		case <-hm.ctx.Done():
			return
		}
	}
}

// checkHealth checks all connections
func (hm *HealthMonitor) checkHealth() {
	hm.pool.mu.RLock()
	defer hm.pool.mu.RUnlock()

	healthyCount := int32(0)

	for _, conn := range hm.pool.connections {
		// Check if connection has been idle too long
		lastUsed := time.Unix(conn.lastUsed.Load(), 0)
		if time.Since(lastUsed) > 5*time.Minute {
			// Reset error counts for idle connections
			conn.errorCount.Store(0)
			conn.successCount.Store(0)
			conn.healthy.Store(true)
		}

		// Check error rate
		errorCount := conn.errorCount.Load()
		successCount := conn.successCount.Load()
		total := errorCount + successCount

		if total > 10 {
			errorRate := float64(errorCount) / float64(total)
			if errorRate > 0.2 {
				conn.healthy.Store(false)
			} else if errorRate < 0.05 {
				conn.healthy.Store(true)
			}
		}

		if conn.healthy.Load() {
			healthyCount++
		}
	}

	hm.pool.stats.HealthyConnections.Store(healthyCount)
}

// Stats returns pool statistics
func (cp *ConnectionPool) Stats() *PoolStats {
	return cp.stats
}

// Close closes all connections
func (cp *ConnectionPool) Close() error {
	if cp.closed.CompareAndSwap(false, true) {
		// Stop health monitor
		if cp.healthMon != nil {
			cp.healthMon.cancel()
			cp.healthMon.wg.Wait()
		}

		// No explicit connection cleanup needed for HTTP/2
		// Transport will handle connection lifecycle
	}
	return nil
}

// GetHealthyConnectionCount returns the number of healthy connections
func (cp *ConnectionPool) GetHealthyConnectionCount() int {
	return int(cp.stats.HealthyConnections.Load())
}
