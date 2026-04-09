package vfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	fuseconfig "github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
)

func newTestCache(cacheDir string) *Cache {
	return &Cache{
		config: &fuseconfig.FuseConfig{
			CacheDir:    cacheDir,
			CacheExpiry: time.Hour,
		},
		items:  xsync.NewMap[string, *CacheItem](),
		logger: zerolog.Nop(),
	}
}

func TestScanDiskCandidates_DoesNotDeleteLegacyFiles(t *testing.T) {
	cacheDir := t.TempDir()
	entryDir := filepath.Join(cacheDir, "entry")
	if err := os.MkdirAll(entryDir, 0755); err != nil {
		t.Fatal(err)
	}

	metaPath := filepath.Join(entryDir, "meta.json")
	dataPath := filepath.Join(entryDir, "data")
	if err := os.WriteFile(metaPath, []byte(`{"size":1}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	c := newTestCache(cacheDir)
	_, _ = c.scanDiskCandidates()

	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("meta.json should not be deleted on scan: %v", err)
	}
	if _, err := os.Stat(dataPath); err != nil {
		t.Fatalf("data should not be deleted on scan: %v", err)
	}
}

func TestEvictCandidates_RemovesOnlyTargetPair(t *testing.T) {
	cacheDir := t.TempDir()
	entryDir := filepath.Join(cacheDir, "entry")
	if err := os.MkdirAll(entryDir, 0755); err != nil {
		t.Fatal(err)
	}

	targetData := filepath.Join(entryDir, "a.mkv")
	targetMeta := targetData + ".json"
	otherData := filepath.Join(entryDir, "b.mkv")
	otherMeta := otherData + ".json"

	for _, path := range []string{targetData, targetMeta, otherData, otherMeta} {
		if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	c := newTestCache(cacheDir)
	now := time.Now()
	candidates := []candidateEntry{
		{
			key:        "entry/a.mkv",
			path:       entryDir,
			dataPath:   targetData,
			metaPath:   targetMeta,
			atime:      now.Add(-2 * time.Hour),
			mtime:      now.Add(-2 * time.Hour),
			cachedSize: 1,
		},
	}

	totalSize, removed := c.evictCandidates(now, candidates, 1, 0)
	if removed != 1 {
		t.Fatalf("expected 1 candidate removed, got %d", removed)
	}
	if totalSize != 0 {
		t.Fatalf("expected totalSize 0 after eviction, got %d", totalSize)
	}

	if _, err := os.Stat(targetData); !os.IsNotExist(err) {
		t.Fatalf("target data should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(targetMeta); !os.IsNotExist(err) {
		t.Fatalf("target metadata should be removed, stat err=%v", err)
	}

	if _, err := os.Stat(otherData); err != nil {
		t.Fatalf("other data should remain, stat err=%v", err)
	}
	if _, err := os.Stat(otherMeta); err != nil {
		t.Fatalf("other metadata should remain, stat err=%v", err)
	}
	if _, err := os.Stat(entryDir); err != nil {
		t.Fatalf("entry directory should remain, stat err=%v", err)
	}
}
