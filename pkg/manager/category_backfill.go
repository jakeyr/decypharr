package manager

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strings"
)

// torrentsJSONEntry mirrors the subset of fields we need from each value in
// `torrents.json` (the persistent qBit-emulated state). Lots more fields exist
// (size, state, files, etc.) — we don't care, just hash + category.
type torrentsJSONEntry struct {
	Hash     string `json:"hash"`
	Category string `json:"category"`
}

// backfillCategoriesFromTorrentsJSON merges the qBit `category` from
// torrents.json into the new Storage entries by infohash. Idempotent — only
// updates entries whose Category is currently empty, never overwrites a
// category set by qBit add at runtime.
//
// Why this exists: cache files (the source the migrator reads) carry only
// RD-side metadata. The qBit category that an *arr sets at add time lives in
// torrents.json. Without this backfill, every torrent migrated from existing
// cache files starts with `Category=""` and is invisible to a virtual folder
// filtered by category — even though the operator has the data on disk.
//
// Returns: matched (rows in torrents.json that mapped to a Storage entry),
// updated (Category was empty and we wrote a value), skipped (Category was
// already set), missing (in torrents.json but no Storage entry).
func (m *Manager) backfillCategoriesFromTorrentsJSON() (matched, updated, skipped, missing int, err error) {
	path := m.config.TorrentsFile()
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// First-run users have no torrents.json — nothing to backfill.
			return 0, 0, 0, 0, nil
		}
		return 0, 0, 0, 0, err
	}

	// torrents.json is a map keyed by "<infohash>|<category>" with object values.
	raw := map[string]torrentsJSONEntry{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return 0, 0, 0, 0, err
	}

	for _, t := range raw {
		hash := strings.ToLower(strings.TrimSpace(t.Hash))
		cat := strings.TrimSpace(t.Category)
		if hash == "" || cat == "" {
			continue
		}

		entry, err := m.storage.Get(hash)
		if err != nil {
			// Storage's Get returns an error for "not found" too; treat as missing.
			missing++
			continue
		}
		if entry == nil {
			missing++
			continue
		}
		matched++

		if entry.Category != "" {
			skipped++
			continue
		}
		entry.Category = cat
		if err := m.storage.AddOrUpdate(entry); err != nil {
			m.logger.Warn().
				Err(err).
				Str("infohash", hash).
				Str("category", cat).
				Msg("category-backfill: failed to update entry")
			continue
		}
		updated++
	}

	return matched, updated, skipped, missing, nil
}
