package manager

import (
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

// backfillCategoriesFromMetadataDB merges qBit categories from an optional
// `arr_metadata.db` SQLite file into the Storage entries by infohash.
//
// Why this exists alongside backfillCategoriesFromTorrentsJSON: qBit's
// `torrents.json` is rewritten by the qBit-emulator on every state change
// and only carries entries currently being tracked. A persistent SQLite
// side-channel captures `infohash → arr_name` mappings durably even across
// cache rewrites and Real-Debrid re-add cycles where the upstream cache
// file's `arr` field is no longer populated. Existence of the file is
// optional — first-run users have no DB and this is a no-op.
//
// Schema (created and maintained by deployments running with the metadata
// store enabled):
//
//	CREATE TABLE torrent_arr_mapping (
//	    infohash TEXT PRIMARY KEY,
//	    arr_name TEXT NOT NULL,
//	    torrent_id TEXT,
//	    torrent_name TEXT,
//	    created_at DATETIME,
//	    updated_at DATETIME
//	);
//
// `arr_name` here stores the qBit category (e.g. "tv-sonarr", "radarr",
// "backup"), not the *arr instance hostname.
//
// Idempotent — only updates entries whose Category is currently empty.
func (m *Manager) backfillCategoriesFromMetadataDB() (matched, updated, skipped, missing int, err error) {
	path := m.config.ArrMetadataDBFile()
	if _, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, fs.ErrNotExist) {
			return 0, 0, 0, 0, nil
		}
		return 0, 0, 0, 0, statErr
	}

	// `?_pragma=busy_timeout(5000)` so we don't fail on a stray writer lock
	// (the existing fork's metadata-handler can hold a write at startup).
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&mode=ro")
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer db.Close()

	rows, err := db.Query(`SELECT infohash, arr_name FROM torrent_arr_mapping`)
	if err != nil {
		// Missing table is fine on a fresh deploy.
		if strings.Contains(err.Error(), "no such table") {
			return 0, 0, 0, 0, nil
		}
		return 0, 0, 0, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var infohash, arrName string
		if scanErr := rows.Scan(&infohash, &arrName); scanErr != nil {
			continue
		}
		hash := strings.ToLower(strings.TrimSpace(infohash))
		cat := strings.TrimSpace(arrName)
		if hash == "" || cat == "" {
			continue
		}

		entry, err := m.storage.Get(hash)
		if err != nil || entry == nil {
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
				Msg("category-backfill-db: failed to update entry")
			continue
		}
		updated++
	}

	if err := rows.Err(); err != nil {
		return matched, updated, skipped, missing, err
	}
	return matched, updated, skipped, missing, nil
}
