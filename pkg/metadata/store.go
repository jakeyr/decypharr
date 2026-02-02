package metadata

import (
	"database/sql"
	"fmt"
	"sync"

	_ "modernc.org/sqlite"
	"github.com/rs/zerolog"
)

// Store manages permanent metadata mappings for torrents
type Store struct {
	db     *sql.DB
	mu     sync.RWMutex
	logger zerolog.Logger
}

// NewStore creates a new metadata store
func NewStore(dbPath string, logger zerolog.Logger) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	store := &Store{
		db:     db,
		logger: logger.With().Str("component", "metadata-store").Logger(),
	}

	if err := store.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return store, nil
}

// initialize creates the necessary tables
func (s *Store) initialize() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS torrent_arr_mapping (
			infohash TEXT PRIMARY KEY,
			arr_name TEXT NOT NULL,
			torrent_id TEXT,
			torrent_name TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_torrent_id ON torrent_arr_mapping(torrent_id);
		CREATE INDEX IF NOT EXISTS idx_arr_name ON torrent_arr_mapping(arr_name);
	`)
	return err
}

// SetArrForTorrent stores the arr mapping for a torrent
func (s *Store) SetArrForTorrent(infohash, torrentID, torrentName, arrName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(`
		INSERT INTO torrent_arr_mapping (infohash, torrent_id, torrent_name, arr_name, updated_at)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(infohash)
		DO UPDATE SET
			arr_name = excluded.arr_name,
			torrent_id = excluded.torrent_id,
			torrent_name = excluded.torrent_name,
			updated_at = CURRENT_TIMESTAMP
	`, infohash, torrentID, torrentName, arrName)

	if err != nil {
		s.logger.Error().Err(err).
			Str("infohash", infohash).
			Str("arr_name", arrName).
			Msg("Failed to store arr mapping")
		return err
	}

	s.logger.Debug().
		Str("infohash", infohash).
		Str("arr_name", arrName).
		Str("torrent_name", torrentName).
		Msg("Stored arr mapping")

	return nil
}

// GetArrForTorrent retrieves the arr name for a torrent by infohash
func (s *Store) GetArrForTorrent(infohash string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var arrName string
	err := s.db.QueryRow(
		"SELECT arr_name FROM torrent_arr_mapping WHERE infohash = ?",
		infohash,
	).Scan(&arrName)

	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		s.logger.Error().Err(err).Str("infohash", infohash).Msg("Failed to get arr mapping")
		return "", false
	}

	return arrName, true
}

// GetArrByTorrentID retrieves the arr name for a torrent by torrent ID
func (s *Store) GetArrByTorrentID(torrentID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var arrName string
	err := s.db.QueryRow(
		"SELECT arr_name FROM torrent_arr_mapping WHERE torrent_id = ?",
		torrentID,
	).Scan(&arrName)

	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		s.logger.Error().Err(err).Str("torrent_id", torrentID).Msg("Failed to get arr mapping")
		return "", false
	}

	return arrName, true
}

// GetStats returns statistics about the metadata store
func (s *Store) GetStats() (total int, byArr map[string]int, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get total count
	err = s.db.QueryRow("SELECT COUNT(*) FROM torrent_arr_mapping").Scan(&total)
	if err != nil {
		return 0, nil, err
	}

	// Get counts by arr
	byArr = make(map[string]int)
	rows, err := s.db.Query("SELECT arr_name, COUNT(*) FROM torrent_arr_mapping GROUP BY arr_name")
	if err != nil {
		return total, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var count int
		if err := rows.Scan(&name, &count); err != nil {
			return total, byArr, err
		}
		byArr[name] = count
	}

	return total, byArr, rows.Err()
}

// Close closes the database connection
func (s *Store) Close() error {
	return s.db.Close()
}
