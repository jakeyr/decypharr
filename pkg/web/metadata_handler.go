package web

import (
	"encoding/json"
	"net/http"

	"github.com/sirrobot01/decypharr/pkg/metadata"
	"github.com/sirrobot01/decypharr/pkg/wire"
)

type MetadataHandler struct {
	store *metadata.Store
}

func NewMetadataHandler(store *metadata.Store) *MetadataHandler {
	return &MetadataHandler{store: store}
}

type MetadataEntry struct {
	Infohash    string `json:"infohash"`
	TorrentID   string `json:"torrent_id"`
	TorrentName string `json:"torrent_name"`
	ArrName     string `json:"arr_name"`
}

type MetadataStats struct {
	Total int            `json:"total"`
	ByArr map[string]int `json:"by_arr"`
}

// GET /api/metadata/stats
func (h *MetadataHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		http.Error(w, "Metadata store not available", http.StatusServiceUnavailable)
		return
	}

	total, byArr, err := h.store.GetStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stats := MetadataStats{
		Total: total,
		ByArr: byArr,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// POST /api/metadata/set
func (h *MetadataHandler) SetMapping(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		http.Error(w, "Metadata store not available", http.StatusServiceUnavailable)
		return
	}

	var entry MetadataEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if entry.Infohash == "" || entry.ArrName == "" {
		http.Error(w, "Infohash and arr_name are required", http.StatusBadRequest)
		return
	}

	if err := h.store.SetArrForTorrent(entry.Infohash, entry.TorrentID, entry.TorrentName, entry.ArrName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

// GET /api/metadata/list
func (h *MetadataHandler) ListMappings(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		http.Error(w, "Metadata store not available", http.StatusServiceUnavailable)
		return
	}

	// For now, we'll list torrents from cache and their arr mappings
	// This requires access to the debrid cache
	w.Header().Set("Content-Type", "application/json")

	// Get all torrents and check their metadata
	debrids := wire.Get().Debrid()
	allTorrents := []map[string]interface{}{}

	for _, cache := range debrids.Caches() {
		torrents := cache.GetListing("__all__")
		for _, torrent := range torrents {
			// Get torrent details to find infohash
			// This is a simplified version - you may need to enhance it
			torrentData := map[string]interface{}{
				"name": torrent.Name(),
			}

			allTorrents = append(allTorrents, torrentData)
		}
	}

	json.NewEncoder(w).Encode(allTorrents)
}
