package webdav

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/stanNthe5/stringbuf"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

func init() {
	chi.RegisterMethod("PROPFIND")
	chi.RegisterMethod("PROPPATCH")
	chi.RegisterMethod("MKCOL")
	chi.RegisterMethod("COPY")
	chi.RegisterMethod("MOVE")
	chi.RegisterMethod("LOCK")
	chi.RegisterMethod("UNLOCK")
}

const (
	PROPFIND = "PROPFIND"
)

type Handler struct {
	logger  zerolog.Logger
	manager *manager.Manager
}

func NewHandler(mgr *manager.Manager) *Handler {
	h := &Handler{
		logger:  logger.New("webdav"),
		manager: mgr,
	}
	return h
}

func (h *Handler) readinessMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-h.manager.IsReady():
			// WebDAV is ready, proceed
			next.ServeHTTP(w, r)
		default:
			// WebDAV is still initializing
			w.Header().Set("Retry-After", "5")
			http.Error(w, "WebDAV service is initializing, please try again shortly", http.StatusServiceUnavailable)
		}
	})
}

func (h *Handler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Use(h.readinessMiddleware)
	r.Use(h.commonMiddleware)
	r.Use(middleware.AllowContentEncoding("gzip"))
	//r.Use(h.authMiddleware)

	//
	r.MethodFunc(PROPFIND, "/", h.handleRoot)
	r.MethodFunc(PROPFIND, "/{mount}", h.handleMount)
	r.MethodFunc(PROPFIND, "/{mount}/{group}", h.handleGroup)

	r.MethodFunc(PROPFIND, "/{mount}/{group}/{subGroup}", h.handleSubGroup)
	r.MethodFunc(PROPFIND, "/{mount}/{group}/{subGroup}/{torrent}", h.handleTorrentFolder)
	r.MethodFunc(PROPFIND, "/{mount}/{group}/{torrent}", h.handleTorrentFolder)

	r.Get("/{mount}/{group}/{subGroup}/{torrent}/{file}", h.handleTorrentFile)
	r.Get("/{mount}/{group}/{torrent}/{file}", h.handleTorrentFile)

	return r
}

func (h *Handler) convertToXML(cleanPath string, currentInfo *manager.FileInfo, children []manager.FileInfo) stringbuf.StringBuf {
	entries := make([]entry, 0, len(children)+1)
	// Add the current file itself
	if currentInfo != nil {
		entries = append(entries, entry{
			escHref: xmlEscape(fastEscapePath(cleanPath)),
			escName: xmlEscape(currentInfo.Name()),
			isDir:   currentInfo.IsDir(),
			size:    currentInfo.Size(),
			modTime: currentInfo.ModTime().Format(time.RFC3339),
		})
	}

	for _, info := range children {

		nm := info.Name()
		// build raw href
		href := path.Join("/", cleanPath, nm)
		if info.IsDir() {
			href += "/"
		}

		entries = append(entries, entry{
			escHref: xmlEscape(fastEscapePath(href)),
			escName: xmlEscape(nm),
			isDir:   info.IsDir(),
			size:    info.Size(),
			modTime: info.ModTime().Format(time.RFC3339),
		})
	}

	sb := stringbuf.New("")

	// XML header and main element
	_, _ = sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	_, _ = sb.WriteString(`<d:multistatus xmlns:d="DAV:">`)

	// Add responses for each entry
	for _, e := range entries {
		_, _ = sb.WriteString(`<d:response>`)
		_, _ = sb.WriteString(`<d:href>`)
		_, _ = sb.WriteString(e.escHref)
		_, _ = sb.WriteString(`</d:href>`)
		_, _ = sb.WriteString(`<d:propstat>`)
		_, _ = sb.WriteString(`<d:prop>`)

		if e.isDir {
			_, _ = sb.WriteString(`<d:resourcetype><d:collection/></d:resourcetype>`)
		} else {
			_, _ = sb.WriteString(`<d:resourcetype/>`)
			_, _ = sb.WriteString(`<d:getcontentlength>`)
			_, _ = sb.WriteString(strconv.FormatInt(e.size, 10))
			_, _ = sb.WriteString(`</d:getcontentlength>`)
		}

		_, _ = sb.WriteString(`<d:getlastmodified>`)
		_, _ = sb.WriteString(e.modTime)
		_, _ = sb.WriteString(`</d:getlastmodified>`)

		_, _ = sb.WriteString(`<d:displayname>`)
		_, _ = sb.WriteString(e.escName)
		_, _ = sb.WriteString(`</d:displayname>`)

		_, _ = sb.WriteString(`</d:prop>`)
		_, _ = sb.WriteString(`<d:status>HTTP/1.1 200 OK</d:status>`)
		_, _ = sb.WriteString(`</d:propstat>`)
		_, _ = sb.WriteString(`</d:response>`)
	}

	// Close root element
	_, _ = sb.WriteString(`</d:multistatus>`)
	return sb
}

func (h *Handler) handleRoot(w http.ResponseWriter, r *http.Request) {
	currentInfo, rawEntries := h.manager.MountPaths()
	sb := h.convertToXML("/", currentInfo, rawEntries)

	// Set headers
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	// Set status code and write response
	w.WriteHeader(http.StatusMultiStatus) // 207 MultiStatus
	_, _ = w.Write(sb.Bytes())
}

func (h *Handler) handleMount(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	mountName := chi.URLParam(r, "mount")
	currentInfo, rawEntries := h.manager.GetSubDir(mountName)

	sb := h.convertToXML(cleanPath, currentInfo, rawEntries)

	// Set headers
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	// Set status code and write response
	w.WriteHeader(http.StatusMultiStatus) // 207 MultiStatus
	_, _ = w.Write(sb.Bytes())
}

func (h *Handler) handleGroup(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	group := utils.PathUnescape(chi.URLParam(r, "group"))

	// group can either be __all__, __bad__, __paginated__ or a custom folder
	currentInfo, rawEntries := h.manager.GetChildren(group)

	sb := h.convertToXML(cleanPath, currentInfo, rawEntries)
	// Set headers
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	// Set status code and write response
	w.WriteHeader(http.StatusMultiStatus) // 207 MultiStatus
	_, _ = w.Write(sb.Bytes())

}

func (h *Handler) handleSubGroup(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	//group := utils.PathUnescape()(chi.URLParam(r, "group"))
	subGroup := utils.PathUnescape(chi.URLParam(r, "subGroup"))

	// subGroup can either be a page number (for __paginated__) or a custom folder name
	currentInfo, rawEntries := h.manager.GetChildrenInSubGroup(subGroup)

	sb := h.convertToXML(cleanPath, currentInfo, rawEntries)
	// Set headers
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	// Set status code and write response
	w.WriteHeader(http.StatusMultiStatus) // 207 MultiStatus
	_, _ = w.Write(sb.Bytes())
}

func (h *Handler) handleTorrentFolder(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	group := utils.PathUnescape(chi.URLParam(r, "group"))
	torrent := utils.PathUnescape(chi.URLParam(r, "torrent"))

	currentInfo, rawEntries := h.manager.GetTorrentFilesInFolder(group, torrent)
	sb := h.convertToXML(cleanPath, currentInfo, rawEntries)
	// Set headers
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	// Set status code and write response
	w.WriteHeader(http.StatusMultiStatus) // 207 MultiStatus
	_, _ = w.Write(sb.Bytes())
}

func (h *Handler) handleTorrentFile(w http.ResponseWriter, r *http.Request) {
	torrentName := utils.PathUnescape(chi.URLParam(r, "torrent"))
	fileName := utils.PathUnescape(chi.URLParam(r, "file"))

	torrent, err := h.manager.GetTorrentByName(torrentName)
	if err != nil || torrent == nil {
		http.Error(w, "Torrent not found", http.StatusNotFound)
		return
	}

	file, ok := torrent.Files[fileName]
	if !ok {
		http.Error(w, "File not found in torrent", http.StatusNotFound)
		return
	}

	etag := fmt.Sprintf("\"%x-%x\"", torrent.AddedOn.Unix(), file.Size)
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", torrent.AddedOn.UTC().Format(http.TimeFormat))

	ext := filepath.Ext(file.Name)
	if contentType := mime.TypeByExtension(ext); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	if err := h.StreamResponse(torrentName, file, w, r); err != nil {
		var streamErr *streamError
		if errors.As(err, &streamErr) {
			// Handle client disconnections silently (just debug log)
			if errors.Is(streamErr.Err, context.Canceled) || errors.Is(streamErr.Err, context.DeadlineExceeded) || streamErr.IsClientDisconnection {
				return
			}
			if streamErr.StatusCode > 0 {
				h.logger.Trace().Err(err).Msgf("Error streaming %s", file.Name)
				http.Error(w, streamErr.Error(), streamErr.StatusCode)
				return
			} else {
				// We've already written a status code, just log the error
				h.logger.Error().Err(streamErr.Err).Msg("Streaming error")
				return
			}
		} else {
			// Generic error
			h.logger.Error().Err(err).Msgf("Error streaming file %s", file.Name)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	}

}

func (h *Handler) commonMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("DAV", "1, 2")
		w.Header().Set("Allow", "OPTIONS, PROPFIND, GET, HEAD, POST, PUT, DELETE, MKCOL, PROPPATCH, COPY, MOVE, LOCK, UNLOCK")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "OPTIONS, PROPFIND, GET, HEAD, POST, PUT, DELETE, MKCOL, PROPPATCH, COPY, MOVE, LOCK, UNLOCK")
		w.Header().Set("Access-Control-Allow-Headers", "Depth, Content-Type, Authorization")

		next.ServeHTTP(w, r)
	})
}

func getContentType(fileName string) string {
	contentType := "application/octet-stream"

	// Determine content type based on file extension
	switch {
	case strings.HasSuffix(fileName, ".mp4"):
		contentType = "video/mp4"
	case strings.HasSuffix(fileName, ".mkv"):
		contentType = "video/x-matroska"
	case strings.HasSuffix(fileName, ".avi"):
		contentType = "video/x-msvideo"
	case strings.HasSuffix(fileName, ".mov"):
		contentType = "video/quicktime"
	case strings.HasSuffix(fileName, ".m4v"):
		contentType = "video/x-m4v"
	case strings.HasSuffix(fileName, ".ts"):
		contentType = "video/mp2t"
	case strings.HasSuffix(fileName, ".srt"):
		contentType = "application/x-subrip"
	case strings.HasSuffix(fileName, ".vtt"):
		contentType = "text/vtt"
	}
	return contentType
}

// Handlers

//func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request) {
//	f, err := h.OpenFile(r.Context(), r.URL.Path, os.O_RDONLY, 0)
//	if err != nil {
//		h.logger.Error().Err(err).Str("path", r.URL.Path).Msg("Failed to open file")
//		http.NotFound(w, r)
//		return
//	}
//	defer func(f webdav.File) {
//		err := f.Close()
//		if err != nil {
//			return
//		}
//	}(f)
//
//	fi, err := f.Stat()
//	if err != nil {
//		h.logger.Error().Err(err).Msg("Failed to stat file")
//		http.Error(w, "Server Error", http.StatusInternalServerError)
//		return
//	}
//	w.Header().Set("Content-Type", getContentType(fi.Name()))
//	w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
//	w.Header().Set("Last-Modified", fi.ModTime().UTC().Format(http.TimeFormat))
//	w.Header().Set("Accept-Ranges", "bytes")
//	w.WriteHeader(http.StatusOK)
//}

func (h *Handler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := config.Get()
		if cfg.UseAuth && cfg.EnableWebdavAuth {
			username, password, ok := r.BasicAuth()
			if !ok || !config.VerifyAuth(username, password) {
				w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) handleOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, PROPFIND")
	w.Header().Set("DAV", "1, 2")
	w.WriteHeader(http.StatusOK)
}
