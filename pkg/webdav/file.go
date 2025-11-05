package webdav

import (
	"fmt"
	"io"
	"net/http"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

type streamError struct {
	Err                   error
	StatusCode            int
	IsClientDisconnection bool
}

func (e *streamError) Error() string {
	return e.Err.Error()
}

func (e *streamError) Unwrap() error {
	return e.Err
}

func getDownloadByteRange(file *storage.File) *[2]int64 {
	return file.ByteRange
}

func (h *Handler) StreamResponse(torrentName string, file *storage.File, w http.ResponseWriter, r *http.Request) error {

	start, end := h.getRange(file, r)

	resp, err := h.manager.Stream(r.Context(), torrentName, file.Name, start, end)
	if err != nil {
		h.logger.Error().Err(err).Str("file", file.Name).Msg("Failed to stream with initial link")
		return &streamError{Err: err, StatusCode: http.StatusRequestedRangeNotSatisfiable}
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)
	return h.handleSuccessfulResponse(w, resp, start, end)
}

func (h *Handler) handleSuccessfulResponse(w http.ResponseWriter, resp *http.Response, start, end int64) error {
	statusCode := http.StatusOK
	if start > 0 || end > 0 {
		statusCode = http.StatusPartialContent
	}

	// Copy relevant headers
	if contentLength := resp.Header.Get("Content-Length"); contentLength != "" {
		w.Header().Set("Content-Length", contentLength)
	}

	if contentRange := resp.Header.Get("Content-Range"); contentRange != "" && statusCode == http.StatusPartialContent {
		w.Header().Set("Content-Range", contentRange)
	}

	// Copy other important headers
	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}

	return h.streamBuffer(w, resp.Body, statusCode)
}

func (h *Handler) streamBuffer(w http.ResponseWriter, src io.Reader, statusCode int) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("response does not support flushing")
	}

	smallBuf := make([]byte, 64*1024) // 64 KB
	if n, err := src.Read(smallBuf); n > 0 {
		// Write status code just before first successful write
		w.WriteHeader(statusCode)

		if _, werr := w.Write(smallBuf[:n]); werr != nil {
			if isClientDisconnection(werr) {
				return &streamError{Err: werr, StatusCode: 0, IsClientDisconnection: true}
			}
			// Headers already sent, can't send HTTP error response
			return &streamError{Err: werr, StatusCode: 0, IsClientDisconnection: false}
		}
		flusher.Flush()
	} else if err != nil && err != io.EOF {
		return &streamError{Err: err, StatusCode: http.StatusInternalServerError}
	}

	buf := make([]byte, 256*1024) // 256 KB
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				if isClientDisconnection(writeErr) {
					return &streamError{Err: writeErr, StatusCode: 0, IsClientDisconnection: true}
				}
				// Headers already sent, can't send HTTP error response
				return &streamError{Err: writeErr, StatusCode: 0, IsClientDisconnection: false}
			}
			flusher.Flush()
		}
		if readErr != nil {
			if readErr == io.EOF {
				return nil
			}
			if isClientDisconnection(readErr) {
				return &streamError{Err: readErr, StatusCode: 0, IsClientDisconnection: true}
			}
			return readErr
		}
	}
}

func (h *Handler) getRange(file *storage.File, r *http.Request) (int64, int64) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		// For video files, apply byte range if exists
		if byteRange := getDownloadByteRange(file); byteRange != nil {
			return byteRange[0], byteRange[1]
		}
		return 0, 0
	}

	// Parse range request
	ranges, err := parseRange(rangeHeader, file.Size)
	if err != nil || len(ranges) != 1 {
		// Invalid range, return full content
		return 0, 0
	}

	// Apply byte range offset if exists
	byteRange := getDownloadByteRange(file)
	start, end := ranges[0].start, ranges[0].end

	if byteRange != nil {
		start += byteRange[0]
		end += byteRange[0]
	}
	return start, end
}
