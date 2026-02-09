package nntp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// YencMetadata contains just the header information
type YencMetadata struct {
	Name     string // filename
	Size     int64  // total file size
	Part     int64  // part number
	Total    int64  // total parts
	Begin    int64  // part start byte
	End      int64  // part end byte
	Offset   int64  // part offset within the file
	PartSize int64  // part size (decoded)
	LineSize int    // line length
	Snippet  []byte
}

// DecodeYencHeadersWithSnippet is kept for potential future use or testing.
// GetHeader now uses rapidyenc.Decoder.Meta directly for efficiency.
func DecodeYencHeadersWithSnippet(data []byte, maxSnippet int) (*YencMetadata, error) {
	buf := bufio.NewReader(strings.NewReader(string(data)))
	metadata := &YencMetadata{}

	// Parse =ybegin
	if err := parseYBeginHeader(buf, metadata); err != nil {
		return nil, fmt.Errorf("failed to parse ybegin: %w", err)
	}

	// Parse =ypart if multipart
	if metadata.Part > 0 {
		if err := parseYPartHeader(buf, metadata); err != nil {
			return nil, fmt.Errorf("failed to parse ypart: %w", err)
		}
	}

	// Decode a small snippet of body data
	snippet, err := decodeYencSnippet(buf, maxSnippet)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to decode yenc snippet: %w", err)
	}
	metadata.Snippet = snippet

	return metadata, nil
}

func decodeYencSnippet(r *bufio.Reader, maxSnippet int) ([]byte, error) {
	snippet := make([]byte, 0, maxSnippet)
	var escaped bool

	for len(snippet) < maxSnippet {
		line, err := r.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return snippet, err
		}

		// Trim newline
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			if err == io.EOF {
				break
			}
			continue
		}

		// Stop on =yend or any new yenc control line
		if line[0] == '=' {
			if bytes.HasPrefix(line, []byte("=yend")) {
				break
			}
			// ignore other control lines (=ypart etc.)
			if err == io.EOF {
				break
			}
			continue
		}

		// Decode this data line
		for i := 0; i < len(line) && len(snippet) < maxSnippet; i++ {
			b := line[i]

			if escaped {
				// decoded = (b - 64 - 42) mod 256
				decoded := byte(int(b) - 64 - 42)
				snippet = append(snippet, decoded)
				escaped = false
				continue
			}

			if b == '=' {
				escaped = true
				continue
			}

			// decoded = (b - 42) mod 256
			decoded := byte(int(b) - 42)
			snippet = append(snippet, decoded)
		}

		if err == io.EOF {
			break
		}
	}

	return snippet, nil
}

func parseYBeginHeader(buf *bufio.Reader, metadata *YencMetadata) error {
	// Safety: Only check the first 100 lines. ybegin is usually line 1.
	maxLines := 100

	for i := 0; i < maxLines; i++ {
		s, err := buf.ReadString('\n')
		if err != nil {
			return err
		}

		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "=ybegin") {
			rest := strings.TrimSpace(s[7:])
			if idx := strings.Index(rest, "name="); idx >= 0 {
				metadata.Name = strings.TrimSpace(rest[idx+5:])
				rest = strings.TrimSpace(rest[:idx])
			}

			for _, field := range strings.Fields(rest) {
				kv := strings.SplitN(field, "=", 2)
				if len(kv) != 2 {
					continue
				}
				switch kv[0] {
				case "line":
					if value, err := strconv.Atoi(kv[1]); err == nil {
						metadata.LineSize = value
					}
				case "size":
					if value, err := strconv.ParseInt(kv[1], 10, 64); err == nil {
						metadata.Size = value
					}
				case "part":
					if value, err := strconv.Atoi(kv[1]); err == nil {
						metadata.Part = int64(value)
					}
				case "total":
					if value, err := strconv.Atoi(kv[1]); err == nil {
						metadata.Total = int64(value)
					}
				}
			}
			return nil
		}
	}
	return fmt.Errorf("ybegin header not found in first %d lines", maxLines)
}

func parseYPartHeader(buf *bufio.Reader, metadata *YencMetadata) error {
	var s string
	var err error

	// Find the =ypart line
	for {
		s, err = buf.ReadString('\n')
		if err != nil {
			return err
		}
		if len(s) >= 6 && s[:6] == "=ypart" {
			break
		}
	}

	// Parse part parameters
	for _, header := range strings.Split(s[6:], " ") {
		kv := strings.SplitN(strings.TrimSpace(header), "=", 2)
		if len(kv) < 2 {
			continue
		}

		switch kv[0] {
		case "begin":
			metadata.Begin, _ = strconv.ParseInt(kv[1], 10, 64)
		case "end":
			metadata.End, _ = strconv.ParseInt(kv[1], 10, 64)
		}
	}

	return nil
}
