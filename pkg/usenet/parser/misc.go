package parser

import (
	"bytes"
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Tensai75/nzbparser"
	"github.com/google/uuid"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// getRARVolumeOrder returns a sort key for RAR volume ordering.
// .rar or .part01.rar = 0 (first volume)
// .r00 = 1, .r01 = 2, etc.
// .part02.rar = 2, .part03.rar = 3, etc.
func getRARVolumeOrder(filename string) int {
	lower := strings.ToLower(filename)
	ext := filepath.Ext(lower)
	base := strings.TrimSuffix(lower, ext)

	// Old-style naming: .rar, .r00, .r01, ...
	if ext == ".rar" {
		// Check for .partXX.rar pattern (new style)
		partPattern := regexp.MustCompile(`\.part(\d+)$`)
		if matches := partPattern.FindStringSubmatch(base); len(matches) == 2 {
			num, _ := strconv.Atoi(matches[1])
			return num // .part01.rar = 1, .part02.rar = 2
		}
		// Plain .rar is the first volume
		return 0
	}

	// .rXX pattern (old style continuation)
	if len(ext) == 4 && ext[0:2] == ".r" {
		numStr := ext[2:]
		if num, err := strconv.Atoi(numStr); err == nil {
			return num + 1 // .r00 = 1, .r01 = 2, etc.
		}
	}

	// Unknown pattern, put at end
	return 999999
}

// sortRARVolumesByOrder sorts a slice in-place by RAR volume order using a key extractor function
func sortRARVolumesByOrder[T any](items []T, getName func(T) string) {
	sort.Slice(items, func(i, j int) bool {
		return getRARVolumeOrder(getName(items[i])) < getRARVolumeOrder(getName(items[j]))
	})
}

func wrapNZBFile(f *storage.NZBFile) ([]*storage.NZBFile, error) {
	if f == nil {
		return nil, fmt.Errorf("nzb file is nil")
	}
	return []*storage.NZBFile{f}, nil
}

// fileMetaKey returns a stable key for associating per-file metadata.
func fileMetaKey(file nzbparser.NzbFile) string {
	if file.Number > 0 {
		return fmt.Sprintf("n:%d", file.Number)
	}
	if file.Subject != "" {
		return "s:" + file.Subject
	}
	if len(file.Segments) > 0 {
		return "m:" + file.Segments[0].Id
	}
	return ""
}

func getGroupsList(groups map[string]struct{}) []string {
	result := make([]string, 0, len(groups))
	for g := range groups {
		result = append(result, g)
	}
	return result
}

func determineNZBName(filename string, meta map[string]string) string {
	// Prefer filename if it exists
	if filename != "" {
		filename = strings.TrimSuffix(filename, filepath.Ext(filename))
	} else if name := meta["Name"]; name != "" {
		filename = name
	} else if title := meta["title"]; title != "" {
		filename = title
	}
	return utils.RemoveInvalidChars(filename)
}

func generateID(nzb *storage.NZB) string {
	return uuid.New().String()
}

func convertRARVolumeParts(parts []*types.RARVolumePart) []types.RARVolumePart {
	result := make([]types.RARVolumePart, len(parts))
	for i, part := range parts {
		result[i] = types.RARVolumePart{
			Name:         filepath.Base(part.Name),
			DataOffset:   part.DataOffset,
			PackedSize:   part.PackedSize,
			UnpackedSize: part.UnpackedSize,
			Stored:       part.Stored,
			Compressed:   !part.Stored,
			PartNumber:   i,
		}
	}
	return result
}

func DetectFileType(b []byte) string {
	if len(b) == 0 {
		return "unknown"
	}

	// --- RAR ---
	if len(b) >= 7 && bytes.Equal(b[:7], []byte("Rar!\x1A\x07\x00")) {
		return "rar4"
	}
	if len(b) >= 8 && bytes.Equal(b[:8], []byte("Rar!\x1A\x07\x01\x00")) {
		return "rar5"
	}

	// --- ZIP ---
	if len(b) >= 4 && bytes.Equal(b[:4], []byte{0x50, 0x4B, 0x03, 0x04}) {
		return "zip"
	}

	// --- 7z ---
	if len(b) >= 6 && bytes.Equal(b[:6], []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}) {
		return "7z"
	}

	// --- GZIP ---
	if len(b) >= 2 && bytes.Equal(b[:2], []byte{0x1F, 0x8B}) {
		return "gzip"
	}

	// --- TAR (ustar magic at offset 257) ---
	if len(b) >= 265 && bytes.Equal(b[257:262], []byte("ustar")) {
		return "tar"
	}

	// --- PDF ---
	if len(b) >= 5 && bytes.Equal(b[:5], []byte("%PDF-")) {
		return "pdf"
	}

	// --- JPEG ---
	if len(b) >= 3 && bytes.Equal(b[:3], []byte{0xFF, 0xD8, 0xFF}) {
		return "jpeg"
	}

	// --- PNG ---
	if len(b) >= 8 && bytes.Equal(b[:8], []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}) {
		return "png"
	}

	// --- GIF ---
	if len(b) >= 6 && (bytes.Equal(b[:6], []byte("GIF87a")) ||
		bytes.Equal(b[:6], []byte("GIF89a"))) {
		return "gif"
	}

	// --- WebP (RIFF + WEBP) ---
	if len(b) >= 12 &&
		bytes.Equal(b[:4], []byte("RIFF")) &&
		bytes.Equal(b[8:12], []byte("WEBP")) {
		return "webp"
	}

	// --- MP4 / MOV (ISO Base Media File) ---
	// "ftyp" is at position 4
	if len(b) >= 12 && bytes.Equal(b[4:8], []byte("ftyp")) {
		return "mp4"
	}

	// --- MKV / WebM (EBML magic) ---
	if len(b) >= 4 && bytes.Equal(b[:4], []byte{0x1A, 0x45, 0xDF, 0xA3}) {
		return "mkv"
	}

	// --- MP3 (ID3 or MPEG frame sync) ---
	if len(b) >= 3 && bytes.Equal(b[:3], []byte("ID3")) {
		return "mp3"
	}
	if len(b) >= 2 && b[0] == 0xFF && (b[1]&0xE0) == 0xE0 {
		return "mp3"
	}

	// --- WAV ---
	if len(b) >= 12 &&
		bytes.Equal(b[:4], []byte("RIFF")) &&
		bytes.Equal(b[8:12], []byte("WAVE")) {
		return "wav"
	}

	// --- FLAC ---
	if len(b) >= 4 && bytes.Equal(b[:4], []byte("fLaC")) {
		return "flac"
	}

	// --- AVI ---
	if len(b) >= 12 &&
		bytes.Equal(b[:4], []byte("RIFF")) &&
		bytes.Equal(b[8:12], []byte("AVI ")) {
		return "avi"
	}

	return "unknown"
}

func determineExtension(group *FileGroup) string {
	// Try to determine extension from filenames
	for _, file := range group.Files {
		ext := filepath.Ext(file.Filename)
		if ext != "" {
			return ext
		}
	}
	return ""
}

func getNZBSegments(index int, file nzbparser.NzbFile, group *FileGroup) (int64, []storage.NZBSegment) {
	if len(file.Segments) == 0 {
		return 0, nil
	}

	sort.Slice(file.Segments, func(i, j int) bool {
		return file.Segments[i].Number < file.Segments[j].Number
	})

	// Find the max segment number to properly size the array
	maxSegNum := 0
	for _, seg := range file.Segments {
		if seg.Number > maxSegNum {
			maxSegNum = seg.Number
		}
	}

	// Handle case where segment numbers start at 0 or 1
	nzbSegments := make([]storage.NZBSegment, maxSegNum)

	currentOffset := int64(0)
	metadata := group.getMetadata()

	fileSize := metadata.fileSize
	if index == len(group.Files)-1 {
		fileSize = metadata.lastFileSize
	}

	for idx, segment := range file.Segments {
		segSize := metadata.segmentSize
		if idx == len(file.Segments)-1 {
			// Last segment may be smaller
			// Last segment calculation
			// Check if the file size metadata assumes a different file (e.g. mixed groups)
			// Expected total size if all segments were full
			fullSegsSize := metadata.segmentSize * int64(len(file.Segments)-1) // size of all previous segments

			// If fileSize is inconsistent with the number of segments (too small or too large),
			// fallback to estimation for this last segment.
			// Threshold: if difference > 1.5 segments
			isSizeMismatch := false
			expectedTotal := fullSegsSize + metadata.segmentSize // rough estimate
			diff := fileSize - expectedTotal
			if diff < 0 {
				diff = -diff
			}
			if diff > (metadata.segmentSize*3)/2 {
				isSizeMismatch = true
			}

			if isSizeMismatch {
				// Fallback: estimate from encoded bytes
				segSize = int64(float64(segment.Bytes) * 0.97)
			} else {
				segSize = fileSize - fullSegsSize
			}
		}
		seg := storage.NZBSegment{
			Number:      segment.Number,
			MessageID:   segment.Id,
			Bytes:       segSize,
			StartOffset: currentOffset,
			EndOffset:   currentOffset + segSize - 1,
			Group:       group.BaseName,
		}

		// Bounds check: segment.Number is 1-indexed, array is 0-indexed
		segIdx := segment.Number - 1
		if segIdx >= 0 && segIdx < len(nzbSegments) {
			nzbSegments[segIdx] = seg
		}
		currentOffset += segSize
	}
	return currentOffset, nzbSegments
}

func buildBaseSegments(group *FileGroup) ([]storage.NZBSegment, []storage.ArchiveVolumeInfo, int64) {
	if len(group.Files) == 0 {
		return nil, nil, 0
	}

	baseSegments := make([]storage.NZBSegment, 0)
	volumeInfos := make([]storage.ArchiveVolumeInfo, 0, len(group.Files))
	currentOffset := int64(0)

	for idx, nzbFile := range group.Files {
		totalSize, segments := getNZBSegments(idx, nzbFile, group)
		if totalSize == 0 || len(segments) == 0 {
			continue
		}
		start := len(baseSegments)
		baseSegments = append(baseSegments, segments...)
		volumeInfos = append(volumeInfos, storage.ArchiveVolumeInfo{
			Name:         nzbFile.Filename,
			Size:         totalSize,
			SegmentStart: start,
			SegmentEnd:   len(baseSegments),
		})
		currentOffset += totalSize
	}

	return baseSegments, volumeInfos, currentOffset
}

func buildArchiveVolumeDescriptors(group *FileGroup) []*types.Volume {
	var volumes []*types.Volume

	if len(group.Files) == 0 {
		return volumes
	}

	for idx, nzbFile := range group.Files {
		if len(nzbFile.Segments) == 0 {
			continue
		}

		totalSize, volumeSegments := getNZBSegments(idx, nzbFile, group)
		if totalSize == 0 || len(volumeSegments) == 0 {
			continue
		}

		volumeName := nzbFile.Filename
		if volumeName == "" {
			volumeName = fmt.Sprintf("%s.part%03d", group.BaseName, idx+1)
		}

		volumes = append(volumes, &types.Volume{
			Index:    idx,
			Name:     volumeName,
			Size:     totalSize,
			Segments: volumeSegments,
		})
	}

	return volumes
}

func buildExtractedArchiveFiles(
	group *FileGroup,
	password string,
	fileType storage.NZBFileType,
	baseSegments []storage.NZBSegment,
	volumeInfos []storage.ArchiveVolumeInfo,
	infos []*storage.ExtractedFileInfo,
) []*storage.NZBFile {
	if len(baseSegments) == 0 {
		return nil
	}
	files := make([]*storage.NZBFile, 0, len(infos))

	for _, info := range infos {
		if info == nil || info.FileSize <= 0 {
			continue
		}
		if info.InternalPath == "" {
			info.InternalPath = NormalizeArchivePath(info.FileName)
		}
		name := info.FileName
		if name == "" {
			name = group.BaseName
		}
		name = utils.RemoveInvalidChars(name)

		// Use pre-sliced segments if available, otherwise slice based on offset
		var segments []storage.NZBSegment
		if len(info.Segments) > 0 {
			// Use pre-computed segments (from RAR parser, etc.)
			segments = info.Segments
		} else if info.DataOffset > 0 || info.FileSize > 0 {
			// Slice segments for this file's byte range
			sliced, err := sliceSegmentsForRangeSimple(baseSegments, info.DataOffset, info.FileSize)
			if err != nil || len(sliced) == 0 {
				// Fallback to all segments if slicing fails
				segments = make([]storage.NZBSegment, len(baseSegments))
				copy(segments, baseSegments)
			} else {
				segments = sliced
			}
		} else {
			// No offset info, use all segments
			segments = make([]storage.NZBSegment, len(baseSegments))
			copy(segments, baseSegments)
		}

		files = append(files, &storage.NZBFile{
			Name:         name,
			InternalPath: info.InternalPath,
			Groups:       getGroupsList(group.Groups),
			Segments:     segments,
			Password:     password,
			FileType:     fileType,
			Size:         info.FileSize,
			IsStored:     info.IsStored,
		})
	}

	return files
}

func NormalizeArchivePath(name string) string {
	trimmed := strings.TrimSpace(name)
	trimmed = strings.TrimLeft(trimmed, "./")
	if trimmed == "" {
		return ""
	}
	trimmed = strings.ReplaceAll(trimmed, "\\", "/")
	trimmed = path.Clean(trimmed)
	trimmed = strings.Trim(trimmed, "/")
	return trimmed
}
