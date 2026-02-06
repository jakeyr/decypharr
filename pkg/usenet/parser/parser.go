package parser

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/Tensai75/nzbparser"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/iter"
)

var ErrMoreRarDataNeeded = fmt.Errorf("rar: need more data")

var (
	defaultMaxSnippetSize = 512 * 1024 // 512KB
)

// NZBParser provides a simplified, robust NZB parser
type NZBParser struct {
	logger        zerolog.Logger
	manager       *nntp.Client // Connection manager for parsing operations
	maxConcurrent int          // Max concurrent connections
}

type fileAnalysisResult struct {
	fileSize     int64 // This is the Size of the entire file(only the last file is smaller)
	lastFileSize int64 // Size of the last file segment
	segmentSize  int64
}

type contentResult struct {
	file           nzbparser.NzbFile
	fileType       storage.NZBFileType
	actualFilename string
}

type FileGroup struct {
	BaseName       string
	ActualFilename string
	Type           storage.NZBFileType
	Files          []nzbparser.NzbFile
	metadata       *fileAnalysisResult
	Groups         map[string]struct{}
}

func (f *FileGroup) getMetadata() *fileAnalysisResult {
	if f.metadata != nil {
		return f.metadata
	}
	// Heuristic: assume segment is ~97% of reported bytes (yEnc overhead)
	if len(f.Files) == 0 || len(f.Files[0].Segments) == 0 {
		return &fileAnalysisResult{}
	}

	metadata := &fileAnalysisResult{}
	// Estimate actual segment size from reported bytes (account for ~3% yEnc overhead)
	reportedBytes := int64(f.Files[0].Segments[0].Bytes)
	if reportedBytes <= 0 {
		reportedBytes = 750000 // Default 750KB segment
	}
	metadata.segmentSize = int64(float64(reportedBytes) * 0.97)
	if metadata.segmentSize <= 0 {
		metadata.segmentSize = reportedBytes
	}
	metadata.fileSize = metadata.segmentSize * int64(len(f.Files[0].Segments))
	metadata.lastFileSize = metadata.segmentSize * int64(len(f.Files[len(f.Files)-1].Segments))
	f.metadata = metadata
	return f.metadata
}

// NewParser creates a new simplified NZB parser with a connection manager
func NewParser(manager *nntp.Client, maxConcurrent int, logger zerolog.Logger) *NZBParser {
	return &NZBParser{
		logger:        logger,
		manager:       manager,
		maxConcurrent: maxConcurrent,
	}
}

var (
	// RAR file patterns - simplified and more accurate
	rarMainPattern       = regexp.MustCompile(`\.rar$`)
	rarPartPattern       = regexp.MustCompile(`\.r\d{2}$`) // .r00, .r01, etc.
	rarVolumePattern     = regexp.MustCompile(`\.part\d+\.rar$`)
	ignoreExtensions     = []string{".sfv", ".nfo", ".jpg", ".png", ".txt", ".srt", ".idx", ".sub"}
	sevenZMainPattern    = regexp.MustCompile(`\.7z$`)
	sevenZPartPattern    = regexp.MustCompile(`\.7z\.\d{3}$`)
	extWithNumberPattern = regexp.MustCompile(`\.[^ "\.]*\.\d+$`)
	volPar2Pattern       = regexp.MustCompile(`(?i)\.vol\d+\+\d+\.par2?$`)
	partPattern          = regexp.MustCompile(`(?i)\.part\d+\.[^ "\.]*$`)
	regularExtPattern    = regexp.MustCompile(`\.[^ "\.]*$`)
)

func (p *NZBParser) Parse(ctx context.Context, filename string, content []byte) (nzb *storage.NZB, groups map[string]*FileGroup, err error) {
	// Recover from panics to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error().Interface("panic", r).Str("filename", filename).Msg("Panic recovered in Parse")
			err = fmt.Errorf("parse panic: %v", r)
		}
	}()

	// Parse raw XML
	raw, err := nzbparser.Parse(bytes.NewReader(content))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse NZB content: %w", err)
	}

	// Create base NZB structure
	nzb = &storage.NZB{
		Files:    []storage.NZBFile{},
		Status:   "parsed",
		Name:     determineNZBName(filename, raw.Meta),
		Title:    raw.Meta["title"],
		Password: raw.Meta["password"],
	}
	// Group files by base Name and type
	fileGroups := p.groupFiles(ctx, raw.Files)

	if len(fileGroups) == 0 {
		return nil, nil, fmt.Errorf("no valid file groups found in NZB")
	}

	// Stat the first segment to confirm connectivity
	checked := false
	for _, group := range fileGroups {
		if len(group.Files) == 0 || len(group.Files[0].Segments) == 0 {
			continue
		}
		segment := group.Files[0].Segments[0]
		err = p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
			_, _, statErr := conn.Stat(segment.Id)
			return statErr
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to stat segment %s <%s>: %w", group.ActualFilename, segment.Id, err)
		}
		checked = true
		break
	}
	if !checked {
		return nil, nil, fmt.Errorf("no segments available to stat in NZB")
	}

	nzb.ID = generateID(nzb)
	return nzb, fileGroups, nil
}

func (p *NZBParser) Process(ctx context.Context, nzb *storage.NZB, groups map[string]*FileGroup) (result *storage.NZB, err error) {
	// Recover from panics to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error().Interface("panic", r).Str("nzb", nzb.Name).Msg("Panic recovered in Process")
			err = fmt.Errorf("process panic: %v", r)
		}
	}()

	// Parse each group (with deferred archive option)
	files := p.processFileGroups(ctx, groups, nzb.Password)

	if len(files) == 0 {
		return nil, fmt.Errorf("no valid files found in NZB")
	}

	cfg := config.Get()

	// Change file name if there's only one file
	hasOneFile := len(files) == 1
	skippedFiles := 0
	// Calculate total Size
	for _, file := range files {
		if hasOneFile {
			file.Name = nzb.Name + filepath.Ext(file.Name)
		}
		if err := cfg.IsFileAllowed(file.Name, file.Size); err != nil {
			skippedFiles++
			continue
		}
		nzb.TotalSize += file.Size
		file.NzbID = nzb.ID
		nzb.Files = append(nzb.Files, file)
	}
	if skippedFiles > 0 {
		p.logger.Info().Int("skipped_files", skippedFiles).Str("nzb", nzb.Name).Msg("Some files were skipped due to size or extension restrictions")
	}
	if len(nzb.Files) == 0 {
		if skippedFiles > 0 {
			return nil, fmt.Errorf("all files were skipped due to size or extension restrictions")
		}
		return nil, fmt.Errorf("no valid files found in NZB after processing")
	}
	return nzb, nil
}

func (p *NZBParser) groupFiles(ctx context.Context, files nzbparser.NzbFiles) map[string]*FileGroup {
	var unknownFiles []nzbparser.NzbFile
	var allFiles []contentResult

	for _, file := range files {
		if len(file.Segments) == 0 {
			continue
		}

		fileType := p.detectFileType(file.Filename)
		if fileType == storage.NZBFileTypePar2 {
			// ignore PAR2 files for now
			continue
		}

		if fileType == storage.NZBFileTypeUnknown {
			unknownFiles = append(unknownFiles, file)
		} else {
			allFiles = append(allFiles, contentResult{
				file:           file,
				fileType:       fileType,
				actualFilename: file.Filename,
			})
		}
	}

	unknownResults := p.batchDetectContentTypes(ctx, unknownFiles)

	// Add unknown results
	allFiles = append(allFiles, unknownResults...)

	return p.groupProcessedFiles(allFiles)
}

// Batch process unknown files in parallel
func (p *NZBParser) batchDetectContentTypes(ctx context.Context, unknownFiles []nzbparser.NzbFile) []contentResult {
	if len(unknownFiles) == 0 {
		return nil
	}

	// Use worker pool for parallel processing
	workers := min(len(unknownFiles), 5) // Max 10 concurrent downloads

	mapper := iter.Mapper[nzbparser.NzbFile, contentResult]{
		MaxGoroutines: workers, // limit concurrency
	}

	mapped := mapper.Map(unknownFiles, func(f *nzbparser.NzbFile) contentResult {
		// You can still pass ctx through to your inner function.
		detectedType, actualFilename, err := p.detectFileTypeByContent(ctx, *f)
		if err != nil {
			p.logger.Trace().
				Err(err).
				Str("file", f.Filename).
				Msg("Failed to detect file type by content")
		}

		return contentResult{
			file:           *f,
			fileType:       detectedType,
			actualFilename: actualFilename,
		}
	})

	processed := make([]contentResult, 0, len(mapped))
	for _, r := range mapped {
		if r.fileType != storage.NZBFileTypeUnknown {
			processed = append(processed, r)
		}
	}
	return processed
}

// Group already processed files (fast)
func (p *NZBParser) groupProcessedFiles(allFiles []contentResult) map[string]*FileGroup {
	groups := make(map[string]*FileGroup)

	for _, item := range allFiles {
		// Skip unwanted files
		if item.fileType == storage.NZBFileTypeIgnore {
			continue
		}

		var groupKey string
		if item.actualFilename != "" && item.actualFilename != item.file.Filename {
			groupKey = p.getBaseFilename(item.actualFilename)
		} else {
			groupKey = item.file.Basefilename
		}

		group, exists := groups[groupKey]
		if !exists {
			group = &FileGroup{
				ActualFilename: item.actualFilename,
				BaseName:       groupKey,
				Type:           item.fileType,
				Files:          []nzbparser.NzbFile{},
				Groups:         make(map[string]struct{}),
			}
			groups[groupKey] = group
		}

		// Update filename
		item.file.Filename = item.actualFilename

		group.Files = append(group.Files, item.file)
		for _, g := range item.file.Groups {
			group.Groups[g] = struct{}{}
		}
	}

	return groups
}

func (p *NZBParser) getBaseFilename(filename string) string {
	if filename == "" {
		return ""
	}

	// First remove any quotes and trim spaces
	cleaned := strings.Trim(filename, `" -`)

	// Check for vol\d+\+\d+\.par2? (PAR2 Volume files)
	if volPar2Pattern.MatchString(cleaned) {
		return volPar2Pattern.ReplaceAllString(cleaned, "")
	}

	// Check for part\d+\.[^ "\.]* (part files like .part01.rar)

	if partPattern.MatchString(cleaned) {
		return partPattern.ReplaceAllString(cleaned, "")
	}

	// Check for [^ "\.]*\.\d+ (extensions with numbers like .7z.001, .r01, etc.)
	if extWithNumberPattern.MatchString(cleaned) {
		return extWithNumberPattern.ReplaceAllString(cleaned, "")
	}

	// Check for regular extensions [^ "\.]*

	if regularExtPattern.MatchString(cleaned) {
		return regularExtPattern.ReplaceAllString(cleaned, "")
	}

	return cleaned
}

// Simplified file type detection
func (p *NZBParser) detectFileType(filename string) storage.NZBFileType {
	lower := strings.ToLower(filename)

	// Check for media first
	if utils.IsMediaFile(lower) {
		return storage.NZBFileTypeMedia
	}

	// Check rar next
	if p.isRarFile(lower) {
		return storage.NZBFileTypeRar
	}

	if strings.HasSuffix(lower, ".par2") {
		return storage.NZBFileTypePar2
	}

	// Check for 7z files
	if sevenZMainPattern.MatchString(lower) || sevenZPartPattern.MatchString(lower) {
		return storage.NZBFileTypeSevenZip
	}

	if strings.HasSuffix(lower, ".zip") || strings.HasSuffix(lower, ".tar") ||
		strings.HasSuffix(lower, ".gz") || strings.HasSuffix(lower, ".bz2") {
		if strings.HasSuffix(lower, ".zip") {
			return storage.NZBFileTypeZip
		}
		return storage.NZBFileTypeUnknown
	}

	// Check for ignored file types
	for _, ext := range ignoreExtensions {
		if strings.HasSuffix(lower, ext) {
			return storage.NZBFileTypeIgnore
		}
	}
	// Default to unknown type
	return storage.NZBFileTypeUnknown
}

// Simplified RAR detection
func (p *NZBParser) isRarFile(filename string) bool {
	return rarMainPattern.MatchString(filename) ||
		rarPartPattern.MatchString(filename) ||
		rarVolumePattern.MatchString(filename)
}

func (p *NZBParser) processFileGroups(ctx context.Context, groups map[string]*FileGroup, password string) []storage.NZBFile {
	if len(groups) == 0 {
		return nil
	}
	rarCounts, sevenZCounts, zipCounts, mediaCounts, deferredCounts := 0, 0, 0, 0, 0

	// Convert map into slice of *values*, not pointers
	fileGroups := make([]FileGroup, 0, len(groups))
	for _, g := range groups {
		if len(g.Files) == 0 {
			continue
		}
		fileGroups = append(fileGroups, *g)
	}

	// Use a Mapper with limited concurrency to prevent goroutine explosion
	// when nested with RAR/archive parsers that also use parallel processing
	mapper := iter.Mapper[FileGroup, []*storage.NZBFile]{
		MaxGoroutines: p.maxConcurrent,
	}

	results := mapper.Map(fileGroups, func(g *FileGroup) []*storage.NZBFile {
		files, err := p.processFileGroup(ctx, g, password)
		if err != nil {
			p.logger.Warn().Err(err).Str("group", g.BaseName).Msg("Failed to process file group")
			return nil
		}
		return files
	})

	// Filter nils
	var files []storage.NZBFile
	for _, groupFiles := range results {
		for _, f := range groupFiles {
			if f != nil {
				files = append(files, *f)
				// Count types
				switch f.FileType {
				case storage.NZBFileTypeRar:
					rarCounts++
				case storage.NZBFileTypeSevenZip:
					sevenZCounts++
				case storage.NZBFileTypeZip:
					zipCounts++
				case storage.NZBFileTypeMedia:
					mediaCounts++
				}
			}
		}
	}

	// Count deferred archives
	for _, g := range fileGroups {
		switch g.Type {
		case storage.NZBFileTypeRar, storage.NZBFileTypeSevenZip, storage.NZBFileTypeZip:
			deferredCounts++
		}
	}

	p.logger.Info().
		Int("rar_files", rarCounts).
		Int("7z_files", sevenZCounts).
		Int("zip_files", zipCounts).
		Int("media_files", mediaCounts).
		Int("deferred_archives", deferredCounts).
		Msg("Processed file groups")

	return files
}

// Simplified individual group processing
func (p *NZBParser) processFileGroup(ctx context.Context, group *FileGroup, password string) ([]*storage.NZBFile, error) {
	if err := p.enrichGroupWithFileInfo(ctx, group); err != nil {
		return nil, err
	}

	switch group.Type {
	case storage.NZBFileTypeMedia:
		return wrapNZBFile(p.processMediaFile(group, password))
	case storage.NZBFileTypeRar:
		rarParser := NewRARParser(p.manager, p.maxConcurrent, p.logger)
		return rarParser.Process(ctx, group, password)
	case storage.NZBFileTypeSevenZip:
		zipParser := NewSevenZParser(p.manager, p.maxConcurrent, p.logger)
		return zipParser.Process(ctx, group, password)
	case storage.NZBFileTypeZip:
		zipParser := NewZIPParser(p.manager, p.maxConcurrent, p.logger)
		return zipParser.Process(ctx, group, password)
	default:
		return nil, fmt.Errorf("unsupported file type: %v", group.Type)
	}
}

func (p *NZBParser) enrichGroupWithFileInfo(ctx context.Context, group *FileGroup) error {
	sort.Slice(group.Files, func(i, j int) bool {
		return group.Files[i].Filename < group.Files[j].Filename
	})

	firstFile := group.Files[0]
	// Find the file with the most segments to use as the reference for segment size
	// This avoids issues where the first file is a small NFO/NZB with different characteristics
	maxSegments := 0
	for _, f := range group.Files {
		if len(f.Segments) > maxSegments {
			maxSegments = len(f.Segments)
			firstFile = f
		}
	}

	if len(firstFile.Segments) == 0 {
		return fmt.Errorf("no Segments in reference file of group %s", group.BaseName)
	}
	firstSegment := firstFile.Segments[0]

	lastFile := group.Files[len(group.Files)-1]
	lastSegment := lastFile.Segments[0]

	// If first and last are the same file, only need one fetch
	sameFile := len(group.Files) == 1

	type headerResult struct {
		data *nntp.YencMetadata
		err  error
	}

	// Fetch both headers in parallel
	firstCh := make(chan headerResult, 1)
	lastCh := make(chan headerResult, 1)

	go func() {
		var data *nntp.YencMetadata
		err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
			d, e := conn.GetHeader(firstSegment.Id, defaultMaxSnippetSize)
			data = d
			return e
		})
		firstCh <- headerResult{data, err}
	}()

	if !sameFile {
		go func() {
			var data *nntp.YencMetadata
			err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
				d, e := conn.GetHeader(lastSegment.Id, defaultMaxSnippetSize)
				data = d
				return e
			})
			lastCh <- headerResult{data, err}
		}()
	}

	// Wait for first result
	var firstResult headerResult
	select {
	case firstResult = <-firstCh:
	case <-ctx.Done():
		return ctx.Err()
	}

	if firstResult.err != nil {
		return fmt.Errorf("failed to fetch first segment header: %w", firstResult.err)
	}
	yencData := firstResult.data

	// Update the group's filename if the header provides a better one
	// This fixes issues where the group name is based on a small .nzb file or similar
	if yencData.Name != "" && group.Type == storage.NZBFileTypeMedia {
		// Only update if it looks like a valid filename
		cleanName := utils.RemoveInvalidChars(yencData.Name)
		if cleanName != "" {
			group.ActualFilename = cleanName
		}
	}

	segmentSize := yencData.End - yencData.Begin + 1
	fileSize := yencData.Size

	// get last file size
	var lastFileSize int64
	if sameFile {
		lastFileSize = fileSize
	} else {
		var lastResult headerResult
		select {
		case lastResult = <-lastCh:
		case <-ctx.Done():
			return ctx.Err()
		}

		if lastResult.err != nil {
			return fmt.Errorf("failed to fetch last segment header: %w", lastResult.err)
		}
		lastFileSize = lastResult.data.Size
	}

	group.metadata = &fileAnalysisResult{
		fileSize:     fileSize,
		lastFileSize: lastFileSize,
		segmentSize:  segmentSize,
	}
	return nil
}

// Process regular media files
func (p *NZBParser) processMediaFile(group *FileGroup, password string) *storage.NZBFile {
	if len(group.Files) == 0 {
		return nil
	}

	// Sort files for consistent ordering
	sort.Slice(group.Files, func(i, j int) bool {
		return group.Files[i].Number < group.Files[j].Number
	})

	// Determine extension
	ext := determineExtension(group)
	if ext == "" {
		ext = filepath.Ext(group.ActualFilename)
	}
	if ext == "" {
		return nil
	}

	name := group.BaseName + ext

	file := &storage.NZBFile{
		Name:     name,
		Groups:   getGroupsList(group.Groups),
		Segments: []storage.NZBSegment{},
		Password: password,
		FileType: group.Type,
	}

	currentOffset := int64(0)
	for index, nzbFile := range group.Files {
		totalSize, segments := getNZBSegments(index, nzbFile, group)
		file.Segments = append(file.Segments, segments...)
		currentOffset += totalSize
	}
	file.Size = currentOffset
	return file
}

func (p *NZBParser) detectFileTypeByContent(ctx context.Context, file nzbparser.NzbFile) (storage.NZBFileType, string, error) {
	if len(file.Segments) == 0 {
		return storage.NZBFileTypeUnknown, "", fmt.Errorf("no segments in file %s", file.Filename)
	}

	// Download first segment to check file signature
	firstSegment := file.Segments[0]
	var data *nntp.YencMetadata
	err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		d, e := conn.GetHeader(firstSegment.Id, defaultMaxSnippetSize)
		data = d
		return e
	})
	if err != nil {
		return storage.NZBFileTypeUnknown, "", fmt.Errorf("failed to fetch segment header for file %s: %w", file.Filename, err)
	}

	if data.Name != "" {
		fileType := p.detectFileType(data.Name)
		if fileType != storage.NZBFileTypeUnknown {
			return fileType, data.Name, nil
		}
	}

	return p.detectFileTypeFromContent(data.Snippet), data.Name, nil
}

func (p *NZBParser) detectFileTypeFromContent(data []byte) storage.NZBFileType {
	if len(data) == 0 {
		return storage.NZBFileTypeUnknown
	}

	// Check for RAR signatures (both RAR 4.x and 5.x)
	if len(data) >= 7 {
		// RAR 4.x signature
		if bytes.Equal(data[:7], []byte("Rar!\x1A\x07\x00")) {
			return storage.NZBFileTypeRar
		}
	}
	if len(data) >= 8 {
		// RAR 5.x signature
		if bytes.Equal(data[:8], []byte("Rar!\x1A\x07\x01\x00")) {
			return storage.NZBFileTypeRar
		}
	}

	// Check for ZIP signature
	if len(data) >= 4 && bytes.Equal(data[:4], []byte{0x50, 0x4B, 0x03, 0x04}) {
		return storage.NZBFileTypeZip
	}

	// Check for 7z signature
	if len(data) >= 6 && bytes.Equal(data[:6], []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}) {
		return storage.NZBFileTypeSevenZip
	}

	// Check for common media file signatures
	if len(data) >= 4 {
		// Matroska (MKV/WebM)
		if bytes.Equal(data[:4], []byte{0x1A, 0x45, 0xDF, 0xA3}) {
			return storage.NZBFileTypeMedia
		}

		// MP4/MOV (check for 'ftyp' at offset 4)
		if len(data) >= 8 && bytes.Equal(data[4:8], []byte("ftyp")) {
			return storage.NZBFileTypeMedia
		}

		// AVI
		if len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) &&
			bytes.Equal(data[8:12], []byte("AVI ")) {
			return storage.NZBFileTypeMedia
		}
	}

	// MPEG checks need more specific patterns
	if len(data) >= 4 {
		// MPEG-1/2 Program Stream
		if bytes.Equal(data[:4], []byte{0x00, 0x00, 0x01, 0xBA}) {
			return storage.NZBFileTypeMedia
		}

		// MPEG-1/2 Video Stream
		if bytes.Equal(data[:4], []byte{0x00, 0x00, 0x01, 0xB3}) {
			return storage.NZBFileTypeMedia
		}
	}

	// Check for Transport Stream (TS files)
	if len(data) >= 1 && data[0] == 0x47 {
		// Additional validation for TS files
		if len(data) >= 188 && data[188] == 0x47 {
			return storage.NZBFileTypeMedia
		}
	}

	return storage.NZBFileTypeUnknown
}

// Calculate total archive Size from all RAR parts in the group
func (p *NZBParser) calculateTotalArchiveSize(group *FileGroup) int64 {
	var total int64
	for _, file := range group.Files {
		for _, segment := range file.Segments {
			total += int64(segment.Bytes)
		}
	}
	return total
}
