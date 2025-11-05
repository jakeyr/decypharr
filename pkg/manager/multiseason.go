package manager

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	torrent2 "github.com/sirrobot01/decypharr/pkg/storage"
)

// Multi-season detection patterns
var (
	// Pre-compiled patterns for multi-season replacement
	multiSeasonReplacements = []multiSeasonPattern{
		// S01-08 -> S01 (or whatever target season)
		{regexp.MustCompile(`(?i)S(\d{1,2})-\d{1,2}`), "S%02d"},

		// S01-S08 -> S01
		{regexp.MustCompile(`(?i)S(\d{1,2})-S\d{1,2}`), "S%02d"},

		// Season 1-8 -> Season 1
		{regexp.MustCompile(`(?i)Season\.?\s*\d{1,2}-\d{1,2}`), "Season %02d"},

		// Seasons 1-8 -> Season 1
		{regexp.MustCompile(`(?i)Seasons\.?\s*\d{1,2}-\d{1,2}`), "Season %02d"},

		// Complete Series -> Season X
		{regexp.MustCompile(`(?i)Complete\.?Series`), "Season %02d"},

		// All Seasons -> Season X
		{regexp.MustCompile(`(?i)All\.?Seasons?`), "Season %02d"},
	}

	// Also pre-compile other patterns
	seasonPattern     = regexp.MustCompile(`(?i)(?:season\.?\s*|s)(\d{1,2})`)
	qualityIndicators = regexp.MustCompile(`(?i)\b(2160p|1080p|720p|BluRay|WEB-DL|HDTV|x264|x265|HEVC)`)

	multiSeasonIndicators = []*regexp.Regexp{
		regexp.MustCompile(`(?i)complete\.?series`),
		regexp.MustCompile(`(?i)all\.?seasons?`),
		regexp.MustCompile(`(?i)season\.?\s*\d+\s*-\s*\d+`),
		regexp.MustCompile(`(?i)s\d+\s*-\s*s?\d+`),
		regexp.MustCompile(`(?i)seasons?\s*\d+\s*-\s*\d+`),
	}
)

type multiSeasonPattern struct {
	pattern     *regexp.Regexp
	replacement string
}

type seasonResult struct {
	debridTorrent *debridTypes.Torrent
	torrent       *torrent2.Torrent
}

// SeasonInfo represents information about a season in a multi-season torrent
type SeasonInfo struct {
	SeasonNumber int
	Files        []debridTypes.File
	InfoHash     string
	Name         string
}

func (m *Manager) replaceMultiSeasonPattern(name string, targetSeason int) string {
	result := name

	// Apply each pre-compiled pattern replacement
	for _, msp := range multiSeasonReplacements {
		if msp.pattern.MatchString(result) {
			replacement := fmt.Sprintf(msp.replacement, targetSeason)
			result = msp.pattern.ReplaceAllString(result, replacement)
			m.logger.Debug().Msgf("Applied pattern replacement: %s -> %s", name, result)
			return result
		}
	}

	// If no multi-season pattern found, try to insert season info intelligently
	return m.insertSeasonIntoName(result, targetSeason)
}

func (m *Manager) insertSeasonIntoName(name string, seasonNum int) string {
	// Check if season info already exists
	if seasonPattern.MatchString(name) {
		return name // Already has season info, keep as is
	}

	// Try to find a good insertion point (before quality indicators)
	if loc := qualityIndicators.FindStringIndex(name); loc != nil {
		// Insert season before quality info
		before := strings.TrimSpace(name[:loc[0]])
		after := name[loc[0]:]
		return fmt.Sprintf("%s S%02d %s", before, seasonNum, after)
	}

	// If no quality indicators found, append at the end
	return fmt.Sprintf("%s S%02d", name, seasonNum)
}

func (m *Manager) detectMultiSeason(debridTorrent *debridTypes.Torrent) (bool, []SeasonInfo, error) {
	torrentName := debridTorrent.Name
	files := debridTorrent.GetFiles()

	m.logger.Debug().Msgf("Analyzing torrent for multi-season: %s", torrentName)

	// Find all seasons present in the files
	seasonsFound := m.findAllSeasons(files)

	// Check if this is actually a multi-season torrent
	isMultiSeason := len(seasonsFound) > 1 || m.hasMultiSeasonIndicators(torrentName)

	if !isMultiSeason {
		return false, nil, nil
	}

	m.logger.Info().Msgf("Multi-season torrent detected with seasons: %v", getSortedSeasons(seasonsFound))

	// Group files by season
	seasonGroups := m.groupFilesBySeason(files, seasonsFound)

	// Create SeasonInfo objects with proper naming
	var seasons []SeasonInfo
	for seasonNum, seasonFiles := range seasonGroups {
		if len(seasonFiles) == 0 {
			continue
		}

		// Generate season-specific name preserving all metadata
		seasonName := m.generateSeasonSpecificName(torrentName, seasonNum)

		seasons = append(seasons, SeasonInfo{
			SeasonNumber: seasonNum,
			Files:        seasonFiles,
			InfoHash:     m.generateSeasonHash(debridTorrent.InfoHash, seasonNum),
			Name:         seasonName,
		})
	}

	return true, seasons, nil
}

// generateSeasonSpecificName creates season name preserving all original metadata
func (m *Manager) generateSeasonSpecificName(originalName string, seasonNum int) string {
	// Find and replace the multi-season pattern with single season
	seasonName := m.replaceMultiSeasonPattern(originalName, seasonNum)

	m.logger.Debug().Msgf("Generated season name for S%02d: %s", seasonNum, seasonName)

	return seasonName
}

func (m *Manager) findAllSeasons(files []debridTypes.File) map[int]bool {
	seasons := make(map[int]bool)

	for _, file := range files {
		// Check filename first
		if season := m.extractSeason(file.Name); season > 0 {
			seasons[season] = true
			continue
		}

		// Check full path
		if season := m.extractSeason(file.Path); season > 0 {
			seasons[season] = true
		}
	}

	return seasons
}

// extractSeason pulls season number from a string
func (m *Manager) extractSeason(text string) int {
	matches := seasonPattern.FindStringSubmatch(text)
	if len(matches) > 1 {
		if num, err := strconv.Atoi(matches[1]); err == nil && num > 0 && num < 100 {
			return num
		}
	}
	return 0
}

func (m *Manager) hasMultiSeasonIndicators(torrentName string) bool {
	for _, pattern := range multiSeasonIndicators {
		if pattern.MatchString(torrentName) {
			return true
		}
	}
	return false
}

// groupFilesBySeason puts files into season buckets
func (m *Manager) groupFilesBySeason(files []debridTypes.File, knownSeasons map[int]bool) map[int][]debridTypes.File {
	groups := make(map[int][]debridTypes.File)

	// Initialize groups
	for season := range knownSeasons {
		groups[season] = []debridTypes.File{}
	}

	for _, file := range files {
		// Try to find season from filename or path
		season := m.extractSeason(file.Name)
		if season == 0 {
			season = m.extractSeason(file.Path)
		}

		// If we found a season and it's known, add the file
		if season > 0 && knownSeasons[season] {
			groups[season] = append(groups[season], file)
		} else {
			// If no season found, try path-based inference
			inferredSeason := m.inferSeasonFromPath(file.Path, knownSeasons)
			if inferredSeason > 0 {
				groups[inferredSeason] = append(groups[inferredSeason], file)
			} else if len(knownSeasons) == 1 {
				// If only one season exists, default to it
				for season := range knownSeasons {
					groups[season] = append(groups[season], file)
				}
			}
		}
	}

	return groups
}

func (m *Manager) inferSeasonFromPath(path string, knownSeasons map[int]bool) int {
	pathParts := strings.Split(path, "/")

	for _, part := range pathParts {
		if season := m.extractSeason(part); season > 0 && knownSeasons[season] {
			return season
		}
	}

	return 0
}

// Helper to get sorted season list for logging
func getSortedSeasons(seasons map[int]bool) []int {
	var result []int
	for season := range seasons {
		result = append(result, season)
	}
	return result
}

// generateSeasonHash creates a unique hash for a season based on original hash
func (m *Manager) generateSeasonHash(originalHash string, seasonNumber int) string {
	source := fmt.Sprintf("%s-season-%d", originalHash, seasonNumber)
	hash := md5.Sum([]byte(source))
	return fmt.Sprintf("%x", hash)
}
