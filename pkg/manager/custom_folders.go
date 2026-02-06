package manager

import (
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

const (
	filterByInclude string = "include"
	filterByExclude string = "exclude"

	filterByStartsWith    string = "starts_with"
	filterByEndsWith      string = "ends_with"
	filterByNotStartsWith string = "not_starts_with"
	filterByNotEndsWith   string = "not_ends_with"

	filterByRegex    string = "regex"
	filterByNotRegex string = "not_regex"

	filterByExactMatch    string = "exact_match"
	filterByNotExactMatch string = "not_exact_match"

	filterBySizeGT string = "size_gt"
	filterBySizeLT string = "size_lt"

	filterBLastAdded string = "last_added"
)

type CustomFolders struct {
	filters map[string][]directoryFilter
	folders []string
}

type directoryFilter struct {
	filterType    string
	value         string
	regex         *regexp.Regexp // only for regex/not_regex
	sizeThreshold int64          // only for size_gt/size_lt
	ageThreshold  time.Duration  // only for last_added
}

func (m *Manager) initCustomFolders() {
	var customFolders []string
	dirFilters := map[string][]directoryFilter{}
	for name, value := range m.config.CustomFolders {
		for filterType, v := range value.Filters {
			df := directoryFilter{filterType: filterType, value: v}
			switch filterType {
			case filterByRegex, filterByNotRegex:
				df.regex = regexp.MustCompile(v)
			case filterBySizeGT, filterBySizeLT:
				df.sizeThreshold, _ = config.ParseSize(v)
			case filterBLastAdded:
				df.ageThreshold, _ = utils.ParseDuration(v)
			}
			dirFilters[name] = append(dirFilters[name], df)
		}
		customFolders = append(customFolders, name)

	}
	m.customFolders = &CustomFolders{
		filters: dirFilters,
		folders: customFolders,
	}
}

func (m *Manager) GetCustomFolders() []string {
	return m.customFolders.folders
}

// matchesFilter checks if a torrent file info matches all filters for a folder
func (cf *CustomFolders) matchesFilter(folderName string, fileInfo os.FileInfo, addedTime time.Time) bool {
	filters, ok := cf.filters[folderName]
	if !ok {
		return false
	}

	// All filters must match (AND logic)
	for _, filter := range filters {
		if !cf.checkSingleFilter(filter, fileInfo, addedTime) {
			return false
		}
	}
	return true
}

// checkSingleFilter checks if a single filter matches
func (cf *CustomFolders) checkSingleFilter(filter directoryFilter, fileInfo os.FileInfo, addedTime time.Time) bool {
	name := fileInfo.Name()
	size := fileInfo.Size()

	switch filter.filterType {
	case filterByInclude:
		return strings.Contains(name, filter.value)
	case filterByExclude:
		return !strings.Contains(name, filter.value)
	case filterByStartsWith:
		return regexp.MustCompile("^" + regexp.QuoteMeta(filter.value)).MatchString(name)
	case filterByEndsWith:
		return regexp.MustCompile(regexp.QuoteMeta(filter.value) + "$").MatchString(name)
	case filterByNotStartsWith:
		return !regexp.MustCompile("^" + regexp.QuoteMeta(filter.value)).MatchString(name)
	case filterByNotEndsWith:
		return !regexp.MustCompile(regexp.QuoteMeta(filter.value) + "$").MatchString(name)
	case filterByRegex:
		return filter.regex.MatchString(name)
	case filterByNotRegex:
		return !filter.regex.MatchString(name)
	case filterByExactMatch:
		return name == filter.value
	case filterByNotExactMatch:
		return name != filter.value
	case filterBySizeGT:
		return size > filter.sizeThreshold
	case filterBySizeLT:
		return size < filter.sizeThreshold
	case filterBLastAdded:
		return time.Since(addedTime) <= filter.ageThreshold
	default:
		return false
	}
}
