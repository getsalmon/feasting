package utils

import (
	"iter"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type IterFileResult struct {
	File  string
	Error error
}

func ListFilesFromFolderWithFilter(dataDir string, dateFrom, dateTo *time.Time) iter.Seq[IterFileResult] {
	return func(yield func(IterFileResult) bool) {
		dataDir, err := filepath.Abs(dataDir)
		if err != nil {
			yield(IterFileResult{Error: err})
			return
		}
		pattern := filepath.Join(dataDir, "*.parquet")
		allFiles, err := filepath.Glob(pattern)
		if err != nil {
			yield(IterFileResult{Error: err})
			return
		}
		sort.Strings(allFiles)

		if dateFrom == nil && dateTo == nil {
			for _, f := range allFiles {
				if !yield(IterFileResult{File: f}) {
					return
				}
			}
			return
		}

		for _, file := range allFiles {
			basename := filepath.Base(file)

			parts := strings.Split(strings.TrimSuffix(basename, ".parquet"), "_")
			if len(parts) == 0 {
				continue
			}
			// формат названия: data_<date>_<chunk_no>.parquet
			dateStr := parts[1]

			fileDate, err := time.Parse(time.DateOnly, dateStr)
			if err != nil {
				log.Printf("Warning: cannot parse date from filename: %s", basename)
				continue
			}

			if dateFrom != nil && fileDate.Before(*dateFrom) {
				continue
			}
			if dateTo != nil && fileDate.After(*dateTo) {
				continue
			}
			if !yield(IterFileResult{File: file}) {
				return
			}
		}
	}
}
