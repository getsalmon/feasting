package event_generator

import (
	"fmt"
	"iter"


	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

type RowWithErr[T any] struct {
	Record T
	Error  error
	Filename string
	RowNumber int
}

func (r *RowWithErr[T]) GetRecordID () uuid.UUID {
	namespace := uuid.NameSpaceURL  // или свой
	rowID := fmt.Sprintf("%s-%d", r.Filename, r.RowNumber)
	rowUUID := uuid.NewSHA1(namespace, []byte(rowID))
	return rowUUID
}

func ProcessFile[T any](filePath string) iter.Seq[RowWithErr[T]] {
	return func(yield func(RowWithErr[T]) bool) {
		rows, err := parquet.ReadFile[T](filePath)
		if err != nil {
			yield(RowWithErr[T]{Error: fmt.Errorf("error reading file: %w", err)})
			return
		}

		for idx, row := range rows {
			if !yield(RowWithErr[T]{Record: row, Filename: filePath, RowNumber: idx}) {
				return
			}
		}
	}
}
