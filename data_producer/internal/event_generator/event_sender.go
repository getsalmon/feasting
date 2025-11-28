package event_generator

import (
	"data_producer/internal/config"
	"data_producer/internal/data_types"
	"data_producer/internal/kafkautils"
	"log"
	"path/filepath"

	"data_producer/internal/utils"
	"time"
)

func LoadEventsToKafka(dataDir string, startDate, endDate *time.Time, cfg config.Config) error {
	kafkaWriter := kafkautils.CreateKafkaWriter(&cfg)
	defer kafkaWriter.Close()
	for f := range utils.ListFilesFromFolderWithFilter(dataDir, startDate, endDate) {
		if f.Error != nil {
			return f.Error
		}
		log.Printf("Processing file: %s", filepath.Base(f.File))
		accum := make([]data_types.KafkaCompatible, 0, cfg.Sender.BatchSize)
		for rowWithErr := range ProcessFile[data_types.DataSetEvent](f.File) {
			if rowWithErr.Error != nil {
				return rowWithErr.Error
			}
			rowWithID := rowWithErr.Record.WithRowID(rowWithErr.GetRecordID())
			accum = append(accum, &rowWithID)
			if len(accum) == cfg.Sender.BatchSize {
				err := kafkautils.PushToKafka(kafkaWriter, accum)
				if err != nil {
					return err
				}
				accum = accum[:0]
			}
		}
		if len(accum) > 0 {
			err := kafkautils.PushToKafka(kafkaWriter, accum)
			if err != nil {
				return err
			}
		}
		log.Printf("Ended processing file: %s", filepath.Base(f.File))
	}
	return nil
}
