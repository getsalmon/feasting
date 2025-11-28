package cmd

import (
	"fmt"
	"time"
	"data_producer/internal/event_generator"
	"github.com/spf13/cobra"
)

var (
	dataDir      string
	startDateStr string
	endDateStr   string
	listDirCmd   = &cobra.Command{
		Use:   "seqLoad",
		Short: "грузить события",
		RunE: func(cmd *cobra.Command, args []string) error {
			var startDate, endDate *time.Time
			if startDateStr == "" && endDateStr != "" {
				return fmt.Errorf("start-date not set when end-date set")
			}
			if startDateStr != "" {
				parsed, err := time.Parse(time.DateOnly, startDateStr)
				if err != nil {
					return fmt.Errorf("invalid start-date: %w", err)
				}
				startDate = &parsed
			}
			if endDateStr != "" {
				parsed, err := time.Parse(time.DateOnly, endDateStr)
				if err != nil {
					return fmt.Errorf("invalid end-date: %w", err)
				}
				endDate = &parsed
			}
			err := event_generator.LoadEventsToKafka(dataDir, startDate, endDate, Cfg)
			return err
		},
	}
)

func init() {
	listDirCmd.Flags().StringVarP(&dataDir, "data-dir", "d", "", "Directory with parquet files (required)")
	listDirCmd.Flags().StringVar(&startDateStr, "start-date", "", "Start date (YYYY-MM-DD, optional)")
	listDirCmd.Flags().StringVar(&endDateStr, "end-date", "", "End date (YYYY-MM-DD, optional)")
	listDirCmd.MarkFlagRequired("data-dir")
	rootCmd.AddCommand(listDirCmd)
}
