package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

var (
	printConfigCmd = &cobra.Command{
		Use:   "printconfig",
		Short: "Показать текущий конфиг",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			log.Printf("Current config is: %s:%d@%s\n", Cfg.Kafka.Host, Cfg.Kafka.Port, Cfg.Kafka.Topic)
		},
	}
)

func init() {
	rootCmd.AddCommand(printConfigCmd)
}
