package cmd

import (
	"data_producer/internal/config"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	Cfg     config.Config
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "data_producer",
		Short: "генератор событий из паркет-файлов в кафку",
		Long:  `генератор событий из паркет-файлов в кафку`,
	}
)

func Execute() error {
	err := rootCmd.Execute()
	return err
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "путь к конфиг-файлу (default: ./config.yaml)")
	rootCmd.PersistentFlags().String("host", "", "kafka host")
	rootCmd.PersistentFlags().Int("port", 0, "kafka port")
	rootCmd.PersistentFlags().String("topic", "", "Kafka topic")
	rootCmd.PersistentFlags().Bool("verbose", false, "Enable verbose logging")

	// Связываем флаги с viper
	viper.BindPFlag("kafka.host", rootCmd.PersistentFlags().Lookup("host"))
	viper.BindPFlag("kafka.port", rootCmd.PersistentFlags().Lookup("port"))
	viper.BindPFlag("kafka.topic", rootCmd.PersistentFlags().Lookup("topic"))
	viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.data_producer")
	}

	if err := viper.ReadInConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "Config read err: %v\n", err)
		os.Exit(1)
	}

	if err := viper.Unmarshal(&Cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Config parse err: %v\n", err)
		os.Exit(1)
	}
}
