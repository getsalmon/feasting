package config

type Config struct {
	Kafka struct {
		Host  string `mapstructure:"host"`
		Port  int    `mapstructure:"port"`
		Topic string `mapstructure:"topic"`
	} `mapstructure:"kafka"`
	Verbose bool `mapstructure:"verbose"`
	Sender  struct {
		BatchSize int `mapstructure:"batch_size"`
	} `mapstructure:"sender"`
}
