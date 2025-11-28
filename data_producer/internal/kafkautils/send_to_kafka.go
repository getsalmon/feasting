package kafkautils

import (
	"context"
	"data_producer/internal/config"
	"data_producer/internal/data_types"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func CreateKafkaWriter(cfg *config.Config) *kafka.Writer {
	addr := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)
	log.Printf("Created writer to %s:%d, topic=%s\n", cfg.Kafka.Host, cfg.Kafka.Port, cfg.Kafka.Topic)

	return &kafka.Writer{
		Addr:     kafka.TCP(addr),
		Topic:    cfg.Kafka.Topic,
		Balancer: &kafka.Hash{},
		Async: true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				// КРИТИЧНО: обработать ошибку
				log.Printf("ERROR: Failed to send %d messages: %v", len(messages), err)
				// TODO: добавить retry логику или dead letter queue
			}
		},
	}
}

func PushToKafka(writer *kafka.Writer, kafkaRecords []data_types.KafkaCompatible) error {
	messages := make([]kafka.Message, 0, len(kafkaRecords))
	for _, rec := range kafkaRecords {
		key, err := rec.GetKey()
		if err != nil {
			return fmt.Errorf("error while parsing key: %w", err)
		}
		if len(key) == 0 {
			key = []byte(uuid.New().String())
		}
		value, err := rec.GetValue()
		if err != nil {
			return fmt.Errorf("error while parsing message: %w", err)
		}
		messages = append(messages, kafka.Message{Key: key, Value: value})
	}

	err := writer.WriteMessages(context.Background(),
		messages...,
	)
	if err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}
	return nil
}
