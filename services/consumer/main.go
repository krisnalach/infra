package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func consumer() {
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")

	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	slog.Info("consumer starting", "bootstrap", bootstrapServers)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "explore-consumer",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{"trades"}, nil)

	// i := 0
	for {
		msg, err := c.ReadMessage(5 * time.Second)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok {
				if kafkaErr.IsTimeout() {
					fmt.Println("waiting...")
					continue
				}
				if kafkaErr.IsFatal() {
					slog.Error("fatal consume error", "err", err)
					break
				}
				// Transient errors (e.g. unknown topic while ingest is starting up)
				slog.Warn("transient consume error, retrying", "err", err)
				time.Sleep(2 * time.Second)
				continue
			}
			slog.Error("consume failed", "err", err)
			break
		}
		logger.Debug("message consumed",
			"partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset,
			"key", string(msg.Key),
			"value", string(msg.Value))
	}

}

func main() {
	consumer()
}
