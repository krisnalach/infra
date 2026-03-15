package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/krisnalach/infra/services/ingest/internal/feed"
	"github.com/krisnalach/infra/services/ingest/internal/feed/coinbase"
)

func producer() {
	symbols := strings.Split(os.Getenv("SYMBOLS"), ",")
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")

	slog.Info("ingest starting", "symbols", symbols, "bootstrap", bootstrapServers)

	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	var f feed.Feed = coinbase.New()
	trades, _ := f.Subscribe(symbols)

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				slog.Error("delivery failed", "err", m.TopicPartition.Error)
			}
		}
	}()

	topic := "trades"

	i := 0
	for t := range trades {
		data, _ := json.Marshal(t)
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
			Key:       []byte(t.ProductID),
			Value:     data,
			Timestamp: t.TradeTime}, nil)
		if err != nil {
			slog.Error("produce failed", "error", err)
		}
		i++
		logger.Info("trade produced",
			"exchange", "coinbase",
			"product", t.ProductID,
			"price", t.Price,
		)
	}

	p.Flush(2000)
}

func main() {
	producer()
}
