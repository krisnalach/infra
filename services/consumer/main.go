package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/krisnalach/infra/pkg/schema"
	"github.com/krisnalach/infra/services/consumer/internal/window"
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
		"group.id":          "kafka-consumer",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{"trades"}, nil)

	// Each symbol has its own window
	windows := make(map[string]*window.Window)

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
				slog.Warn("transient consume error, retrying", "err", err)
				time.Sleep(2 * time.Second)
				continue
			}
			slog.Error("consume failed", "err", err)
			break
		}
		/* logger.Debug("message consumed",
		"partition", msg.TopicPartition.Partition,
		"offset", msg.TopicPartition.Offset,
		"key", string(msg.Key),
		"value", string(msg.Value)) */

		var trade schema.Trade
		if err := json.Unmarshal(msg.Value, &trade); err != nil {
			continue
		}

		w, ok := windows[trade.ProductID]
		if !ok {
			slog.Info("window info", "new window", trade.ProductID)

			w = &window.Window{
				Symbol:   trade.ProductID,
				Duration: 30,
				Emit:     make(chan window.Stats, 1),
			}

			go func(w *window.Window) {
				for stats := range w.Emit {
					slog.Info("window data", "stats", stats.String())
				}
			}(w)

			windows[trade.ProductID] = w
		}

		w.Add(trade)

	}

}

func main() {
	consumer()
}
