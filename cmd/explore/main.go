package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/krisnalach/infra/cmd/explore/internal/feed"
	"github.com/krisnalach/infra/cmd/explore/internal/feed/coinbase"
)

func producer() {
	var f feed.Feed = coinbase.New()
	trades, _ := f.Subscribe([]string{"BTC-USD"})

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all",
	})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	topic := "trades"

	i := 0
	for t := range trades {
		data, _ := json.Marshal(t)
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		}, Value: data,
			Timestamp: t.TradeTime}, nil)
		i++
		if i == 1 {
			break
		}
	}

	p.Flush(2000)
}

func consumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "explore-consumer",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.SubscribeTopics([]string{"trades"}, nil)

	for {
		msg, err := c.ReadMessage(5 * time.Second)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.IsTimeout() {
				fmt.Println("waiting...")
				continue
			}
			break
		}
		fmt.Printf("partition=%d offset=%d key=%s value=%s\n",
			msg.TopicPartition.Partition,
			msg.TopicPartition.Offset,
			string(msg.Key),
			string(msg.Value))
	}

}

func main() {
	go producer()

	consumer()

}
