package main

import (
	"fmt"
	"log"
	"time"

	"github.com/lekan-pvp/grade/gokafka/internal/client"
	"github.com/lekan-pvp/grade/gokafka/internal/config"
	"github.com/lekan-pvp/grade/gokafka/internal/message"
	"github.com/lekan-pvp/grade/gokafka/internal/monitor"
	"github.com/lekan-pvp/grade/gokafka/pkg/gokafka"
)

func main() {
	// Load configuration.
	cfg, err := config.LoadConfig("config.yml")
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	// Monitor.
	mon := monitor.NewMonitor()

	// Kafka client (owns the writer).
	kafkaClient, err := client.NewKafkaClient(cfg.Kafka.Brokers, cfg.Kafka.Topics[0], cfg.Kafka.GroupID)
	if err != nil {
		log.Fatal("Failed to create Kafka client:", err)
	}

	// Producer.
	producer := gokafka.NewProducer(
		kafkaClient.GetWriter(),
		mon,
		cfg.Kafka.RetryCount,
		cfg.Kafka.RetryDelay,
	)
	if err := producer.SetPartitionStrategy(cfg.Kafka.PartitionStrategy); err != nil {
		log.Printf("Failed to set partition strategy: %v", err)
	}

	// Consumer.
	consumer, err := gokafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topics,
		cfg.Kafka.GroupID,
		mon,
		cfg.Kafka.AutoCommitInterval,
	)
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}

	// Run the consumer in a goroutine.
	go func() {
		err := consumer.ConsumeJSON(func(msg *message.Message) bool {
			fmt.Printf("Received message: %+v\n", msg)
			return true // acknowledge
		})
		if err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Publish test messages.
	for i := 0; i < 5; i++ {
		testMsg := message.NewMessage(map[string]any{
			"id":      i,
			"text":    fmt.Sprintf("Test message %d", i),
			"created": time.Now().Format(time.RFC3339),
		})

		if err := producer.PublishJSON(cfg.Kafka.Topics[0], testMsg); err != nil {
			log.Printf("Failed to publish message %d: %v", i, err)
		} else {
			fmt.Printf("Published message %d\n", i)
		}
		time.Sleep(1 * time.Second)
	}

	// Wait a bit for consumption.
	time.Sleep(5 * time.Second)

	// Close resources.
	if err := consumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}
	if err := kafkaClient.Close(); err != nil {
		log.Printf("Error closing Kafka client: %v", err)
	}

	// Print statistics.
	stats := mon.Stats()
	fmt.Println("Final statistics:")
	fmt.Printf("AVG latency: %v\n", stats["avg_latency"])
	fmt.Printf("Errors: %v\n", stats["errors"])
	fmt.Printf("Max latency: %v\n", stats["max_latency"])
	fmt.Printf("Received: %v\n", stats["received"])
	fmt.Printf("Sent: %v\n", stats["sent"])
	fmt.Printf("Total msgs: %v\n", stats["total_msgs"])
}
