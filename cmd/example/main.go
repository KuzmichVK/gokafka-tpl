package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	// Загружаем конфигурацию
	//cfg, err := config.LoadConfig("config.yml")
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	// Создаём монитор
	mon := monitor.NewMonitor()

	// Создаём продюсера
	kafkaClient, err := client.NewKafkaClient(cfg.Kafka.Brokers, cfg.Kafka.Topics[0], cfg.Kafka.GroupID)
	if err != nil {
		log.Fatal("Failed to create Kafka client:", err)
	}

	producer := gokafka.NewProducer(
		kafkaClient.GetWriter(),
		mon,
		cfg.Kafka.RetryCount,
		cfg.Kafka.RetryDelay,
	)

	// Создаём потребителя
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

	// Запускаем потребитель в отдельной горутине
	go func() {
		err := consumer.ConsumeJSON(func(msg *message.Message) bool {
			fmt.Printf("Received message: %+v\n", msg)
			// Обрабатываем сообщение
			return true // Подтверждаем обработку
		})
		if err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Публикуем тестовые сообщения
	for i := 0; i < 5; i++ {
		testMsg := message.NewMessage(map[string]any{
			"id":      i,
			"text":    fmt.Sprintf("Test message %d", i),
			"created": time.Now().Format(time.RFC3339),
		})

		err := producer.PublishJSON(cfg.Kafka.Topics[0], testMsg)
		if err != nil {
			log.Printf("Failed to publish message %d: %v", i, err)
		} else {
			fmt.Printf("Published message %d\n", i)
		}
		time.Sleep(1 * time.Second)
	}

	// Ждём немного для демонстрации
	time.Sleep(5 * time.Second)

	// Закрываем ресурсы
	if err := consumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}
	if err := kafkaClient.Close(); err != nil {
		log.Printf("Error closing Kafka client: %v", err)
	}

	// Выводим статистику
	stats := mon.Stats()
	fmt.Println("Final statistics:")
	fmt.Printf("AVG latency: %v\n", stats["avg_latency"])
	fmt.Printf("Errors: %v\n", stats["errors"])
	fmt.Printf("Max latency: %v\n", stats["max_latency"])
	fmt.Printf("Received: %v\n", stats["received"])
	fmt.Printf("Sent: %v\n", stats["sent"])
	fmt.Printf("Total msgs: %v\n", stats["total_msgs"])
}
