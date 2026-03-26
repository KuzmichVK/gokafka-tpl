# GoKafka — Go клиент для Apache Kafka

Надёжный Go‑клиент для работы с Apache Kafka с поддержкой JSON, партиционирования, повторных попыток и мониторинга.

## Особенности

* Отправка и получение сообщений в формате JSON.
* Поддержка партиционирования с разными стратегиями.
* Механизм повторных попыток отправки сообщений.
* Встроенный мониторинг статистики.
* Управление смещениями (offsets).
* Конфигурация через YAML‑файл.

## Установка

```bash
go get github.com/lekan-pvp/gokafka
```

## Использование

1. Создайте `config.yaml` с настройками:

```yaml
kafka:
  brokers:
    - "localhost:9092"
  topics:
    - "main_topic"
  group_id: "go-kafka-consumer-group"
  auto_commit_interval: 5s
  retry_count: 3
  retry_delay: 1s
  partition_strategy: "round_robin"


```

## Конфигурация

Параметр|Описание|Значение по умолчанию
|-|-|-|
|`brokers`|Список брокеров Kafka|--|
|`topics`|Темы для подписки|--|
|`group_id`|ID группы потребителей|--|
|`auto_commit_interval`|Интервал автоматического подтверждения|5s|
|`retry_count`|Количество повторных попыток|3|
|`retry_delay`|Задержка между попытками|1s|
|`partition_strategy`|Стратегия партиционирования|`round_robin`|

2. Инициализируйте клиент и начните работу:

```go
package main

import (
    "log"
    "github.com/yourname/gokafka/internal/config"
    "github.com/yourname/gokafka/pkg/gokafka"
    "github.com/yourname/gokafka/internal/message"
)

func main() {
    // Загружаем конфигурацию
    cfg, err := config.LoadConfig("config.yaml")
    if err != nil {
        log.Fatal("Failed to load config:", err)
    }

    // Создаём клиент Kafka
    client, err := gokafka.NewClient(cfg.Kafka.Brokers, cfg.Kafka.Topics[0], cfg.Kafka.GroupID)
    if err != nil {
        log.Fatal("Failed to create Kafka client:", err)
    }
    defer client.Close()

    // Создаём продюсера
    producer := client.NewProducer(
        cfg.Kafka.RetryCount,
        cfg.Kafka.RetryDelay,
    )

    // Устанавливаем стратегию партиционирования
    err = producer.SetPartitionStrategy(cfg.Kafka.PartitionStrategy)
    if err != nil {
        log.Fatal("Failed to set partition strategy:", err)
    }

    // Создаём потребителя
    consumer := client.NewConsumer(cfg.Kafka.AutoCommitInterval)

    // Запускаем потребитель в отдельной горутине
    go func() {
        err := consumer.ConsumeJSON(
            cfg.Kafka.Topics,
            func(msg *message.Message) bool {
                log.Printf("Received message: %+v\n", msg)
                return true // Подтверждаем обработку
            },
            cfg.Kafka.GroupID,
        )
        if err != nil {
            log.Printf("Consumer error: %v", err)
        }
    }()

    // Отправляем тестовое сообщение
    testMsg := &message.Message{
        Value: map[string]interface{}{
            "text": "Hello, Kafka!",
        },
    }

    err = producer.PublishJSON(cfg.Kafka.Topics[0], testMsg)
    if err != nil {
        log.Printf("Failed to publish message: %v", err)
    }
}
```

## API

### Продюсер

- `PublishJSON(topic string, msg *Message, partition ...int) error` — отправка JSON‑сообщения.

- `SetPartitionStrategy(strategy string) error` — установка стратегии распределения по партициям.

### Потребитель

- `ConsumeJSON(topics []string, handler func(*Message) bool, groupID string) error` — подписка и обработка сообщений.

- `EnableAutoCommit(interval time.Duration) error` — включение автоподтверждения.

- `SeekTo(offset int64) error` — перемещение к указанному смещению.

### Монитор

- `IncSent()` — увеличить счётчик отправленных.

- `IncReceived()` — увеличить счётчик полученных.

- `IncError()` — увеличить счётчик ошибок.

- `AddLatency(latency time.Duration)` — добавить задержку.

- `Stats() map[string]interface{}` — получить статистику.

### Docker‑окружение

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    ports:
      - "9092:9092"
    environment:
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

### Тестирование

1. Соберите проект:

```bash
go build -o gokafka cmd/example/main.go
```

2. Запустите:

```
./gokafka
```

3. Проверьте статистику в консоли.

