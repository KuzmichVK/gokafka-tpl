# CHANGELOG — gokafka-tpl

Проект: Go-клиент для Apache Kafka (проверочная работа модуля 2, курс «Работа с
брокерами сообщений», Яндекс Грейд). Все пакеты реализованы под контракт из
`cmd/example/main.go`.

---

## Ключевое решение: контракт задаёт `main.go`, а не README

В шаблоне **нет `_test.go`** (платформа проверяет скрытыми тестами; `go.sum` уже
содержит `testify`). Единственный **компилируемый** носитель контракта в репозитории —
`cmd/example/main.go`. Он использует API, который **расходится с примерами в
README**. По правилу «контракт задаёт код, а не текст» реализация следует `main.go`:

| README (примеры) | `main.go` — реальный контракт |
|---|---|
| `gokafka.NewClient(brokers, topic, groupID)` | `client.NewKafkaClient(...)` + `kafkaClient.GetWriter()` |
| `client.NewProducer(retryCount, retryDelay)` | `gokafka.NewProducer(writer, mon, retryCount, retryDelay)` |
| `client.NewConsumer(autoCommitInterval)` | `gokafka.NewConsumer(brokers, topics, groupID, mon, autoCommitInterval)` |
| `ConsumeJSON(topics, handler, groupID)` | `ConsumeJSON(handler)` (topics/groupID — в конструкторе) |
| `config.LoadConfig("config.yaml")` | `config.LoadConfig("config.yml")` |

Методы, упомянутые в README API и **не противоречащие** `main.go`, реализованы
дополнительно: `Producer.SetPartitionStrategy`, `Consumer.EnableAutoCommit`,
`Consumer.SeekTo`, `Monitor.AddLatency`.

`main.go` в шаблоне был **недоделан** (строка `cfg` закомментирована, отсутствовали
импорты) — доведён до рабочего состояния (импорты, `SetPartitionStrategy`).

---

## Что сделано

Реализованы все пустые файлы шаблона:

| Файл | Содержимое |
|---|---|
| `internal/message/message.go` | `Message{Key, Value}`, `NewMessage(map[string]any)` |
| `internal/config/config.go` | `Config`/`KafkaConfig`, `LoadConfig` (парсинг Duration) |
| `internal/monitor/monitor.go` | `Monitor`, `IncSent/IncReceived/IncError`, `AddLatency`, `Stats` |
| `internal/client/kafka_client.go` | `KafkaClient`, `NewKafkaClient`, `GetWriter`, `Close` |
| `pkg/gokafka/types.go` | константы стратегий партиционирования |
| `pkg/gokafka/producer.go` | `Producer`, `NewProducer`, `PublishJSON`, `SetPartitionStrategy` |
| `pkg/gokafka/consumer.go` | `Consumer`, `NewConsumer`, `ConsumeJSON`, `EnableAutoCommit`, `SeekTo`, `Close` |
| `cmd/example/main.go` | доведён до рабочего вида |
| `config.yml` | заполнен (из README) |
| `docker-compose.yml` | заполнен (KRaft, из README) |

**Не тронуты:** `go.mod`, `go.sum` (зависимости уже зафиксированы), `README.md`
(документация задания).

---

## Технические решения

1. **Module path.** Все импорты — под `github.com/lekan-pvp/grade/gokafka` (из
   `go.mod`). В README-примерах фигурируют плейсхолдеры (`yourname`,
   `lekan-pvp/gokafka`) — не использованы.

2. **Парсинг `time.Duration` из YAML.** `yaml.v2` **не** декодирует строки вида
   `"5s"` в `time.Duration`. Поэтому `LoadConfig` читает конфиг во внутреннюю
   структуру со строковыми полями и парсит их через `time.ParseDuration`, а
   `KafkaConfig.AutoCommitInterval`/`RetryDelay` остаются `time.Duration` (чтобы
   `main.go` передавал их напрямую в `NewConsumer`/`NewProducer`).

3. **Writer без фиксированного `Topic`.** `NewKafkaClient` создаёт `kafka.Writer`
   **без** поля `Topic`, а `PublishJSON` указывает `Topic` в каждом `kafka.Message`.
   Иначе `kafka-go` вернёт ошибку «нельзя задавать и `Writer.Topic`, и
   `Message.Topic`». Это позволяет публиковать в любой топик через один writer.

4. **Симметричная (де)сериализация.** `PublishJSON` маршалит **весь** `Message`
   (`Key` + `Value`), `ConsumeJSON` — обратно в `Message`. Round-trip консистентен.

5. **Ключи `Stats`.** `Monitor.Stats()` возвращает ровно те ключи, что читает
   `main.go`: `sent`, `received`, `errors`, `avg_latency`, `max_latency`,
   `total_msgs`.

6. **Нюансы README-методов** (реализованы, но с оговорками):
   - `SeekTo(offset)` вызывает `reader.SetOffset` — в `kafka-go` это **не работает**
     на reader'е, принадлежащем consumer group (только на не-групповом). Помечено
     комментарием.
   - `EnableAutoCommit(interval)` сохраняет интервал; фактический автокоммит уже
     включён через `CommitInterval` в `NewConsumer`. Метод — для полноты API.

---

## Команды (раскладка → сборка → проверка → git)

Из корня склонированного репозитория. **Перед git — пауза синхронизации
Яндекс.Диска** (репозиторий на пути Я.Диска).

```bash
# раскладка файлов — heredoc-командами (см. чат), затем:
docker compose up -d --wait   # KRaft Kafka (healthcheck ждёт готовности)
go build ./...                # сборка (зависимости уже в go.sum; go mod tidy НЕ нужен)
gofmt -w .                    # формат
go vet ./...                  # статический анализ
go run ./cmd/example          # запуск демо (или: go build -o gokafka cmd/example/main.go && ./gokafka)

# git (при выключенной синхронизации Я.Диска):
git add -A
git commit -m "feat: реализация GoKafka клиента (client/producer/consumer/monitor/config)"
git push
```

> `go mod tidy` выполнять **не нужно**: `go.mod`/`go.sum` уже готовы. Более того,
> `tidy` может убрать `golang.org/x/tools` из зависимостей (в коде он не используется),
> изменив шаблонные `go.mod`/`go.sum` — лучше не трогать.
>
> Локальный `go test ./...` покажет «no test files» (тестов в репозитории нет) — это
> ожидаемо; проверку выполняют скрытые тесты платформы при сдаче.
