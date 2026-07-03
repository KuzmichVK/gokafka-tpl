package gokafka

import (
	"context"
	"encoding/json"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/lekan-pvp/grade/gokafka/internal/message"
	"github.com/lekan-pvp/grade/gokafka/internal/monitor"
)

// Consumer reads JSON messages from a set of topics as part of a group.
type Consumer struct {
	reader             *kafka.Reader
	monitor            *monitor.Monitor
	autoCommitInterval time.Duration
}

// NewConsumer creates a group consumer over the given brokers and topics.
func NewConsumer(brokers []string, topics []string, groupID string, mon *monitor.Monitor, autoCommitInterval time.Duration) (*Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    topics,
		CommitInterval: autoCommitInterval, // auto-commit at this interval
	})
	return &Consumer{
		reader:             reader,
		monitor:            mon,
		autoCommitInterval: autoCommitInterval,
	}, nil
}

// ConsumeJSON reads messages in a loop and dispatches each to handler.
// The offset is auto-committed by the reader at autoCommitInterval.
func (c *Consumer) ConsumeJSON(handler func(*message.Message) bool) error {
	for {
		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			c.monitor.IncError()
			return err
		}
		var msg message.Message
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			c.monitor.IncError()
			continue
		}
		if handler(&msg) {
			c.monitor.IncReceived()
		}
	}
}

// EnableAutoCommit records the auto-commit interval. The reader already
// auto-commits at the interval passed to NewConsumer; this setter is provided
// for API completeness.
func (c *Consumer) EnableAutoCommit(interval time.Duration) error {
	c.autoCommitInterval = interval
	return nil
}

// SeekTo sets the reader offset. Note: kafka-go does not allow SetOffset on a
// reader that belongs to a consumer group; use on a non-group reader.
func (c *Consumer) SeekTo(offset int64) error {
	return c.reader.SetOffset(offset)
}

// Close closes the underlying reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
