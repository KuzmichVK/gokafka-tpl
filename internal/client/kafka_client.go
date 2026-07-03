// Package client wraps a Kafka connection and writer.
package client

import (
	kafka "github.com/segmentio/kafka-go"
)

// KafkaClient owns a shared writer and connection settings.
type KafkaClient struct {
	writer  *kafka.Writer
	brokers []string
	topic   string
	groupID string
}

// NewKafkaClient creates a client with a writer bound to the given brokers.
// The writer intentionally has no fixed Topic so PublishJSON can target any
// topic via Message.Topic.
func NewKafkaClient(brokers []string, topic string, groupID string) (*KafkaClient, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.RoundRobin{},
	}
	return &KafkaClient{
		writer:  writer,
		brokers: brokers,
		topic:   topic,
		groupID: groupID,
	}, nil
}

// GetWriter returns the underlying Kafka writer.
func (c *KafkaClient) GetWriter() *kafka.Writer {
	return c.writer
}

// Close closes the writer.
func (c *KafkaClient) Close() error {
	return c.writer.Close()
}
