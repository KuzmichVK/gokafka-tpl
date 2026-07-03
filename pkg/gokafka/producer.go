package gokafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/lekan-pvp/grade/gokafka/internal/message"
	"github.com/lekan-pvp/grade/gokafka/internal/monitor"
)

// Producer publishes JSON messages with retries and monitoring.
type Producer struct {
	writer     *kafka.Writer
	monitor    *monitor.Monitor
	retryCount int
	retryDelay time.Duration
}

// NewProducer creates a Producer over the given writer.
func NewProducer(writer *kafka.Writer, mon *monitor.Monitor, retryCount int, retryDelay time.Duration) *Producer {
	return &Producer{
		writer:     writer,
		monitor:    mon,
		retryCount: retryCount,
		retryDelay: retryDelay,
	}
}

// SetPartitionStrategy switches the writer balancer.
func (p *Producer) SetPartitionStrategy(strategy string) error {
	switch strategy {
	case PartitionRoundRobin:
		p.writer.Balancer = &kafka.RoundRobin{}
	case PartitionHash:
		p.writer.Balancer = &kafka.Hash{}
	case PartitionLeastBytes:
		p.writer.Balancer = &kafka.LeastBytes{}
	default:
		return fmt.Errorf("unknown partition strategy: %s", strategy)
	}
	return nil
}

// PublishJSON marshals msg to JSON and writes it to the topic, retrying on
// failure. An optional explicit partition can be provided.
func (p *Producer) PublishJSON(topic string, msg *message.Message, partition ...int) error {
	data, err := json.Marshal(msg)
	if err != nil {
		p.monitor.IncError()
		return err
	}

	kmsg := kafka.Message{
		Topic: topic,
		Value: data,
	}
	if msg.Key != "" {
		kmsg.Key = []byte(msg.Key)
	}
	if len(partition) > 0 {
		kmsg.Partition = partition[0]
	}

	start := time.Now()
	var lastErr error
	for attempt := 0; attempt <= p.retryCount; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		lastErr = p.writer.WriteMessages(ctx, kmsg)
		cancel()
		if lastErr == nil {
			p.monitor.AddLatency(time.Since(start))
			p.monitor.IncSent()
			return nil
		}
		if attempt < p.retryCount {
			time.Sleep(p.retryDelay)
		}
	}

	p.monitor.IncError()
	return lastErr
}
