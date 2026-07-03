// Package gokafka provides producer and consumer helpers over segmentio/kafka-go.
package gokafka

// Partition strategy names accepted by Producer.SetPartitionStrategy.
const (
	PartitionRoundRobin = "round_robin"
	PartitionHash       = "hash"
	PartitionLeastBytes = "least_bytes"
)
