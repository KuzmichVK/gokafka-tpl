// Package config loads the application configuration from YAML.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

// Config is the root configuration.
type Config struct {
	Kafka KafkaConfig
}

// KafkaConfig holds Kafka-related settings.
type KafkaConfig struct {
	Brokers            []string
	Topics             []string
	GroupID            string
	AutoCommitInterval time.Duration
	RetryCount         int
	RetryDelay         time.Duration
	PartitionStrategy  string
}

// rawConfig mirrors the YAML layout; durations are parsed from strings
// because yaml.v2 does not decode "5s" into time.Duration directly.
type rawConfig struct {
	Kafka struct {
		Brokers            []string `yaml:"brokers"`
		Topics             []string `yaml:"topics"`
		GroupID            string   `yaml:"group_id"`
		AutoCommitInterval string   `yaml:"auto_commit_interval"`
		RetryCount         int      `yaml:"retry_count"`
		RetryDelay         string   `yaml:"retry_delay"`
		PartitionStrategy  string   `yaml:"partition_strategy"`
	} `yaml:"kafka"`
}

func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// LoadConfig reads and parses the YAML configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	aci, err := parseDuration(raw.Kafka.AutoCommitInterval)
	if err != nil {
		return nil, fmt.Errorf("auto_commit_interval: %w", err)
	}
	rd, err := parseDuration(raw.Kafka.RetryDelay)
	if err != nil {
		return nil, fmt.Errorf("retry_delay: %w", err)
	}
	return &Config{Kafka: KafkaConfig{
		Brokers:            raw.Kafka.Brokers,
		Topics:             raw.Kafka.Topics,
		GroupID:            raw.Kafka.GroupID,
		AutoCommitInterval: aci,
		RetryCount:         raw.Kafka.RetryCount,
		RetryDelay:         rd,
		PartitionStrategy:  raw.Kafka.PartitionStrategy,
	}}, nil
}
