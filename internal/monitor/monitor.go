// Package monitor collects producer/consumer statistics.
package monitor

import (
	"sync"
	"time"
)

// Monitor tracks counters and latency in a thread-safe way.
type Monitor struct {
	mu           sync.Mutex
	sent         int64
	received     int64
	errors       int64
	totalLatency time.Duration
	maxLatency   time.Duration
	latencyCount int64
}

// NewMonitor creates a new Monitor.
func NewMonitor() *Monitor {
	return &Monitor{}
}

// IncSent increments the sent counter.
func (m *Monitor) IncSent() {
	m.mu.Lock()
	m.sent++
	m.mu.Unlock()
}

// IncReceived increments the received counter.
func (m *Monitor) IncReceived() {
	m.mu.Lock()
	m.received++
	m.mu.Unlock()
}

// IncError increments the error counter.
func (m *Monitor) IncError() {
	m.mu.Lock()
	m.errors++
	m.mu.Unlock()
}

// AddLatency records a latency sample and updates the maximum.
func (m *Monitor) AddLatency(latency time.Duration) {
	m.mu.Lock()
	m.totalLatency += latency
	m.latencyCount++
	if latency > m.maxLatency {
		m.maxLatency = latency
	}
	m.mu.Unlock()
}

// Stats returns a snapshot of the collected statistics.
func (m *Monitor) Stats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	var avg time.Duration
	if m.latencyCount > 0 {
		avg = m.totalLatency / time.Duration(m.latencyCount)
	}
	return map[string]interface{}{
		"sent":        m.sent,
		"received":    m.received,
		"errors":      m.errors,
		"avg_latency": avg,
		"max_latency": m.maxLatency,
		"total_msgs":  m.sent + m.received,
	}
}
