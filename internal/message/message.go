// Package message defines the message model exchanged through Kafka.
package message

// Message is a JSON message: an optional Key and a Value payload.
type Message struct {
	Key   string                 `json:"key,omitempty"`
	Value map[string]interface{} `json:"value"`
}

// NewMessage builds a Message from a value map.
func NewMessage(value map[string]any) *Message {
	return &Message{Value: value}
}
