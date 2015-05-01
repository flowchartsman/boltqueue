package boltqueue

// Message represents a message in the priority queue
type Message struct {
	key      []byte
	value    []byte
	priority int
}

// NewMessage generates a new priority queue message with a priority range of
// 0-255
func NewMessage(value string) *Message {
	return &Message{nil, []byte(value), -1}
}

// Priority returns the priority the message had in the queue in the range of
// 0-255 or -1 if the message is new.
func (m *Message) Priority() int {
	return m.priority
}

// ToString outputs the string representation of the message's value
func (m *Message) ToString() string {
	return string(m.value)
}
