package boltqueue

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

var (
	levelName = map[int][]byte{
		1: []byte("1"),
		2: []byte("2"),
		3: []byte("3"),
		4: []byte("4"),
		5: []byte("5"),
	}
)

// TODO: Interfacification of messages

// Message represents a message in the priority queue
type Message struct {
	key      int64
	value    []byte
	priority int
}

// NewMessage generates a new priority queue message with a priority range of
// 1-5
func NewMessage(priority int, value string) *Message {
	return &Message{getKey(), []byte(value), priority}
}

// ToString outputs the string representation of the message's value
func (m *Message) ToString() string {
	return string(m.value)
}

// PQueue is a priority queue backed by a Bolt database on disk
type PQueue struct {
	conn *bolt.DB
}

// NewPQueue loads or creates a new PQueue with the given filename
func NewPQueue(filename string) (*PQueue, error) {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, err
	}

	// Ensure the priority buckets exist before using the queue
	err = db.Update(func(tx *bolt.Tx) error {
		for i := 1; i <= 5; i++ {
			_, err = tx.CreateBucketIfNotExists(levelName[int(i)])
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &PQueue{db}, nil
}

// Enqueue adds a message to the queue at its appropriate priority level
func (b *PQueue) Enqueue(m *Message) error {
	if _, ok := levelName[m.priority]; !ok {
		return errors.New("invalid priority")
	}

	return b.conn.Update(func(tx *bolt.Tx) error {
		dbucket := tx.Bucket(levelName[m.priority])
		if dbucket == nil {
			return errors.New("data bucket does not exist")
		}

		// Push
		kb := make([]byte, 8)
		binary.BigEndian.PutUint64(kb, uint64(m.key))
		if err := dbucket.Put(kb, m.value); err != nil {
			return err
		}

		return nil
	})
}

// Dequeue removes the oldest, highest priority message from the queue and
// returns it
func (b *PQueue) Dequeue() (*Message, error) {
	var m *Message
	err := b.conn.Update(func(tx *bolt.Tx) error {

		for i := 1; i <= 5; i++ {
			dbucket := tx.Bucket(levelName[int(i)])
			if dbucket == nil {
				return errors.New("data bucket does not exist")
			}
			// Skip empty queues
			if dbucket.Stats().KeyN == 0 {
				continue
			}
			cur := dbucket.Cursor()
			k, v := cur.First()
			ki, _ := binary.Varint(k)

			m = &Message{ki, cloneBytes(v), i}
			if err := cur.Delete(); err != nil {
				return err
			}
			break
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Size returns the number of entries of a given priority from 1 to 5
func (b *PQueue) Size(priority int) (int, error) {
	if _, ok := levelName[priority]; !ok {
		return 0, errors.New("invalid priority")
	}

	var size int
	err := b.conn.View(func(tx *bolt.Tx) error {
		dbucket := tx.Bucket(levelName[priority])
		if dbucket == nil {
			return errors.New("data bucket does not exist")
		}
		size = dbucket.Stats().KeyN
		return nil
	})
	if err != nil {
		return 0, err
	}

	return size, nil
}

var lastTime int64

// Inspired by DavidHui/httpq/boltqueue
func getKey() int64 {
	t := time.Now().UnixNano()
	if t <= lastTime {
		t = lastTime + 1
	}
	lastTime = t
	return t
}

// taken from boltDB. Avoids corruption when re-queueing
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
