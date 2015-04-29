package boltqueue

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

// TODO: Interfacification of messages

// Message represents a message in the priority queue
type Message struct {
	priority []byte
	key      []byte
	value    []byte
}

var foundItem = errors.New("item found")
var UlastTime int64 //keep track of the last UnixNano in case there's somehow a dup

// NewMessage generates a new priority queue message with a priority range of
// 0-255
func NewMessage(priority int, value string) (*Message, error) {
	if priority < 0 || priority > 255 {
		return nil, errors.New("Invalid priority")
	}
	p := make([]byte, 1)
	p[0] = byte(priority)
	t := time.Now().UnixNano()
	if t <= UlastTime {
		t = UlastTime + 1
	}
	lastTime = t
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(t))
	return &Message{p, k, []byte(value)}, nil
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
	return &PQueue{db}, nil
}

// Enqueue adds a message to the queue
func (b *PQueue) Enqueue(m *Message) error {
	return b.conn.Update(func(tx *bolt.Tx) error {
		// Get bucket for this priority level
		pb, err := tx.CreateBucketIfNotExists(m.priority)
		// Add the message
		err = pb.Put(m.key, m.value)
		if err != nil {
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
		err := tx.ForEach(func(bname []byte, bucket *bolt.Bucket) error {
			if bucket.Stats().KeyN == 0 { //empty bucket
				return nil
			}
			cur := bucket.Cursor()
			k, v := cur.First() //Should not be empty by definition
			m = &Message{cloneBytes(bname), cloneBytes(k), cloneBytes(v)}

			// Remove message
			if err := cur.Delete(); err != nil {
				return err
			}
			return foundItem //to stop the iteration
		})
		if err != nil && err != foundItem {
			return err
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
	if priority < 0 || priority > 255 {
		return 0, errors.New("Invalid priority")
	}
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	bucket := tx.Bucket([]byte{byte(uint8(priority))})
	if bucket == nil {
		return 0, nil
	}
	count := bucket.Stats().KeyN
	tx.Rollback()
	return count, nil
}

func (b *PQueue) Close() error {
	err := b.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// taken from boltDB. Avoids corruption when re-queueing
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
