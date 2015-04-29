package boltqueue

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

var (
	cBucket = []byte("c")
	dBucket = []byte("d")
)

// TODO: Interfacification of messages

// Message represents a message in the priority queue
type NMessage struct {
	key   []byte
	value []byte
}

var NlastTime int64 //keep track of the last UnixNano in case there's somehow a dup

// NewMessage generates a new priority queue message with a priority range of
// 0-255
func NewNMessage(priority int, value string) (*NMessage, error) {
	if priority < 0 || priority > 255 {
		return nil, errors.New("Invalid priority")
	}
	k := make([]byte, 9)
	k[0] = byte(priority)
	t := time.Now().UnixNano()
	if t <= NlastTime {
		t = NlastTime + 1
	}
	lastTime = t
	binary.BigEndian.PutUint64(k[1:], uint64(t))
	return &NMessage{k, []byte(value)}, nil
}

// ToString outputs the string representation of the message's value
func (m *NMessage) ToString() string {
	return string(m.value)
}

// PQueue is a priority queue backed by a Bolt database on disk
type NPQueue struct {
	conn *bolt.DB
}

// NewPQueue loads or creates a new PQueue with the given filename
func NewNPQueue(filename string) (*NPQueue, error) {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, err
	}

	// Ensure the queue and count buckets exist
	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(dBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(cBucket)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &NPQueue{db}, nil
}

// Enqueue adds a message to the queue
func (b *NPQueue) Enqueue(m *NMessage) error {
	return b.conn.Update(func(tx *bolt.Tx) error {
		// Increment count for current priority
		countb := tx.Bucket(cBucket).Get(m.key[0:1])
		if countb == nil {
			countb = make([]byte, 8)
			binary.BigEndian.PutUint64(countb, 1)
		} else {
			countb = cloneBytes(countb)
			binary.BigEndian.PutUint64(countb, binary.BigEndian.Uint64(countb)+1)
		}
		err := tx.Bucket(cBucket).Put(m.key[0:1], countb)
		if err != nil {
			return err
		}

		// Add the message
		err = tx.Bucket(dBucket).Put(m.key, m.value)
		if err != nil {
			return err
		}
		return nil
	})
}

// Dequeue removes the oldest, highest priority message from the queue and
// returns it
func (b *NPQueue) Dequeue() (*NMessage, error) {
	var m *NMessage
	err := b.conn.Update(func(tx *bolt.Tx) error {
		// Get the first message from the data bucket
		cur := tx.Bucket(dBucket).Cursor()
		k, v := cur.First()
		if k == nil { // Empty
			m = nil
			return nil
		}
		m = &NMessage{cloneBytes(k), cloneBytes(v)}

		// Remove message
		if err := cur.Delete(); err != nil {
			return err
		}

		// Decrement the count for this priority
		countb := cloneBytes(tx.Bucket(cBucket).Get(m.key[0:1]))
		if binary.BigEndian.Uint64(countb) == 0 {
			panic("queue underflow")
		}
		binary.BigEndian.PutUint64(countb, binary.BigEndian.Uint64(countb)-1)
		err := tx.Bucket(cBucket).Put(m.key[0:1], countb)
		if err != nil {
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
func (b *NPQueue) Size(priority int) (uint64, error) {
	if priority < 0 || priority > 255 {
		return 0, errors.New("Invalid priority")
	}
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	pk := []byte{byte(uint8(priority))}
	countb := tx.Bucket(dBucket).Get(pk)
	tx.Commit()
	if countb == nil {
		return 0, nil
	}
	return binary.BigEndian.Uint64(countb), nil
}

func (b *NPQueue) Close() error {
	err := b.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// taken from boltDB. Avoids corruption when re-queueing
//func cloneBytes(v []byte) []byte {
//	var clone = make([]byte, len(v))
//	copy(clone, v)
//	return clone
//}
