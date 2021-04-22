package boltqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/McSwitch/bolt"
)

// TODO: Interfacification of messages

var foundItem = errors.New("item found")

// aKey singleton for assigning keys to messages
var aKey = new(atomicKey)

// PQueue is a priority queue backed by a Bolt database on disk
type PQueue struct {
	conn *bolt.DB
}

// Options represents the options that can be set when opening a database.
type Options struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	Timeout time.Duration

	// Sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync bool

	// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly bool

	// Sets the DB.MmapFlags flag before memory mapping the file.
	MmapFlags int

	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <=0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	InitialMmapSize int
}

// NewPQueue loads or creates a new PQueue with the given filename
func NewPQueue(filename string, options *Options) (*PQueue, error) {
	// Options below represent the default options used if nil options are passed into NewPQueue().
	// No timeout is used which will cause Bolt to wait indefinitely for a lock.
	var boltOptions *bolt.Options = &bolt.Options{
		Timeout:    0,
		NoGrowSync: false,
	}
	if options != nil {
		boltOptions.Timeout = options.Timeout
		boltOptions.NoGrowSync = options.NoGrowSync
		boltOptions.ReadOnly = options.ReadOnly
		boltOptions.MmapFlags = options.MmapFlags
		boltOptions.InitialMmapSize = options.InitialMmapSize
	}
	db, err := bolt.Open(filename, 0600, boltOptions)
	if err != nil {
		return nil, err
	}
	return &PQueue{db}, nil
}

func (b *PQueue) enqueueMessage(priority int, key []byte, message *Message) error {
	if priority < 0 || priority > 255 {
		return fmt.Errorf("Invalid priority %d on Enqueue", priority)
	}
	p := make([]byte, 1)
	p[0] = byte(uint8(priority))
	return b.conn.Update(func(tx *bolt.Tx) error {
		// Get bucket for this priority level
		pb, err := tx.CreateBucketIfNotExists(p)
		if err != nil {
			return err
		}
		err = pb.Put(key, message.value)
		if err != nil {
			return err
		}
		return nil
	})
}

// Enqueue adds a message to the queue
func (b *PQueue) Enqueue(priority int, message *Message) error {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, aKey.Get())
	return b.enqueueMessage(priority, k, message)
}

// Requeue adds a message back into the queue, keeping its precedence.
// If added at the same priority, it should be among the first to dequeue.
// If added at a different priority, it will dequeue before newer messages
// of that priority.
func (b *PQueue) Requeue(priority int, message *Message) error {
	if message.key == nil {
		return fmt.Errorf("Cannot requeue new message")
	}
	return b.enqueueMessage(priority, message.key, message)
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
			priority, _ := binary.Uvarint(bname)
			m = &Message{priority: int(priority), key: cloneBytes(k), value: cloneBytes(v)}

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
		return 0, fmt.Errorf("Invalid priority %d for Size()", priority)
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

// Close closes the queue and releases all resources
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
