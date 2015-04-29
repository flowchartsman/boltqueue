package boltqueue

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func BenchmarkUPQueue(b *testing.B) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	queue, err := NewUPQueue(queueFile)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		for p := 1; p <= 6; p++ {
			m, _ := NewUMessage(p, fmt.Sprintf("test message %d-%d", p, n))
			queue.Enqueue(m)
		}
	}

	for n := 0; n < b.N; n++ {
		for p := 1; p <= 6; p++ {
			_, err := queue.Dequeue()
			if err != nil {
				b.Error(err)
			}
		}
	}
	err = queue.Close()
	os.Remove(queueFile)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkPQueue(b *testing.B) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	queue, err := NewPQueue(queueFile)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		for p := 1; p <= 6; p++ {
			queue.Enqueue(NewMessage(p, fmt.Sprintf("test message %d-%d", p, n)))
		}
	}

	for n := 0; n < b.N; n++ {
		for p := 1; p <= 6; p++ {
			_, err := queue.Dequeue()
			if err != nil {
				b.Error(err)
			}
		}
	}
	err = queue.Close()
	os.Remove(queueFile)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkNPQueue(b *testing.B) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	queue, err := NewNPQueue(queueFile)
	if err != nil {
		b.Error(err)
	}
	for n := 0; n < b.N; n++ {
		for p := 1; p <= 6; p++ {
			m, _ := NewNMessage(p, fmt.Sprintf("test message %d-%d", p, n))
			queue.Enqueue(m)
		}
	}

	for n := 0; n < b.N; n++ {
		for p := 1; p <= 6; p++ {
			_, err := queue.Dequeue()
			if err != nil {
				b.Error(err)
			}
		}
	}
	err = queue.Close()
	os.Remove(queueFile)
	if err != nil {
		b.Error(err)
	}
}
