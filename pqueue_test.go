package boltqueue

import (
	"fmt"
	"os"
	"testing"
	"time"
)

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
