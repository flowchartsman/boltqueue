package boltqueue

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestEnqueue(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueue(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	// Enqueue 50 messages
	for p := 1; p <= 5; p++ {
		for n := 1; n <= 10; n++ {
			err := testPQueue.Enqueue(NewMessage(p, fmt.Sprintf("test message %d-%d", p, n)))
			if err != nil {
				t.Error(err)
			}
		}
	}

	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Size(p)
		if err != nil {
			t.Error(err)
		}
		if s != 10 {
			t.Errorf("Expected queue size 10 for priority %d. Got: %d", p, s)
		}
	}
}

func TestDequeue(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueue(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	//Put them in in reverse priority order
	for p := 5; p >= 1; p-- {
		for n := 1; n <= 10; n++ {
			err := testPQueue.Enqueue(NewMessage(p, fmt.Sprintf("test message %d-%d", p, n)))
			if err != nil {
				t.Error(err)
			}
		}
	}

	for p := 1; p <= 5; p++ {
		for n := 1; n <= 10; n++ {
			mStrComp := fmt.Sprintf("test message %d-%d", p, n)
			m, err := testPQueue.Dequeue()
			if err != nil {
				t.Error("Error dequeueing:", err)
			}
			mStr := m.ToString()
			if mStr != mStrComp {
				t.Errorf("Expected message: \"%s\" got: \"%s\"", mStrComp, mStr)
			}
			if m.Priority != p {
				t.Errorf("Expected priority: %d, got: %d", p, m.Priority)
			}
		}
	}
	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Size(p)
		if err != nil {
			t.Error(err)
		}
		if s != 0 {
			t.Errorf("Expected queue size 0 for priority %d. Got: %d", p, s)
		}
	}
}

func TestRequeue(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueue(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	for p := 1; p <= 5; p++ {
		err := testPQueue.Enqueue(NewMessage(p, fmt.Sprintf("test message %d", p)))
		if err != nil {
			t.Error(err)
		}
	}
	mp1, err := testPQueue.Dequeue()
	if err != nil {
		t.Error(err)
	}
	//Remove the priority 2 message
	_, _ = testPQueue.Dequeue()

	//Re-enqueue the priority 1 message
	err = testPQueue.Enqueue(mp1)
	if err != nil {
		t.Error(err)
	}

	//And it should be the first to emerge
	mp1, err = testPQueue.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if mp1.ToString() != "test message 1" {
		t.Errorf("Expected: \"%s\", got: \"%s\"", "test message 1", mp1.ToString())
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
