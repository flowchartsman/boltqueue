package boltqueue

import (
	"sync"
	"time"
)

type atomicKey struct {
	sync.Mutex
	key int64
}

func (a *atomicKey) Get() uint64 {
	a.Lock()
	defer a.Unlock()
	t := time.Now().UnixNano()
	if t <= a.key {
		t = a.key + 1
	}
	a.key = t
	return uint64(t)
}
