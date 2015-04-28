# boltqueue [![GoDoc](https://godoc.org/github.com/alaska/boltqueue?status.svg)](https://godoc.org/github.com/alaska/boltqueue)
A persistent queue based on boltdb
--
    import "github.com/alaska/boltqueue"

Note: at the moment, the only queue is a priority queue. Adding the regular one shortly

## Usage

#### type Message

```go
type Message struct {
}
```

Message represents a message in the priority queue

#### func  NewMessage

```go
func NewMessage(priority int, value string) *Message
```
NewMessage generates a new priority queue message with a priority range of 1-5

#### func (*Message) ToString

```go
func (m *Message) ToString() string
```
ToString outputs the string representation of the message's value

#### type PQueue

```go
type PQueue struct {
}
```

PQueue is a priority queue backed by a Bolt database on disk

#### func  NewPQueue

```go
func NewPQueue(filename string) (*PQueue, error)
```
NewPQueue loads or creates a new PQueue with the given filename

#### func (*PQueue) Dequeue

```go
func (b *PQueue) Dequeue() (*Message, error)
```
Dequeue removes the oldest, highest priority message from the queue and returns
it

#### func (*PQueue) Enqueue

```go
func (b *PQueue) Enqueue(m *Message) error
```
Enqueue adds a message to the queue at its appropriate priority level

#### func (*PQueue) Size

```go
func (b *PQueue) Size(priority int) (int, error)
```
Size returns the number of entries of a given priority from 1 to 5
