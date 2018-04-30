package fqueue

import (
	"testing"
	"fmt"
	"math/rand"
	"io"
)


func DefaultPartitionConfig(id uint32, topic string) *PartitionConfig {
	return &PartitionConfig{
		Id: id,
		Topic: topic,
		DataPath: HomePath() + "/fqueue",
	}
}

func NewPartition() (*FilePartition, error) {
	return NewFilePartition(DefaultPartitionConfig(rand.Uint32(), "temp-topic"))
}

func NoError(t *testing.T, err error) {
	if err != nil {
		if err == io.EOF {
			t.Log("no more data")
			return
		}
		t.Fatal(err)
	}
}

func BuildMultiMsg(t *testing.T, size, count int) (msgs []*Msg) {
	msgs = make([]*Msg, 0, count)
	for i := 0; i < count; i++ {
		value := make([]byte, size)
		for j := 0; j < size; j++ {
			value[j] = '0' + byte(rand.Int() % 10)
		}
		msg := NewMessage([]byte(fmt.Sprintf("this is a key - %d", i)), value)
		msgs = append(msgs, msg)
	}
	return
}

func WriteMsg(t *testing.T, part *FilePartition, count, size int) {
	msgs := BuildMultiMsg(t, count, size)
	for _, m := range msgs {
		NoError(t, part.WriteMsg(m))
	}
}

func WriteMultiMsg(t *testing.T, part *FilePartition, count, size int) {
	msgs := BuildMultiMsg(t, count, size)
	NoError(t, part.WriteMultiMsg(msgs))
}

func ReadMsg(t *testing.T, part *FilePartition, start, count int) {
	for i := 0; i < count; i++ {
		source, err := part.ReadMsg(uint64(start + i))
		NoError(t, err)
		if err != io.EOF {
			t.Logf("[read] %v", NewMessageFromSource(source))
		} else {
			break
		}
	}
}

func ReadMultiMsg(t *testing.T, part *FilePartition, start, size int) {
	sources, err := part.ReadMultiMsg(uint64(start), uint32(size))
	NoError(t, err)
	if err != io.EOF {
		for _, m := range sources {
			t.Logf("[read] %v", NewMessageFromSource(m))
		}
	}
}

func TestNewFilePartition(t *testing.T) {
	part, err := NewPartition()
	NoError(t, err)
	ReadMsg(t, part, 0, 1)
	ReadMultiMsg(t, part, 0, 1)

	WriteMsg(t, part, 100, 1000)
	WriteMultiMsg(t, part, 100, 1000)

	ReadMsg(t, part, 0, 100)
	ReadMsg(t, part, 100, 100)
	ReadMultiMsg(t, part, 0, 200)
}

func TestMultiPartition(t *testing.T)  {
	signalChan := make(chan int, 5)
	for i := 0; i < 5; i++ {
		go func() {
			TestNewFilePartition(t)
			signalChan <- 1
		}()
	}
	for range "00000"{
		<- signalChan
	}
}