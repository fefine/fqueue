package fqueue

import (
	"testing"
	"fmt"
	"golang.org/x/exp/mmap"
	"math/rand"
)

func TestNewFilePartition(t *testing.T) {
	part, err := NewPartition()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(part)

	WriteMsg(t, part, 100)
	t.Log("--------------------------------------------------")
	msgs := ReadMsg(t, part, 100)
	for _, e := range msgs {
		t.Log(e)
	}

	t.Log(part)
	err = part.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func NewPartition() (*FilePartition, error) {
	return NewFilePartition(DefaultPartitionConfig(0, "temp-topic"))
}

func WriteMsg(t *testing.T, part *FilePartition, count int) {
	for i := 0; i < count; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("this is a key - %d", i)), []byte(fmt.Sprintf("this is the value - %d", i)))
		err := part.WriteMsg(msg)
		t.Log(msg)
		NoError(err, t)
	}
}

func WriteMsgWithSize(b *testing.B, part *FilePartition, count, size int) {
	for i := 0; i < count; i++ {
		value := make([]byte, size)
		for j := 0; j < size; j++ {
			value[j] = '0' + byte(rand.Int() % 10)
		}
		msg := NewMessage([]byte(fmt.Sprintf("this is a key - %d", i)), value)
		err := part.WriteMsg(msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func ReadMsg(t *testing.T, part *FilePartition, count int) (msgs []*Msg) {
	msgs = make([]*Msg, 0, count)
	for i := 0; i < count; i++ {
		source, err := part.ReadMsg(uint64(i))
		NoError(err, t)
		msgs = append(msgs, NewMessageFromSource(source))
	}
	return
}
// 仅测试读取，不解析
func ReadMsgWithBen(b *testing.B, part *FilePartition, count int) (msgs []*Msg) {
	msgs = make([]*Msg, 0, count)
	for i := 0; i < count; i++ {
		_, err := part.ReadMsg(uint64(i))
		if err != nil {
			b.Fatal(err)
		}
	}
	return
}

func NoError(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkFilePartition_WriteMsg(b *testing.B) {
	part, err := NewPartition()
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		WriteMsgWithSize(b, part, 1, 10000)
	}
	b.StopTimer()
	part.Close()
	// count  ns/op  size    count
	// 100000 13985   100       1
	// 30000  39643   1000      1  --
	// 5000   307326  10000     1  --
	// 500    2996304 100000    1
}

func BenchmarkFilePartition_ReadMsg(b *testing.B) {
	part, err := NewPartition()
	if err != nil {
		b.Fatal(err)
	}
	WriteMsgWithSize(b, part, 5000, 1000)
	b.StartTimer()
	for i := 0; i < b.N && i < 5000; i++ {
		source, err := part.ReadMsg(uint64(i))
		if err != nil {
			b.Fatal(err)
		}
		_ = NewMessageFromSource(source)
	}
	b.StopTimer()
	part.Close()
}

func BenchmarkFilePartition_ReadMsg2(b *testing.B) {
	part, err := NewPartition()
	if err != nil {
		b.Fatal(err)
	}
	WriteMsgWithSize(b, part, 5000, 1000)
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		offset := rand.Uint64() % 5000
		for pb.Next() {
			_, err := part.ReadMsg(offset)
			if err != nil {
				b.Fatal(err)
			}
		}

	})
	b.StopTimer()
	part.Close()
}

func TestMMP(t *testing.T) {
	readat, err := mmap.Open("G:\\test\\fqueue.msg")
	if err != nil {
		t.Fatal(err)
	}
	source := make([]byte, 100)
	_, err = readat.ReadAt(source, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(source)
	msg := NewMessageFromSource(source)
	t.Log(msg)
	readat.Close()
}