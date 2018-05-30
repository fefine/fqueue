package producer

import (
	"context"
	"fmt"
	queue "github.com/fefine/fqueue"
	"testing"
	"time"
	"bytes"
)

func TestNewProducer(t *testing.T) {
	config := &ProducerConfig{EtcdEndpoints: []string{"127.0.0.1:2379"}, Debug: true, BatchCount: 20}
	producer, err := NewProducer(config)
	if err != nil {
		t.Fatal(err)
	}
	count := 300

	// not provide partition
	kvs := createKV(count, 100)
	for i := 0; i < count; i++ {
		producer.Push(context.Background(), "local-topic", []byte(kvs[i<<1]), []byte(kvs[i<<1+1]),
			func(msg *SendMsg, resp *queue.Resp, e error) {
				fmt.Printf("topic: %s partition: %d key: %s value: %s\n", msg.topic, msg.partition, string(msg.key), string(msg.value))
			})
		//producer.Push(context.Background(), "local-topic", []byte(kvs[i * 2]), []byte(kvs[i * 2 + 1]))
		time.Sleep(time.Millisecond * 100)
	}
	producer.Flush()
	//producer.Push()
	time.Sleep(2)
}

func createKV(count, size int) []string {
	res := make([]string, count*2)
	var buffer bytes.Buffer
	for i := 0; i < count; i++ {
		res[i*2] = fmt.Sprintf("this is key - %d", i)
		for j := 0; j < size; j++ {
			buffer.WriteString(string('a' + (j % 26)))
		}
		res[i*2+1] = buffer.String()
		buffer.Reset()
	}
	return res
}
