package producer

import (
	"context"
	"fmt"
	"fqueue"
	"math/rand"
	"testing"
	"time"
)

func TestNewProducer(t *testing.T) {
	config := &ProducerConfig{EtcdEndpoints: []string{"127.0.0.1:2379"}, Debug: true, BatchCount: 20}
	producer, err := NewProducer(config)
	if err != nil {
		t.Fatal(err)
	}
	count := 3

	// not provide partition
	kvs := createKV(count, 100)
	for i := 0; i < count; i++ {
		producer.Push(context.Background(), "local-topic", []byte(kvs[i*2]), []byte(kvs[i*2+1]), func(msg *sendMsg, resp *fqueue.Resp, e error) {
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
	for i := 0; i < count; i++ {
		res[i*2] = fmt.Sprintf("this is key - %d", i)
		value := make([]byte, size)
		for j := 0; j < size; j++ {
			value[j] = byte(rand.Int())
		}
		res[i*2+1] = string(value)
	}
	return res
}
