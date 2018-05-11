package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewConsumer(t *testing.T) {
	etcdEndpoints := []string{"127.0.0.1:2379"}
	subTopics := []string{"local-topic"}
	config := &ConsumerConfig{Id: 1, GroupName: "consumer-group-1",
		EtcdServer:   etcdEndpoints,
		SubTopics:    subTopics,
		pullStrategy: fromBegin,
		Debug:        true}
	consumer, err := NewConsumer(config)
	if err != nil {
		t.Fatal(err)
	}
	for {
		msgBatchs, err := consumer.Pull(context.Background(), 100)
		if err != nil {
			t.Error(err)
			continue
		}
		for _, mb := range msgBatchs {
			for _, v := range mb.Messages {
				fmt.Printf("topic{%s} partition{%d} msg{%v}\n", mb.Topic, mb.Partition, v)
			}
		}
		consumer.Commit()
		time.Sleep(time.Second)
	}
}
