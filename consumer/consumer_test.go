package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewConsumer1(t *testing.T) {
	etcdEndpoints := []string{"127.0.0.1:2379"}
	subTopics := []string{"local-topic"}
	config := &ConsumerConfig{Id: 1, GroupName: "consumer-group-1",
		EtcdServer:   etcdEndpoints,
		SubTopics:    subTopics,
		pullStrategy: fromBegin,
		Debug:        true}
	runConsumer(t, config)
}

func TestNewConsumer2(t *testing.T) {
	etcdEndpoints := []string{"127.0.0.1:2379"}
	subTopics := []string{"local-topic"}
	config := &ConsumerConfig{Id: 2, GroupName: "consumer-group-1",
		EtcdServer:   etcdEndpoints,
		SubTopics:    subTopics,
		pullStrategy: fromBegin,
		Debug:        true}
	runConsumer(t, config)
}

func runConsumer(t *testing.T, config *ConsumerConfig)  {
	consumer, err := NewConsumer(config)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		msgBatchs, err := consumer.Pull(ctx, 10)
		if err != nil {
			t.Error(err)
			continue
		}
		for _, mb := range msgBatchs {
			for _, v := range mb.Messages {
				fmt.Printf("topic{%s} partition{%d} msg{%v}\n", mb.Topic, mb.Partition, v)
				count++
			}
		}
		if count > 0 {
			consumer.Commit()
			count = 0
		}
		time.Sleep(time.Second)
	}
}