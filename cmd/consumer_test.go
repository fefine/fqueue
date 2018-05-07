package main

import (
	"context"
	"testing"
)

func TestNewConsumer(t *testing.T) {
	etcdEndpoints := []string{"192.168.1.84:2379"}
	subTopics := []string{"demo-topic-multi"}
	config := &ConsumerConfig{Id: 1, GroupName: "consumer-group-1",
		EtcdServer:   etcdEndpoints,
		SubTopics:    subTopics,
		pullStrategy: fromBegin,
		Debug:        true}
	consumer := NewConsumer(config)
	msgBatchs, err := consumer.Pull(context.Background(), 100)
	if err != nil {
		t.Error(err)
	}
	for _, mb := range msgBatchs {
		//for _, m := range mb.Messages {
		//
		//}
		t.Logf("topic{%s} partition{%d} msg{%v}", mb.Topic, mb.Partition, mb.Messages[mb.Length-1])
	}
	consumer.Commit()
}
