package fqueue

import (
	"fmt"
	"testing"
	"google.golang.org/grpc"
	"context"
	"time"
	"io"
)

func DefaultBrokerConfig(id int, port int) *BrokerConfig {
	return &BrokerConfig{
		Name: fmt.Sprintf("broker-%d", id),
		ListenerAddress: fmt.Sprintf("127.0.0.1:%d", port),
		//EtcdEndPoints:nil,
		DataPath:fmt.Sprintf("%s/broker-%d", HomePath(), id),
		Debug: true}
}

// 单broker
func TestSingleBrokerAndStart(t *testing.T) {
	port := 8090
	config := DefaultBrokerConfig(1, port)
	broker, err := NewBrokerAndStart(config)
	NoError(t, err)
	client := GetBrokerServiceClient(t, port)
	topic := "demo-topic"
	pCount := 10
	CreateTopic(t, broker, client, topic, pCount, 1)

	//Push(t, client, topic, pCount)
	//time.Sleep(3 * time.Second)
	Pull(t, client, topic, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	time.Sleep(3 * time.Second)
	broker.Close()
}

// 多broker
func TestMultiBrokerAndStart(t *testing.T) {
	port := 8090
	broker1, err := NewBrokerAndStart(DefaultBrokerConfig(1, port))
	NoError(t, err)

	broker2, err := NewBrokerAndStart(DefaultBrokerConfig(2, port + 1))
	NoError(t, err)

	broker3, err := NewBrokerAndStart(DefaultBrokerConfig(3, port + 2))
	NoError(t, err)

	broker1.AddBrokerMember(DefaultBrokerConfig(2, port + 1))
	broker1.AddBrokerMember(DefaultBrokerConfig(3, port + 2))

	client := GetBrokerServiceClient(t, port)
	topic := "demo-topic-multi"
	pCount := 10

	CreateTopic(t, broker1, client, topic, pCount, 2)
	time.Sleep(1 * time.Second)

	Push(t, client, topic, pCount)
	time.Sleep(1 * time.Second)

	Pull(t, client, topic, []uint32{0, 3, 6, 9})


	broker1.Close()
	broker2.Close()
	broker3.Close()
}

func GetBrokerServiceClient(t *testing.T, port int) BrokerServiceClient {
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithInsecure())
	NoError(t, err)
	return NewBrokerServiceClient(conn)
}

func CreateTopic(t *testing.T, broker *Broker, client BrokerServiceClient, topic string, pCount , re int) {
	t.Logf("create topic: %s, part: %d, replica: %d", topic, pCount, re)
	req := &CreateTopicReq{
		Topic: topic,
		PartitionCount: uint32(pCount),
		ReplicaCount:uint32(re)}
	resp, err := client.CreateTopic(context.TODO(), req)
	NoError(t, err)
	t.Logf("resp: %v", resp.Status)
}

func Push(t *testing.T, client BrokerServiceClient, topic string, pCount int) {
	t.Logf("push to %s count: %d", topic, pCount)
	pushClient, err := client.Push(context.TODO())
	NoError(t, err)
	sources := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		msgs := BuildMultiMsg(t, 100, 100)
		for i, m := range msgs {
			sources[i] = m.Source
		}
		NoError(t, pushClient.Send(&MsgBatch{Topic:topic, Partition:uint32(i % pCount), Msgs:sources}))
	}
	resp, err := pushClient.CloseAndRecv()
	NoError(t, err)
	t.Logf("resp: %v", resp.Status)
}

func Pull(t *testing.T, client BrokerServiceClient, topic string, partitions []uint32) {
	t.Logf("pull msg from %s %v", topic, partitions)
	po := make(map[uint32]uint64)
	for _, v := range partitions {
		po[v] = 0
	}
	tpSet := &TopicPartitionOffset{
		Topic: topic,
		PartitionOffset: po}
	req := &PullReq{
		TpSet: tpSet,
		Count: 100}

	resp, err := client.Pull(context.TODO(), req)
	NoError(t, err)
	for {
		msgBatch, err := resp.Recv()
		if err != nil {
			if err == io.EOF {
				resp.CloseSend()
				return
			}
			NoError(t, err)
		}
		for _, source := range msgBatch.Msgs {
			t.Logf("pull: topic: %s, partition: %d, msg: %v",
				msgBatch.Topic, msgBatch.Partition, NewMessageFromSource(source))
		}
	}
}