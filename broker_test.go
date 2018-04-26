package fqueue

import (
	"fmt"
	"testing"
	"google.golang.org/grpc"
	"context"
	"time"
)

func DefaultBrokerConfig(id int, port int) *BrokerConfig {
	return &BrokerConfig{
		Name: fmt.Sprintf("broker-%d", id),
		ListenerAddress: fmt.Sprintf("127.0.0.1:%d", port),
		//EtcdEndPoints:nil,
		DataPath:fmt.Sprintf("%s/broker-%d", HomePath(), id)}
}


func TestNewBrokerAndStart(t *testing.T) {
	port := 8090
	config := DefaultBrokerConfig(1, port)
	broker, err := NewBrokerAndStart(config)

	if err != nil {
		t.Fatal(err)
	}
	client := GetBrokerServiceClient(t, port)
	topic := "demo-topic"
	pCount := 10
	CreateTopic(t, broker, client, topic, pCount)

	Push(t, client, topic, pCount)

	time.Sleep(10 * time.Second)
}

func GetBrokerServiceClient(t *testing.T, port int) BrokerServiceClient {
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	return NewBrokerServiceClient(conn)
}

func CreateTopic(t *testing.T, broker *Broker, client BrokerServiceClient, topic string, pCount int) {
	req := &CreateTopicReq{
		Topic: topic,
		PartitionCount: uint32(pCount),
		ReplicaCount:1}
	resp, err := client.CreateTopic(context.TODO(), req)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("resp: %v", resp)
}

func Push(t *testing.T, client BrokerServiceClient, topic string, pCount int) {
	pushClient, err := client.Push(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		msgs := BuildMultiByteMsg(t, 100, 100)
		pushClient.Send(&MsgBatch{Topic:topic, Partition:uint32(i % pCount), Msgs:msgs})
	}
	resp, err := pushClient.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("resp: %v", resp)
}