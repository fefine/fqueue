package fqueue

import (
	"testing"
	"github.com/coreos/etcd/clientv3"
	"context"
)

func TestConnect(t *testing.T) {
	client := getClient(t)
	Put(t, client, "/brokers/topics/demo-topic-1",
		`{"version": 1, "partitions": {"0": ["0"], "1": ["0"], "2": ["0"]}}`)
	Put(t, client, "/brokers/topics/demo-topic-2",
		`{"version": 1, "partitions": {"0": ["0"], "1": ["0"], "2": ["0"]}}`)
	Put(t, client, "/brokers/topics/demo-topic-3",
		`{"version": 1, "partitions": {"0": ["0"], "1": ["0"], "2": ["0"]}}`)
	Get(t, client, "/brokers/topics")
}
func getClient(t *testing.T) *clientv3.Client {
	endpoints := []string{"192.168.1.121:2379"}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:endpoints,
	})
	NoError(t, err)
	return cli
}

func Put(t *testing.T, client *clientv3.Client, key, value string) {
	_, err := client.Put(context.TODO(), key, value)
	NoError(t, err)
}

func Get(t *testing.T, client *clientv3.Client, key string) {
	getResp, err := client.Get(context.TODO(), key, clientv3.WithPrefix())
	NoError(t, err)
	if getResp.Count == 0 {
		t.Errorf("Not found key: %s", key)
	} else {
		for _, v := range getResp.Kvs {
			t.Logf("key: %s; value: %s", string(v.Key), string(v.Value))
		}
	}
}
