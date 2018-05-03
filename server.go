package fqueue

import "github.com/coreos/etcd/clientv3"

const (
	VERSION = 1
)

// 包含etcd
type FQueueServer struct {
	Broker *Broker
	EtcdClient *clientv3.Client
}


