package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"fqueue"
	"github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc"
	"log"
	"regexp"
	"strings"
)

// command line

// create topic     cli -endpoints='' -topic -create name=xxx partition=x replica=x
// get topic info   cli -endpoints='' -topic -list [name=xxx]

func main() {
	var endpoint string
	var topic bool
	var create bool
	var listTopic bool
	var name string
	var partition uint
	var replica uint
	flag.StringVar(&endpoint, "endpoints", "127.0.0.1:2379", "etcd endpoints")
	flag.BoolVar(&topic, "topic", false, "topic operation")
	flag.BoolVar(&create, "create", false, "create topic operation")
	flag.BoolVar(&listTopic, "list", false, "list topic operation")
	flag.StringVar(&name, "name", "", "new topic name")
	flag.UintVar(&partition, "partitionCount", 0, "topic partition count")
	flag.UintVar(&replica, "replicaCount", 0, "topic partition replicate count")
	flag.Parse()

	log.Println("endpoint: \t", endpoint)
	log.Println("topic: \t", topic)
	log.Println("create: \t", create)
	log.Println("list: \t", listTopic)
	log.Println("name: \t", name)
	log.Println("partition: \t", partition)
	log.Println("replica: \t", replica)

	if topic {
		if listTopic {
			listTopics(strings.Split(endpoint, ","), name)
			return
		}
		if create {
			if name == "" {
				log.Fatalln("create topic must provide topic name.")
			}
			if partition == 0 {
				log.Fatalln("partition count must > 0")
			}
			if replica == 0 {
				log.Fatalln("replica count must > 0")
			}
			createTopic(strings.Split(endpoint, ","), name, uint32(partition), uint32(replica))
		}
	}
}

func createTopic(endpoints []string, topic string, partition, replicate uint32) {
	etcdClient := getEtcdClient(endpoints)
	brokers := getBroker(etcdClient)
	if replicate > uint32(len(brokers)) {
		log.Fatalf("replica count [%d] > brokers count[%d]\n", replicate, len(brokers))
	}
	client := connectBroker(brokers[0])
	req := &fqueue.CreateTopicReq{Topic: topic, PartitionCount: partition, ReplicaCount: replicate}
	_, err := client.CreateTopic(context.Background(), req)
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("create topic{%s} success", topic)
	log.Println()
	listTopicByKey(etcdClient, nil, fmt.Sprintf("/brokers/topics/%s", topic))
}

func listTopicByKey(etcdClient *clientv3.Client, endpoints []string, key string) {
	if etcdClient == nil {
		etcdClient = getEtcdClient(endpoints)
	}
	getResp, err := etcdClient.Get(context.Background(), key, clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("scan topic info error, err: %v", err)
		return
	}
	if getResp.Count == 0 {
		log.Fatal("not found topics")
		return
	}
	for _, kv := range getResp.Kvs {
		key := string(kv.Key)
		if ok, err := regexp.Match("^/brokers/topics/[\\w\\d-_]+$", kv.Key); err == nil && ok {
			// topic
			topic := new(fqueue.EtcdTopic)
			err := json.Unmarshal(kv.Value, topic)
			if err != nil {
				continue
			}
			tName := strings.Split(key, "/")[3]
			log.Printf("topic: %s partition: %d version: %d\n", tName, len(topic.Partitions), topic.Version)
			for part, bs := range topic.Partitions {
				log.Printf("\t partition: %d brokers: %v\n", part, bs)
			}
		}
	}
}

func listTopics(endpoints []string, topic string) {
	if topic == "" {
		listTopicByKey(nil, endpoints, fmt.Sprintf("/brokers/topics/"))
	} else {
		listTopicByKey(nil, endpoints, fmt.Sprintf("/brokers/topics/%s", topic))
	}

}

func getEtcdClient(endpoints []string) *clientv3.Client {
	client, er := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if er != nil {
		log.Fatal(er)
	}
	return client
}

// get a broker
func getBroker(client *clientv3.Client) []string {
	resp, err := client.Get(context.Background(), "/brokers/ids/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalln(err)
	}
	if resp.Count == 0 {
		log.Fatal("not found broker")
	}
	brokers := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		etcdBroker := new(fqueue.EtcdBroker)
		err := json.Unmarshal(kv.Value, etcdBroker)
		if err != nil {
			log.Fatalf("parse etcd broker failed, err: %v", err)
		}
		brokers = append(brokers, etcdBroker.Address)
	}
	return brokers
}

func connectBroker(address string) fqueue.BrokerServiceClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connect to %s failed, err: %v", address, err)
	}
	return fqueue.NewBrokerServiceClient(conn)
}
