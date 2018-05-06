package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	queue "fqueue"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	PULL_COUNT = 100
)

// consumer
// 包含etcd， broker的信息
// 包含consumer group，consumerID
// 订阅的topic， topic下的partition， partition的leader
// 分配的topic， topic下的partition， partition的leader
// 二阶段提交， 阻塞型
// 本地保存消费的offset， 然后根据策略刷新到etcd
type Consumer struct {
	Id              uint32
	GroupName       string
	EtcdEndpoints   []string
	EtcdClient      *clientv3.Client
	EtcdLeaseId     clientv3.LeaseID
	BrokersInfo     map[string]*queue.BrokerConfig
	brokerInfoMutex sync.Mutex
	Brokers         map[string]*queue.BrokerMember
	SubscribeTopics []string
	// 此Consumer group订阅的
	Subscribe                map[string]*queue.TopicPartitionLeader
	SubscribeMutex           sync.Mutex
	SubscribedPartitionCount int
	// 此consumer分配的
	Assign                 map[string]*queue.TopicPartitionLeader
	AssignMutex            sync.Mutex
	AssignedPartitionCount int
	ConsumeOffset          map[string]*queue.TopicPartitionOffset
	// pull message策略
	pullStrategy PullStrategy
	// 所有的消费者
	consumers      []uint32
	consumersMutex sync.Mutex
}

// pull策略
type PullStrategy uint32

// 从开始位置, offset = 0
var fromBegin PullStrategy = 0

// 从最新位置, offset = latestOffset
var fromLatest PullStrategy = 1

type ConsumerConfig struct {
	Id         uint32
	GroupName  string
	EtcdServer []string
	SubTopics  []string
	// pull message 策略
	pullStrategy PullStrategy
}

func NewConsumer(config *ConsumerConfig) (consumer *Consumer) {

	if config == nil {
		panic(errors.New("not found consumer config"))
	}
	if config.SubTopics == nil {
		panic(errors.New("must provide subscribe topics"))
	}
	consumer.Id = config.Id
	consumer.GroupName = config.GroupName
	consumer.EtcdEndpoints = config.EtcdServer
	consumer.SubscribeTopics = config.SubTopics
	consumer.pullStrategy = config.pullStrategy

	consumer.BrokersInfo = make(map[string]*queue.BrokerConfig)
	consumer.Brokers = make(map[string]*queue.BrokerMember)
	consumer.Subscribe = make(map[string]*queue.TopicPartitionLeader)
	consumer.Assign = make(map[string]*queue.TopicPartitionLeader)
	consumer.ConsumeOffset = make(map[string]*queue.TopicPartitionOffset)

	// 1，连接etcd，获取leaseId
	consumer.EtcdClient = consumer.connectEtcd()
	consumer.EtcdLeaseId = consumer.getLeaseId()
	// 2，获取broker，topic，leader信息，
	consumer.getBrokerInfo()
	consumer.getTopicInfo()
	// 3，注册consumer信息
	consumer.registerConsumer()
	// TODO 4，获取consumer信息和分配Partition
	consumer.getOtherConsumerInfo()
	consumer.ReassignPartition()
	// TODO 5，获取Assign Partition Offset
	// TODO 6，监听consumer group， 如果consumer增加或减少，重新分配
	// TODO 7，建立消息管道, 循环获取Assign中的消息, pull之后更新本地offset
	// TODO 8，提供二阶段提交, commit方法, commit之后更新etcd server
	// TODO 9, (可选) SeekToBegin(), SeekTo()
	return
}

func (consumer *Consumer) ReassignPartition() {
	// 1, 排序, 并找到当前consumer的位置
	// sort consumer
	sort.Slice(consumer.consumers, func(i, j int) bool { return i < j })
	// sort topics
	/*sort.Strings(consumer.SubscribeTopics)
	positition := -1
	for i, c := range consumer.consumers {
		if c == consumer.Id {
			positition = i
		}
	}
	n :=  / len(consumer.consumers)
	*/
}

func (consumer *Consumer) getOtherConsumerInfo() {
	// 获取consumer
	consumersKey := fmt.Sprintf("/consumers/%s/ids")
	resp, err := consumer.EtcdClient.Get(context.Background(), consumersKey, clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("get other consumer error, err: %v", err)
	}
	reg, _ := regexp.Compile(fmt.Sprintf("%s/(\\d+)", consumersKey))
	for _, kvs := range resp.Kvs {
		key := string(kvs.Key)
		if matches := reg.FindStringSubmatch(key); len(matches) > 0 {
			id, _ := strconv.ParseUint(matches[1], 10, 32)
			consumer.addConsumer(uint32(id))
		} else {
			log.Debug(key, "not match ", reg.String())
		}
	}
}

func (consumer *Consumer) addConsumer(consumerId uint32) {
	consumer.consumersMutex.Lock()
	defer consumer.consumersMutex.Unlock()
	consumer.consumers = append(consumer.consumers, uint32(consumerId))
}

func (consumer *Consumer) removeConsumer(consumerId uint32) {
	consumer.consumersMutex.Lock()
	defer consumer.consumersMutex.Unlock()
	cs := consumer.consumers
	for i, c := range cs {
		if c == consumerId {
			for j := i + 1; j < len(cs) && j > 0; j++ {
				cs[j-1] = cs[j]
			}
			break
		}
	}
	consumer.consumers = cs[:len(cs)-1]
}

// register consumer
func (consumer *Consumer) registerConsumer() {
	key := fmt.Sprintf("consumers/%s/ids/%d", consumer.GroupName, consumer.Id)
	_, err := consumer.EtcdClient.Put(context.Background(), key, "", clientv3.WithLease(consumer.EtcdLeaseId))
	if err != nil {
		log.Fatalf("register consumer{%d} error, err: %v", consumer.Id, err)
	}
	log.Infof("register consumer{%d} success", consumer.Id)
}

// 连接etcd
func (consumer *Consumer) connectEtcd() *clientv3.Client {
	client, er := clientv3.New(clientv3.Config{Endpoints: consumer.EtcdEndpoints})
	if er != nil {
		log.Fatalln("consumer connect etcd failed, err:", er)
	}
	return client
}

func (consumer *Consumer) getLeaseId() clientv3.LeaseID {
	resp, er := consumer.EtcdClient.Grant(context.Background(), queue.LEASE_TTL)
	if er != nil {
		log.Fatalln("consumer get etcd leaseId failed, err: ", er)
	}
	return resp.ID
}
func (consumer *Consumer) getBrokerInfo() {
	// 获取broker并连接
	log.Debugf("%s scan broker", consumer.Id)
	getResp, err := consumer.EtcdClient.Get(context.Background(), "/brokers/ids/", clientv3.WithPrefix())
	if err != nil {
		log.Errorf("scan broker info error, err: %v", err)
		return
	}
	if getResp.Count == 0 {
		log.Debug("etcd not contain brokers")
		return
	}
	for _, kv := range getResp.Kvs {
		log.Debugf("find broker {%s - %s}", string(kv.Key), string(kv.Value))
		key := string(kv.Key)
		brokerName := strings.Split(key, "/")[3]
		etcdBroker := new(queue.EtcdBroker)
		err := json.Unmarshal(kv.Value, etcdBroker)
		if err != nil {
			log.Error("parse etcd broker failed, err: %v", err)
			continue
		}
		consumer.AddBroker(&queue.BrokerConfig{Name: brokerName, ListenerAddress: etcdBroker.Address})
	}
}

// get topic partition leader
func (consumer *Consumer) getTopicInfo() {

	consumer.SubscribeMutex.Lock()
	defer consumer.SubscribeMutex.Unlock()

	consumer.SubscribedPartitionCount = 0
	client := consumer.EtcdClient
	for _, topic := range consumer.SubscribeTopics {
		topicKey := fmt.Sprintf("/brokers/topics/%s/partitions", topic)
		resp, err := client.Get(context.Background(), topicKey, clientv3.WithPrefix())
		if err != nil {
			log.Errorf("get topic{%s} partition info error, err: %v", topic, err)
			continue
		}
		if resp.Count > 0 {
			partitionLeader := make(map[uint32]string)
			for _, kvs := range resp.Kvs {
				if ok, err := regexp.Match(fmt.Sprintf("/brokers/topics/%s/partitions/\\d+/leader", topic), kvs.Key); err == nil && ok {
					key := string(kvs.Key)
					partition, _ := strconv.Atoi(strings.Split(key, "/")[5])
					leader := string(kvs.Value)
					partitionLeader[uint32(partition)] = leader
					log.Debugf("topic{%s}, partition{%d}, leader{%s}", topic, partition, leader)
					consumer.SubscribedPartitionCount++
				}
			}
			tpl := &queue.TopicPartitionLeader{Topic: topic, PartitionLeader: partitionLeader}
			consumer.Subscribe[topic] = tpl
		} else {
			log.Warnf("topic {%s} not contains partition", topic)
		}
	}
	log.Debug("subscribed partition count: ", consumer.SubscribedPartitionCount)
}

// 初始化时获取各个分区的offset
// TODO 应该是获取Assign的
func (consumer *Consumer) getOffsetInfo() {
	client := consumer.EtcdClient
	for _, topic := range consumer.SubscribeTopics {
		topicKey := fmt.Sprintf("/consumers/%s/offsets/%s", consumer.GroupName, topic)
		resp, err := client.Get(context.Background(), topicKey, clientv3.WithPrefix())
		if err != nil {
			log.Errorf("get topic{%s} partition info error, err: %v", topic, err)
			continue
		}
		if resp.Count > 0 {
			partitionOffset := make(map[uint32]uint64)
			for _, kvs := range resp.Kvs {
				if ok, err := regexp.Match(fmt.Sprintf("%s/\\d+", topic), kvs.Key); err == nil && ok {
					key := string(kvs.Key)
					partition, _ := strconv.Atoi(strings.Split(key, "/")[5])
					offset, _ := strconv.ParseUint(string(kvs.Value), 10, 64)
					log.Debugf("topic{%s}, partition{%d}, offset{%s}", topic, partition, offset)
				}
			}
			tpl := &queue.TopicPartitionOffset{Topic: topic, PartitionOffset: partitionOffset}
			consumer.ConsumeOffset[topic] = tpl
		} else {
			log.Infof("topic {%s} not register offset", topic)
			sub := consumer.Subscribe[topic]
			partitionOffset := make(map[uint32]uint64)
			// 根据策略来决定是从最新开始读还是从头开始
			for partition, _ := range sub.PartitionLeader {
				partitionOffset[partition] = 0
				consumer.commitOffset(topic, partition, 0)
			}
			consumer.ConsumeOffset[topic] = &queue.TopicPartitionOffset{Topic: topic, PartitionOffset: partitionOffset}
		}
	}
}

// 获取最新的offset, 需要改进
func (consumer *Consumer) getLeastOffset(topic string, partition uint32) uint64 {
	brokerName := consumer.Subscribe[topic].PartitionLeader[partition]
	leaderClient := consumer.Brokers[brokerName]
	topicPartition := &queue.TopicPartition{Topic: topic, Partition: []uint32{partition}}
	resp, err := leaderClient.Client.Subscribe(context.Background(), &queue.SubReq{Topics: []*queue.TopicPartition{topicPartition}})
	if err != nil {
		log.Errorf("get leastOffset error, err: %v", err)
		return 0
	} else {
		if len(resp.TopicPartitionOffset) > 0 {
			if offset, ok := resp.TopicPartitionOffset[0].PartitionOffset[partition]; ok {
				return offset
			}
		}
	}
	return 0
}

// 提交offset到etcd
func (consumer *Consumer) commitOffset(topic string, partition uint32, offset uint64) error {
	log.Debugf("commit topic{%s} partition{%d} offset{%d}", topic, partition, offset)
	key := fmt.Sprintf("/consumers/%s/offsets/%s/%d", consumer.GroupName, topic, partition)
	_, err := consumer.EtcdClient.Put(context.Background(), key, strconv.FormatUint(offset, 64))
	return err
}

func (consumer *Consumer) registerLeaseConsumer() {
	groupKey := fmt.Sprintf("/consumers/%s", consumer.GroupName)
	// 判断consumer group是否存在, 如果不存在则注册
	resp, err := consumer.EtcdClient.Get(context.Background(), groupKey)
	if err != nil {
		log.Error("get consumer group error, err: ", err)
		return
	}
	if resp.Count > 0 {
		// 说明已经存在, 暂时不进行内容判断
		log.Debugf("consumer group %s already registed, jump", consumer.GroupName)
	} else {
		// 不存在, 注册
		cg := &queue.EtcdConsumerGroup{
			Version: queue.VERSION,
			Topics:  consumer.SubscribeTopics}
		groupValue, err := json.Marshal(cg)
		if err != nil {
			log.Fatalf("marshal consumer group error, err: %v", err)
		}
		_, err = consumer.EtcdClient.Put(context.Background(), groupKey, string(groupValue))
		if err != nil {
			log.Fatalf("register consumer group error, err: %v", err)
		}
	}

	// 注册consumer
	consumerKey := fmt.Sprintf("/consumers/%s/ids/%d", consumer.GroupName, consumer.Id)
	_, err = consumer.EtcdClient.Put(context.Background(), consumerKey, "")
	if err != nil {
		log.Fatalf("register consumer %d error, err: %v", consumer.Id, err)
	}
}

// 增加broker, 增加brokerServiceCLient
func (consumer *Consumer) AddBroker(config *queue.BrokerConfig) {
	consumer.brokerInfoMutex.Lock()
	defer consumer.brokerInfoMutex.Unlock()
	conn, err := grpc.Dial(config.ListenerAddress, grpc.WithInsecure())
	if err != nil {
		log.Errorf("connect broker{%s} error, err: %v", config.Name, err)
		return
	}
	consumer.BrokersInfo[config.Name] = config
	brokerClient := queue.NewBrokerServiceClient(conn)
	consumer.Brokers[config.Name] = &queue.BrokerMember{Client: brokerClient, Name: config.Name,
		ListenerAddress: config.ListenerAddress}
}

// 移除已经退出的broker
func (consumer *Consumer) RemoveBroker(brokerName string) {
	consumer.brokerInfoMutex.Lock()
	defer consumer.brokerInfoMutex.Unlock()
	if _, ok := consumer.BrokersInfo[brokerName]; ok {
		log.Infof("remove broker{%s}", brokerName)
		delete(consumer.BrokersInfo, brokerName)
		delete(consumer.Brokers, brokerName)
	} else {
		log.Warnf("remove broker{%s}, but not exist")
	}
}

func (consumer *Consumer) Pull() {

}
