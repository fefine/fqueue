package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	queue "fqueue"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
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
	//Subscribe                map[string]*queue.TopicPartitionLeader
	Subscribe      []*queue.PartitionInfo
	SubscribeMutex sync.Mutex
	//SubscribedPartitionCount int
	// 此consumer分配的
	//Assign                 map[string]*queue.TopicPartitionLeader
	Assign []*queue.PartitionInfo
	// 方便获取
	assignMap   map[string]*queue.PartitionInfo
	AssignMutex sync.Mutex
	//AssignedPartitionCount int
	//ConsumeOffset          map[string]*queue.TopicPartitionOffset
	// pull message策略
	pullStrategy PullStrategy
	// 所有的消费者
	consumers      []uint32
	consumersMutex sync.Mutex

	msgBatchChan   chan *MessageBatch
	errorChan      chan error
	partitionIndex int
}

type MessageBatch struct {
	Topic       string
	Partition   uint32
	StartOffset uint64
	Length      int
	Messages    []*queue.Msg
}

// pull策略
type PullStrategy uint32

// 从开始位置, offset = 0
var fromBegin PullStrategy = 0

// 从最新位置, offset = latestOffset
var fromLatest PullStrategy = 1
var generateKey = queue.GeneratorKey

type ConsumerConfig struct {
	Id         uint32
	GroupName  string
	EtcdServer []string
	SubTopics  []string
	// pull message 策略
	pullStrategy PullStrategy
	Debug        bool
}

func NewConsumer(config *ConsumerConfig) (consumer *Consumer) {

	if config == nil {
		panic(errors.New("not found consumer config"))
	}
	if config.SubTopics == nil {
		panic(errors.New("must provide subscribe topics"))
	}

	consumer = new(Consumer)
	consumer.Id = config.Id
	consumer.GroupName = config.GroupName
	consumer.EtcdEndpoints = config.EtcdServer
	consumer.SubscribeTopics = config.SubTopics
	consumer.pullStrategy = config.pullStrategy

	consumer.BrokersInfo = make(map[string]*queue.BrokerConfig)
	consumer.Brokers = make(map[string]*queue.BrokerMember)
	consumer.msgBatchChan = make(chan *MessageBatch, queue.DEFAULT_CHAN_COUNT)
	consumer.assignMap = make(map[string]*queue.PartitionInfo)
	//consumer.Subscribe = make(map[string]*queue.TopicPartitionLeader)
	//consumer.Assign = make(map[string]*queue.TopicPartitionLeader)
	//consumer.ConsumeOffset = make(map[string]*queue.TopicPartitionOffset)

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	// 1，连接etcd，获取leaseId
	consumer.EtcdClient = consumer.connectEtcd()
	consumer.createLeaseAndKeepAlive()
	// 2，获取broker，topic，leader信息，
	consumer.getBrokerInfo()
	consumer.getTopicInfo()
	// 3，注册consumer信息
	consumer.registerConsumer()
	// 4，获取consumer信息和分配Partition
	consumer.getOtherConsumerInfo()
	consumer.ReassignPartition()
	// 5，获取Assign Partition Offset
	consumer.getOffsetInfo()
	//  6，监听consumer group， 如果consumer增加或减少，重新分配,  监听broker, 如果broker宕机, 进行更换
	go consumer.watchConsumerAndBroker()
	// 7，建立消息管道, 循环获取Assign中的消息, pull之后更新本地offset
	go consumer.asyncPullMessage()
	// 8，提供二阶段提交, commit方法, commit之后更新etcd server
	// 9, (可选) SeekToBegin(), SeekTo()
	return
}

func (consumer *Consumer) Commit() {
	consumer.AssignMutex.Lock()
	defer consumer.AssignMutex.Unlock()
	log.Debug("commit offset")
	for _, pi := range consumer.Assign {
		consumer.commitOffset(pi.Topic, pi.Partition, pi.Offset)
	}
}

func (consumer *Consumer) Pull(ctx context.Context, count int) (mbs []*MessageBatch, err error) {
	recvCount := 0
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case mb := <-consumer.msgBatchChan:
			//
			mbs = append(mbs, mb)
			recvCount += mb.Length
			if recvCount >= count {
				for _, mb := range mbs {
					// 获取latestOffset并更新
					pi := consumer.assignMap[generateKey(mb.Topic, mb.Partition)]
					pi.Offset = mb.Messages[len(mb.Messages)-1].Offset + 1
					log.Debugf("topic{%s} partition{%d} newOffset{%d}", pi.Topic, pi.Partition, pi.Offset)
				}
				return
			}
		}
	}
}

func (consumer *Consumer) asyncPullMessage() {
	for {
		consumer.syncPullMessage()
	}
}

func (consumer *Consumer) syncPullMessage() error {
	//consumer.AssignMutex.Lock()
	//defer consumer.AssignMutex.Unlock()
	partitionInfo := consumer.Assign[consumer.partitionIndex%len(consumer.Assign)]
	client := consumer.Brokers[partitionInfo.Leader]
	po := make(map[uint32]uint64)
	po[partitionInfo.Partition] = partitionInfo.Offset
	topicPartitionOffset := &queue.TopicPartitionOffset{Topic: partitionInfo.Topic, PartitionOffset: po}
	req := &queue.PullReq{Count: PULL_COUNT, Timeout: 0, TpSet: topicPartitionOffset}
	log.Debugf("pull topic{%s} partition{%d} offset{%d} count{%d}",
		partitionInfo.Topic, partitionInfo.Partition, partitionInfo.Offset, PULL_COUNT)
	resp, err := client.Client.Pull(context.Background(), req)
	if err != nil {
		log.Error("sync pull message error, err: ", err)
		return err
	}
	for {
		mb, err := resp.Recv()
		if err != nil {
			resp.CloseSend()
			if err == io.EOF {
				consumer.partitionIndex++
				return nil
			}
			log.Error(err)
			consumer.partitionIndex++
			return err
		}
		if len(mb.Msgs) > 0 {
			log.Debugf("recv topic{%s} partition{%d} startOffset{%d} count{%d}",
				mb.Topic, mb.Partition, mb.StartOffset, len(mb.Msgs))
			msgs := convertMessage(mb)
			// 满了之后会阻塞
			consumer.msgBatchChan <- msgs

		}
	}
}

func (consumer *Consumer) watchConsumerAndBroker() {
	log.Debug("watch consumers and brokers")
	watchChan := consumer.EtcdClient.Watch(context.Background(), "/", clientv3.WithPrefix())
	consumerPattern, _ := regexp.Compile(fmt.Sprintf("^/consumers/%s/ids/(\\d+)$", consumer.GroupName))
	leaderParttern, _ := regexp.Compile("^/brokers/topics/([\\w\\d_-]+)/partitions/(\\d+)/leader$")
	// /consumers/[groupId]/ids/[consumerId]
	for watchResp := range watchChan {
		if watchResp.Canceled {
			log.Error(watchResp.Err())
			break
		} else {
			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					// add consumer
					if consumerPattern.Match(event.Kv.Key) {
						matcheGroups := consumerPattern.FindStringSubmatch(string(event.Kv.Key))
						consumerId, _ := strconv.ParseUint(matcheGroups[2], 10, 32)
						consumer.addConsumer(uint32(consumerId))
						consumer.ReassignPartition()
						continue
					}
					if leaderParttern.Match(event.Kv.Key) {
						matcherGroups := leaderParttern.FindStringSubmatch(string(event.Kv.Key))
						topic := matcherGroups[1]
						if !consumer.isSubscribe(topic) {
							continue
						}
						part, _ := strconv.ParseUint(matcherGroups[2], 10, 32)
						partition := uint32(part)
						if pi, ok := consumer.assignMap[generateKey(topic, partition)]; ok {
							newLeader := string(event.Kv.Value)
							pi.Leader = newLeader
							log.Infof("topic{%s} partiton{%d} new leader - %s", topic, partition, newLeader)
						}
					}
				case mvccpb.DELETE:
					// delete consumer
					// add consumer
					if consumerPattern.Match(event.Kv.Key) {
						matcheGroups := consumerPattern.FindStringSubmatch(string(event.Kv.Key))
						consumerId, _ := strconv.ParseUint(matcheGroups[2], 10, 32)
						consumer.removeConsumer(uint32(consumerId))
						consumer.ReassignPartition()
						continue
					}
				}
			}
		}
	}
}

func (consumer *Consumer) isSubscribe(topic string) bool {
	for _, t := range consumer.SubscribeTopics {
		if t == topic {
			return true
		}
	}
	return false
}

func (consumer *Consumer) ReassignPartition() {

	consumer.AssignMutex.Lock()
	defer consumer.AssignMutex.Unlock()
	log.Debug("reassign partition")
	// len == 0
	consumer.Assign = consumer.Assign[:0]
	for k := range consumer.assignMap {
		delete(consumer.assignMap, k)
	}
	// 1, 排序, 并找到当前consumer的位置
	// sort consumer
	sort.Slice(consumer.consumers, func(i, j int) bool { return consumer.consumers[i] < consumer.consumers[j] })
	// sort partition
	cs := consumer.Subscribe
	sort.Slice(consumer.Subscribe, func(i, j int) bool {
		if cs[i].Partition == cs[j].Partition {
			return cs[i].Topic < cs[j].Topic
		} else {
			return cs[i].Partition < cs[j].Partition
		}
	})

	n := len(cs) / len(consumer.consumers)
	position := sort.Search(len(consumer.consumers), func(i int) bool {
		return consumer.consumers[i] == consumer.Id
	})
	last := len(cs) % len(consumer.consumers)
	if last == 0 {
		for i := position * n; i < (position+1)*n; i++ {
			pi := consumer.Subscribe[i]
			consumer.putPartitionOwner(pi.Topic, pi.Partition)
			consumer.Assign = append(consumer.Assign, pi)
			consumer.assignMap[generateKey(pi.Topic, pi.Partition)] = pi
			log.Infof("consumer{%d} assigned topic{%s} partition{%d} offset{%d}",
				consumer.Id, pi.Topic, pi.Partition, pi.Offset)
		}
	} else {
		if position <= last {
			n += 1
			for i := position * n; i < (position+1)*n; i++ {
				pi := consumer.Subscribe[i]
				consumer.putPartitionOwner(pi.Topic, pi.Partition)
				consumer.Assign = append(consumer.Assign, pi)
				consumer.assignMap[generateKey(pi.Topic, pi.Partition)] = pi
				log.Infof("consumer{%d} assigned topic{%s} partition{%d} offset{%d}",
					consumer.Id, pi.Topic, pi.Partition, pi.Offset)
			}
		} else {
			start := len(consumer.Subscribe) - (n+1)*last - (position-last)*n
			for i := start; i < start+n; i++ {
				pi := consumer.Subscribe[i]
				consumer.putPartitionOwner(pi.Topic, pi.Partition)
				consumer.Assign = append(consumer.Assign, pi)
				consumer.assignMap[generateKey(pi.Topic, pi.Partition)] = pi
				log.Infof("consumer{%d} assigned topic{%s} partition{%d} offset{%d}",
					consumer.Id, pi.Topic, pi.Partition, pi.Offset)
			}
		}
	}
}

// remove owner
func (consumer *Consumer) putPartitionOwner(topic string, partition uint32) {
	key := fmt.Sprintf("/consumers/%s/owners/%s/%d", consumer.GroupName, topic, partition)
	_, err := consumer.EtcdClient.Put(context.Background(), key, string(consumer.Id))
	if err != nil {
		log.Errorf("put topic{%s} partition{%d} owner failed, err: %v", topic, partition, err)
	}
}

// remove owner
func (consumer *Consumer) removePartitionOwner(topic string, partition uint32) {
	key := fmt.Sprintf("/consumers/%s/owners/%s/%d", consumer.GroupName, topic, partition)
	_, err := consumer.EtcdClient.Delete(context.Background(), key)
	if err != nil {
		log.Errorf("delete topic{%s} partition{%d} owner failed, err: %v", topic, partition, err)
	}
}

func (consumer *Consumer) getOtherConsumerInfo() {
	log.Debug("getOther consumer info")
	// 获取consumer
	consumersKey := fmt.Sprintf("/consumers/%s/ids", consumer.GroupName)
	resp, err := consumer.EtcdClient.Get(context.Background(), consumersKey, clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("get other consumer error, err: %v", err)
	}
	reg, _ := regexp.Compile(fmt.Sprintf("%s/(\\d+)", consumersKey))
	for _, kvs := range resp.Kvs {
		key := string(kvs.Key)
		if matches := reg.FindStringSubmatch(key); len(matches) > 0 {
			id, _ := strconv.ParseUint(matches[1], 10, 32)
			// 会把自己也添加进去
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
	log.Debug("add new consumer: ", consumerId)
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
	log.Debug("remove consumer: ", consumerId)
}

// register consumer
func (consumer *Consumer) registerConsumer() {
	key := fmt.Sprintf("/consumers/%s/ids/%d", consumer.GroupName, consumer.Id)
	_, err := consumer.EtcdClient.Put(context.Background(),
		key, fmt.Sprintf("%d", consumer.Id), clientv3.WithLease(consumer.EtcdLeaseId))
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

func (consumer *Consumer) createLeaseAndKeepAlive() {
	// 1s
	resp, err := consumer.EtcdClient.Grant(context.Background(), queue.LEASE_TTL)
	if err != nil {
		log.Fatalf("create etcd lease error err: %v", err)
	}
	_, err = consumer.EtcdClient.KeepAlive(context.Background(), resp.ID)
	if err != nil {
		log.Fatalf("keep lease alive error, err: %v", err)
	}
	consumer.EtcdLeaseId = resp.ID
	log.Infof("create leaseID{%v} success", resp.ID)
}

func (consumer *Consumer) getBrokerInfo() {
	// 获取broker并连接
	log.Debugf("consumer{%d} scan broker", consumer.Id)
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

	client := consumer.EtcdClient
	for _, topic := range consumer.SubscribeTopics {
		topicKey := fmt.Sprintf("/brokers/topics/%s/partitions", topic)
		resp, err := client.Get(context.Background(), topicKey, clientv3.WithPrefix())
		if err != nil {
			log.Errorf("get topic{%s} partition info error, err: %v", topic, err)
			continue
		}
		if resp.Count > 0 {
			for _, kvs := range resp.Kvs {
				if ok, err := regexp.Match(fmt.Sprintf("/brokers/topics/%s/partitions/\\d+/leader", topic), kvs.Key); err == nil && ok {
					key := string(kvs.Key)
					partition, _ := strconv.Atoi(strings.Split(key, "/")[5])
					leader := string(kvs.Value)
					log.Debugf("topic{%s}, partition{%d}, leader{%s}", topic, partition, leader)
					pi := &queue.PartitionInfo{Leader: leader, Topic: topic, Partition: uint32(partition)}
					consumer.Subscribe = append(consumer.Subscribe, pi)
				}
			}
		} else {
			log.Warnf("topic {%s} not contains partition", topic)
		}
	}
}

// 初始化时获取各个分区的offset
// TODO 应该是获取Assign的
func (consumer *Consumer) getOffsetInfo() {
	client := consumer.EtcdClient
	for _, pi := range consumer.Assign {
		topicKey := fmt.Sprintf("/consumers/%s/offsets/%s/%d", consumer.GroupName, pi.Topic, pi.Partition)
		resp, err := client.Get(context.Background(), topicKey, clientv3.WithPrefix())
		if err != nil {
			log.Errorf("get partitionInfo{%v} partition info error, err: %v", pi, err)
			continue
		}
		var offset uint64
		if resp.Count > 0 {
			// 已经存在offset
			offset, _ = strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
			log.Debug("topic{%d} partition{%d} offset{%d}", pi.Topic, pi.Partition, offset)
		} else {
			// 并不存在offset
			if consumer.pullStrategy == fromBegin {
				offset = 0
			} else {
				offset = consumer.getLeastOffset(pi.Topic, pi.Partition, pi.Leader)
			}
		}
		pi.Offset = offset
		log.Debugf("topic{%v} partition{%d} offset: %d", pi.Topic, pi.Partition, pi.Offset)
	}
}

// 获取最新的offset, 需要改进
func (consumer *Consumer) getLeastOffset(topic string, partition uint32, leader string) uint64 {
	leaderClient := consumer.Brokers[leader]
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
	_, err := consumer.EtcdClient.Put(context.Background(), key, strconv.FormatUint(offset, 10))
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

func convertMessage(src *queue.MsgBatch) *MessageBatch {
	l := len(src.Msgs)
	dest := &MessageBatch{
		Topic:       src.Topic,
		Partition:   src.Partition,
		StartOffset: src.StartOffset,
		Length:      l}
	if l > 0 {
		dest.Messages = make([]*queue.Msg, l)
		for i, m := range src.Msgs {
			//log.Debugf("%d-len{%d}, cap{%d}", i, len(m), cap(m))
			dest.Messages[i] = queue.NewMessageFromSource(m)
		}
	}
	return dest

}
