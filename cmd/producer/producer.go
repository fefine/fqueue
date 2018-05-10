package producer

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
	"regexp"
	"strconv"
	"strings"
)

type Partitioner interface {
	Partition(topic string, key, value []byte, leader *queue.TopicPartitionLeader) uint32
}

type DefaultPartitioner struct {
}

func (part DefaultPartitioner) Partition(topic string, key, value []byte, leader *queue.TopicPartitionLeader) uint32 {
	if len(key) > 0 {
		return queue.MurmurHash2(key) % uint32(len(leader.PartitionLeader))
	}
	return 0
}

type Callback func(*sendMsg, *queue.Resp, error)

// producer
type Producer struct {
	EtcdEndpoints []string
	etcdClient    *clientv3.Client
	Brokers       map[string]*queue.BrokerMember
	Topics        map[string]*queue.TopicPartitionLeader
	Partitioner   func(string, []byte, []byte, *queue.TopicPartitionLeader) uint32
	msgChan       chan *sendMsg
	BatchCount    uint32
	bufferedMsgs  map[string][]*sendMsg
}

type ProducerConfig struct {
	EtcdEndpoints []string
	Partitioner   Partitioner
	Debug         bool
	BatchCount    uint32
}

type sendMsg struct {
	key       []byte
	value     []byte
	topic     string
	partition uint32
	callback  Callback
}

var generatorKey = queue.GeneratorKey

func NewProducer(config *ProducerConfig) (producer *Producer, err error) {
	if config == nil {
		panic("not found configure")
	}

	// TODO test, will be delete
	if config.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	producer = new(Producer)
	producer.EtcdEndpoints = config.EtcdEndpoints
	producer.BatchCount = config.BatchCount
	producer.Brokers = make(map[string]*queue.BrokerMember)
	producer.Topics = make(map[string]*queue.TopicPartitionLeader)
	producer.msgChan = make(chan *sendMsg, queue.DEFAULT_CHAN_COUNT)
	producer.bufferedMsgs = make(map[string][]*sendMsg)

	if config.Partitioner != nil {
		producer.Partitioner = config.Partitioner.Partition
	} else {
		producer.Partitioner = DefaultPartitioner{}.Partition
	}

	// TODO 1, connect etcd
	producer.etcdClient, err = producer.connectEtcd()
	if err != nil {
		return
	}
	// TODO 2, get brokers
	producer.getBrokerInfo()
	// TODO 3, watch partition leaders
	go producer.watchLeaderAndBroker()

	go producer.bufferedMsg()

	log.Info("consumer start")
	return
}

// 暂存, 批量发送
func (producer *Producer) bufferedMsg() {
	var bufferedCount uint32 = 0
	for {
		select {
		case sm := <-producer.msgChan:
			topic := sm.topic
			partition := sm.partition
			if sms, ok := producer.bufferedMsgs[generatorKey(topic, partition)]; ok {
				sms = append(sms, sm)
				producer.bufferedMsgs[generatorKey(topic, partition)] = sms
			} else {
				sms = make([]*sendMsg, 0, producer.BatchCount)
				sms = append(sms, sm)
				producer.bufferedMsgs[generatorKey(topic, partition)] = sms
			}
			bufferedCount++
		}
		if bufferedCount >= producer.BatchCount {
			producer.Flush()
			bufferedCount = 0
		}
	}
}

func (producer *Producer) Flush() {
	for key, sms := range producer.bufferedMsgs {
		//delete(producer.bufferedMsgs, key)
		if len(sms) == 0 {
			continue
		}
		resp, err := producer.sendReq(sms)
		// len() == 0
		producer.bufferedMsgs[key] = producer.bufferedMsgs[key][:0]
		for _, sm := range sms {
			if sm.callback != nil {
				sm.callback(sm, resp, err)
			}
		}
	}
}

func (producer *Producer) sendReq(sms []*sendMsg) (*queue.Resp, error) {
	mb := new(queue.MsgBatch)
	mb.Topic = sms[0].topic
	mb.Partition = sms[0].partition
	mb.Msgs = make([][]byte, len(sms))
	for i, sm := range sms {
		mb.Msgs[i] = queue.NewMessage(sm.key, sm.value).Source
		log.Debugf("send: %s", string(sm.key))
	}
	tpl, _ := producer.topicInfo(mb.Topic)
	leader := tpl.PartitionLeader[mb.Partition]
	client, err := producer.brokerClient(leader)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	pushClient, err := client.Push(context.Background())
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Debugf("send broker{%s}: topic: %s partition: %d len: %d", leader, mb.Topic, mb.Partition, len(mb.Msgs))
	pushClient.Send(mb)
	resp, err := pushClient.CloseAndRecv()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Debugf("resp: %v", resp.Status)
	return resp, err
}

func (producer *Producer) Push(ctx context.Context, topic string, key []byte, value []byte, callback Callback) error {
	tpl, err := producer.topicInfo(topic)
	if err != nil {
		log.Error(err)
		return err
	}
	if key == nil || value == nil || len(key) == 0 || len(value) == 0 {
		log.Error("key and value must not empty")
		return errors.New("key and value must not empty")
	}
	partition := producer.Partitioner(topic, key, value, tpl)
	return producer.PushWithPartition(ctx, topic, partition, key, value, callback)
}

func (producer *Producer) PushWithPartition(ctx context.Context,
	topic string, partition uint32, key []byte, value []byte, callback Callback) error {

	if key == nil || value == nil || len(key) == 0 || len(value) == 0 {
		log.Error("key and value must not empty")
		return errors.New("key and value must not empty")
	}
	tpl, err := producer.topicInfo(topic)
	if err != nil {
		return err
	}
	if partition >= uint32(len(tpl.PartitionLeader)) {
		return errors.New(fmt.Sprintf("%d large than max partition number", partition))
	}

	if err != nil {
		return err
	}

	sm := &sendMsg{topic: topic, partition: partition, key: key, value: value, callback: callback}
	select {
	case producer.msgChan <- sm:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (producer *Producer) connectEtcd() (*clientv3.Client, error) {
	client, er := clientv3.New(clientv3.Config{Endpoints: producer.EtcdEndpoints})
	if er != nil {
		log.Error("consumer connect etcd failed, err:", er)
		return nil, er
	}
	return client, nil
}

func (producer *Producer) brokerClient(brokerNmae string) (queue.BrokerServiceClient, error) {
	if broker, ok := producer.Brokers[brokerNmae]; ok {
		return producer.getBrokerClient(broker)
	} else {
		return nil, errors.New(fmt.Sprintf("not found broker{%s}", brokerNmae))
	}
}

func (producer *Producer) topicInfo(topic string) (*queue.TopicPartitionLeader, error) {
	if tpl, ok := producer.Topics[topic]; ok {
		return tpl, nil
	} else {
		if err := producer.appendTopicInfo(topic); err == nil {
			return producer.topicInfo(topic)
		} else {
			return nil, err
		}
	}
}

func (producer *Producer) getBrokerInfo() {
	// 获取broker并连接
	log.Debugf("producer scan broker")
	getResp, err := producer.etcdClient.Get(context.Background(), "/brokers/ids/", clientv3.WithPrefix())
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
		broker := &queue.BrokerMember{Name: brokerName, ListenerAddress: etcdBroker.Address}
		producer.Brokers[brokerName] = broker
		log.Debugf("add new broker{%v}", broker)
	}
}

func (producer *Producer) appendTopicInfo(name string) error {
	topicKey := fmt.Sprintf("/brokers/topics/%s/partitions", name)
	resp, err := producer.etcdClient.Get(context.Background(), topicKey, clientv3.WithPrefix())
	if err != nil {
		log.Errorf("get topic{%s} partition info error, err: %v", name, err)
		return err
	}
	if resp.Count > 0 {
		partitionMap := make(map[uint32]string)
		for _, kvs := range resp.Kvs {
			if ok, err := regexp.Match(fmt.Sprintf("/brokers/topics/%s/partitions/\\d+/leader", name), kvs.Key); err == nil && ok {
				key := string(kvs.Key)
				partition, _ := strconv.Atoi(strings.Split(key, "/")[5])
				leader := string(kvs.Value)
				log.Debugf("topic{%s}, partition{%d}, leader{%s}", name, partition, leader)
				//pi := &queue.PartitionInfo{Leader: leader, Topic: name, Partition: uint32(partition)}
				partitionMap[uint32(partition)] = leader
			}
		}
		tpl := &queue.TopicPartitionLeader{Topic: name, PartitionLeader: partitionMap}
		producer.Topics[name] = tpl
		return nil
	} else {
		log.Warnf("topic {%s} not contains partition", name)
		return errors.New(fmt.Sprintf("topic {%s} not contains partition", name))
	}
}

func (producer *Producer) getBrokerClient(broker *queue.BrokerMember) (queue.BrokerServiceClient, error) {
	if broker.Client == nil {
		conn, err := grpc.Dial(broker.ListenerAddress, grpc.WithInsecure())
		if err != nil {
			log.Errorf("connect broker{%s} error, err: %v", broker.Name, err)
			return nil, err
		}
		return queue.NewBrokerServiceClient(conn), nil
	}
	return broker.Client, nil
}

func (producer *Producer) watchLeaderAndBroker() {
	log.Debug("watch consumers and brokers")
	watchChan := producer.etcdClient.Watch(context.Background(), "/", clientv3.WithPrefix())
	brokerPattern, _ := regexp.Compile("^/brokers/ids/([\\w\\d_-]+)$")
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
					if brokerPattern.Match(event.Kv.Key) {
						matcheGroups := brokerPattern.FindStringSubmatch(string(event.Kv.Key))
						brokerName := matcheGroups[1]
						etcdBroker := new(queue.EtcdBroker)
						err := json.Unmarshal(event.Kv.Value, etcdBroker)
						if err != nil {
							log.Error("parse etcd broker failed, err: %v", err)
							continue
						}
						broker := &queue.BrokerMember{Name: brokerName, ListenerAddress: etcdBroker.Address}
						producer.Brokers[brokerName] = broker
						log.Debugf("producer add new broker{%v}", broker)
						continue
					}
					if leaderParttern.Match(event.Kv.Key) {
						matcherGroups := leaderParttern.FindStringSubmatch(string(event.Kv.Key))
						topic := matcherGroups[1]
						part, _ := strconv.ParseUint(matcherGroups[2], 10, 32)
						// 仅修改已经存在的topic
						if tpl, ok := producer.Topics[topic]; ok {
							tpl.PartitionLeader[uint32(part)] = string(event.Kv.Value)
							log.Debugf("topic{%s} partition{%d} new broker{%s}", topic, part, string(event.Kv.Value))
							continue
						}
					}
				case mvccpb.DELETE:
					// delete broker
					if brokerPattern.Match(event.Kv.Key) {
						matcheGroups := brokerPattern.FindStringSubmatch(string(event.Kv.Key))
						brokerName := matcheGroups[1]
						if _, ok := producer.Brokers[brokerName]; ok {
							delete(producer.Brokers, brokerName)
						}
						log.Debugf("producer remove new broker{%v}", brokerName)
						continue
					}
				}
			}
		}
	}
}
