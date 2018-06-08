package fqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAX_SEND_COUNT     = 1000
	DEFAULT_CHAN_COUNT = 10
	VERSION            = 0x01
	DefaultTimeout            = 6 * time.Second
)

type BrokerConfig struct {
	Name string
	// rpc地址
	ListenerAddress string
	// etcd地址
	EtcdEndPoints []string
	// 存放数据的地址
	DataPath string

	Debug bool
}

// broke需要和etcd进行结合
type Broker struct {
	Name string
	// rpc地址
	ListenerAddress string
	// etcd地址
	EtcdEndPoints []string
	// etcdClient
	etcdClient  *clientv3.Client
	etcdLeaseId clientv3.LeaseID
	// 存放数据的地址
	DataPath string
	// rpc
	RpcServer *grpc.Server
	//
	Topics map[string]*FileTopic
	// append chan, 收到别的broker发来的append
	AppendMsgChan chan *MsgBatch
	// error chan, 发生错误
	ErrorChan chan error
	//
	cancelFuncs []context.CancelFunc
	// goroutine msg chan
	partitionMsgChan map[string]chan *MsgBatch
	// 其他的broker, 加锁
	brokerClients      map[string]*BrokerMember
	brokerClientsMutex sync.Mutex
	// 每个partition关联的broker, brokers用[]string表示, 加锁
	// 换成brokerName方便查询
	partitionBrokers      map[string][]string
	partitionBrokersMutex sync.Mutex
	// 以此broker为partitionLeader的分区, 怎么做到内存可见, 加锁
	leaderPartitions      map[string]bool
	leaderPartitionsMutex sync.Mutex
	// partition - leader, 记录所有partition的leader, 方便client获取
	topicPartitionLeader map[string]string
	// 此broker包含的topic-partitions
	topicPartitions []TopicPartition
}

type BrokerMember struct {
	Name            string
	ListenerAddress string
	Client          BrokerServiceClient
}

type PartitionInfo struct {
	Partition    uint32
	Leader       string
	Brokers      []string
	brokersMutex sync.Mutex
	Topic        string
	Offset       uint64
}

func (pi *PartitionInfo) addBroker(broker string) {
	pi.brokersMutex.Lock()
	defer pi.brokersMutex.Unlock()
	pi.Brokers = append(pi.Brokers, broker)
}

func (pi *PartitionInfo) removeBroker(brokerName string) {
	pi.brokersMutex.Lock()
	defer pi.brokersMutex.Unlock()
	for i, c := range pi.Brokers {
		if c == brokerName {
			for j := i + 1; j < len(pi.Brokers) && j > 0; j++ {
				pi.Brokers[j-1] = pi.Brokers[j]
			}
			break
		}
	}
	pi.Brokers = pi.Brokers[:len(pi.Brokers)-1]
}

func NewBrokerAndStart(config *BrokerConfig) (broker *Broker, err error) {
	if config == nil {
		panic(errors.New("not found configuration"))
	}
	// 初始化工作
	broker = new(Broker)
	broker.Name = config.Name
	broker.DataPath = config.DataPath
	broker.EtcdEndPoints = config.EtcdEndPoints
	broker.DataPath = config.DataPath
	broker.ListenerAddress = config.ListenerAddress
	broker.AppendMsgChan = make(chan *MsgBatch, DEFAULT_CHAN_COUNT)
	broker.Topics = make(map[string]*FileTopic)
	broker.brokerClients = make(map[string]*BrokerMember)
	broker.partitionMsgChan = make(map[string]chan *MsgBatch)
	broker.partitionBrokers = make(map[string][]string)
	broker.ErrorChan = make(chan error, DEFAULT_CHAN_COUNT)
	broker.cancelFuncs = make([]context.CancelFunc, 10)
	broker.leaderPartitions = make(map[string]bool)
	broker.topicPartitionLeader = make(map[string]string)
	broker.topicPartitions = make([]TopicPartition, 10)

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	// 1, start rpc server
	rpcLis, err := net.Listen("tcp", broker.ListenerAddress)
	if err != nil {
		log.Fatal("broker server listener rpc error")
	}
	// 这里可以用加密, 暂不提供
	grpcServer := grpc.NewServer()
	RegisterBrokerServiceServer(grpcServer, NewBrokerServerServer(broker, broker.AppendMsgChan))
	log.Info("rpc server start!")

	go grpcServer.Serve(rpcLis)

	appendCtx, ctxFunc := context.WithCancel(context.Background())
	broker.cancelFuncs = append(broker.cancelFuncs, ctxFunc)

	go broker.appendMsgService(appendCtx)
	// 2, connect etcd, recovery broker info
	// 2.1 connect etcd
	broker.etcdClient, err = clientv3.New(clientv3.Config{Endpoints: broker.EtcdEndPoints})
	if err != nil {
		log.Fatalf("broker{%s} connect to etcd{%v} failed, error: %v", broker.Name, broker.EtcdEndPoints, err)
		return
	}
	log.Info("etcd server connect success")
	//
	// 判断是否重名, 重名
	broker.checkBrokerExist()
	broker.createLeaseAndKeepAlive()
	// 2.2 get topic info, and cteate local topic
	// 连接etcd, 获取到topic信息,
	// 获得当前broker包含的partition和topic, 然后新建topic, FileTopic检测到当前文件路径下包含的partition会自动恢复;
	// 检测leader, 检测到此broker领导的partition, 会去注册leader(lease), 然后更新本地的leader partition
	// broker启动的时候, 并不会检测它所有的partition是否有leader, 仅仅检测属于它领导的partition,
	// 因此当一些broker启动失败的时候, 属于它领导的partition会无人领导, 而同样包含相同partition的不会去检测并领导它,
	// 为了解决这个问题, 当客户端订阅topic的时候, 需要接收订阅请求的broker去确定这个topic的所有partition是否已经有leader
	// 如果没有, 则会由这个leader去领导此topic与此broker交集的partition, 差集的partition则先创建一个临时的leader, 然后删除,
	// 剩下包含此partition的就会自动去尝试领导
	broker.scanAndCreateTopic()
	// 2.3 register lease broker
	// 注册当前broker到etcd(lease)上, 连接另外的broker
	broker.registerBroker()
	broker.scanAndConnectOtherBroker()
	// 2.5 watch
	go broker.watchEtcd()
	log.Infof("broker {%s} start!", broker.Name)
	return
}

func (broker *Broker) checkBrokerExist() {
	ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
	//defer fun()
	resp, err := broker.etcdClient.Get(ctx, fmt.Sprintf("/brokers/ids/%s", broker.Name))
	if err != nil {
		log.Error(err)
		return
	}
	if resp.Count > 0 {
		// 说明存在
		log.Fatalf("broker{%s} already exist", broker.Name)
	}
}

// 扫描并且连接其他broker
func (broker *Broker) scanAndConnectOtherBroker() {
	log.Debugf("%s scan broker", broker.Name)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	getResp, err := broker.etcdClient.Get(ctx, "/brokers/ids/", clientv3.WithPrefix())
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
		if brokerName == broker.Name {
			log.Debugf("broker{%s} find self, jump", key)
			continue
		}
		etcdBroker := new(EtcdBroker)
		err := json.Unmarshal(kv.Value, etcdBroker)
		if err != nil {
			log.Error("parse etcd broker failed, err: %v", err)
			continue
		}
		broker.AddBrokerMember(&BrokerConfig{Name: brokerName, ListenerAddress: etcdBroker.Address})
	}
}

// 监听etcd的活动
func (broker *Broker) watchEtcd() {
	// watch brokers
	//watchChan := broker.etcdClient.Watch(context.Background(), "/brokers", clientv3.WithPrefix(), clientv3.WithPrevKV())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchChan := broker.etcdClient.Watch(ctx, "/brokers", clientv3.WithPrefix())
	for watchResp := range watchChan {
		if watchResp.Canceled {
			log.Error(watchResp.Err())
			break
		} else {
			for _, event := range watchResp.Events {
				//preKV := event.PrevKv
				key := string(event.Kv.Key)
				log.Debugf("Watch event: %v", event)
				switch event.Type {
				case mvccpb.PUT:
					if ok, err := regexp.Match("/brokers/ids/", event.Kv.Key); err == nil && ok {
						// 新broker
						brokerName := strings.Split(key, "/")[3]
						etcdBroker := new(EtcdBroker)
						err := json.Unmarshal(event.Kv.Value, etcdBroker)
						if err != nil {
							log.Error("parse etcd broker failed, err: %v", err)
							continue
						}
						broker.AddBrokerMember(&BrokerConfig{Name: brokerName, ListenerAddress: etcdBroker.Address})
					}
				case mvccpb.DELETE:
					if ok, err := regexp.Match("/brokers/topics/[\\w\\d_-]+/partitions/\\d+/leader", event.Kv.Key); err == nil && ok {
						// partition leader删除的时候
						params := strings.Split(key, "/")
						topic := params[3]
						partition, _ := strconv.Atoi(params[5])
						broker.tryToBecomePartitionLeader(TopicPartition{Topic: topic, Partition: []uint32{uint32(partition)}})
					} else if ok, err := regexp.Match("/brokers/ids/[\\w\\d_-]+", event.Kv.Key); err == nil && ok {
						// broker 删除
						broker.checkoutOrCreateLeader()
						brokerName := strings.Split(key, "/")[3]
						// 移除broker client
						broker.RemoveBroker(&BrokerConfig{Name: brokerName, ListenerAddress: string(event.Kv.Value)})
					}
				}
			}
		}
	}
}

// 检查broker所领导的partition是否已经注册了leader
// 不去考虑其它broker的partition
// 暂时不用
func (broker *Broker) checkoutOrCreateLeader() {
	for _, tp := range broker.topicPartitions {
		for _, p := range tp.Partition {
			leader := fmt.Sprintf("brokers/topics/%s/partitions/%d/leader", tp.Topic, p)
			ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
			resp, err := broker.etcdClient.Get(ctx, leader)
			if err != nil {
				log.Errorf("get topic %s, part: %d leader failed, err: %v", tp.Topic, p, err)
				//broker.tryToBecomePartitionLeader(TopicPartition{Topic: tp.Topic, Partition: []uint32{p}})
				continue
			}
			if resp.Count == 0 {
				log.Errorf("get topic %s, part: %d leader failed, broker %s tye to become the leader",
					tp.Topic, p, broker.Name)
				// TODO 首先需要判断本broker是否包含这个分区
				// 判断是否领导这个分区
				if !broker.isLeaderPartition(tp.Topic, p) {
					log.Warnf("broker{%s} not lead topic{%s} partition{%d}", broker.Name, tp.Topic, p)
					continue
				}
				broker.tryToBecomePartitionLeader(TopicPartition{Topic: tp.Topic, Partition: []uint32{p}})
			}
		}
	}
}

// 给partition注册leader, 需要改state中的leader
func (broker *Broker) tryToBecomePartitionLeader(tp TopicPartition) {
	for _, p := range tp.Partition {
		key := fmt.Sprintf("/brokers/topics/%s/partitions/%d/leader", tp.Topic, p)
		value := broker.Name
		kvc := clientv3.NewKV(broker.etcdClient)
		ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
		tr, err := kvc.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, value, clientv3.WithLease(broker.etcdLeaseId))).
			Commit()
		if err != nil {
			log.Errorf("update topic %s part: %d; leader error, err: %v", tp.Topic, p, err)
			continue
		}
		if tr.Succeeded {
			log.Debugf("update leader success, topic: %s, part: %d", tp.Topic, p)
			broker.becomePartitionLeader(TopicPartition{Topic: tp.Topic, Partition: []uint32{p}})
			// 更新state
			key = fmt.Sprintf("/brokers/topics/%s/partitions/%d/state", tp.Topic, p)
			ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
			_, err := broker.etcdClient.Put(ctx, key, value)
			if err != nil {
				log.Errorf("update leader state error, err: %v", err)
				continue
			}
		}
	}

}

// 扫描所有topic的信息: broker包含的所有topic, broker领导的所有partition
func (broker *Broker) scanAndCreateTopic() {
	ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
	getResp, err := broker.etcdClient.Get(ctx, "/brokers/topics/", clientv3.WithPrefix())
	if err != nil {
		log.Errorf("scan topic info error, err: %v", err)
		return
	}
	if getResp.Count == 0 {
		log.Info("not found topics")
		return
	}
	for _, kv := range getResp.Kvs {
		key := string(kv.Key)
		log.Debugf("scan topic get etcd key: %s value: %v", key, string(kv.Value))
		if ok, err := regexp.Match("^/brokers/topics/[\\w\\d-_]+$", kv.Key); err == nil && ok {
			// topic
			topic := new(EtcdTopic)
			err := json.Unmarshal(kv.Value, topic)
			if err != nil {
				log.Error("unmarshal topic value error, err: %v", err)
				continue
			}
			tName := strings.Split(key, "/")[3]
			createParts := make([]uint32, 0, len(topic.Partitions))
			// 忽略version
			// 找到此broker保存的topic-partitions
			for part, bs := range topic.Partitions {
				log.Debugf("{%s} - topic: %v part: %d brokers: %v", broker.Name, tName, part, bs)
				broker.UpdatePartitionBrokers(TopicPartition{Topic: tName, Partition: []uint32{part}}, bs, true)
				for _, b := range bs {
					if b == broker.Name {
						createParts = append(createParts, part)
						break
					}
				}
			}
			if len(createParts) > 0 {
				broker.recoveryTopic(TopicPartition{Topic: tName, Partition: createParts}, []uint32{})
			}
		} else if ok, err := regexp.Match("^/brokers/topics/[\\w\\d_-]+/partitions/\\d+/state$", kv.Key); err == nil && ok {
			// topic state
			splitResult := strings.Split(key, "/")
			topic := splitResult[3]
			partition, _ := strconv.Atoi(splitResult[5])
			leader := string(kv.Value)
			if leader == broker.Name {
				// 本broker的分区, 方法里面会调用
				broker.tryToBecomePartitionLeader(TopicPartition{Topic: topic, Partition: []uint32{uint32(partition)}})
			} else {
				broker.topicPartitionLeader[GeneratorKey(topic, uint32(partition))] = leader
			}
		} else {
			log.Debugf("jump etcd key: %s value: %v", key, string(kv.Value))
		}
	}
}

// 扫描某一个topic
func (broker *Broker) scanTopic(topicName string) {
	ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
	getResp, err := broker.etcdClient.Get(ctx, fmt.Sprintf("/brokers/topics/%s", topicName))
	if err != nil {
		log.Errorf("scan topic info error, err: %v", err)
		return
	}
	if getResp.Count == 0 {
		log.Info("not found topic ", topicName)
		return
	}
	for _, kv := range getResp.Kvs {
		key := string(kv.Key)
		log.Debugf("scan topic get etcd key: %s value: %v", key, string(kv.Value))
		// topic
		topic := new(EtcdTopic)
		err := json.Unmarshal(kv.Value, topic)
		if err != nil {
			log.Error("unmarshal topic value error, err: %v", err)
			continue
		}
		// 找到此broker保存的topic-partitions
		for part, bs := range topic.Partitions {
			log.Debugf("{%s} - topic: %s part: %d brokers: %v", broker.Name, topic, part, bs)
			broker.UpdatePartitionBrokers(TopicPartition{Topic: topicName, Partition: []uint32{part}}, bs, true)
		}

	}
}

func (broker *Broker) checkTopic(topic string) error {
	key := fmt.Sprintf("/brokers/topics/%s", topic)
	ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
	resp, err := broker.etcdClient.Get(ctx, key)
	if err != nil {
		return err
	}
	if resp.Count > 0 {
		return errors.New("duplicate topic name")
	}
	return nil
}

// 创建leaseID
func (broker *Broker) createLeaseAndKeepAlive() {
	ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)

	// 1s
	resp, err := broker.etcdClient.Grant(ctx, LEASE_TTL)
	if err != nil {
		log.Fatalf("create etcd lease error err: %v", err)
	}
	_, err = broker.etcdClient.KeepAlive(context.Background(), resp.ID)
	if err != nil {
		log.Fatalf("keep lease alive error, err: %v", err)
	}
	broker.etcdLeaseId = resp.ID
	log.Infof("create leaseID{%v} success", resp.ID)
}

// 创建broker, 使用lease进行创建
func (broker *Broker) registerBroker() {
	// create broker to etcd
	etcdBroker := EtcdBroker{Version: VERSION, Address: broker.ListenerAddress}
	key := fmt.Sprintf(BROKER_FORMATER, broker.Name)
	value, err := json.Marshal(etcdBroker)
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)

	_, err = broker.etcdClient.Put(ctx, key, string(value), clientv3.WithLease(broker.etcdLeaseId))
	if err != nil {
		log.Fatal(err)
	}
	log.Info("register broker to etcd success")
}

// 写入消息的服务
func (broker *Broker) appendMsgService(ctx context.Context) {
	for {
		select {
		case msgBatch := <-broker.AppendMsgChan:
			broker.dispatcherMsg(msgBatch)
		case <-ctx.Done():
			log.Info("append msg server stop")
			return
		}
	}
}

// 将消息分配到合适的partition进行append
func (broker *Broker) dispatcherMsg(batch *MsgBatch) {
	topic := batch.Topic
	partition := batch.Partition
	key := fmt.Sprintf("%s-%d", topic, partition)
	if ch, ok := broker.partitionMsgChan[key]; ok {
		// 已经存在goroutine
		ch <- batch
	} else {
		// 并不存在
		msgChan := make(chan *MsgBatch, DEFAULT_CHAN_COUNT)
		ctx, cancelFunc := context.WithCancel(context.Background())
		broker.partitionMsgChan[key] = msgChan
		broker.cancelFuncs = append(broker.cancelFuncs, cancelFunc)
		msgChan <- batch
		go broker.appendMsg(msgChan, ctx)
	}
}

// 写入消息
func (broker *Broker) appendMsg(msgChan chan *MsgBatch, ctx context.Context) {
	for {
		select {
		case batch := <-msgChan:
			topic := batch.Topic
			partition := batch.Partition
			log.Debugf("topic: %s part: %d recv: %d", topic, partition, len(batch.Msgs))
			// TODO 当append offset > current offset 时如何处理?
			err := broker.Topics[topic].WriteMultiBytes(batch.Msgs, partition)
			if err != nil {
				if err == LOST_MSG_ERR {
					// 需要从其他broker获取剩下的
				} else {
					log.Error(err)
					broker.ErrorChan <- err
				}
			}
			// 如果是leader则进行dispatcher
			// TODO 这里建议迁移到 Push 中
			if res, ok := broker.leaderPartitions[GeneratorKey(topic, partition)]; ok && res {
				// 如果append失败怎么办?
				err = broker.appendToSlaveBroker(batch)
				if err != nil {
					log.Error(err)
					broker.ErrorChan <- err
				}
			}
		case <-ctx.Done():
			log.Info("append msg exit, err: ", ctx.Err())
			// 退出
			return
		}
	}
}

// 发送给此包含此partition的其他broker
func (broker *Broker) appendToSlaveBroker(batch *MsgBatch) error {
	key := GeneratorKey(batch.Topic, batch.Partition)
	if clientNames, ok := broker.partitionBrokers[key]; ok {
		for _, clientName := range clientNames {
			if clientName == broker.Name {
				// self, jump
				continue
			}
			log.Debugf("append log to broker %s, key %s", clientName, key)
			// 当broker宕机时，broker已经从clients中移除，因此这里需要进行判断
			if _, ok := broker.brokerClients[clientName]; !ok {
				log.Warnf("append log to broker %s, key %s, but broker clint not found", clientName, key)
				continue
			}
			client := broker.brokerClients[clientName].Client
			ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
			appendClient, err := client.Append(ctx)
			if err != nil {
				log.Error("[append] get client error")
				continue
			}
			err = appendClient.Send(batch)
			if err != nil {
				log.Error("[append] send error")
				continue
			}
			resp, err := appendClient.CloseAndRecv()
			if err != nil {
				log.Error("[append] close error")
				continue
			}
			if resp.Status == RespStatus_ERROR {
				log.Error("[append] append resp error")
				continue
			}
		}
	}
	return nil
}

// 创建新的topic, 并且发布到etcd(仅发布state和leader)
func (broker *Broker) createTopic(t string, assignPartitions, leadPartitions []uint32) (err error) {
	tp := TopicPartition{Topic: t, Partition: assignPartitions}
	broker.recoveryTopic(tp, leadPartitions)
	// 扫描topic，添加对应的broker
	broker.scanTopic(t)
	// put state & leader(lease)
	for _, p := range leadPartitions {
		key := fmt.Sprintf("/brokers/topics/%s/partitions/%d", t, p)
		value := broker.Name
		ctx1, _ := context.WithTimeout(context.Background(), DefaultTimeout)
		ctx2, _ := context.WithTimeout(context.Background(), DefaultTimeout)
		broker.etcdClient.Put(ctx1, fmt.Sprintf("%s/state", key), value)
		broker.etcdClient.Put(ctx2, fmt.Sprintf("%s/leader", key), value,
			clientv3.WithLease(broker.etcdLeaseId))
	}
	return
}

// 创建topic, 不发布到etcd, 启动的时候调用
func (broker *Broker) recoveryTopic(tp TopicPartition, leadPartitions []uint32) (err error) {
	log.Infof("create topic %s, partitions: %v, lead: %v", tp.Topic, tp.Partition, leadPartitions)
	topicConfig := &TopicConfig{
		Name:         tp.Topic,
		PartitionIds: tp.Partition,
		BatchCount:   MAX_SEND_COUNT,
		BasePath:     broker.DataPath}
	topic, err := NewFileTopic(topicConfig)
	if err != nil {
		log.Errorf("create topic %s error", tp.Topic)
		return
	}
	broker.Topics[tp.Topic] = topic
	for _, v := range leadPartitions {
		broker.leaderPartitions[GeneratorKey(tp.Topic, v)] = true
	}
	broker.topicPartitions = append(broker.topicPartitions, tp)
	return
}

// 1，brokerClients上添加此broker
// 2，找到此broker上存在的partition
// 3. 找到此broker上leader的partition
func (broker *Broker) AddBrokerMember(config *BrokerConfig) (err error) {
	log.Infof("add new broker %s %s", config.Name, config.ListenerAddress)
	if config == nil {
		log.Error("not found broker configuration")
		return errors.New("not found broker configuration")
	}
	broker.brokerClientsMutex.Lock()
	defer broker.brokerClientsMutex.Unlock()
	conn, err := grpc.Dial(config.ListenerAddress, grpc.WithInsecure())
	if err != nil {
		log.Error(err)
		return
	}
	brokerClient := NewBrokerServiceClient(conn)
	broker.brokerClients[config.Name] = &BrokerMember{Client: brokerClient, Name: config.Name,
		ListenerAddress: config.ListenerAddress}
	log.Infof("%s connect to new broker %s success", broker.Name, config.Name)
	return
}

// 移除broker
func (broker *Broker) RemoveBroker(config *BrokerConfig) (err error) {
	log.Infof("remove broker %s", config.Name)
	if config == nil {
		log.Error("not found broker configuration")
		return errors.New("not found broker configuration")
	}
	broker.brokerClientsMutex.Lock()
	defer broker.brokerClientsMutex.Unlock()
	delete(broker.brokerClients, config.Name)
	return
}

// 更新分区leader
// broker并不会保存所有partition的leader， 仅仅保存自己leader的partition
// 当其他broker lost, 自己变成分区leader的时候会调用
func (broker *Broker) becomePartitionLeader(partitions TopicPartition) {
	broker.leaderPartitionsMutex.Lock()
	defer broker.leaderPartitionsMutex.Unlock()
	for _, p := range partitions.Partition {
		key := GeneratorKey(partitions.GetTopic(), p)
		broker.leaderPartitions[key] = true
		broker.topicPartitionLeader[key] = broker.Name
	}
}

// 更新partition关联的broker, 即包含此partition的所有broker
// 1，当broker添加或者lost的时候； 2， 当新建partition的时候
// add=true增加broker， false减少
func (broker *Broker) UpdatePartitionBrokers(partitions TopicPartition, newBroker []string, add bool) {
	log.Debugf("update topic %s partition %v broker %s add: %v", partitions.Topic, partitions.Partition, newBroker, add)
	broker.partitionBrokersMutex.Lock()
	defer broker.partitionBrokersMutex.Unlock()

	if add {
		for _, p := range partitions.Partition {
			key := GeneratorKey(partitions.GetTopic(), p)
			for _, tempBroker := range newBroker {
				broker.partitionBrokers[key] = append(broker.partitionBrokers[key], tempBroker)
			}
		}
	} else {
		for _, p := range partitions.Partition {
			key := GeneratorKey(partitions.GetTopic(), p)
			brokers := broker.partitionBrokers[key]
			for i, b := range brokers {
				for _, tempBroker := range newBroker {
					if b == tempBroker {
						for i += 1; i < len(brokers); i++ {
							brokers[i-1] = brokers[i]
						}
						brokers = brokers[:len(brokers)-1]
						return
					}
				}
			}
		}
	}
}

func (broker *Broker) isLeaderPartition(topic string, partition uint32) bool {
	lead, ok := broker.leaderPartitions[GeneratorKey(topic, partition)]
	return ok && lead
}

func (broker *Broker) Close() {
	for _, topic := range broker.Topics {
		topic.Close()
	}
	broker.etcdClient.Close()
	broker.RpcServer.GracefulStop()
	log.Infof("broker %v close", broker)
}

type brokerServiceServer struct {
	AppendMsgBatchChan chan *MsgBatch
	broker             *Broker
	//GetMsgBatchChan    chan *MsgBatch
}

func NewBrokerServerServer(broker *Broker, appendChan chan *MsgBatch) *brokerServiceServer {
	return &brokerServiceServer{
		AppendMsgBatchChan: appendChan,
		broker:             broker,
	}
}

// broker

// append the msgs come from other broker
// rpc Append(stream MsgBatch) returns (Resp) {}
func (bss *brokerServiceServer) Append(appendServer BrokerService_AppendServer) error {
	log.Debug("[GRPC] Append")
	for {
		msgBatch, err := appendServer.Recv()
		if err != nil {
			if err == io.EOF {
				appendServer.SendAndClose(&Resp{
					Status: RespStatus_OK,
				})
				return nil
			} else {
				log.Error(err)
				appendServer.SendAndClose(&Resp{
					Status:  RespStatus_ERROR,
					Comment: err.Error(),
				})
			}
			return err
		}
		bss.broker.AppendMsgChan <- msgBatch
		log.Debug("append success, topic: %s partition: %d", msgBatch.Topic, msgBatch.Partition)
	}
}

// get msg
// rpc Get(GetReq) returns (GetResp) {}
// TODO get需要涉及并发情况, 带完善
func (bss *brokerServiceServer) Get(ctx context.Context, req *GetReq) (resp *GetResp, err error) {
	log.Debug("[GRPC] Get")
	broker := bss.broker
	key := GeneratorKey(req.Topic, req.Partition)
	resp = new(GetResp)
	// 判断是否为partition的master
	if isLeader, ok := broker.leaderPartitions[key]; !ok || !isLeader {
		resp.Resp.Status = RespStatus_ERROR
		resp.Resp.Comment = errors.New("request wrong broker, the broker not the partition leader").Error()
		return resp, errors.New("request wrong broker, the broker not the partition leader")
	}
	if part, ok := broker.Topics[req.Topic].Partitions[req.Partition]; ok {
		// get lag
		readLength := part.OffsetLag(req.StartOffset)
		enough := true
		if readLength > MAX_SEND_COUNT {
			readLength = MAX_SEND_COUNT
			enough = false
		}
		msgs, err := part.ReadMultiMsg(req.StartOffset, uint32(readLength))
		if err != nil {
			log.Error(err)
			resp.Resp.Status = RespStatus_ERROR
			resp.Resp.Comment = err.Error()
			return resp, err
		} else {
			resp.Resp.Status = RespStatus_OK
		}
		resp.Msgs = &MsgBatch{
			Msgs:      msgs,
			Partition: req.Partition,
			Topic:     req.Topic,
		}
		resp.Enough = enough
	} else {
		resp.Resp.Status = RespStatus_ERROR
		resp.Resp.Comment = errors.New("the broker not have this partition").Error()
	}
	return
}

// producer
// rpc Push(stream MsgBatch) returns (Resp) {}
func (bss *brokerServiceServer) Push(pushServer BrokerService_PushServer) error {
	log.Debug("[GRPC] push")
	for {
		msgBatch, err := pushServer.Recv()
		if err != nil {
			if err == io.EOF {
				pushServer.SendAndClose(&Resp{
					Status: RespStatus_OK,
				})
				return nil
			} else {
				log.Error(err)
				pushServer.SendAndClose(&Resp{
					Status:  RespStatus_ERROR,
					Comment: err.Error(),
				})
			}
			return err
		}
		if contain, ok := bss.broker.leaderPartitions[GeneratorKey(msgBatch.Topic, msgBatch.Partition)]; ok && contain {
			bss.broker.AppendMsgChan <- msgBatch
		} else {
			log.Warnf("broker %s not lead topic %s-%d", bss.broker.Name, msgBatch.Topic, msgBatch.Partition)
		}
	}
}

// rpc CreatePartition(CreatePartitionReq) returns (Resp) {}
// 创建分区
// 先把broker分好， 然后创建到etcd
func (bss *brokerServiceServer) CreateTopic(ctx context.Context, req *CreateTopicReq) (resp *Resp, err error) {
	log.Debug("[GRPC] CreateTopic")
	broker := bss.broker
	topic := req.Topic
	partitionCount := req.PartitionCount
	replicaCount := req.ReplicaCount
	brokerCount := len(broker.brokerClients) + 1

	resp = new(Resp)
	log.Infof("create topic %s partition %d replica %d", topic, partitionCount, replicaCount)
	if err = broker.checkTopic(topic); err != nil {
		log.Error(err)
		resp.Status = RespStatus_ERROR
		resp.Comment = err.Error()
		return
	}

	if int(replicaCount) > brokerCount {
		resp.Status = RespStatus_ERROR
		resp.Comment = fmt.Sprintf("replicaCount: %d must small or equals then broker count: %d", replicaCount, brokerCount)
		return resp, errors.New(resp.Comment)
	}

	assignParts := calcAssignPartitions(int(partitionCount), int(replicaCount), brokerCount)
	assignLeaders := calcLeaders(int(partitionCount), brokerCount)

	clientIndex := 0
	broker.UpdatePartitionBrokers(
		TopicPartition{Topic: topic, Partition: assignParts[clientIndex]},
		[]string{broker.Name}, true)

	clientIndex++
	if err != nil {
		resp.Status = RespStatus_ERROR
		resp.Comment = err.Error()
		return
	}

	// 为了防止创建topic的时候并不知道某个partition对应的所有broker，
	// 因此，先分配，将结果发布到etcd，然后创建topic，这样就能获取到所有的broker
	assRes := make(map[string]*AssignTopicReq)

	// 分配给其他broker
	// warring 创建topic的时候有broker exit
	for name, member := range broker.brokerClients {
		parts := assignParts[clientIndex]
		assignReq := &AssignTopicReq{
			Topic:            topic,
			Partitions:       parts,
			LeaderPartitions: assignLeaders[clientIndex]}
		assRes[name] = assignReq
		broker.UpdatePartitionBrokers(TopicPartition{Partition: parts, Topic: topic}, []string{member.Name}, true)
		clientIndex++
	}
	// put topic on etcd
	bss.putTopic(topic, int(partitionCount))
	// 分配给自己
	err = broker.createTopic(topic, assignParts[0], assignLeaders[0])
	// 分配到其他人
	for name, req := range assRes {
		member := bss.broker.brokerClients[name]
		assResp, assErr := member.Client.AssignTopic(ctx, req)
		if assErr != nil || assResp.Status == RespStatus_ERROR {
			log.Errorf("error: %s, info: %s", assErr, assResp.Comment)
			// 创建失败进行回滚
		}
	}
	resp.Status = RespStatus_OK
	return
}

// 发布到etcd topic /brokers/topics/[topic]
// 包含partition的broker
func (bss *brokerServiceServer) putTopic(topic string, partitionCount int) {
	etcdTopic := EtcdTopic{Version: VERSION, Partitions: make(map[uint32][]string)}
	for i := 0; i < partitionCount; i++ {
		etcdTopic.Partitions[uint32(i)] = bss.broker.partitionBrokers[GeneratorKey(topic, uint32(i))]
	}
	key := fmt.Sprintf("/brokers/topics/%s", topic)
	value, _ := json.Marshal(etcdTopic)
	ctx, _ := context.WithTimeout(context.Background(), DefaultTimeout)
	bss.broker.etcdClient.Put(ctx, key, string(value))
}

// 计算分区分配
func calcAssignPartitions(partCount, replicaCount, brokerCount int) [][]uint32 {
	parts := make([][]uint32, brokerCount)
	assignCount := (partCount * replicaCount) / brokerCount
	if (partCount+replicaCount)%brokerCount != 0 {
		assignCount += 1
	}
	for i := 0; i < brokerCount; i++ {
		parts[i] = make([]uint32, 0, assignCount)
	}
	index := 0
	for i := 0; i < partCount; i++ {
		for j := 0; j < replicaCount; j++ {
			parts[(index+j)%brokerCount] = append(parts[(index+j)%brokerCount], uint32(i))
		}
		index++
	}
	return parts
}

// 计算leader分配
func calcLeaders(partCount, brokerCount int) [][]uint32 {
	return calcAssignPartitions(partCount, 1, brokerCount)
}

// 分配分区
func (bss *brokerServiceServer) AssignTopic(ctx context.Context, req *AssignTopicReq) (resp *Resp, err error) {
	log.Debug("[GRPC] AssignTopic")
	err = bss.broker.createTopic(req.Topic, req.Partitions, req.LeaderPartitions)
	resp = new(Resp)
	if err != nil {
		log.Error("Assign topic error")
		resp.Status = RespStatus_ERROR
		resp.Comment = err.Error()
		return
	}
	resp.Status = RespStatus_OK
	return
}

// consumer
// rpc Subscribe(SubReq) returns (SubResp) {}
func (bss *brokerServiceServer) Subscribe(ctx context.Context, req *SubReq) (*SubResp, error) {
	log.Debug("[GRPC] Subscribe")
	broker := bss.broker
	tpos := make([]*TopicPartitionOffset, 0, len(req.Topics))
	// 该consumer group第一次订阅， 把该broker包含的partition最新的offset返回回去
	for _, topicPartition := range req.Topics {
		topic := topicPartition.Topic
		po := make(map[uint32]uint64)
		for _, partition := range topicPartition.Partition {
			if broker.isLeaderPartition(topic, partition) {
				log.Debugf("broker{%s} topic{%s} partition{%d} latestOffset{%d}", broker.Name, topic, partition)
				po[partition] = broker.Topics[topic].Partitions[partition].LatestMsgOffset
			} else {
				log.Warnf("broker{%s} not lead topic{%s} partition{%d}", broker.Name, topic, partition)
			}
		}
		tpos = append(tpos, &TopicPartitionOffset{Topic: topic, PartitionOffset: po})
	}
	return &SubResp{Resp: &Resp{Status: RespStatus_OK}, TopicPartitionOffset: tpos}, nil
}

// rpc Pull(PullReq) returns (stream MsgBatch) {}
// 单个分区的数量可能不能满足
func (bss *brokerServiceServer) Pull(req *PullReq, pullServer BrokerService_PullServer) error {
	// 如何均匀的从各个分区进行取数据, 只pull leader分区的的msg
	// client请求的时候会携带分区信息
	//log.Debug("[GRPC] Pull")
	broker := bss.broker
	topic := req.TpSet.Topic
	partitionOffset := req.TpSet.PartitionOffset
	allCount := req.Count // 必须大于0, 客户端保证
	partitionCount := len(partitionOffset)

	msgChan := make(chan []*MsgBatch, 1)
	errChan := make(chan error, 1)

	pullMsg := func() {
		// 均匀读取
		aveCount := int(allCount) / int(partitionCount)
		delta := 0
		msgBatchs := make([]*MsgBatch, 0, partitionCount)
		for part, offset := range partitionOffset {
			//log.Debugf("pull topic{%s} partition{%d} count{%d} startOffset{%d}", topic, part, aveCount, offset)
			key := GeneratorKey(topic, part)
			if have, ok := broker.leaderPartitions[key]; !ok || !have {
				err := errors.New("this broker don't lead this partition")
				log.Error(err)
				errChan <- err
				return
			}
			// 如果一次读取的数目小于aveCount, 差值为delta, 则下一次读取数目为aveCount + delta, 保证数目尽可能达到
			msgs, l := broker.Topics[topic].ReadMulti(offset, part, uint32(aveCount+delta))
			delta = (aveCount + delta - l)
			if l == 0 {
				continue
			}
			msgBatch := &MsgBatch{
				Topic:     topic,
				Partition: part,
				Msgs:      msgs}
			msgBatchs = append(msgBatchs, msgBatch)
			log.Debugf("return topic{%s} partition{%d} count{%d} startOffset{%d}", topic, part, l, offset)
			// 更新offset
			partitionOffset[part] += uint64(l)
		}
		if len(msgBatchs) == 0 {
			//log.Debug("no more data")
		}
		msgChan <- msgBatchs
		return
	}

	// timeout == 0表示取完立即返回
	if req.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout)*time.Millisecond)
		defer cancel()
		signalChan := make(chan int, 1)
		for {
			select {
			case <-signalChan:
				pullMsg()
			case msgBatches := <-msgChan:
				for _, msgBatch := range msgBatches {
					err := pullServer.Send(msgBatch)
					if err != nil {
						log.Error(err)
						return err
					}
				}
				signalChan <- 1
			case <-ctx.Done():
				return nil
			}
		}
	} else {
		pullMsg()
		select {
		case msgBatches := <-msgChan:
			for _, msgBatch := range msgBatches {
				log.Debugf("get data count{%d}", len(msgBatch.Msgs))
				err := pullServer.Send(msgBatch)
				if err != nil {
					log.Error(err)
					return err
				}
			}
		case err := <-errChan:
			log.Error(err)
		default:
			//log.Debug("pull no more data")
		}
	}
	return nil
}

func GeneratorKey(topic string, partition uint32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}
