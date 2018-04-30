package fqueue

import (
	"errors"
	"net"
	"google.golang.org/grpc"
	log "github.com/sirupsen/logrus"
	"context"
	"fmt"
	"io"
	"time"
	"sync"
)

const (
	MAX_SEND_COUNT = 1000
	DEFAULT_CHAN_COUNT = 10
)


type BrokerConfig struct {
	Name            string
	// rpc地址
	ListenerAddress string
	// etcd地址
	EtcdEndPoints   []string
	// 存放数据的地址
	DataPath        string

	Debug           bool
}


// broke需要和etcd进行结合
type Broker struct {
	Name            string
	// rpc地址
	ListenerAddress string
	// etcd地址
	EtcdEndPoints   []string
	// 存放数据的地址
	DataPath        string
	// rpc
	RpcServer       *grpc.Server
	//
	Topics          map[string]*FileTopic
	// append chan, 收到别的broker发来的append
	AppendMsgChan   chan *MsgBatch
	// error chan, 发生错误
	ErrorChan       chan error
	//
	cancelFuncs     []context.CancelFunc
	// goroutine msg chan
	partitionMsgChan map[string]chan *MsgBatch
	// 其他的broker, 加锁
	brokerClients   map[string]*BrokerMember
	brokerClientsMutex sync.Mutex
	// 每个partition关联的broker, brokers用[]client表示, 加锁
	// 换成brokerName方便查询
	partitionBrokers map[string][]string
	partitionBrokersMutex sync.Mutex
	// 以此broker为partitionLeader的分区, 怎么做到内存可见, 加锁
	leaderPartitions map[string]bool
	leaderPartitionsMutex sync.Mutex
	// 以此broker为partitionLeader的分区数
	leaderPartitionCount uint
}

type BrokerMember struct {
	name            string
	listenerAddress string
	client          BrokerServiceClient
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
	broker.AppendMsgChan = make(chan *MsgBatch, DEFAULT_CHAN_COUNT)
	broker.Topics = make(map[string]*FileTopic)
	broker.brokerClients = make(map[string]*BrokerMember)
	broker.partitionMsgChan = make(map[string]chan *MsgBatch)
	broker.partitionBrokers = make(map[string][]string)
	broker.ErrorChan = make(chan error, DEFAULT_CHAN_COUNT)
	broker.cancelFuncs = make([]context.CancelFunc, 10)
	broker.leaderPartitions = make(map[string]bool)
	if config.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	// TODO 1, start rpc server
	rpcLis, err := net.Listen("tcp", config.ListenerAddress)
	if err != nil {
		log.Error("broker server listener rpc error")
		return
	}
	// 这里可以用加密, 暂不提供
	grpcServer := grpc.NewServer()
	RegisterBrokerServiceServer(grpcServer, NewBrokerServerServer(broker, broker.AppendMsgChan))
	log.Infof("broker {%s} rpc server start!", broker.Name)

	go grpcServer.Serve(rpcLis)

	appendCtx, ctxFunc := context.WithCancel(context.Background())
	broker.cancelFuncs = append(broker.cancelFuncs, ctxFunc)

	go broker.appendMsgService(appendCtx)
	// TODO 2, connect etcd, recovery broker info
	log.Infof("broker {%s} start!", broker.Name)
	return
}

// 写入消息的服务
func (broker *Broker) appendMsgService(ctx context.Context) {
	for {
		select {
		case msgBatch := <- broker.AppendMsgChan:
			//key := GeneratorKey(msgBatch.Topic, msgBatch.Partition)
			//if contain, ok := broker.leaderPartitions[key]; ok && contain {
				broker.dispatcherMsg(msgBatch)
			//} else {
			//	log.Errorf("broker %s not lead topic %s-%d", broker.Name, msgBatch.Topic, msgBatch.Partition)
			//}
		case <- ctx.Done():
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
		case batch := <- msgChan:
			topic := batch.Topic
			partition := batch.Partition
			log.Debugf("topic: %s part: %d recv: %d", topic, partition, len(batch.Msgs))
			err := broker.Topics[topic].WriteMultiBytes(batch.Msgs, partition)
			if err != nil {
				log.Error(err)
				broker.ErrorChan <- err
			}
			// 如果append失败怎么办?
			err = broker.appendToSlaveBroker(batch)
			if err != nil {
				log.Error(err)
				broker.ErrorChan <- err
			}
		case <- ctx.Done():
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
			log.Debugf("append log to broker %s, key %s", clientName, key)
			client := broker.brokerClients[clientName].client
			appendClient, err := client.Append(context.TODO())
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

func (broker *Broker) createTopic(t string, assignPartitions, leadPartitions []uint32) (err error) {
	log.Infof("create topic %s, partitions: %v, lead: %v", t, assignPartitions, leadPartitions)
	topicConfig := &TopicConfig{
		Name:t,
		PartitionIds:assignPartitions,
		BatchCount:MAX_SEND_COUNT,
		BasePath:broker.DataPath}
	topic, err := NewFileTopic(topicConfig)
	if err != nil {
		log.Errorf("create topic %s error", t)
		return
	}
	broker.Topics[t] = topic
	for _, v := range leadPartitions {
		broker.leaderPartitions[GeneratorKey(t, v)] = true
	}
	// TODO 1, 把leader partition发布到etcd, 然后放到leadPartition中
	return
}

// 1，brokerClients上添加此broker
// 2，找到此broker上存在的partition
// 3. 找到此broker上leader的partition
func (broker *Broker) AddBrokerMember(config *BrokerConfig) (err error) {
	log.Debug("add new broker %s %s", config.Name, config.ListenerAddress)
	if config == nil {
		log.Error("not found broker configuration")
		return errors.New("not found broker configuration")
	}
	broker.brokerClientsMutex.Lock()
	defer broker.brokerClientsMutex.Unlock()
	conn, err := grpc.Dial(config.ListenerAddress, grpc.WithInsecure())
	if err != nil {
		return
	}
	brokerClient := NewBrokerServiceClient(conn)
	broker.brokerClients[config.Name] = &BrokerMember{client:brokerClient, name: config.Name, listenerAddress: config.ListenerAddress}
	return
}

// 移除broker
func (broker *Broker) RemoveBroker(config *BrokerConfig) (err error) {
	log.Debug("remove broker %s", config.Name)
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
func (broker *Broker) UpdatePartitionLeader(partitions TopicPartition) {
	broker.leaderPartitionsMutex.Lock()
	defer broker.leaderPartitionsMutex.Unlock()
	for _, p := range partitions.Partition {
		key := GeneratorKey(partitions.GetTopic(), p)
		broker.leaderPartitions[key] = true
	}
}
// 更新partition关联的broker, 即包含此partition的所有broker
// 1，当broker添加或者lost的时候； 2， 当新建partition的时候
// add=true增加broker， false减少
func (broker *Broker) UpdatePartitionBrokers(partitions TopicPartition, newBroker string, add bool) {
	log.Debugf("update topic %s partition %v broker %s add: %v", partitions.Topic, partitions.Partition, newBroker, add)
	broker.partitionBrokersMutex.Lock()
	defer broker.partitionBrokersMutex.Unlock()

	if add {
		for _, p := range partitions.Partition {
			key := GeneratorKey(partitions.GetTopic(), p)
			broker.partitionBrokers[key] = append(broker.partitionBrokers[key], newBroker)
		}
	} else {
		for _, p := range partitions.Partition {
			key := GeneratorKey(partitions.GetTopic(), p)
			brokers := broker.partitionBrokers[key]
			for i, v := range brokers {
				if v == newBroker {
					for i += 1; i < len(brokers); i++ {
						brokers[i - 1] = brokers[i]
					}
					brokers = brokers[:len(brokers) - 1]
					return
				}
			}
		}
	}
}

func (broker *Broker) Close() {
	for _, topic := range broker.Topics {
		topic.Close()
	}
	//for _, cancelFunc := range broker.cancelFuncs {
	//	cancelFunc()
	//}
	log.Infof("broker %v close", broker)
}

type brokerServiceServer struct {
	AppendMsgBatchChan chan *MsgBatch
	broker             *Broker
	//GetMsgBatchChan    chan *MsgBatch
}

func NewBrokerServerServer(broker *Broker, appendChan chan *MsgBatch) (*brokerServiceServer) {
	return &brokerServiceServer{
		AppendMsgBatchChan: appendChan,
        broker: broker,
    }
}

// broker

// append the msgs come from other broker
// rpc Append(stream MsgBatch) returns (Resp) {}
func (bss *brokerServiceServer) Append(appendServer BrokerService_AppendServer) error {
	for {
		msgBatch, err := appendServer.Recv()
		if err != nil {
			if err == io.EOF {
				appendServer.SendAndClose(&Resp{
					Status:RespStatus_OK,
				})
				return nil
			} else {
				log.Error(err)
				appendServer.SendAndClose(&Resp{
					Status:RespStatus_ERROR,
					Comment: err.Error(),
				})
			}
			return err
		}
		bss.broker.AppendMsgChan <- msgBatch
		log.Debug("push success")
	}
}
// get msg
// rpc Get(GetReq) returns (GetResp) {}
// TODO get需要涉及并发情况, 带完善
func (bss *brokerServiceServer) Get(ctx context.Context, req *GetReq) (resp *GetResp, err error) {
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
			Msgs:msgs,
			Partition: req.Partition,
			Topic: req.Topic,
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
					Status:RespStatus_OK,
				})
				return nil
			} else {
				log.Error(err)
				pushServer.SendAndClose(&Resp{
					Status:RespStatus_ERROR,
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
func (bss *brokerServiceServer) CreateTopic(ctx context.Context, req *CreateTopicReq) (resp *Resp, err error) {
	log.Debug("[GRPC] CreateTopic")
	broker := bss.broker
	topic := req.Topic
	partitionCount := req.PartitionCount
	replicaCount := req.ReplicaCount
	brokerCount := len(broker.brokerClients) + 1
	resp = new(Resp)
	log.Infof("create topic %s partition %d replica %d", topic, partitionCount, replicaCount)

	if int(replicaCount) > brokerCount {
		resp.Status = RespStatus_ERROR
		resp.Comment = fmt.Sprintf("replicaCount: %d must small or equals then broker count: %d", replicaCount, brokerCount)
		return resp, errors.New(resp.Comment)
	}
	assignParts := calcAssignPartitions(int(partitionCount), int(replicaCount), brokerCount)
	assignLeaders := calcLeaders(int(partitionCount), brokerCount)
	clientIndex := 0
	// 分配给自己
	err = broker.createTopic(topic, assignParts[clientIndex], assignLeaders[clientIndex])
	clientIndex++
	if err != nil {
		resp.Status = RespStatus_ERROR
		resp.Comment = err.Error()
		return
	}
	// 分配给其他broker
	for _, member := range broker.brokerClients {
		parts := assignParts[clientIndex]
		assignReq := &AssignTopicReq{
			Topic:topic,
			Partitions: parts,
			LeaderPartitions: assignLeaders[clientIndex]}
		assResp, assErr := member.client.AssignTopic(ctx, assignReq)
		if assErr != nil || assResp.Status == RespStatus_ERROR{
			log.Errorf("error: %s, info: %s", assErr, assResp.Comment)
			// TODO 创建失败进行回滚
		}
		broker.UpdatePartitionBrokers(TopicPartition{Partition:parts, Topic:topic}, member.name, true)

		clientIndex++
	}
	// 包含partition的broker

	resp.Status = RespStatus_OK
	return
}

// 计算分区分配
func calcAssignPartitions(partCount, replicaCount, brokerCount int) [][]uint32 {
	parts := make([][]uint32, brokerCount)
	assignCount := (partCount * replicaCount) / brokerCount
	if (partCount + replicaCount) % brokerCount != 0 {
		assignCount += 1;
	}
	for i := 0; i < brokerCount; i++ {
		parts[i] = make([]uint32, 0, assignCount)
	}
	index := 0
	for i := 0; i < partCount; i++ {
		for j := 0; j < replicaCount; j++ {
			parts[(index + j) % brokerCount] = append(parts[(index + j) % brokerCount], uint32(i))
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
// TODO 暂时不实现
func (bss *brokerServiceServer) Subscribe(context.Context, *SubReq) (*SubResp, error) {
	return nil, errors.New("not implement this method")
}

// rpc Pull(PullReq) returns (stream MsgBatch) {}
// 单个分区的数量可能不能满足
func (bss *brokerServiceServer) Pull(req *PullReq, pullServer BrokerService_PullServer) error {
	// 如何均匀的从各个分区进行取数据, 只pull leader分区的的msg
	// client请求的时候会携带分区信息
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
			key := GeneratorKey(topic, part)
			if have, ok := broker.leaderPartitions[key]; !ok || !have {
				err := errors.New("this broker don't have this partition")
				log.Error(err)
				errChan <- err
				return
			}
			// 如果一次读取的数目小于aveCount, 差值为delta, 则下一次读取数目为aveCount + delta, 保证数目尽可能达到
			msgs, l := broker.Topics[topic].ReadMulti(offset, part, uint32(aveCount + delta))
			delta = (aveCount + delta - l)
			if l ==0 {
				continue
			}
			msgBatch := &MsgBatch{
				Topic:topic,
				Partition:part,
				Msgs:msgs}
			msgBatchs = append(msgBatchs, msgBatch)
			// 更新offset
			partitionOffset[part] += uint64(l)
		}
		msgChan <- msgBatchs
		log.Debug("pullMsg success")
		return
	}

	// timeout == 0表示取完立即返回
	if req.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout) * time.Millisecond)
		defer cancel()
		signalChan := make(chan int, 1)
		for {
			select {
			case <- signalChan:
				pullMsg()
			case msgBatches := <- msgChan:
				for _, msgBatch := range msgBatches {
					err := pullServer.Send(msgBatch)
					if err != nil {
						log.Error(err)
						return err
					}
				}
				signalChan <- 1
			case <- ctx.Done():
				return nil
			}
		}
	} else {
		pullMsg()
		select {
		case msgBatches := <- msgChan:
			log.Debug("get data")
			for _, msgBatch := range msgBatches {
				err := pullServer.Send(msgBatch)
				if err != nil {
					log.Error(err)
					return err
				}
			}
		case err := <- errChan:
			log.Error(err)
		default:
			log.Debug("pull no more data")
		}
	}
	return nil
}

func GeneratorKey(topic string, partition uint32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}