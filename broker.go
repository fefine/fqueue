package fqueue

import (
	"errors"
	"net"
	"google.golang.org/grpc"
	"log"
	"context"
	"fmt"
	"io"
	"time"
)

const (
	MAX_SEND_COUNT = 1000
)

type BrokerConfig struct {
	Name            string
	// rpc地址
	ListenerAddress string
	// etcd地址
	EtcdEndPoints   []string
	// 存放数据的地址
	DataPath        string
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
	brokerClients   map[string]BrokerServiceClient
	// 每个partition关联的broker, brokers用[]client表示, 加锁
	partitionBrokers map[string][]BrokerServiceClient
	// 以此broker为partitionLeader的分区, 怎么做到内存可见, 加锁
	leaderPartitions map[string]bool
	// 以此broker为partitionLeader的分区数
	leaderPartitionCount uint


}

func NewBrokerAndStart(config *BrokerConfig) (broker *Broker, err error) {
	if config != nil {
		panic(errors.New("not found configuration"))
	}
	broker = new(Broker)
	broker.Name = config.Name
	broker.DataPath = config.DataPath
	broker.EtcdEndPoints = config.EtcdEndPoints
	broker.DataPath = config.DataPath
	broker.AppendMsgChan = make(chan *MsgBatch)

	// TODO 1, start rpc server
	rpcLis, err := net.Listen("tcp", config.ListenerAddress)
	if err != nil {
		log.Fatalln("broker server listener rpc error")
		return
	}
	// 这里可以用加密, 暂不提供
	grpcServer := grpc.NewServer()
	RegisterBrokerServiceServer(grpcServer, NewBrokerServerServer(broker, broker.AppendMsgChan))
	log.Println("rpc server start!")

	go grpcServer.Serve(rpcLis)

	appendCtx, ctxFunc := context.WithCancel(context.Background())
	broker.cancelFuncs = append(broker.cancelFuncs, ctxFunc)

	go broker.appendMsgService(appendCtx)
	// TODO 2, connect etcd, recovery broker info

	return
}

// 写入消息的服务
func (broker *Broker) appendMsgService(ctx context.Context) {
	for {
		select {
		case msgBatch := <- broker.AppendMsgChan:
			// TODO 每个topic下的每个partition生成一个goroutine
			broker.dispatcherMsg(msgBatch)
		case <- ctx.Done():
			log.Println("append msg server stop")
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
		msgChan := make(chan *MsgBatch)
		ctx, cancelFunc := context.WithCancel(context.Background())
		broker.partitionMsgChan[key] = msgChan
		broker.cancelFuncs = append(broker.cancelFuncs, cancelFunc)
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
			err := broker.Topics[topic].Partitions[uint(partition)].WriteMultiMsgByBytes(batch.Msgs)
			broker.ErrorChan <- err
		case <- ctx.Done():
			// 退出
			return
		}
	}
}

func (broker *Broker) pull(ctx context.Context, count uint) (msgs [][]byte, err error) {

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

// append from other broker
// rpc Append(stream MsgBatch) returns (Resp) {}
func (bss *brokerServiceServer) Append(appendServer BrokerService_AppendServer) error {
	for {
		msgBatch, err := appendServer.Recv()
		if err != nil {
			if err == io.EOF {
				appendServer.SendAndClose(&Resp{
					Status:RespStatus_OK,
				})
			} else {
				log.Fatal(err)
				appendServer.SendAndClose(&Resp{
					Status:RespStatus_ERROR,
					Comment: err.Error(),
				})
			}
			return err
		}
		bss.broker.AppendMsgChan <- msgBatch
	}
}
// get msg
// rpc Get(GetReq) returns (GetResp) {}
// TODO get需要涉及并发情况, 带完善
func (bss *brokerServiceServer) Get(ctx context.Context, req *GetReq) (resp *GetResp, err error) {
	broker := bss.broker
	key := GeneratorKey(req.Topic, req.Partition)
	// 判断是否为partition的master
	if isLeader, ok := broker.leaderPartitions[key]; !ok || !isLeader {
		resp.Resp.Status = RespStatus_ERROR
		resp.Resp.Comment = errors.New("request wrong broker, the broker not the partition leader").Error()
		return resp, errors.New("request wrong broker, the broker not the partition leader")
	}
	if part, ok := broker.Topics[req.Topic].Partitions[uint(req.Partition)]; ok {
		// get lag
		readLength := part.OffsetLag(req.StartOffset)
		enough := true
		if readLength > MAX_SEND_COUNT {
			readLength = MAX_SEND_COUNT
			enough = false
		}
		msgs, err := part.ReadMultiMsg(req.StartOffset, uint32(readLength))
		if err != nil {
			log.Fatalln(err)
			resp.Resp.Status = RespStatus_ERROR
			resp.Resp.Comment = err.Error()
			return
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
	for {
		msgBatch, err := pushServer.Recv()
		if err != nil {
			if err == io.EOF {
				pushServer.SendAndClose(&Resp{
					Status:RespStatus_OK,
				})
			} else {
				log.Fatal(err)
				pushServer.SendAndClose(&Resp{
					Status:RespStatus_ERROR,
					Comment: err.Error(),
				})
			}
			return err
		}
		bss.broker.AppendMsgChan <- msgBatch
	}
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

	msgChan := make(chan []*MsgBatch)
	errChan := make(chan error)

	pullMsg := func() {
		// 均匀读取
		aveCount := int(allCount) / int(partitionCount)
		delta := 0
		msgBatchs := make([]*MsgBatch, 0, partitionCount)
		for part, offset := range partitionOffset {
			key := GeneratorKey(topic, part)
			if have, ok := broker.leaderPartitions[key]; !ok || !have {
				errChan <- errors.New("this broker don't have this partition")
				return
			}
			// 如果一次读取的数目小于aveCount, 差值为delta, 则下一次读取数目为aveCount + delta, 保证数目尽可能达到
			msgs, l := broker.Topics[topic].ReadMulti(offset, uint(part), uint32(aveCount + delta))
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
						log.Fatalln(err)
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
		msgBatches := <- msgChan
		for _, msgBatch := range msgBatches {
			err := pullServer.Send(msgBatch)
			if err != nil {
				log.Fatalln(err)
				return err
			}
		}
	}
	return nil
}

func GeneratorKey(topic string, partition uint32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}