package fqueue

import (
	"github.com/go-log/log"
	"io"
	"errors"
)

const (
	DEFAULT_BATCH_COUNT = 3
)

// 负责发送和接收
type Topic interface {
	// 写到指定的分区
	Write(msg *Msg, partition uint) error
	// 批量写入同一分区，保证同时写入成功或者同时失败
	WriteMulti(msgs []*Msg, partition uint) error
	// 读取单条消息
	Read(offset uint64, partition uint) ([]byte, error)
	// 批量读取
	ReadMulti(offset uint64, partition uint, count uint32) ([][]byte, int)

	OffsetLag(offset uint64, partition uint) uint64

	Close() error
}

type FileTopic struct {
	Name            string
	PartitionIds    []uint
	PartitionNumber uint
	Partitions      map[uint]*FilePartition
	// 存放，可能没用
	//MsgChan         []chan *Msg
	// 一次读取的数量
	BatchCount      uint
}

type TopicConfig struct {
	Name            string
	PartitionIds    []uint
	BatchCount      uint
	BasePath        string
}

func NewFileTopic(config *TopicConfig) (ft *FileTopic, err error) {

	if config == nil {
		panic("Not found configuration file")
	}

	ft = new(FileTopic)
	ft.Name = config.Name
	ft.PartitionIds = config.PartitionIds
	ft.PartitionNumber = uint(len(ft.PartitionIds))
	ft.BatchCount = config.BatchCount
	basePath := config.BasePath

	partitionConfig := &PartitionConfig{
		Topic:    ft.Name,
		DataPath: basePath,
	}
	ft.Partitions = make(map[uint]*FilePartition, ft.PartitionNumber)
	for _, id := range ft.PartitionIds {
		partitionConfig.Id = id
		log.Logf("[%s] partition: %v", ft.Name, partitionConfig)
		part, err := NewFilePartition(partitionConfig)
		ft.Partitions[id] = part
		if err != nil {
			log.Logf("start topic error: %v", err)
			goto CLOSE
		}
	}

	// 这里创建goroutine专门往chan中放消息


	return
CLOSE:
	log.Log("close already start filePartitions")
	for _, fp := range ft.Partitions {
		if fp != nil {
			fp.Close()
		}
	}
	return
}

func (ft *FileTopic) checkPartition(partition uint) error {
	if _, ok := ft.Partitions[partition]; !ok {
		return errors.New("wrong partition")
	}
	return nil
}

// 写到指定的分区
func (ft *FileTopic) Write(msg *Msg, partition uint) (err error) {
	if err := ft.checkPartition(partition); err != nil {
		return err
	}
	return ft.Partitions[partition].WriteMsg(msg)
}
// 批量写入同一分区，保证同时写入成功或者同时失败
func (ft *FileTopic) WriteMulti(msgs []*Msg, partition uint) error {
	if err := ft.checkPartition(partition); err != nil {
		return err
	}
	return ft.Partitions[partition].WriteMultiMsg(msgs)
}
// 读取单条消息
func (ft *FileTopic) Read(offset uint64, partition uint) ([]byte, error) {
	if err := ft.checkPartition(partition); err != nil {
		return nil, err
	}
	return ft.Partitions[partition].ReadMsg(offset)
}

// 批量读取
func (ft *FileTopic) ReadMulti(offset uint64, partition uint, count uint32) ([][]byte, int) {
	if err := ft.checkPartition(partition); err != nil {
		return nil, 0
	}
	part := ft.Partitions[partition]
	sourceMsg, err := part.ReadMultiMsg(offset, count)
	if err != nil {
		if err == io.EOF {
			// 判断长度，小于一半就重新构建slice，否则直接返回
			if len(sourceMsg) < cap(sourceMsg) / 2 {
				newSourceMsg := make([][]byte, 0, len(sourceMsg))
				copy(newSourceMsg, sourceMsg)
				sourceMsg = newSourceMsg
			}
		} else {
			return nil, 0
		}
	}
	return sourceMsg, len(sourceMsg)
}

func (ft *FileTopic) OffsetLag(offset uint64, partition uint) (lag uint64) {
	if err := ft.checkPartition(partition); err != nil {
		return 0
	}
	return ft.Partitions[partition].LatestMsgOffset - offset
}

func (ft *FileTopic) Close() (err error) {
	for _, p := range ft.Partitions {
		err = p.Close()
		// TODO 待改进
		if err != nil {
			return
		}
	}
	return
}