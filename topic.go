package fqueue

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
)

const (
	DEFAULT_BATCH_COUNT = 3
)

var LOST_MSG_ERR = errors.New("lost some message, can't append")

// 负责发送和接收
type Topic interface {
	// 写到指定的分区
	Write(msg *Msg, partition uint32) error
	// 批量写入同一分区，保证同时写入成功或者同时失败
	WriteMulti(msgs []*Msg, partition uint32) error
	// 读取单条消息
	Read(offset uint64, partition uint32) ([]byte, error)
	// 批量读取
	ReadMulti(offset uint64, partition, count uint32) ([][]byte, int)

	OffsetLag(offset uint64, partition uint32) uint64

	Close() error
}

type FileTopic struct {
	Name            string
	PartitionIds    []uint32
	PartitionNumber uint32
	Partitions      map[uint32]*FilePartition
	// 存放，可能没用
	//MsgChan         []chan *Msg
	// 一次读取的数量
	BatchCount uint32
}

type TopicConfig struct {
	Name         string
	PartitionIds []uint32
	BatchCount   uint32
	BasePath     string
}

func NewFileTopic(config *TopicConfig) (ft *FileTopic, err error) {

	if config == nil {
		panic("Not found configuration file")
	}

	ft = new(FileTopic)
	ft.Name = config.Name
	ft.PartitionIds = config.PartitionIds
	ft.PartitionNumber = uint32(len(ft.PartitionIds))
	ft.BatchCount = config.BatchCount
	basePath := config.BasePath

	partitionConfig := &PartitionConfig{
		Topic:    ft.Name,
		DataPath: basePath,
	}
	ft.Partitions = make(map[uint32]*FilePartition, ft.PartitionNumber)
	for _, id := range ft.PartitionIds {
		partitionConfig.Id = id
		log.Infof("[%s] partition: %v", ft.Name, partitionConfig)
		part, err := NewFilePartition(partitionConfig)
		ft.Partitions[id] = part
		if err != nil {
			log.Errorf("start topic error: %v", err)
			goto CLOSE
		}
	}
	log.Debugf("create topic %v success", ft)
	return
CLOSE:
	log.Info("close already start filePartitions")
	for _, fp := range ft.Partitions {
		if fp != nil {
			fp.Close()
		}
	}
	return
}

func (ft *FileTopic) checkPartition(partition uint32) error {
	if _, ok := ft.Partitions[partition]; !ok {
		return errors.New("wrong partition")
	}
	return nil
}

// 写到指定的分区
func (ft *FileTopic) Write(msg *Msg, partition uint32) (err error) {
	if err := ft.checkPartition(partition); err != nil {
		return err
	}
	if !ft.canAppend(msg.Offset, partition) {
		return LOST_MSG_ERR
	}
	return ft.Partitions[partition].WriteMsg(msg)
}

// 批量写入同一分区，保证同时写入成功或者同时失败
func (ft *FileTopic) WriteMulti(msgs []*Msg, partition uint32) error {
	if err := ft.checkPartition(partition); err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}
	if !ft.canAppend(msgs[0].Offset, partition) {
		return LOST_MSG_ERR
	}
	return ft.Partitions[partition].WriteMultiMsg(msgs)
}

// 批量写入同一分区，保证同时写入成功或者同时失败
func (ft *FileTopic) WriteMultiBytes(bytes [][]byte, partition uint32) error {
	if err := ft.checkPartition(partition); err != nil {
		return err
	}
	if len(bytes) == 0 {
		return errors.New("msgs must contain at least one msg")
	}
	msgs := make([]*Msg, len(bytes))
	for i, b := range bytes {
		msgs[i] = NewMessageFromSource(b)
	}
	return ft.WriteMulti(msgs, partition)
}

// 读取单条消息
func (ft *FileTopic) Read(offset uint64, partition uint32) ([]byte, error) {
	if err := ft.checkPartition(partition); err != nil {
		return nil, err
	}
	return ft.Partitions[partition].ReadMsg(offset)
}

// 批量读取
func (ft *FileTopic) ReadMulti(offset uint64, partition, count uint32) ([][]byte, int) {
	if err := ft.checkPartition(partition); err != nil {
		return nil, 0
	}
	part := ft.Partitions[partition]
	sourceMsg, err := part.ReadMultiMsg(offset, count)
	if err != nil && err != io.EOF {
		return nil, 0
	}
	if err != nil {
		log.Error(err)
	}
	// 判断长度，小于一半就重新构建slice，否则直接返回
	if len(sourceMsg) < cap(sourceMsg)/2 {
		newSourceMsg := make([][]byte, len(sourceMsg))
		copy(newSourceMsg, sourceMsg)
		sourceMsg = newSourceMsg
	}
	return sourceMsg, len(sourceMsg)
}

func (ft *FileTopic) OffsetLag(offset uint64, partition uint32) (lag uint64) {
	if err := ft.checkPartition(partition); err != nil {
		return 0
	}
	return ft.Partitions[partition].OffsetLag(offset)
}

func (ft *FileTopic) Close() (err error) {
	for _, p := range ft.Partitions {
		p.Close()
	}
	log.Infof("topic %v close", ft)
	return
}

// 判断offset是否可以append, true为可以, false不行
func (ft *FileTopic) canAppend(newOffset uint64, partition uint32) bool {
	return newOffset == 0 || ft.OffsetLag(newOffset, partition) == 0
}
