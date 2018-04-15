package fqueue

import (
	"sync"
	"golang.org/x/exp/mmap"
	"os"
	"fmt"
	"github.com/go-log/log"
	"errors"
	"time"
	"io/ioutil"
	"encoding/json"
)

const (
	IndexFilePostfix = "index"
	MsgFilePostfix   = "msg"
	MetaFilePostfix  = "meta"
)

// 为了
type Partition interface {
	// 写入新数据
	WriteMsg(*Msg) error

	WriteMultiMsg([]*Msg) error

	// 读取指定offset的数据
	ReadMsg(uint64) ([]byte, error)

	ReadMultiMsg(uint64, uint32) ([][]byte, error)

}

type Index interface {

	// 获取消息位置
	Position(offset uint64) (position uint64, length uint32, err error)

	// 写入消息时包含长度
	WriteIndex(offset, position uint64, length uint32) error

}

// 分区
// 需要写入消息，获取消息
// 不支持切换文件
type FilePartition struct {
	Id                uint

	Index             *FileIndex

	msgMutex          sync.Mutex
	// 最新消息的offset
	latestMsgOffset   uint64
	// 最新写入位置
	latestPosition uint64
	// 上一次写入的位置偏移量，用于rollback
	lastWritePositionOffset   uint
	// 上一次写入消息offset的偏移量， 用于rollback
	lastWriteMsgOffset uint

	msgWriter         *os.File
	// reader
	msgReader         *mmap.ReaderAt // 用于读取msg

	metaRWer          *os.File
}

// 保存 8byte offset， 8 byte position， 4 byte len = 20
type FileIndex struct {
	indexMutex        sync.Mutex
	// 最新保存的消息的索引
	latestIndexOffset uint64
	// 最后写入索引的位置
	latestPosition uint64
	// 上一次写入的偏移量
	lastWritePositionOffset uint
	// writer
	indexWriter       *os.File

	indexReader       *os.File
}

type PartitionConfig struct {
	Id          uint
	Topic       string
	DataPath    string
	SaveInterval time.Duration
}

type PartitionInfo struct {
	PartitionLatestPosition          uint64 `json:"partition.latest_position"`
	PartitionLatestMsgOffset         uint64 `json:"partition.latest_msg_offset"`
	PartitionLastWritePositionOffset uint   `json:"partition.last_write_position_offset"`
	PartitionLastWriteMsgOffset      uint   `json:"partition.last_write_msg_offset"`
	IndexLatestPosition              uint64 `json:"index.latest_position"`
	IndexLatestIndexOffset           uint64 `json:"index.latest_index_offset"`
	IndexLastWritePositionOffset     uint `json:"index.last_write_position_offset"`
}

func DefaultPartitionConfig(id uint, topic string) *PartitionConfig {
	return &PartitionConfig{
		Id: id,
		Topic: topic,
		DataPath: HomePath() + "/fqueue",
		SaveInterval: 500 * time.Millisecond, // 500ms
	}
}

// TODO 当第二次启动时应先读取偏移量(在文件中保存)， 保存index和msg的最新位置和索引
func NewFilePartition(id uint) (fp *FilePartition, err error) {
	fp = new(FilePartition)

	config := DefaultPartitionConfig(id, "temp-topic")
	// 创建文件夹
	path := fmt.Sprintf("%s/%s/partition-%d", config.DataPath, config.Topic,  config.Id)

	name := fmt.Sprintf("%s/partition-%d", path, config.Id)

	err = os.MkdirAll(path, 0755)

	if err != nil {
		log.Logf("create partition dir failed: %v", err)
		return
	}
	// msg
	msgFileName := fmt.Sprintf("%s.%s", name, MsgFilePostfix)
	fp.msgWriter, err = os.OpenFile(msgFileName, os.O_CREATE | os.O_WRONLY, 0755)
	if err != nil {
		log.Logf("open msg write file error: %v", err)
		return
	}

	fp.msgReader, err = mmap.Open(msgFileName)
	if err != nil {
		log.Logf("open mmap msg read file error: %v", err)
		return
	}

	// index
	fp.Index = new(FileIndex)
	indexFileName := fmt.Sprintf("%s.%s", name, IndexFilePostfix)

	fp.Index.indexWriter, err = os.OpenFile(indexFileName, os.O_CREATE | os.O_WRONLY, 0755);
	if err != nil {
		log.Logf("open index write file error: %v", err)
		return
	}

	fp.Index.indexReader, err = os.OpenFile(indexFileName, os.O_CREATE | os.O_RDONLY, 0755);
	if err != nil {
		log.Logf("open index read file error: %v", err)
		return
	}

	metaFileName := fmt.Sprintf("%s.%s", name, MetaFilePostfix)

	fp.metaRWer, err = os.OpenFile(metaFileName, os.O_CREATE | os.O_RDWR, 0755)
	if err != nil {
		log.Logf("open meta file error: %v", err)
	}
	fp.recoveryInfo()

	return
}

// index: 8byte offset 8 byte realPosition 4 byte len(msg len)
// 0xffffffffffffffff 0x0000000000000000 0xffffffff
func (idx *FileIndex) write(offset, position uint64, length uint32) (err error) {
	// 清空上一次的偏移量，防止错误rollback
	idx.lastWritePositionOffset = 0
	// 不允许写入比当前偏移值小的偏移值
	if offset < idx.latestIndexOffset {
		return errors.New(fmt.Sprintf("forbid write %s small then %d offset index", offset, idx.latestIndexOffset))
	}
	idx.indexMutex.Lock()
	defer idx.indexMutex.Unlock()
	bytes := make([]byte, 20)
	Uint64ToByte(offset, bytes[:8])
	Uint64ToByte(position, bytes[8:16])
	Uint32ToByte(length, bytes[16:])
	_, err = idx.indexWriter.WriteAt(bytes, int64(idx.latestPosition))
	if err != nil {
		return
	}
	idx.latestIndexOffset = offset + 1
	idx.latestPosition += 20
	idx.lastWritePositionOffset = 20
	return
}

// 撤销上一次的写入操作
func (idx *FileIndex) rollback() (err error) {
	idx.latestPosition -= uint64(idx.lastWritePositionOffset)
	idx.latestIndexOffset -= uint64(idx.lastWritePositionOffset / 20)
	return
}

func (idx *FileIndex) read(off uint64) (offset, position uint64, length uint32, err error) {
	if off > idx.latestIndexOffset {
		err = errors.New(fmt.Sprintf("%d large then the latest offset %d in this partition", offset, idx.latestIndexOffset))
		return
	}
	dOff := (idx.latestIndexOffset - off) * 20
	realAtIndex := idx.latestPosition - dOff
	bytes := make([]byte, 20)

	_, err = idx.indexReader.ReadAt(bytes, int64(realAtIndex))
	if err != nil {
		return
	}
	offset = ByteToUint64(bytes[:8])
	position = ByteToUint64(bytes[8:16])
	length = ByteToUint32(bytes[16:])
	return
}

func (idx *FileIndex) Position(offset uint64) (position uint64, length uint32, err error) {
	_, position, length, err = idx.read(offset)
	return
}

func (idx *FileIndex) WriteIndex(offset, position uint64, length uint32) (err error) {
	return idx.write(offset, position, length)
}

func (idx *FileIndex) Close() (err error) {
	err = idx.indexWriter.Close()
	if err != nil {
		return
	}
	err = idx.indexReader.Close()
	return
}

func (idx *FileIndex) String() string {
	return fmt.Sprintf("FileIndex: {latestIndexOffset: %d, latestPosition: %d, lastPositionOffset: %d}",
		idx.latestIndexOffset, idx.latestPosition, idx.lastWritePositionOffset)
}

// ---------------------------------------------------------------------------------------------------------------------

// TODO 使用raft时，会接收到重做的log，这时候msg中会携带offset，所以应该支持能够写入带offset的msg
func (p *FilePartition) write(msg []byte) (err error) {
	p.lastWritePositionOffset = 0
	p.lastWriteMsgOffset = 0
	if len(msg) < MSG_HEADER_LENGTH {
		err = errors.New("unsupported msg")
		return
	}
	p.msgMutex.Lock()
	defer p.msgMutex.Unlock()
	// 直接写入
	le, err := p.msgWriter.WriteAt(msg, int64(p.latestPosition))
	if err != nil {
		return
	}
	p.latestPosition += uint64(le)
	p.latestMsgOffset += 1

	p.lastWritePositionOffset = uint(le)
	p.lastWriteMsgOffset = 1
	return
}

// TODO 如何解决批量插入索引的问题,
// 如果实现，必须要返回每个消息的position，offset，但是此方法并不会读取offset，应交由上层调用者来实现
func (p *FilePartition) writeMulti(msgs [][]byte) (err error) {
	for _, m := range msgs {
		if len(m) < MSG_HEADER_LENGTH {
			err = errors.New("unsupported msg")
			return
		}
	}
	p.msgMutex.Lock()
	defer p.msgMutex.Unlock()
	oldPosition := p.latestPosition
	for _, m := range msgs {
		l, err := p.msgWriter.WriteAt(m, int64(p.latestPosition))
		if err != nil {
			// 发生错误, 回滚写入位置
			p.latestPosition = oldPosition
			return err
		}
		p.latestPosition += uint64(l)
	}
	p.lastWritePositionOffset = uint(p.latestPosition - oldPosition)
	p.lastWriteMsgOffset = uint(len(msgs))
	return
}

func (p *FilePartition) read(startPosition uint64, msg []byte) (err error) {
	_, err = p.msgReader.ReadAt(msg, int64(startPosition))
	return
}

func (p *FilePartition) rollback() {
	p.latestMsgOffset -= uint64(p.lastWriteMsgOffset)
	p.latestPosition -= uint64(p.lastWritePositionOffset)
}

// 写入新数据
// 保留的offset和position均为下一条消息的起点
func (p *FilePartition) WriteMsg(msg *Msg) (err error) {
	// offset这个时候生成
	offset := p.latestMsgOffset
	position := p.latestPosition
	length := uint32(len(msg.Source))

	msg.Offset = offset
	log.Log(offset)
	for i := 0; i < 8; i++ {
		msg.Source[i] = byte(offset >> uint((7 - i) * 8) & 0xff)
	}
	err = p.write(msg.Source)
	if err != nil {
		p.rollback()
		return
	}
	err = p.Index.WriteIndex(offset, position, length)
	if err != nil {
		p.Index.rollback()
		p.rollback()
	}
	return
}

func (p *FilePartition) WriteMultiMsg(msgs []*Msg) (err error) {
	// 暂不实现
	return
}

// 读取指定offset的数据
func (p *FilePartition) ReadMsg(offset uint64) (msg []byte, err error) {
	position, length, err := p.Index.Position(offset)
	if err != nil {
		return
	}
	msg = make([]byte, length)
	err = p.read(position, msg)
	return
}

func (p *FilePartition) ReadMultiMsg(position uint64, length uint32) (msgs [][]byte, err error) {
	// 暂不实现
	return
}

func (p *FilePartition) saveInfo() {
	// 在空闲时间进行写入
	p.msgMutex.Lock()
	defer p.msgMutex.Unlock()
}

func (p *FilePartition) recoveryInfo() (err error) {
	// 启动时调用
	bytes, err := ioutil.ReadAll(p.metaRWer)
	if err != nil || len(bytes) == 0 {
		return
	}
	var info PartitionInfo
	err = json.Unmarshal(bytes, &info)
	if err != nil {
		log.Log("recovery info failed, file content: %s", string(bytes))
		return
	}
	p.latestPosition = info.PartitionLatestPosition
	p.latestMsgOffset = info.PartitionLatestPosition
	p.lastWritePositionOffset = info.PartitionLastWritePositionOffset
	p.lastWriteMsgOffset = info.PartitionLastWriteMsgOffset
	p.Index.latestPosition = info.IndexLatestPosition
	p.Index.latestIndexOffset = info.IndexLatestIndexOffset
	p.Index.lastWritePositionOffset = info.IndexLastWritePositionOffset
	return
}

func (p *FilePartition) Close() (err error) {
	err = p.Index.Close()
	if err != nil {
		return
	}
	err = p.msgReader.Close()
	if err != nil {
		return
	}
	err = p.msgWriter.Close()
	return
}

func (p *FilePartition) String() string {
	return fmt.Sprintf("FilePartition: {" +
		"id: %d, latestMsgOffset: %d, lastestPosition: %d, lastMsgOffset: %d, lastPositionOffset: %d, Index: %v}",
			p.Id, p.latestMsgOffset, p.latestPosition, p.lastWriteMsgOffset, p.lastWritePositionOffset, p.Index)
}