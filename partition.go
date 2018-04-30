package fqueue

import (
	"sync"
	"golang.org/x/exp/mmap"
	"os"
	"fmt"
	log "github.com/sirupsen/logrus"
	"errors"
)

const (
	IndexFilePostfix = "index"
	MsgFilePostfix   = "msg"
)

// 为了
type Partition interface {
	// 写入新数据
	WriteMsg(*Msg) error

	WriteMultiMsg([]*Msg) error

	// 读取指定offset的数据
	ReadMsg(uint64) ([]byte, error)

	ReadMultiMsg(uint64, uint32) ([][]byte, error)

	OffsetLag(uint64) uint64

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
	Id                uint32

	Index             *FileIndex

	msgMutex          sync.Mutex
	// 最新消息的offset
	LatestMsgOffset uint64
	// 最新写入位置
	//latestPosition uint64
	// 上一次写入的位置偏移量，用于rollback
	lastWritePositionOffset   uint32
	// 上一次写入消息offset的偏移量， 用于rollback
	lastWriteMsgOffset uint32

	msgWriter         *os.File
	// reader
	msgReader         *mmap.ReaderAt // 用于读取msg

	metaRWer          *os.File
}

// 保存 8byte offset， 8 byte position， 4 byte len = 20
type FileIndex struct {
	indexMutex        sync.Mutex
	// 最新保存的消息的索引
	//latestIndexOffset uint64
	// 最后写入索引的位置
	//latestPosition uint64
	// 上一次写入的偏移量
	lastWritePositionOffset uint32
	// writer
	indexWriter       *os.File

	indexReader       *os.File
}

type PartitionConfig struct {
	Id          uint32
	Topic       string
	DataPath    string
}


// TODO 增加file lock，防止冲突
func NewFilePartition(config *PartitionConfig) (fp *FilePartition, err error) {

	if config == nil {
		panic(errors.New("not found configuration file"))
	}

	fp = new(FilePartition)
	fp.Id = config.Id

	// 创建文件夹
	path := fmt.Sprintf("%s/%s/partition-%d", config.DataPath, config.Topic,  config.Id)

	name := fmt.Sprintf("%s/partition-%d", path, config.Id)

	err = os.MkdirAll(path, 0755)

	if err != nil {
		log.Errorf("create partition dir failed: %v", err)
		return
	}
	// msg
	msgFileName := fmt.Sprintf("%s.%s", name, MsgFilePostfix)
	if _, err := os.Stat(msgFileName); os.IsNotExist(err) {
		if _, err = os.Create(msgFileName); err != nil {
			log.Errorf("create msg file %s failed, error: %v)", msgFileName, err)
			return nil, err
		}
	}
	fp.msgWriter, err = os.OpenFile(msgFileName, os.O_WRONLY | os.O_APPEND, 0755)
	if err != nil {
		log.Errorf("open msg write file error: %v", err)
		return
	}

	// 当文件未创建的时候，会报错
	fp.msgReader, err = mmap.Open(msgFileName)
	if err != nil {
		log.Errorf("open mmap msg read file error: %v", err)
		return
	}

	// index
	fp.Index = new(FileIndex)
	indexFileName := fmt.Sprintf("%s.%s", name, IndexFilePostfix)

	fp.Index.indexWriter, err = os.OpenFile(indexFileName, os.O_CREATE | os.O_WRONLY | os.O_APPEND, 0755);
	if err != nil {
		log.Errorf("open index write file error: %v", err)
		return
	}

	fp.Index.indexReader, err = os.OpenFile(indexFileName, os.O_CREATE | os.O_RDONLY, 0755);
	if err != nil {
		log.Errorf("open index read file error: %v", err)
		return
	}

	fp.recovery()

	log.Debugf("create partition: %v success", fp)

	return
}

// index: 8byte offset 8 byte realPosition 4 byte len(msg len)
// 0xffffffffffffffff 0x0000000000000000 0xffffffff
func (idx *FileIndex) write(offset, position uint64, length uint32) (err error) {
	// 清空上一次的偏移量，防止错误rollback
	bytes := make([]byte, 20)
	Uint64ToByte(offset, bytes[:8])
	Uint64ToByte(position, bytes[8:16])
	Uint32ToByte(length, bytes[16:])
	idx.indexWriter.Seek(int64(offset * 20), 0)
	_, err = idx.indexWriter.Write(bytes)
	if err != nil {
		return
	}
	return
}

// 撤销上一次的写入操作
func (idx *FileIndex) rollback() (err error) {
	_, err = idx.indexWriter.Seek(int64(-idx.lastWritePositionOffset), 1)
	return
}

func (idx *FileIndex) read(off uint64) (offset, position uint64, length uint32, err error) {
	bytes := make([]byte, 20)
	_, err = idx.indexReader.ReadAt(bytes, int64(off * 20))
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
	// 先找位置
	idx.indexMutex.Lock()
	defer idx.indexMutex.Unlock()

	idx.lastWritePositionOffset = 0
	err = idx.write(offset, position, length)
	if err != nil {
		return
	}
	idx.lastWritePositionOffset = 20
	return
}

func (idx *FileIndex) Close() (err error) {
	err = idx.indexWriter.Close()
	if err != nil {
		return
	}
	err = idx.indexReader.Close()
	return
}

func (idx *FileIndex) recovery() (offset, position uint64, length uint32, err error) {
	// 这里write seek到文件末尾
	size, err := idx.indexWriter.Seek(0, 2)
	if err != nil || size == 0 {
		return
	}

	bytes := make([]byte, 20)
	_, err = idx.indexReader.ReadAt(bytes, size - 20)
	if err != nil {
		return
	}
	offset = ByteToUint64(bytes[:8])
	position = ByteToUint64(bytes[8:16])
	length = ByteToUint32(bytes[16:])
	return
}

func (idx *FileIndex) String() string {
	return fmt.Sprintf("lastPositionOffset: %d", idx.lastWritePositionOffset)
}

// ---------------------------------------------------------------------------------------------------------------------

func (p *FilePartition) write(msg []byte) (err error) {
	if len(msg) < MSG_HEADER_LENGTH {
		err = errors.New("unsupported msg")
		return
	}
	// 直接写入
	_, err = p.msgWriter.Write(msg)
	return
}

func (p *FilePartition) writeMulti(msgs [][]byte) (err error) {
	// 如果实现，必须要返回每个消息的position，offset，但是此方法并不会读取offset，应交由上层调用者来实现
	for _, m := range msgs {
		if len(m) < MSG_HEADER_LENGTH {
			err = errors.New("unsupported msg")
			return
		}
	}

	p.lastWriteMsgOffset = 0
	p.lastWritePositionOffset = 0
	p.Index.lastWritePositionOffset = 0

	position, _ := p.msgWriter.Seek(0, 1)
	for _, m := range msgs {
		l, err := p.msgWriter.Write(m)
		if err != nil {
			return err
		}
		p.lastWriteMsgOffset += 1
		p.lastWritePositionOffset += uint32(l)
		p.lastWriteMsgOffset += 1
		// index
		p.Index.write(p.LatestMsgOffset, uint64(position), uint32(l))
		p.Index.lastWritePositionOffset += 20
		position += int64(l)
		p.LatestMsgOffset += 1
	}
	return
}

func (p *FilePartition) read(startPosition uint64, msg []byte) (err error) {
	_, err = p.msgReader.ReadAt(msg, int64(startPosition))
	return
}

// 用来撤销上一次操作, 当读写同时进行时，不能保证撤销成功，因此不对外提供
func (p *FilePartition) rollback() {
	p.LatestMsgOffset -= uint64(p.lastWriteMsgOffset)
	p.msgWriter.Seek(int64(-p.lastWritePositionOffset), 1)
	p.Index.rollback()
}

// 写入新数据
// 保留的offset和position均为下一条消息的起点
func (p *FilePartition) WriteMsg(msg *Msg) (err error) {

	p.msgMutex.Lock()
	defer p.msgMutex.Unlock()

	p.lastWritePositionOffset = 0
	p.lastWriteMsgOffset = 0

	// offset这个时候生成
	offset := p.LatestMsgOffset
	position, _ := p.msgWriter.Seek(0, 1)
	length := msg.Size
	// 如果为0，认为并未提供offset，
	if msg.Offset == 0 {
		msg.Offset = offset
		Uint64ToByte(offset, msg.Source[:8])
	} else {
		// 可以小于，不能大于
		if msg.Offset > offset {
			return errors.New(fmt.Sprintf("wrong offset %d, large then current offset %d"))
		}
		offset = msg.Offset
		// 此pos必定存在
		_, pos, _, err := p.Index.read(offset)
		if err != nil {
			panic(errors.New("wrong offset"))
		}
		p.msgWriter.Seek(int64(pos), 0)
	}
	err = p.write(msg.Source)
	if err != nil {
		return
	}

	p.lastWritePositionOffset = msg.Size
	p.lastWriteMsgOffset = 1
	p.LatestMsgOffset = offset + 1

	err = p.Index.WriteIndex(offset, uint64(position), length)
	if err != nil {
		// 当写入index错误时，其中的offset均为0，不需要rollback
		p.rollback()
	}
	return
}

func (p *FilePartition) WriteMultiMsg(msgs []*Msg) (err error) {
	if len(msgs) <= 0 {
		return
	}

	p.msgMutex.Lock()
	defer p.msgMutex.Unlock()

	// 先补上offset
	if msgs[0].Offset == 0 {
		for i, m := range msgs {
			m.Offset = p.LatestMsgOffset + uint64(i)
			Uint64ToByte(m.Offset, m.Source[:8])
		}
	}
	sources := make([][]byte, len(msgs))
	for i, e := range msgs {
		sources[i] = e.Source
	}
	err = p.writeMulti(sources)
	if err != nil {
		p.Index.rollback()
		p.rollback()
	}
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

// 当数量不够时，读取多少返回多少
func (p *FilePartition) ReadMultiMsg(offset uint64, size uint32) (msgs [][]byte, err error) {
	msgs = make([][]byte, size)
	for i := 0; i < int(size); i++ {
		msgs[i], err = p.ReadMsg(offset + uint64(i))
		if err != nil {
			break
		}
	}
	return
}

func (p *FilePartition) Close() (err error) {
	err = p.Index.Close()
	p.msgReader.Close()
	err = p.msgWriter.Close()
	log.Infof("partition %v close", p)
	return
}

func (p *FilePartition) recovery() (err error) {
	offset, position, length, err := p.Index.recovery()
	if err != nil || (offset == 0 && position == 0 && length == 0) {
		return
	}
	_, err = p.msgWriter.Seek(int64(position + uint64(length)), 0)
	p.LatestMsgOffset = offset + 1
	log.Infof("[recovery] offset: %d, position: %d", offset, position)
	return
}

func (p *FilePartition) OffsetLag(offset uint64) uint64 {
	return p.LatestMsgOffset - offset
}

func (p *FilePartition) String() string {
	return fmt.Sprintf("FilePartition: {" +
		"id: %d, LatestMsgOffset: %d, lastMsgOffset: %d, lastPositionOffset: %d, Index: %v}",
			p.Id, p.LatestMsgOffset, p.lastWriteMsgOffset, p.lastWritePositionOffset, p.Index)
}