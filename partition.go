package fqueue

import (
	"sync"
	"golang.org/x/exp/mmap"
	"os"
	"fmt"
	"github.com/go-log/log"
	"errors"
)

const (
	INDEX_FILE_POSTFIX = "index"
	MSG_FILE_POSTFIX   = "msg"
)

// 为了
type Partition interface {
	// 写入新数据
	WriteMsg(*Msg) error

	WriteMultiMsg([]*Msg) error

	// 读取指定offset的数据
	ReadMsg(offset int) ([]byte, error)

	ReadMultiMsg(startOffset, msgLen int) ([][]byte, error)

}

type Index interface {

	// 获取消息位置
	Position(offset uint64) (uint64, uint32, error)

	// 写入消息时包含长度
	WriteIndex(uint64, uint64, uint32)

}

// 分区
// 需要写入消息，获取消息
// 不支持切换文件
type FilePartition struct {
	Name              string

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

}

// 保存 8byte offset， 8 byte position， 4 byte len = 20
type FileIndex struct {
	Name              string

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

// TODO 当第二次启动时应先读取偏移量(建议保存在raft中统一保存)， 保存index和msg的最新位置和索引
func NewFilePartition(name string) (fp *FilePartition, err error) {
	fp = new(FilePartition)

	// msg
	fp.Name = fmt.Sprintf("%s.%s", name, MSG_FILE_POSTFIX)
	fp.msgWriter, err = os.OpenFile(fp.Name, os.O_CREATE | os.O_WRONLY, 0755)
	if err != nil {
		log.Logf("open msg write file error: %v", err)
		return
	}

	fp.msgReader, err = mmap.Open(fp.Name)
	if err != nil {
		log.Logf("open mmap msg read file error: %v", err)
		return
	}

	// index
	fp.Index = new(FileIndex)
	fp.Index.Name = fmt.Sprintf("%s.%s", name, INDEX_FILE_POSTFIX)

	fp.Index.indexWriter, err = os.OpenFile(fp.Index.Name, os.O_CREATE | os.O_WRONLY, 0755);
	if err != nil {
		log.Logf("open index write file error: %v", err)
		return
	}

	fp.Index.indexReader, err = os.OpenFile(fp.Index.Name, os.O_CREATE | os.O_RDONLY, 0755);
	if err != nil {
		log.Logf("open index read file error: %v", err)
		return
	}

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
	return fmt.Sprintf("FileIndex: {name: %s, latestIndexOffset: %d, latestPosition: %d, lastPositionOffset: %d}",
		idx.Name, idx.latestIndexOffset, idx.latestPosition, idx.lastWritePositionOffset)
}

// ---------------------------------------------------------------------------------------------------------------------

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
		"name: %s, latestMsgOffset: %d, lastestPosition: %d, lastMsgOffset: %d, lastPositionOffset: %d, Index: %v}",
			p.Name, p.latestMsgOffset, p.latestPosition, p.lastWriteMsgOffset, p.lastWritePositionOffset, p.Index)
}