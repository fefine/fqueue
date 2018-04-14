package fqueue

import (
	"fmt"
)

const (
	MSG_HEADER_LENGTH = 22
	INDEX_LENGTH = 20
)

// 包含消息
type Msg struct {
	Offset   uint64  // 8 byte
	Size     uint32  // 4 byte
	Crc      uint32  // 4 byte
	Type     byte   // 1 byte
	Version  byte   // 1 byte
	KeyLen   uint32  // 4 byte  // fixed_header_length = 22
	Key      []byte // KeyLen byte
 	Value    []byte // Size = fixed_header_length + keyLen + ValueLen
 	Source   []byte
}

// 消息索引
type MsgIndex struct {
	Offset   uint64
	Position uint64
	Len      uint32
}

func NewMessage(key, value []byte) (msg *Msg) {
	msg = &Msg{
		Size: uint32(len(key) + len(value) + MSG_HEADER_LENGTH),
		Crc: 0,
		Type: 0,
		Version: 0,
		KeyLen: uint32(len(key)),
		Value: value,
	}
	source := make([]byte, 0, msg.Size)
	Uint64ToByte(msg.Offset, source[:8])
	Uint32ToByte(msg.Size, source[8:12])
	Uint32ToByte(msg.Crc, source[12:16])
	source[16] = msg.Type
	source[17] = msg.Version
	Uint32ToByte(msg.KeyLen, source[18:22])
	source = append(source[22:22 + msg.KeyLen], key...)
	source = append(source[22 + msg.KeyLen:], value...)
	msg.Source = source
	return
}

func NewMessageFromSource(source []byte) (msg *Msg) {
	msg = new(Msg)
	msg.Source = source
	msg.Offset = ByteToUint64(source[:8])
	msg.Size = ByteToUint32(source[8:12])
	msg.Crc = ByteToUint32(source[12:16])
	msg.Type = source[16]
	msg.Version = source[17]
	msg.KeyLen = ByteToUint32(source[18:22])
	msg.Key = source[22:msg.KeyLen +22]
	msg.Value = source[msg.KeyLen + 22:]
	return msg
}

func (m *Msg) String() string {
	return fmt.Sprintf("MSG{" +
		"offset: %d, size: %d, " +
		"crc: %v, type: %d, " +
		"version: %v, keyLen: %d, " +
		"key: %s, value: %s}",
		m.Offset, m.Size,
		m.Crc, m.Type,
		m.Version, m.KeyLen,
		string(m.Key), string(m.Value))
}