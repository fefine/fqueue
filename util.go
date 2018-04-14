package fqueue

import (
	"encoding/binary"
	"errors"
)

func BytesToIndex(bytes []byte) (offset, position uint64, l uint32, err error) {
	if len(bytes) != 20 {
		err = errors.New("bytes len must be 20")
		return
	}
	offset = binary.LittleEndian.Uint64(bytes[:8])
	position = binary.LittleEndian.Uint64(bytes[8:16])
	l = binary.LittleEndian.Uint32(bytes[16:])
	return
}

func Uint64ToByte(num uint64, bytes []byte) {
	for i := 0; i < 8; i++ {
		bytes[i] = byte(num >> uint((7 - i) * 8) & 0xff)
	}
}

func ByteToUint64(bytes []byte) (num uint64) {
	for i := 0; i < 8; i++ {
		num <<= 8
		num |= uint64(bytes[i])
	}
	return
}

func Uint32ToByte(num uint32, bytes []byte) {
	for i := 0; i < 4; i++ {
		bytes[i] = byte(num >> uint((4 - i) * 8) & 0xff)
	}
}

func ByteToUint32(bytes []byte) (num uint32) {
	for i := 0; i < 4; i++ {
		num <<= 8
		num |= uint32(bytes[i])
	}
	return
}

func UintToByte(num uint, bytes []byte) {
	Uint32ToByte(uint32(num), bytes)
}

func ByteToInt(bytes []byte) uint {
	return uint(ByteToUint32(bytes))
}
