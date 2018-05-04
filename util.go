package fqueue

import (
	"hash/crc32"
	"log"
	"os/user"
)

func Uint64ToByte(num uint64, bytes []byte) {
	for i := 0; i < 8; i++ {
		bytes[i] = byte(num >> uint((7-i)*8) & 0xff)
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
		bytes[i] = byte(num >> uint((3-i)*8) & 0xff)
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

func CalcCrc32(bytes []byte) uint32 {
	return crc32.ChecksumIEEE(bytes)
}

// out: C:\Users\xxx or /home/xxx
func HomePath() string {
	usr, err := user.Current()
	if err != nil {
		log.Fatal("get home path error")
	}
	return usr.HomeDir
}
