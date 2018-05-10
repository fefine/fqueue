package fqueue

import (
	"hash/crc32"
	"log"
	"os/user"
)

// Mixing constants; generated offline.
const (
	M = 0x5bd1e995
	R = 24
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

// 32-bit mixing function.
func mmix(h uint32, k uint32) (uint32, uint32) {
	k *= M
	k ^= k >> R
	k *= M
	h *= M
	h ^= k
	return h, k
}

// The original MurmurHash2 32-bit algorithm by Austin Appleby.
func MurmurHash2(data []byte) (h uint32) {
	var k uint32
	var seed uint32 = 0x9747b28c
	// Initialize the hash to a 'random' value
	h = seed ^ uint32(len(data))

	// Mix 4 bytes at a time into the hash
	for l := len(data); l >= 4; l -= 4 {
		k = uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		h, k = mmix(h, k)
		data = data[4:]
	}

	// Handle the last few bytes of the input array
	switch len(data) {
	case 3:
		h ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[0])
		h *= M
	}

	// Do a few final mixes of the hash to ensure the last few bytes are well incorporated
	h ^= h >> 13
	h *= M
	h ^= h >> 15

	return
}
