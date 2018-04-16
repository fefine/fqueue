package fqueue

type Topic interface {
	// msg, partition
	Write(msg *Msg, partition uint) error
	// 批量写入同一分区，保证同时写入成功或者同时失败
	WriteMulti(msgs []*Msg, partition uint) error
	Read(offset uint64) ([]byte, error)
}
