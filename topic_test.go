package fqueue

import (
	"fmt"
	"testing"
	"github.com/coreos/etcd/pkg/testutil"
)

func DefaultTopicConfig(name string, partitions []uint) *TopicConfig {
	return &TopicConfig{
		Name:name,
		PartitionIds: partitions,
		BatchCount:DEFAULT_BATCH_COUNT,
		BasePath: fmt.Sprintf("%s/fqueue/%s", HomePath(), name),
	}
}

func NewTopic(t *testing.T) *FileTopic {
	config := DefaultTopicConfig("demo-topic", []uint{1, 3, 5})
	topic, err := NewFileTopic(config)
	if err != nil {
		t.Fatal(err)
	}
	return topic
}

func TestNewFileTopic(t *testing.T) {
	topic := NewTopic(t)
	t.Log(topic)
	defer topic.Close()
}

func TestFileTopic_Write(t *testing.T) {
	topic := NewTopic(t)
	defer topic.Close()
	msgs := BuildMultiMsg(t, 100, 1000)
	// write multi
	for _, p := range []uint{1, 3, 5} {
		NoError(t, topic.WriteMulti(msgs, p))
	}

	// write single

	for _, p := range []uint{1, 3, 5} {
		for _, m := range msgs {
			NoError(t, topic.Write(m, p))
		}
	}

}

func TestFileTopic_Read(t *testing.T) {
	topic := NewTopic(t)
	defer topic.Close()

	for _, p := range []uint{1, 3, 5} {
		msgs, l := topic.ReadMulti(0, p, 1000)
		testutil.AssertEqual(t, l, 1000)
		for i := 0; i < 1000; i++ {
			m, err := topic.Read(uint64(i), p)
			NoError(t, err)
			testutil.AssertEqual(t, msgs[i], m)
		}
	}
}