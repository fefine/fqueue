package fqueue

const (
	BROKER_FORMATER      = "/brokers/ids/%s"
	TOPIC_PATTERN_PREFIX = "/brokers/topics/"
	LEASE_TTL            = 1
)

type EtcdTopic struct {
	Version    uint32              `json:"version"`
	Partitions map[uint32][]string `json:"partitions"`
}

type EtcdBroker struct {
	Version uint32 `json:"version"`
	Address string `json:"address"`
}

// consumer订阅的topic和partition
type EtcdConsumer struct {
	Version      uint32              `json:"version"`
	Subscription map[string][]uint32 `json:"subscription"`
}

// consumer group订阅的topics
type EtcdConsumerGroup struct {
	Version uint32
	Topics  []string
}
