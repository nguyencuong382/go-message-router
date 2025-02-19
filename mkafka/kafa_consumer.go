package mkafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConfig struct {
	Host            string
	Port            int
	Group           *string
	AutoOffsetReset *string
	ExtConfig       map[string]interface{}
	Topics          []string
	ManualCommit    bool
	KeyPrefix       *string
}

func NewKafkaConsumer(config *KafkaConfig) (*kafka.Consumer, error) {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%d", config.Host, config.Port),
		"auto.offset.reset": "earliest",
	}

	if config.AutoOffsetReset != nil {
		configMap["auto.offset.reset"] = *config.AutoOffsetReset
	}

	if config.Group != nil {
		configMap["group.id"] = *config.Group
	}

	if config.ManualCommit {
		configMap["enable.auto.offset.store"] = false
		configMap["enable.auto.commit"] = false
	}

	for k, v := range config.ExtConfig {
		configMap[k] = v
	}

	kafkaC, err := kafka.NewConsumer(&configMap)
	return kafkaC, err
}
