package mkafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewKafkaConsumer(config *KafkaConfig) (*kafka.Consumer, error) {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", config.Host, config.Port),
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
