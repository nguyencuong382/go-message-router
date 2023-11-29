package mkafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConfig struct {
	Host  string
	Port  int
	Group *string
}

func NewKafkaConsumer(config *KafkaConfig) (*kafka.Consumer, error) {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%d", config.Host, config.Port),
	}

	if config.Group != nil {
		configMap["group.id"] = *config.Group
	}

	kafkaC, err := kafka.NewConsumer(&configMap)
	return kafkaC, err
}
