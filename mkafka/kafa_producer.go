package mkafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewKafkaProducer(config *KafkaConfig) (*kafka.Producer, error) {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%s", config.Host, config.Port),
	}
	kafkaC, err := kafka.NewProducer(&configMap)
	return kafkaC, err
}
