package mkafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"strings"
)

func NewKafkaProducer(config *KafkaConfig) (*kafka.Producer, error) {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Hosts, ","),
	}
	kafkaC, err := kafka.NewProducer(&configMap)
	if err == nil {
		log.Println("[Kafka] Connected to Kafka Producer", config.Hosts)
	}
	return kafkaC, err
}
