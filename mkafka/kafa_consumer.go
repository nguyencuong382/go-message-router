package mkafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"strings"
)

func NewKafkaConsumer(config *KafkaConfig) (*kafka.Consumer, error) {
	configMap := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Hosts, ","),
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
	if err == nil {
		log.Println("[Kafka] Connected to Kafka Consumer", config.Hosts, "- csg:", configMap["group.id"])
	}
	return kafkaC, err
}
