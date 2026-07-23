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

	// Apply extra librdkafka options (e.g. security.protocol / ssl.* for a TLS proxy
	// like kroxylicious). Mirrors NewKafkaConsumer — without this the producer could
	// never enable SSL, only the consumer could.
	for k, v := range config.ExtConfig {
		configMap[k] = v
	}

	kafkaC, err := kafka.NewProducer(&configMap)
	if err == nil {
		log.Println("[Kafka] Connected to Kafka Producer", config.Hosts)
	}
	return kafkaC, err
}
