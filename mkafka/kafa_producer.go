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
	//
	// Consumer-only keys are skipped: ExtConfig is shared with the consumer, so
	// without the filter librdkafka logs a CONFWARN per key and ignores it.
	applyExtConfig(configMap, config.ExtConfig, true)

	kafkaC, err := kafka.NewProducer(&configMap)
	if err == nil {
		log.Println("[Kafka] Connected to Kafka Producer", config.Hosts)
	}
	return kafkaC, err
}
