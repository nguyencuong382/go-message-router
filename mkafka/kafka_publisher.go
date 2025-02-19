package mkafka

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
)

type kafkaPub struct {
	kafkaProducer *kafka.Producer
	config        *KafkaConfig
}

type KafkaPublishArgs struct {
	dig.In
	KafkaProducer *kafka.Producer
	Config        *KafkaConfig
}

func NewKafkaPublisher(args KafkaPublishArgs) mrouter.IPublisher {
	return &kafkaPub{
		kafkaProducer: args.KafkaProducer,
		config:        args.Config,
	}
}

func (_this *kafkaPub) Publish(req *mrouter.PublishReq) error {
	var b1ByteValue []byte
	var err error
	if req.Json {
		b1ByteValue, err = json.Marshal(req.Value)
		if err != nil {
			return err
		}
	} else {
		b1ByteValue = req.Value.([]byte)
	}

	topic := req.Channel
	if _this.config.KeyPrefix != nil {
		topic = fmt.Sprintf("%s-%s", *_this.config.KeyPrefix, topic)
	}

	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          b1ByteValue,
	}

	if req.ID != "" {
		msg.Key = []byte(req.ID)
	}

	err = _this.kafkaProducer.Produce(&msg, nil)

	if err != nil {
		return err
	}

	// Wait for message deliveries before shutting down
	_this.kafkaProducer.Flush(15 * 1000)

	return nil
}
