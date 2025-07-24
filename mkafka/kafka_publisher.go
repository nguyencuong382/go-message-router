package mkafka

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
	"log"
	"time"
)

type kafkaPub struct {
	kafkaProducer *kafka.Producer
	config        *mrouter.PubsubConfig
}

type KafkaPublishArgs struct {
	dig.In
	KafkaProducer *kafka.Producer
	Config        *KafkaConfig
}

func NewKafkaPublisher(args KafkaPublishArgs) mrouter.IPublisher {
	return &kafkaPub{
		kafkaProducer: args.KafkaProducer,
		config:        &args.Config.PubsubConfig,
	}
}

func (_this *kafkaPub) Publish(req *mrouter.PublishReq) (int64, error) {
	var b1ByteValue []byte
	var err error
	if req.Json {
		b1ByteValue, err = json.Marshal(req.Value)
		if err != nil {
			return -1, err
		}
	} else {
		b1ByteValue = req.Value.([]byte)
	}

	topic := req.Channel
	if _this.config.ChannelPrefix != nil {
		topic = mrouter.MergeKeys(*_this.config.ChannelPrefix, topic)
	}
	log.Printf("[Kafka] Publishing message %s to %v:%s\n", req.ID, _this.config.Hosts, topic)

	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          b1ByteValue,
	}

	if req.ID != "" {
		msg.Key = []byte(req.ID)
	}

	var deliveryChan chan kafka.Event
	if req.TimeoutSecond > 0 {
		deliveryChan = make(chan kafka.Event, 1)
	}

	err = _this.kafkaProducer.Produce(&msg, deliveryChan)
	if err != nil {
		return -1, err
	}

	if req.TimeoutSecond > 0 {
		select {
		case e := <-deliveryChan:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				return -1, fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
			} else {
				offset := int64(m.TopicPartition.Offset)
				log.Println("[Kafka] Published message", m)
				return offset, nil
			}
		case <-time.After(time.Duration(req.TimeoutSecond) * time.Second):
			return -1, fmt.Errorf("delivery timeout after %ds", req.TimeoutSecond)
		}
	}

	// If no timeout configured, offset is unknown at this point
	return -1, nil
}
