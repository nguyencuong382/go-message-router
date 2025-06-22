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
	if _this.config.ChannelPrefix != nil {
		topic = mrouter.MergeKeys(*_this.config.ChannelPrefix, topic)
	}

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
		return err
	}

	if req.TimeoutSecond > 0 {
		select {
		case e := <-deliveryChan:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
			} else {
				log.Println("[Kafka] Published message", m)
			}
		case <-time.After(time.Duration(req.TimeoutSecond) * time.Second):
			return fmt.Errorf("delivery timeout after %ds", req.TimeoutSecond)
		}

		close(deliveryChan)
	}

	// Wait for message deliveries before shutting down
	//_this.kafkaProducer.Flush(15 * 1000)

	return nil
}
