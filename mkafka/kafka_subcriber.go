package mkafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
	"time"
)

type kafkaSubscriber struct {
	routing       mrouter.MessageRoutingFn
	router        *mrouter.Engine
	kafkaConsumer *kafka.Consumer
}

type KafkaSubscriberArgs struct {
	dig.In
	Routing       mrouter.MessageRoutingFn
	Router        *mrouter.Engine
	KafkaConsumer *kafka.Consumer
}

func NewKafkaSubscriber(params KafkaSubscriberArgs) mrouter.ISubscriber {
	return &kafkaSubscriber{
		router:        params.Router,
		routing:       params.Routing,
		kafkaConsumer: params.KafkaConsumer,
	}
}

func (_this *kafkaSubscriber) Open(channels []string) error {
	_this.routing(_this.router)
	_this.Run(channels...)
	return nil
}

func (_this *kafkaSubscriber) Run(channels ...string) {
	err := _this.kafkaConsumer.SubscribeTopics(channels, nil)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			msg, err := _this.kafkaConsumer.ReadMessage(time.Second)
			if err == nil {
				//log.Info("Received msg on channel [", msg.Channel, "]")
				fmt.Println("Received msg on channel [", *msg.TopicPartition.Topic, "]")
				err := _this.router.Route(*msg.TopicPartition.Topic, msg.Value)
				if err != nil {
					//log.Info("Error when handling [", msg.Channel, "]", err)
				}

			} else if !err.(kafka.Error).IsFatal() {
				// The client will automatically try to recover from all errors.
				// Timeout is not considered an error because it is raised by
				// ReadMessage in absence of messages.
				//fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}()
}
