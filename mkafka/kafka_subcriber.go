package mkafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
	"log"
	"os"
)

type kafkaSubscriber struct {
	routing       mrouter.MessageRoutingFn
	router        *mrouter.Engine
	kafkaConsumer *kafka.Consumer
	config        *KafkaConfig
}

type KafkaSubscriberArgs struct {
	dig.In
	Routing       mrouter.MessageRoutingFn
	Router        *mrouter.Engine
	KafkaConsumer *kafka.Consumer
	Config        *KafkaConfig
}

func NewKafkaSubscriber(params KafkaSubscriberArgs) mrouter.ISubscriber {
	return &kafkaSubscriber{
		router:        params.Router,
		routing:       params.Routing,
		kafkaConsumer: params.KafkaConsumer,
		config:        params.Config,
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

	//go func() {
	for {
		msg, err := _this.kafkaConsumer.ReadMessage(-1)
		if err == nil {

			if _this.config.ManualCommit {
				_, sErr := _this.kafkaConsumer.StoreMessage(msg)
				if sErr != nil {
					_, _ = fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s: %v\n", msg.TopicPartition, sErr)
					continue
				}
			}

			//log.Info("Received msg on channel [", msg.Channel, "]")
			fmt.Println("Received msg on channel [", msg.TopicPartition, "]")
			rErr := _this.router.Route(*msg.TopicPartition.Topic, msg.Value)
			if rErr != nil {
				fmt.Println("Error when handling [", msg.String(), "]", rErr)
				continue
			}

			if _this.config.ManualCommit {
				_, cErr := _this.kafkaConsumer.Commit()
				if cErr != nil {
					log.Printf("Commit error: %v\n", cErr)
					continue
				}
			}

			fmt.Println("Finish handle msg on channel [", msg.TopicPartition, "]")

		} else if !err.(kafka.Error).IsFatal() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	//}()
}
