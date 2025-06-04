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
	config        *mrouter.PubsubConfig
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
		config:        &params.Config.PubsubConfig,
	}
}

func (_this *kafkaSubscriber) Open() error {
	_this.routing(_this.router)
	log.Printf("[Kafka] Subscribe channels: %v\n", _this.config.Channels)
	_this.Run(_this.config.Channels...)
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
			log.Println("[Kafka] Received msg on channel [", msg.TopicPartition, "]")
			rErr := _this.router.Route(*msg.TopicPartition.Topic, msg.Value)
			if rErr != nil {
				log.Println("[Kafka] Error when handling [", msg.String(), "]", rErr)
				continue
			}

			if _this.config.ManualCommit {
				_, cErr := _this.kafkaConsumer.Commit()
				if cErr != nil {
					log.Printf("[Kafka] Commit error: %v\n", cErr)
					continue
				}
			}

			log.Println("[Kafka] Finish handle msg on channel [", msg.TopicPartition, "]")

		} else if !err.(kafka.Error).IsFatal() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			log.Printf("[Kafka] Consumer error: %v (%v)\n", err, msg)
		}
	}
	//}()
}
