package mkafka

import (
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
	"log"
	"strings"
)

type kafkaSubscriber struct {
	routing mrouter.MessageRoutingFn
	router  *mrouter.Engine
	config  *KafkaConfig
}

type KafkaSubscriberArgs struct {
	dig.In
	Routing mrouter.MessageRoutingFn
	Router  *mrouter.Engine
	Config  *KafkaConfig
}

func NewKafkaSubscriber(params KafkaSubscriberArgs) mrouter.ISubscriber {
	return &kafkaSubscriber{
		router:  params.Router,
		routing: params.Routing,
		config:  params.Config,
	}
}

func (_this *kafkaSubscriber) Open(args *mrouter.OpenServerArgs) error {
	_this.routing(_this.router)
	args.Channels = _this.config.GetChannels(args.Channels...)
	log.Printf("[Kafka] Subscribe channels: %v\n", args.Channels)
	_this.Run(args)
	return nil
}

func (_this *kafkaSubscriber) Run(args *mrouter.OpenServerArgs) {
	consumer := CreateKafkaConsumer(_this.config, args.Channels...)

	worker := &TopicWorker{
		Topic:    strings.Join(args.Channels, ","),
		Consumer: consumer,
		Config:   _this.config,
	}

	defer func() {
		log.Println("[Kafka] Closing consumer...")
		if err := consumer.Close(); err != nil {
			log.Printf("[Kafka] Error closing consumer: %v", err)
		} else {
			log.Println("[Kafka] Consumer closed")
		}
	}()

	ConsumeSingleTopic(args, worker, _this.router)
}
