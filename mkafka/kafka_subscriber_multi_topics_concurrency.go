package mkafka

import (
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
	"log"
)

type kafkaMultiTopicsConcurrencySubscriber struct {
	routing mrouter.MessageRoutingFn
	router  *mrouter.Engine
	config  *KafkaConfig
}

type KafkaMultiTopicsConcurrencySubscriberArgs struct {
	dig.In
	Routing mrouter.MessageRoutingFn
	Router  *mrouter.Engine
	Config  *KafkaConfig
}

func NewKafkaMultiTopicsConcurrencySubscriber(params KafkaMultiTopicsConcurrencySubscriberArgs) mrouter.ISubscriber {
	return &kafkaMultiTopicsConcurrencySubscriber{
		router:  params.Router,
		routing: params.Routing,
		config:  params.Config,
	}
}

func (_this *kafkaMultiTopicsConcurrencySubscriber) Open(args *mrouter.OpenServerArgs) error {
	_this.routing(_this.router)
	args.Channels = _this.config.GetChannels(args.Channels...)
	if args.MaxConcurrentWorker == 0 {
		args.MaxConcurrentWorker = 1
	}
	log.Printf("[Kafka] Subscribe channels: %v - concurrent: %d\n", args.Channels, args.MaxConcurrentWorker)
	_this.Run(args)
	return nil
}

func (_this *kafkaMultiTopicsConcurrencySubscriber) Run(args *mrouter.OpenServerArgs) {
	ctx := args.AppCtx
	var workers []*TopicWorker

	for _, topic := range args.Channels {
		consumer := CreateKafkaConsumer(_this.config, topic)
		workers = append(workers, &TopicWorker{
			Topic:    topic,
			Consumer: consumer,
			Config:   _this.config,
		})
	}

	defer CloseWorker(workers...)

	log.Println("[Kafka] 🚀 Started consumers, one worker per topic")

	// Start one goroutine per topic
	for _, w := range workers {
		go ConsumeSingleTopic(args, w, _this.router)
	}

	// Block until context is canceled
	<-ctx.Done()
	log.Println("[Kafka] Context canceled, stopping all topic workers")
}
