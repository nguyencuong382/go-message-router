package mkafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
	"log"
	"time"
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

func (_this *kafkaSubscriber) Open(args *mrouter.OpenServerArgs) error {
	_this.routing(_this.router)
	args.Channels = _this.config.GetChannels(args.Channels...)
	log.Printf("[Kafka] Subscribe channels: %v\n", args.Channels)
	_this.Run(args)
	return nil
}

func (_this *kafkaSubscriber) Run(args *mrouter.OpenServerArgs) {
	ctx := args.AppCtx
	err := _this.kafkaConsumer.SubscribeTopics(args.Channels, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		log.Println("[Kafka] Closing consumer...")
		if err := _this.kafkaConsumer.Close(); err != nil {
			log.Printf("[Kafka] Error closing consumer: %v", err)
		} else {
			log.Println("[Kafka] Consumer closed")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Kafka] Context canceled, stopping Kafka consumer loop")
			return
		default:
			// Set timeout để không block ReadMessage mãi
			msg, err := _this.kafkaConsumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Nếu là timeout thì bỏ qua, tiếp tục loop
				kafkaErr, ok := err.(kafka.Error)
				if ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				if !kafkaErr.IsFatal() {
					log.Printf("[Kafka] Consumer error: %v (%v)\n", err, msg)
					continue
				}
				// Fatal error → thoát vòng lặp
				log.Printf("[Kafka] Fatal error: %v\n", err)
				return
			}

			// Đoạn xử lý message như cũ
			starTime := time.Now()

			if _this.config.ManualCommit {
				_, sErr := _this.kafkaConsumer.StoreMessage(msg)
				if sErr != nil {
					log.Printf("[Kafka] StoreMessage error: %v", sErr)
					continue
				}
			}

			log.Println("[Kafka] Received msg on channel [", msg.TopicPartition, "]")
			rErr := _this.router.Route(args, *msg.TopicPartition.Topic, msg.Value)
			if rErr != nil {
				log.Println("[Kafka] Error when handling [", msg.String(), "]", rErr)
				continue
			}

			if _this.config.ManualCommit {
				_, cErr := _this.kafkaConsumer.Commit()
				if cErr != nil {
					log.Printf("[Kafka] Commit error: %v", cErr)
					continue
				}
			}

			log.Println("[Kafka] Finish handle msg on channel [", msg.TopicPartition, "]", fmt.Sprintf("elapsed: %v", time.Since(starTime).String()))
		}
	}
}
