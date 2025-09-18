package mkafka

import (
	"errors"
	"fmt"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
	"log"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaMultiTopicsPrioritySubscriber struct {
	routing mrouter.MessageRoutingFn
	router  *mrouter.Engine
	config  *KafkaConfig
	closed  atomic.Bool
}

type KafkaMultiTopicsPrioritySubscriberArgs struct {
	dig.In
	Routing mrouter.MessageRoutingFn
	Router  *mrouter.Engine
	Config  *KafkaConfig
}

func NewKafkaMultiTopicsPrioritySubscriber(params KafkaMultiTopicsPrioritySubscriberArgs) mrouter.ISubscriber {
	return &kafkaMultiTopicsPrioritySubscriber{
		router:  params.Router,
		routing: params.Routing,
		config:  params.Config,
	}
}

func (_this *kafkaMultiTopicsPrioritySubscriber) Open(args *mrouter.OpenServerArgs) error {
	_this.routing(_this.router)
	args.Channels = _this.config.GetChannels(args.Channels...)
	if args.MaxConcurrentWorker == 0 {
		args.MaxConcurrentWorker = 1
	}
	log.Printf("[Kafka] Subscribe channels: %v - concurrent: %d\n", args.Channels, args.MaxConcurrentWorker)
	_this.Run(args)
	return nil
}

type TopicWorker struct {
	Topic    string
	Consumer *kafka.Consumer
	Config   *KafkaConfig
}

func (_this *kafkaMultiTopicsPrioritySubscriber) Run(args *mrouter.OpenServerArgs) {
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

	log.Println("[Kafka] 🚀 Started concurrent consumer")

	var Semaphore = make(chan struct{}, args.MaxConcurrentWorker)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Kafka] Context canceled, stopping Kafka consumer loop")
			return
		default:
			if len(Semaphore) >= args.MaxConcurrentWorker {
				//fmt.Printf("Max concurrent limit reached: %d\n", args.MaxConcurrentWorker)
				time.Sleep(300 * time.Millisecond)
				continue // all workers busy
			}
			handled := false

			for _, w := range workers {
				select {
				case <-ctx.Done():
					log.Println("[Kafka] Context canceled during topic iteration")
					return
				default:
					// continue to read message
				}

				if _this.closed.Load() {
					log.Println("[Kafka] Closed, stopping topic iteration")
					return
				}

				msg, err := w.Consumer.ReadMessage(100 * time.Millisecond)
				if err != nil {
					// Nếu là timeout thì bỏ qua, tiếp tục loop
					var kafkaErr kafka.Error
					if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
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
				log.Println("[Kafka] Received msg on channel [", msg.TopicPartition, "]", string(msg.Key))

				//// 🧠 Deduplication check
				//msgKeyStr := string(msg.Key)
				//if msgKeyStr != "" {
				//	if _, exists := _this.seenKeys.Get(msgKeyStr); exists {
				//		log.Printf("[Kafka] Skipping duplicated %v: %s\n", msg.TopicPartition, msgKeyStr)
				//		if _this.config.ManualCommit {
				//			_, cErr := w.Consumer.CommitMessage(msg)
				//			if cErr != nil {
				//				log.Printf("[Kafka] Commit error for duplicated msg: %v", cErr)
				//			} else {
				//				log.Printf("[Kafka] Committed offset for duplicated key: %s\n", msgKeyStr)
				//			}
				//		}
				//		continue
				//	}
				//	_this.seenKeys.Add(msgKeyStr, struct{}{})
				//}

				Semaphore <- struct{}{}
				handled = true

				starTime := time.Now()

				if _this.config.ManualCommit {
					_, sErr := w.Consumer.StoreMessage(msg)
					if sErr != nil {
						log.Printf("[Kafka] StoreMessage error: %v", sErr)
						continue
					}
				}

				go func(worker *TopicWorker, msg *kafka.Message) {
					defer func() { <-Semaphore }() // release slot
					rErr := _this.router.Route(args, *msg.TopicPartition.Topic, msg.Value, int64(msg.TopicPartition.Offset))
					if rErr != nil {
						log.Println("[Kafka] Error when handling [", msg.String(), "]", rErr)
						return
					}

					if _this.config.ManualCommit {
						if _this.closed.Load() {
							log.Println("[Kafka] Closed, stopping commit")
							return
						}
						//log.Printf("[Kafka] Committing offset for %v: %s\n", msg.TopicPartition, msgKeyStr)
						_, cErr := worker.Consumer.Commit()
						if cErr != nil {
							log.Printf("[Kafka] Commit error: %v", cErr)
							return
						}
					}

					log.Println("[Kafka] Finish handle msg on channel [", msg.TopicPartition, "]", fmt.Sprintf("elapsed: %v", time.Since(starTime)))
				}(w, msg)

				// After 1 message polled, go back to top priority
				break
			}

			if !handled {
				time.Sleep(100 * time.Millisecond)
			}
		}

	}
}
