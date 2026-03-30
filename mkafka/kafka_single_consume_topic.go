package mkafka

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"log"
	"os"
	"sync"
	"time"
)

func ConsumeSingleTopic(
	wgConsumers *sync.WaitGroup,
	args *mrouter.OpenServerArgs,
	worker *TopicWorker,
	router *mrouter.Engine,
) {
	defer wgConsumers.Done()

	ctx := args.AppCtx

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Kafka] Context canceled, stopping consumer for topic %s", worker.Topic)
			return
		default:
		}

		msg, err := worker.Consumer.ReadMessage(200 * time.Millisecond)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			if !kafkaErr.IsFatal() {
				log.Printf("[Kafka] Consumer error on topic %s: %v\n", worker.Topic, err)
				continue
			}
			log.Printf("[Kafka] Fatal error on topic %s: %v\n", worker.Topic, err)
			return
		}

		// ✅ check ctx again after message
		select {
		case <-ctx.Done():
			log.Printf("[Kafka] Stop consuming %s (after message)", worker.Topic)
			return
		default:
		}

		startTime := time.Now()
		msgID := fmt.Sprintf("topic [%s] partition [%v] offset [%v]", *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)

		if worker.Config.ManualCommit {
			_, sErr := worker.Consumer.StoreMessage(msg)
			if sErr != nil {
				log.Printf("[Kafka] StoreMessage error %s: %v", msgID, sErr)
				continue
			}
		}

		// process message sequentially for this topic
		log.Printf("[Kafka] Received msg %s", msgID)

		rErr := router.Route(args, *msg.TopicPartition.Topic, msg.Value, int64(msg.TopicPartition.Offset))
		if rErr != nil {
			log.Printf("[Kafka] Error when handling %s: %v", msg, rErr)
			continue
		}

		if worker.Config.ManualCommit {
			_, cErr := worker.Consumer.Commit()
			if cErr != nil {
				log.Printf("[Kafka] Commit error %s: %v", msgID, cErr)
				continue
			}
		}

		log.Printf("[Kafka] Finished msg on topic [%s], elapsed: %v", msgID, time.Since(startTime))
	}
}

func CreateKafkaConsumer(config *KafkaConfig, topics ...string) *kafka.Consumer {
	consumer, err := NewKafkaConsumer(config)
	if err != nil {
		fmt.Printf("Failed to create consumer for %v: %v\n", topics, err)
		os.Exit(1)
	}

	if len(topics) == 1 {
		err = consumer.Subscribe(topics[0], nil)
	} else {
		err = consumer.SubscribeTopics(topics, nil)
	}
	if err != nil {
		fmt.Printf("Failed to subscribe to topic %v: %v\n", topics, err)
		os.Exit(1)
	} else {
		log.Printf("[Kafka] Subscribed to topic(s) %v\n", topics)
	}

	return consumer
}

func CloseWorker(workers ...*TopicWorker) {
	for _, w := range workers {
		log.Printf("[Kafka] Closing consumer... %v", w.Topic)
		if err := w.Consumer.Close(); err != nil {
			log.Printf("[Kafka] Error closing consumer: %v", err)
		} else {
			log.Printf("[Kafka] Consumer for topic %s closed", w.Topic)
		}
	}
}
