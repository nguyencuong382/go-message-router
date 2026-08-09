package msqs

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
)

type sqsPub struct {
	client   ISqsClient
	config   *SqsConfig
	resolver *queueResolver
}

type SqsPublishArgs struct {
	dig.In
	Client ISqsClient
	Config *SqsConfig
}

func NewSqsPublisher(args SqsPublishArgs) mrouter.IPublisher {
	return &sqsPub{
		client:   args.Client,
		config:   args.Config,
		resolver: newQueueResolver(args.Client, args.Config),
	}
}

// Publish trả offset 0 — SQS không có khái niệm offset. KHÔNG bịa số thứ tự;
// mredis cũng trả 0. Chỉ mkafka trả offset thật (được ghi vào cột kafka_offset).
func (_this *sqsPub) Publish(req *mrouter.PublishReq) (int64, error) {
	var (
		body []byte
		err  error
	)
	if req.Json {
		body, err = json.Marshal(req.Value)
		if err != nil {
			return -1, err
		}
	} else {
		body = req.Value.([]byte)
	}

	ctx := context.Background()
	if req.TimeoutSecond > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSecond)*time.Second)
		defer cancel()
	}

	queueUrl, err := _this.resolver.resolve(ctx, req.Channel)
	if err != nil {
		return -1, err
	}

	if _this.config.Debug {
		log.Printf("[SQS] Publish %s tới %s\n", req.ID, queueUrl)
	}

	bodyStr := string(body)
	_, err = _this.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &queueUrl,
		MessageBody: &bodyStr,
	})
	if err != nil {
		return -1, err
	}

	return 0, nil
}
