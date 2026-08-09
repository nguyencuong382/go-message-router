package msqs

import (
	"context"
	"fmt"
	"strings"
	"sync"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nguyencuong382/go-message-router/mrouter"
)

// ISqsClient tách ra để bên gọi thay được bằng bản giả lúc test.
type ISqsClient interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
}

// NewSqsClient dựng client từ chuỗi cấu hình mặc định của AWS SDK.
// Trên ECS, thông tin xác thực đến từ VAI TRÒ TASK — không đọc khoá tĩnh.
func NewSqsClient(config *SqsConfig) (ISqsClient, error) {
	opts := []func(*awsconfig.LoadOptions) error{}
	if config.Region != "" {
		opts = append(opts, awsconfig.WithRegion(config.Region))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("[SQS] không nạp được cấu hình AWS: %w", err)
	}

	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if config.EndpointUrl != "" {
			o.BaseEndpoint = &config.EndpointUrl
		}
	}), nil
}

// queueResolver dịch tên kênh sang URL hàng đợi, có nhớ lại để khỏi gọi lặp.
type queueResolver struct {
	client ISqsClient
	config *SqsConfig
	cache  sync.Map // channel (đã ghép tiền tố) -> queue URL
}

func newQueueResolver(client ISqsClient, config *SqsConfig) *queueResolver {
	return &queueResolver{client: client, config: config}
}

// prefixedChannel ghép tiền tố cụm vào tên kênh, cùng quy tắc với mredis/mkafka
// (mrouter.MergeKeys nối bằng dấu "-", hợp lệ cho tên hàng đợi SQS).
func (_this *queueResolver) prefixedChannel(channel string) string {
	if _this.config.ChannelPrefix != nil {
		return mrouter.MergeKeys(*_this.config.ChannelPrefix, channel)
	}
	return channel
}

// resolve dùng cho BÊN PHÁT: PublishReq.Channel là kênh THÔ nên phải ghép tiền tố.
func (_this *queueResolver) resolve(ctx context.Context, channel string) (string, error) {
	return _this.resolveByName(ctx, _this.prefixedChannel(channel))
}

// resolveByName dùng cho BÊN ĐỌC: PubsubConfig.GetChannels đã ghép tiền tố sẵn
// (aionj gọi SetChannels lúc dựng config) — ghép lần nữa là sai tên hàng đợi.
func (_this *queueResolver) resolveByName(ctx context.Context, name string) (string, error) {
	if v, ok := _this.cache.Load(name); ok {
		return v.(string), nil
	}

	if _this.config.QueueUrlPrefix != "" {
		url := strings.TrimRight(_this.config.QueueUrlPrefix, "/") + "/" + name
		_this.cache.Store(name, url)
		return url, nil
	}

	out, err := _this.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &name})
	if err != nil {
		return "", fmt.Errorf("[SQS] không tìm được hàng đợi %q: %w", name, err)
	}
	url := *out.QueueUrl
	_this.cache.Store(name, url)
	return url, nil
}
