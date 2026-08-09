package msqs

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
)

type sqsSubscriber struct {
	routing  mrouter.MessageRoutingFn
	router   *mrouter.Engine
	client   ISqsClient
	config   *SqsConfig
	resolver *queueResolver
}

type SqsSubscriberArgs struct {
	dig.In
	Routing mrouter.MessageRoutingFn
	Router  *mrouter.Engine
	Client  ISqsClient
	Config  *SqsConfig
}

func NewSqsSubscriber(params SqsSubscriberArgs) mrouter.ISubscriber {
	return &sqsSubscriber{
		router:   params.Router,
		routing:  params.Routing,
		client:   params.Client,
		config:   params.Config,
		resolver: newQueueResolver(params.Client, params.Config),
	}
}

// Open CHẶN cho tới khi AppCtx bị huỷ — cùng hợp đồng với mredis/mkafka.
// Trả về sớm = worker im lặng ngừng nhận việc.
func (_this *sqsSubscriber) Open(args *mrouter.OpenServerArgs) error {
	_this.routing(_this.router)
	args.Channels = _this.config.GetChannels(args.Channels...)
	log.Printf("[SQS] Subscribe channels: %v\n", args.Channels)
	_this.Run(args)
	return nil
}

// Run mở MỘT vòng kéo cho MỖI hàng đợi. Khác mredis: Redis ghép nhiều kênh vào một
// kết nối, SQS thì mỗi kênh là một hàng đợi riêng, không có lời gọi nghe nhiều cùng lúc.
func (_this *sqsSubscriber) Run(args *mrouter.OpenServerArgs) {
	var wg sync.WaitGroup
	for _, channel := range args.Channels {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			_this.consumeQueue(args, name)
		}(channel)
	}
	wg.Wait()
	log.Println("[SQS] Tất cả vòng kéo đã dừng")
}

func (_this *sqsSubscriber) consumeQueue(args *mrouter.OpenServerArgs, channel string) {
	ctx := args.AppCtx

	queueUrl, err := _this.resolver.resolveByName(ctx, channel)
	if err != nil {
		// Không phân giải được hàng đợi = worker không bao giờ nhận việc trên kênh này.
		// Kêu to rồi thoát vòng của riêng kênh này, KHÔNG kéo sập các kênh khác.
		log.Printf("[SQS] BỎ kênh %s: %v", channel, err)
		return
	}
	log.Printf("[SQS] Kéo hàng đợi %s (%s)", channel, queueUrl)

	concurrency := _this.config.MaxConcurrentPerQueue
	if args.MaxConcurrentWorker > 0 {
		concurrency = args.MaxConcurrentWorker
	}
	slots := make(chan struct{}, concurrency)

	var inFlight sync.WaitGroup
	defer func() {
		inFlight.Wait()
		log.Printf("[SQS] Dừng kéo hàng đợi %s", channel)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 🚨 GIÀNH CHỖ TRƯỚC, LẤY TIN SAU. Đảo thứ tự này là hỏng, và hỏng lặng lẽ:
		//
		// Hạn ẩn của một tin bắt đầu đếm NGAY khi ReceiveMessage trả về. Nếu lấy tin về
		// rồi mới chờ chỗ trống, tin nằm chờ mà KHÔNG có nhịp tim nào giữ — hết hạn ẩn là
		// nó được giao lại cho lượt khác, handle cũ vô hiệu, DeleteMessage hỏng, và job
		// chạy lại từ đầu.
		//
		// Đo thật 2026-08-08 (hạn ẩn 30s, job ~2 phút, 4 tin trong hàng đợi): tiến độ tụt
		// `4/5 → 0/5`, 4 lỗi ReceiptHandleIsInvalid, một job xuất hiện 19 lần trong log.
		// Chỉ lộ khi số tin NHIỀU HƠN số thợ — chạy một tin một lượt thì không bao giờ thấy.
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}

		out, rErr := _this.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &queueUrl,
			MaxNumberOfMessages: _this.config.MaxNumberOfMessages,
			WaitTimeSeconds:     _this.config.WaitTimeSeconds,
			VisibilityTimeout:   _this.config.VisibilityTimeoutSS,
		})
		if rErr != nil {
			<-slots // trả chỗ — không trả thì mỗi lần lỗi lại rò một chỗ, cạn dần rồi đứng im
			if ctx.Err() != nil {
				return
			}
			log.Printf("[SQS] Lỗi nhận tin trên %s: %v", channel, rErr)
			// Chờ ngắn rồi thử lại — tránh quay vòng nóng khi mạng/quyền hỏng.
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}

		// Long-poll hết giờ mà không có tin nào → trả chỗ, vòng lại.
		if len(out.Messages) == 0 {
			<-slots
			continue
		}

		// Đã giành 1 chỗ ở đầu vòng; tin thứ hai trở đi (nếu MaxNumberOfMessages > 1)
		// phải giành thêm chỗ của riêng nó.
		for i := range out.Messages {
			msg := out.Messages[i]

			if i > 0 {
				select {
				case <-ctx.Done():
					return
				case slots <- struct{}{}:
				}
			}

			inFlight.Add(1)
			go func(m types.Message) {
				defer inFlight.Done()
				defer func() { <-slots }()
				_this.handleMessage(args, channel, queueUrl, m)
			}(msg)
		}
	}
}

// handleMessage chạy MỘT tin: gia hạn hạn ẩn trong lúc chạy, xoá tin CHỈ KHI xử lý xong.
//
// 🚨 Ranh giới sống-chết của job nằm ở giá trị trả về của router.Route:
//   - trả nil  → DeleteMessage → tin biến mất VĨNH VIỄN
//   - trả lỗi  → không xoá → hết hạn ẩn thì tin hiện lại cho bản khác nhặt
//
// Đây đúng vị trí Commit() của mkafka (kafka_single_consume_topic.go:74-80).
func (_this *sqsSubscriber) handleMessage(args *mrouter.OpenServerArgs, channel, queueUrl string, msg types.Message) {
	if msg.Body == nil || msg.ReceiptHandle == nil {
		log.Printf("[SQS] Bỏ tin rỗng trên %s", channel)
		return
	}

	start := time.Now()
	msgID := ""
	if msg.MessageId != nil {
		msgID = *msg.MessageId
	}

	// Nhịp tim: đẩy hạn ẩn ra xa trong suốt thời gian chạy. Nhờ nó, việc dài bao lâu
	// cũng không bị giao lại — tin chỉ hiện lại khi nhịp tim NGỪNG, tức worker đã chết.
	hbCtx, stopHeartbeat := context.WithCancel(context.Background())
	go _this.heartbeat(hbCtx, queueUrl, *msg.ReceiptHandle, msgID)

	err := _this.router.Route(args, channel, []byte(*msg.Body), 0)

	stopHeartbeat()

	if err != nil {
		log.Printf("[SQS] Lỗi xử lý tin %s trên %s: %v — KHÔNG xoá, sẽ giao lại", msgID, channel, err)
		return
	}

	// Dùng context nền: AppCtx có thể đã huỷ lúc tắt máy, nhưng việc ĐÃ xong nên
	// phải xoá cho được, nếu không job chạy lại lần nữa một cách vô ích.
	delCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, dErr := _this.client.DeleteMessage(delCtx, &sqs.DeleteMessageInput{
		QueueUrl:      &queueUrl,
		ReceiptHandle: msg.ReceiptHandle,
	}); dErr != nil {
		// Xoá hỏng = tin sẽ được giao lại. Job idempotent nên chạy lại an toàn,
		// nhưng vẫn phải kêu để người vận hành thấy.
		log.Printf("[SQS] Xoá tin %s hỏng: %v — tin sẽ được giao lại", msgID, dErr)
		return
	}

	log.Printf("[SQS] Xong tin %s trên %s, mất %v", msgID, channel, time.Since(start))
}

// heartbeat gia hạn hạn ẩn theo chu kỳ tới khi ctx bị huỷ hoặc chạm trần.
// Chạm trần = job hỏng thật; thôi gia hạn để nó rơi xuống hàng đợi thu chết.
func (_this *sqsSubscriber) heartbeat(ctx context.Context, queueUrl, receiptHandle, msgID string) {
	interval := time.Duration(_this.config.HeartbeatIntervalSS) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var extendedSS int32
	var beats int

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		extendedSS += _this.config.HeartbeatIntervalSS
		if extendedSS >= _this.config.MaxVisibilityExtendSS {
			log.Printf("[SQS] Tin %s chạm trần gia hạn %ds — thôi gia hạn", msgID, _this.config.MaxVisibilityExtendSS)
			return
		}

		_, err := _this.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          &queueUrl,
			ReceiptHandle:     &receiptHandle,
			VisibilityTimeout: _this.config.VisibilityTimeoutSS,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Thiếu quyền ChangeMessageVisibility là lỗi hay sót nhất của thiết kế này:
			// nhịp tim im lặng hỏng → job dài bị giao lại giữa chừng. Phải kêu rõ.
			log.Printf("[SQS] Gia hạn hạn ẩn cho tin %s hỏng: %v", msgID, err)
			return
		}

		beats++
		// Ghi CẢ lần thành công khi bật SQS_ENABLE_DEBUG. Chỉ ghi lúc hỏng thì không
		// phân biệt được "nhịp tim chạy tốt" với "nhịp tim chưa từng chạy" — cả hai đều
		// cho ra một khoảng log trống. Đã trả giá vì chỗ này: báo nhầm là nhịp tim đang
		// giữ tin, trong khi thật ra job chỉ vừa kịp xong trước hạn ẩn (2026-08-08).
		if _this.config.Debug {
			log.Printf("[SQS] Nhịp tim tin %s: lần %d, đẩy hạn ẩn thêm %ds (đã giữ %ds)",
				msgID, beats, _this.config.VisibilityTimeoutSS, extendedSS)
		}
	}
}
