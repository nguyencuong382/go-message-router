package msqs

import "github.com/nguyencuong382/go-message-router/mrouter"

// Giá trị mặc định — chốt tại proposal-05-durable-queue-transport D-05.2.
// KHÔNG có khoá nào cho số lần thử lại: chính sách đó thuộc job SDK
// (cột retry / retry_count, jrk_job_finish.go), không khai lần hai ở đây.
const (
	DefaultWaitTimeSeconds        = 20   // long polling, mức tối đa của SQS
	DefaultVisibilityTimeoutSS    = 120  // hạn ẩn khởi điểm
	DefaultHeartbeatIntervalSS    = 30   // chu kỳ gia hạn, bằng 1/4 hạn ẩn
	DefaultMaxVisibilityExtendSS  = 6 * 60 * 60 // trần gia hạn = chặn trên của SQS
	DefaultMaxConcurrentPerQueue  = 1
	DefaultMaxNumberOfMessages    = 1
)

type SqsConfig struct {
	*mrouter.PubsubConfig

	// Region rỗng thì SDK tự đọc từ môi trường / vai trò task.
	Region string

	// QueueUrlPrefix ví dụ "https://sqs.ap-southeast-1.amazonaws.com/454015599378".
	// Tên hàng đợi = kênh đã ghép tiền tố, nối vào sau prefix.
	QueueUrlPrefix string

	// EndpointUrl chỉ dùng khi chạy với bản giả lập cục bộ. Rỗng = AWS thật.
	EndpointUrl string

	WaitTimeSeconds       int32
	VisibilityTimeoutSS   int32
	HeartbeatIntervalSS   int32
	MaxVisibilityExtendSS int32

	// MaxConcurrentPerQueue: số tin xử lý đồng thời trong MỘT tiến trình, mỗi hàng đợi.
	//
	// 🚨 KHÔNG có khoá cấu hình riêng cho trường này. Số thợ đồng thời đã khai một lần ở
	// PUBSUB_MAX_CONCURRENT_WORKER (dùng chung mọi driver) và tới qua
	// OpenServerArgs.MaxConcurrentWorker. Trường ở đây chỉ là giá trị sàn khi bên gọi
	// không truyền gì — thêm khoá SQS riêng là hai chỗ khai cho một khái niệm.
	MaxConcurrentPerQueue int

	// MaxNumberOfMessages: số tin lấy về mỗi lần gọi ReceiveMessage (1..10).
	MaxNumberOfMessages int32
}

// WithDefaults điền các giá trị chưa đặt. Gọi ở nơi dựng config.
func (_this *SqsConfig) WithDefaults() *SqsConfig {
	if _this.WaitTimeSeconds <= 0 {
		_this.WaitTimeSeconds = DefaultWaitTimeSeconds
	}
	if _this.VisibilityTimeoutSS <= 0 {
		_this.VisibilityTimeoutSS = DefaultVisibilityTimeoutSS
	}
	if _this.HeartbeatIntervalSS <= 0 {
		_this.HeartbeatIntervalSS = DefaultHeartbeatIntervalSS
	}
	if _this.MaxVisibilityExtendSS <= 0 {
		_this.MaxVisibilityExtendSS = DefaultMaxVisibilityExtendSS
	}
	if _this.MaxConcurrentPerQueue <= 0 {
		_this.MaxConcurrentPerQueue = DefaultMaxConcurrentPerQueue
	}
	if _this.MaxNumberOfMessages <= 0 || _this.MaxNumberOfMessages > 10 {
		_this.MaxNumberOfMessages = DefaultMaxNumberOfMessages
	}
	return _this
}
