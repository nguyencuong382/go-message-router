package mrouter

type ISubscriber interface {
	Open(channels []string) error
}

type PublishReq struct {
	ID      string
	Channel string
	Value   interface{}
	Json    bool
	URL     string
}

type IPublisher interface {
	Publish(req *PublishReq) error
}
