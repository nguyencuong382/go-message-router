package mrouter

type ISubscriber interface {
	Open(channels []string) error
}

type IPublisher interface {
	Publish(channel string, value interface{}) error
}
