package mrouter

type ISubscriber interface {
	Open() error
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

type PubsubConfig struct {
	Host            *string
	Port            *string
	Hosts           []string
	Channels        []string
	ChannelPrefix   *string
	ManualCommit    bool
	Group           *string
	AutoOffsetReset *string
	ExtConfig       map[string]interface{}
	Debug           bool
}

func (_this *PubsubConfig) SetChannels(channels []string) {
	var _channels []string
	for _, c := range channels {
		_channels = append(_channels, MergeKeys(*_this.ChannelPrefix, c))
	}
	_this.Channels = _channels
}
