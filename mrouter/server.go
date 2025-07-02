package mrouter

import (
	"context"
	"log"
	"sync"
)

type OpenServerArgs struct {
	AppCtx       context.Context
	AppWaitGroup *sync.WaitGroup
	Channels     []string
}

type ISubscriber interface {
	Open(args *OpenServerArgs) error
}

type PublishReq struct {
	ID            string
	Channel       string
	Value         interface{}
	Json          bool
	URL           string
	TimeoutSecond int64
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

func (_this *PubsubConfig) GetChannels(channels ...string) []string {
	if len(channels) > 0 {
		log.Printf("Override channels: %v -> %v\n", _this.Channels, channels)
		return channels
	}
	return _this.Channels
}
