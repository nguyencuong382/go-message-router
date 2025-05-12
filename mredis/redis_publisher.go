package mredis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
)

type redisPub struct {
	redis  IRedisClient
	config *mrouter.PubsubConfig
}

type RedisPublishArgs struct {
	dig.In
	Redis  IRedisClient
	Config *RedisConfig
}

func NewRedisPublisher(args RedisPublishArgs) mrouter.IPublisher {
	return &redisPub{
		redis:  args.Redis,
		config: args.Config.PubsubConfig,
	}
}

func (_this *redisPub) Publish(req *mrouter.PublishReq) error {
	var b1ByteValue []byte
	var err error
	if req.Json {
		b1ByteValue, err = json.Marshal(req.Value)
		if err != nil {
			return err
		}
	} else {
		b1ByteValue = req.Value.([]byte)
	}

	ctx := context.Background()

	_channel := req.Channel

	if _this.config.ChannelPrefix != nil {
		_channel = mrouter.MergeKeys(*_this.config.ChannelPrefix, _channel)
	}

	if _this.config.Debug {
		fmt.Printf("[Redis] Publish chanel: %v\n", _channel)
	}

	return _this.redis.MrPublish(ctx, _channel, b1ByteValue)
}
