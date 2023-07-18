package mredis

import (
	"context"
	"encoding/json"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
)

type redisPub struct {
	redis IRedisClient
}

type RedisPublishArgs struct {
	dig.In
	Redis IRedisClient
}

func NewRedisPublisher(args RedisPublishArgs) mrouter.IPublisher {
	return &redisPub{
		redis: args.Redis,
	}
}

func (_this *redisPub) Publish(channel string, value interface{}) error {
	b1ByteValue, err := json.Marshal(value)
	if err != nil {
		return err
	}
	ctx := context.Background()
	return _this.redis.Publish(ctx, channel, b1ByteValue)
}
