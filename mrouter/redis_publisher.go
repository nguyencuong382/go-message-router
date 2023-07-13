package mrouter

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"go.uber.org/dig"
)

type redisPub struct {
	redis *redis.Client
}

type RedisPublishArgs struct {
	dig.In
	Redis *redis.Client
}

func NewRedisPublisher(args RedisPublishArgs) IPublisher {
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
	return _this.redis.Publish(ctx, channel, b1ByteValue).Err()
}
