package mrouter

import (
	"context"
	"github.com/redis/go-redis/v9"
	"go.uber.org/dig"
	"encoding/json"
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

func (_this *redisPub) Publish(channel string, value interface{}, isJson bool) error {
	var b1ByteValue []byte
	var err error
	if isJson {
		b1ByteValue, err = json.Marshal(value)
		if err != nil {
			return err
		}
	} else {
		b1ByteValue = value.([]byte)
	}

	ctx := context.Background()
	return _this.redis.Publish(ctx, channel, b1ByteValue).Err()
}
