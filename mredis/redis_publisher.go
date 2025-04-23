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

	return _this.redis.MrPublish(ctx, req.Channel, b1ByteValue)
}
