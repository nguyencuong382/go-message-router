package mredis

import (
	"context"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"github.com/redis/go-redis/v9"
	"go.uber.org/dig"
)

type redisSubscriber struct {
	routing mrouter.MessageRoutingFn
	router  *mrouter.Engine
	redis   IRedisClient
}

type RedisSubscriberArgs struct {
	dig.In
	Routing mrouter.MessageRoutingFn
	Router  *mrouter.Engine
	Redis   IRedisClient
}

func NewRedisSubscriber(params RedisSubscriberArgs) mrouter.ISubscriber {
	return &redisSubscriber{
		router:  params.Router,
		routing: params.Routing,
		redis:   params.Redis,
	}
}

func (_this *redisSubscriber) Open(channels []string) error {
	_this.routing(_this.router)
	_this.Run(channels...)
	return nil
}

func (_this *redisSubscriber) Run(channels ...string) {
	go func() {
		ctx := context.Background()
		subscriber := _this.redis.MrSubscribe(ctx, channels...)
		for {
			msg, err := subscriber.ReceiveMessage(ctx)
			if err != nil {
				panic(err)
			}
			switch interface{}(msg).(type) {
			case *redis.Message:
				//log.Info("Received msg on channel [", msg.Channel, "]")
				err := _this.router.Route(msg.Channel, []byte(msg.Payload))
				if err != nil {
					//log.Info("Error when handling [", msg.Channel, "]", err)
				}
			default:
				//log.Info("Got control message", msg)
			}
		}
	}()
}
