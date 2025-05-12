package mredis

import (
	"context"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"github.com/redis/go-redis/v9"
	"go.uber.org/dig"
	"log"
)

type redisSubscriber struct {
	routing mrouter.MessageRoutingFn
	router  *mrouter.Engine
	redis   IRedisClient
	config  *mrouter.PubsubConfig
}

type RedisSubscriberArgs struct {
	dig.In
	Routing mrouter.MessageRoutingFn
	Router  *mrouter.Engine
	Redis   IRedisClient
	Config  *RedisConfig
}

func NewRedisSubscriber(params RedisSubscriberArgs) mrouter.ISubscriber {
	return &redisSubscriber{
		router:  params.Router,
		routing: params.Routing,
		redis:   params.Redis,
		config:  params.Config.PubsubConfig,
	}
}

func (_this *redisSubscriber) Open() error {
	_this.routing(_this.router)
	log.Printf("[Redis] Subscribe channels: %v\n", _this.config.Channels)
	_this.Run(_this.config.Channels...)
	return nil
}

func (_this *redisSubscriber) Run(channels ...string) {
	ctx := context.Background()
	subscriber := _this.redis.MrSubscribe(ctx, channels...)

	for {
		msg, err := subscriber.ReceiveMessage(ctx)
		if err != nil {
			panic(err)
		}
		switch interface{}(msg).(type) {
		case *redis.Message:
			log.Println("[Redis] Received msg on channel [", msg.Channel, "]")
			err := _this.router.Route(msg.Channel, []byte(msg.Payload))
			if err != nil {
				log.Println("[Redis] Error when handling [", msg.Channel, "]", err)
			} else {
				log.Println("[Redis] Finish handle msg on channel [", msg.Channel, "]")
			}
		default:
			log.Println("[Redis] Got control message", msg)
		}
	}
}
