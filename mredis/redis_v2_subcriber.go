package mredis

import (
	"context"
	"errors"
	"fmt"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"log"
)

type redisSubscriberV2 struct {
	routing mrouter.MessageRoutingFn
	router  *mrouter.Engine
	redis   IRedisClient
	config  *mrouter.PubsubConfig
}

func NewRedisSubscriberV2(params RedisSubscriberArgs) mrouter.ISubscriber {
	return &redisSubscriberV2{
		router:  params.Router,
		routing: params.Routing,
		redis:   params.Redis,
		config:  params.Config.PubsubConfig,
	}
}

func (_this *redisSubscriberV2) Open() error {
	_this.routing(_this.router)
	log.Printf("[Redis][V2] Subscribe channels: %v\n", _this.config.Channels)
	_this.Run(_this.config.Channels...)
	return nil
}

func (_this *redisSubscriberV2) Run(channels ...string) {
	ctx := context.Background()
	pubsub := _this.redis.MrSubscribe(ctx, channels...)

	if _, err := pubsub.Receive(context.Background()); err != nil {
		panic(errors.New(fmt.Sprintf("failed to receive from control PubSub %v", err)))
	}
	go func() {
		controlCh := pubsub.Channel()
		fmt.Println("start listening redis channel")

		// Endlessly listen to control channel,
		for msg := range controlCh {
			err := _this.router.Route(msg.Channel, []byte(msg.Payload))
			if err != nil {
				fmt.Println("Error when handling [", msg.Channel, "]", err)
			}
		}
	}()
}
