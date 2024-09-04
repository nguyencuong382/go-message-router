package mredis

import (
	"context"
	"errors"
	"fmt"
	"github.com/nguyencuong382/go-message-router/mrouter"
)

type redisSubscriberV2 struct {
	routing mrouter.MessageRoutingFn
	router  *mrouter.Engine
	redis   IRedisClient
}

func NewRedisSubscriberV2(params RedisSubscriberArgs) mrouter.ISubscriber {
	return &redisSubscriberV2{
		router:  params.Router,
		routing: params.Routing,
		redis:   params.Redis,
	}
}

func (_this *redisSubscriberV2) Open(channels []string) error {
	_this.routing(_this.router)
	_this.Run(channels...)
	return nil
}

func (_this *redisSubscriberV2) Run(channels ...string) {
	ctx := context.Background()
	pubsub := _this.redis.Subscribe(ctx, channels...)

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
