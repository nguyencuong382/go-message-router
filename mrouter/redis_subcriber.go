package mrouter

import (
	"context"
	"github.com/redis/go-redis/v9"
	"go.uber.org/dig"
)

type server struct {
	routing MessageRoutingFn
	router  *Engine
	redis   *redis.Client
}

type Params struct {
	dig.In
	Routing MessageRoutingFn
	Router  *Engine
	Redis   *redis.Client
}

func NewRedisSubscriber(params Params) ISubscriber {
	return &server{
		router:  params.Router,
		routing: params.Routing,
		redis:   params.Redis,
	}
}

func (_this *server) Open(channels []string) error {
	_this.routing(_this.router)
	for _, channel := range channels {
		_this.Run(channel)
	}
	return nil
}

func (_this *server) Run(channel string) {
	go func() {
		ctx := context.Background()
		subscriber := _this.redis.Subscribe(ctx, channel)
		for {
			msg, err := subscriber.ReceiveMessage(ctx)
			if err != nil {
				panic(err)
			}
			switch msg := interface{}(msg).(type) {
			case *redis.Message:
				//log.Info("Received msg on channel [", msg.Channel, "]")
				err := _this.router.RouteChannel(channel, []byte(msg.Payload))
				if err != nil {
					//log.Info("Error when handling [", msg.Channel, "]", err)
				}
			default:
				//log.Info("Got control message", msg)
			}
		}
	}()
}
