package mredis

import (
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

func (_this *redisSubscriber) Open(args *mrouter.OpenServerArgs) error {
	_this.routing(_this.router)
	args.Channels = _this.config.GetChannels(args.Channels...)
	log.Printf("[Redis] Subscribe channels: %v\n", args.Channels)
	_this.Run(args)
	return nil
}

func (_this *redisSubscriber) Run(args *mrouter.OpenServerArgs) {
	ctx := args.AppCtx
	subscriber := _this.redis.MrSubscribe(ctx, args.Channels...)
	defer func() {
		if err := subscriber.Close(); err != nil {
			log.Println("[Redis] Error closing subscriber:", err)
		} else {
			log.Println("[Redis] Redis subscriber closed")
		}
		//if err := _this.redis.MrClose(); err != nil {
		//	log.Println("[Redis] Error closing redis:", err)
		//} else {
		//	log.Println("[Redis] Redis redis closed")
		//}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Redis] Context canceled, stopping Redis subscriber loop")
			return
		default:
			msg, err := subscriber.ReceiveMessage(ctx)
			if err != nil {
				// Nếu context bị huỷ thì không panic
				if ctx.Err() != nil {
					log.Println("[Redis] ReceiveMessage stopped:", ctx.Err())
					return
				}
				panic(err)
			}
			switch interface{}(msg).(type) {
			case *redis.Message:
				log.Println("[Redis] Received msg on channel [", msg.Channel, "]")
				err := _this.router.Route(args, msg.Channel, []byte(msg.Payload))
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

}
