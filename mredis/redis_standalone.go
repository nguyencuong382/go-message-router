package mredis

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type redisCmd struct {
	*BaseRedisClient
	client *redis.Client
}

func NewRedisStandaloneClient(config *RedisConfig) (IRedisClient, error) {
	options := redis.Options{
		Addr:     config.Hosts[0],
		Password: config.Password,
		DB:       config.DB,
	}
	if config.Username != nil {
		options.Username = *config.Username
	}
	client := redis.NewClient(&options)

	ctx := context.Background()
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, err
	}

	c := &redisCmd{
		BaseRedisClient: &BaseRedisClient{
			Config:  config,
			Cmdable: client,
		},
		client: client,
	}

	//client.AddHook(c)

	return c, nil
}

func (_this *redisCmd) MrSubscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return _this.client.Subscribe(ctx, channels...)
}
