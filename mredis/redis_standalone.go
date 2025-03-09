package mredis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

type redisCmd struct {
	*BaseRedisClient
	client *redis.Client
}

func NewRedisStandaloneClient(config *RedisConfig) (IRedisClient, error) {
	options := redis.Options{
		Addr:     fmt.Sprintf("%s:%s", *config.Host, *config.Port),
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

	return &redisCmd{
		BaseRedisClient: &BaseRedisClient{
			Config:  config,
			Cmdable: client,
		},
		client: client,
	}, nil
}

func (_this *redisCmd) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	var _channels []string
	for _, c := range channels {
		_channels = append(_channels, _this.prefixedKey(c))
	}
	return _this.client.Subscribe(ctx, _channels...)
}
