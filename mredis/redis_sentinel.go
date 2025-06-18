package mredis

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type redisSentinelClient struct {
	*BaseRedisClient
	client *redis.Client
}

func NewRedisSentinelClient(config *RedisConfig) (IRedisClient, error) {

	options := redis.FailoverOptions{
		SentinelAddrs: config.Hosts,
		Password:      config.Password,
	}
	if config.Username != nil {
		options.Username = *config.Username
	}
	if config.SentinelPassword != nil {
		options.SentinelPassword = *config.SentinelPassword
	}

	rdb := redis.NewFailoverClient(&options)

	ctx := context.Background()
	err := rdb.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	c := &redisSentinelClient{
		BaseRedisClient: &BaseRedisClient{
			Config:  config,
			Cmdable: rdb,
		},
		client: rdb,
	}

	return c, nil
}

func (_this *redisSentinelClient) MrSubscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return _this.client.Subscribe(ctx, channels...)
}
