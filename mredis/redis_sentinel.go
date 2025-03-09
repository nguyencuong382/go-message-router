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
		SentinelAddrs: config.Addrs,
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

	return &redisSentinelClient{
		BaseRedisClient: &BaseRedisClient{
			Config:  config,
			Cmdable: rdb,
		},
		client: rdb,
	}, nil
}

func (_this *redisSentinelClient) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	var _channels []string
	for _, c := range channels {
		_channels = append(_channels, _this.prefixedKey(c))
	}
	return _this.client.Subscribe(ctx, _channels...)
}
