package mredis

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type redisClusterClient struct {
	*BaseRedisClient
	client *redis.ClusterClient
}

func NewRedisClusterClient(config *RedisConfig) (IRedisClient, error) {
	options := redis.ClusterOptions{
		Addrs:    config.Hosts,
		Password: config.Password,
	}
	if config.Username != nil {
		options.Username = *config.Username
	}
	rdb := redis.NewClusterClient(&options)

	ctx := context.Background()
	err := rdb.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	c := &redisClusterClient{
		BaseRedisClient: &BaseRedisClient{
			Cmdable: rdb,
			Config:  config,
		},
		client: rdb,
	}

	//rdb.AddHook(c)

	return c, nil
}

func (_this *redisClusterClient) MrSubscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return _this.client.Subscribe(ctx, channels...)
}

func (_this *redisClusterClient) MrClose() error {
	return _this.client.Close()
}
