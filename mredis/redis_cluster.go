package mredis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type redisClusterClient struct {
	client *redis.ClusterClient
	config *RedisConfig
}

func NewRedisClusterClient(config *RedisConfig) (IRedisClient, error) {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.Addrs,
		Password: config.Password,
	})

	ctx := context.Background()
	err := rdb.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	})
	if err != nil {
		return nil, err
	}

	return &redisClusterClient{
		client: rdb,
		config: config,
	}, nil
}

func (_this *redisClusterClient) Ping(ctx context.Context) error {
	err := _this.client.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	})
	return err
}

func (_this *redisClusterClient) Set(ctx context.Context, key string, value interface{}, expireTime int64) error {
	return _this.client.Set(ctx, _this.GetKey(key), value, time.Duration(expireTime)*time.Second).Err()
}

func (_this *redisClusterClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return _this.client.Get(ctx, _this.GetKey(key))
}

func (_this *redisClusterClient) Del(ctx context.Context, key string) (int64, error) {
	return _this.client.Del(ctx, _this.GetKey(key)).Result()
}

func (_this *redisClusterClient) Expire(ctx context.Context, key string, expire int64) (bool, error) {
	value, err := _this.client.Expire(ctx, key, time.Duration(expire)*time.Second).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return value, nil
}

func (_this *redisClusterClient) Exist(ctx context.Context, key string) (bool, error) {
	value, err := _this.client.Exists(ctx, _this.GetKey(key)).Result()
	if err != nil {
		return false, err
	}
	return value == 1, nil
}

func (_this *redisClusterClient) Incr(ctx context.Context, key string) (int64, error) {
	return _this.client.Incr(ctx, key).Result()
}

func (_this *redisClusterClient) TTL(ctx context.Context, key string) (int64, error) {
	result, err := _this.client.TTL(ctx, _this.GetKey(key)).Result()
	if err != nil {
		return 0, err
	}
	return int64(result.Seconds()), nil
}

func (_this *redisClusterClient) Publish(ctx context.Context, channel string, value interface{}) error {
	return _this.client.Publish(ctx, _this.GetChannel(channel), value).Err()
}

func (_this *redisClusterClient) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	var _channels []string
	for _, c := range channels {
		_channels = append(_channels, _this.GetChannel(c))
	}
	return _this.client.Subscribe(ctx, _channels...)
}

func (_this *redisClusterClient) GetKey(key string) string {
	if _this.config.KeyPrefix != nil {
		return fmt.Sprintf("%s-%s", *_this.config.KeyPrefix, key)
	}
	return key
}

func (_this *redisClusterClient) GetChannel(key string) string {
	if _this.config.KeyPrefix != nil {
		return fmt.Sprintf("%s-%s", *_this.config.ChannelPrefix, key)
	}
	return key
}