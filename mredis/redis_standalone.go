package mredis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type redisCmd struct {
	client *redis.Client
	config *RedisConfig
}

func NewRedisStandaloneClient(config *RedisConfig) (IRedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", *config.Host, *config.Port),
		Password: config.Password,
		DB:       config.DB,
	})

	ctx := context.Background()
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, err
	}

	return &redisCmd{
		client: client,
		config: config,
	}, nil
}

func (_this *redisCmd) Ping(ctx context.Context) error {
	if err := _this.client.Ping(ctx).Err(); err != nil {
		return err
	}
	return nil
}

func (_this *redisCmd) Set(ctx context.Context, key string, value interface{}, expireTime int64) error {
	_, err := _this.client.Set(ctx, _this.GetKeyName(key), value, time.Duration(expireTime)*time.Second).Result()
	return err
}

func (_this *redisCmd) Get(ctx context.Context, key string) *redis.StringCmd {
	return _this.client.Get(ctx, _this.GetKeyName(key))
}

func (_this *redisCmd) Del(ctx context.Context, key string) (int64, error) {
	return _this.client.Del(ctx, _this.GetKeyName(key)).Result()
}

func (_this *redisCmd) Expire(ctx context.Context, key string, expire int64) (bool, error) {
	value, err := _this.client.Expire(ctx, _this.GetKeyName(key), time.Duration(expire)*time.Second).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return value, nil
}

func (_this *redisCmd) Exist(ctx context.Context, key string) (bool, error) {
	value, err := _this.client.Exists(ctx, _this.GetKeyName(key)).Result()
	if err != nil {
		return false, err
	}
	return value == 1, nil
}

func (_this *redisCmd) Incr(ctx context.Context, key string) (int64, error) {
	result, err := _this.client.Incr(ctx, _this.GetKeyName(key)).Result()
	if err != nil {
		return 0, err
	}
	return result, nil
}

func (_this *redisCmd) TTL(ctx context.Context, key string) (int64, error) {
	result, err := _this.client.TTL(ctx, _this.GetKeyName(key)).Result()
	if err != nil {
		return 0, err
	}
	return int64(result.Seconds()), nil
}

func (_this *redisCmd) Publish(ctx context.Context, channel string, value interface{}) error {
	return _this.client.Publish(ctx, _this.GetChannelName(channel), value).Err()
}

func (_this *redisCmd) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	var _channels []string
	for _, c := range channels {
		_channels = append(_channels, _this.GetChannelName(c))
	}
	return _this.client.Subscribe(ctx, _channels...)
}

func (_this *redisCmd) GetKeyName(key string) string {
	if _this.config.KeyPrefix != nil {
		return fmt.Sprintf("%s-%s", *_this.config.KeyPrefix, key)
	}
	return key
}

func (_this *redisCmd) GetChannelName(key string) string {
	if _this.config.ChannelPrefix != nil {
		return fmt.Sprintf("%s-%s", *_this.config.ChannelPrefix, key)
	}
	return key
}
