package mredis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type redisSentinelClient struct {
	client *redis.Client
	config *RedisConfig
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
		client: rdb,
		config: config,
	}, nil
}

func (_this *redisSentinelClient) Ping(ctx context.Context) error {
	return _this.client.Ping(ctx).Err()
}

func (_this *redisSentinelClient) Set(ctx context.Context, key string, value interface{}, expireTime int64) error {
	return _this.client.Set(ctx, _this.GetKeyName(key), value, time.Duration(expireTime)*time.Second).Err()
}

func (_this *redisSentinelClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return _this.client.Get(ctx, _this.GetKeyName(key))
}

func (_this *redisSentinelClient) Del(ctx context.Context, key string) (int64, error) {
	return _this.client.Del(ctx, _this.GetKeyName(key)).Result()
}

func (_this *redisSentinelClient) Expire(ctx context.Context, key string, expire int64) (bool, error) {
	value, err := _this.client.Expire(ctx, key, time.Duration(expire)*time.Second).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return value, nil
}

func (_this *redisSentinelClient) Exist(ctx context.Context, key string) (bool, error) {
	value, err := _this.client.Exists(ctx, _this.GetKeyName(key)).Result()
	if err != nil {
		return false, err
	}
	return value == 1, nil
}

func (_this *redisSentinelClient) Incr(ctx context.Context, key string) (int64, error) {
	return _this.client.Incr(ctx, key).Result()
}

func (_this *redisSentinelClient) TTL(ctx context.Context, key string) (int64, error) {
	result, err := _this.client.TTL(ctx, _this.GetKeyName(key)).Result()
	if err != nil {
		return 0, err
	}
	return int64(result.Seconds()), nil
}

func (_this *redisSentinelClient) Publish(ctx context.Context, channel string, value interface{}) error {
	return _this.client.Publish(ctx, _this.GetChannelName(channel), value).Err()
}

func (_this *redisSentinelClient) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	var _channels []string
	for _, c := range channels {
		_channels = append(_channels, _this.GetChannelName(c))
	}
	return _this.client.Subscribe(ctx, _channels...)
}

func (_this *redisSentinelClient) GetKeyName(key string) string {
	if _this.config.KeyPrefix != nil {
		return fmt.Sprintf("%s-%s", *_this.config.KeyPrefix, key)
	}
	return key
}

func (_this *redisSentinelClient) GetChannelName(key string) string {
	if _this.config.ChannelPrefix != nil {
		return fmt.Sprintf("%s-%s", *_this.config.ChannelPrefix, key)
	}
	return key
}

func (_this *redisSentinelClient) DelWithPrefix(ctx context.Context, prefix string) (int64, error) {
	var keysDeleted int64

	// Initialize the cursor
	var cursor uint64

	for {
		// Scan for keys with the specified prefix
		keys, nextCursor, err := _this.client.Scan(ctx, cursor, fmt.Sprintf("%s*", _this.GetKeyName(prefix)), 10).Result()
		if err != nil {
			return keysDeleted, err
		}

		// Delete the keys
		for _, key := range keys {
			if err := _this.client.Del(ctx, key).Err(); err != nil {
				return keysDeleted, err
			} else {
				keysDeleted++
			}
		}

		// Update the cursor for the next iteration
		cursor = nextCursor

		// Check if we reached the end of the iteration
		if cursor == 0 {
			break
		}
	}

	return keysDeleted, nil
}
