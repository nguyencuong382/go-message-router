package mredis

import (
	"context"
	"fmt"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"github.com/redis/go-redis/v9"
	"time"
)

type IRedisSubscribe func(ctx context.Context, channels ...string) *redis.PubSub

type BaseRedisClient struct {
	Config *RedisConfig
	redis.Cmdable
}

func (_this *BaseRedisClient) PrefixedKey(key string) string {
	if _this.Config.KeyPrefix != nil {
		return mrouter.MergeKeys(*_this.Config.KeyPrefix, key)
	}
	return key
}

func (_this *BaseRedisClient) GetConfig() *RedisConfig {
	return _this.Config
}

func (_this *BaseRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return _this.Cmdable.Set(ctx, _this.PrefixedKey(key), value, expiration)
}

func (_this *BaseRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return _this.Cmdable.Get(ctx, _this.PrefixedKey(key))
}

func (_this *BaseRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	for i, key := range keys {
		keys[i] = _this.PrefixedKey(key)
	}
	return _this.Cmdable.Del(ctx, keys...)
}

func (_this *BaseRedisClient) DelWithPrefix(ctx context.Context, prefix string) (int64, error) {
	var keysDeleted int64

	// Initialize the cursor
	var cursor uint64

	for {
		// Scan for keys with the specified prefix
		keys, nextCursor, err := _this.Scan(ctx, cursor, fmt.Sprintf("%s*", _this.PrefixedKey(prefix)), 10).Result()
		if err != nil {
			return keysDeleted, err
		}

		// Delete the keys
		for _, key := range keys {
			if err := _this.Del(ctx, key).Err(); err != nil {
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

type IRedisClient interface {
	redis.Cmdable // Embeds all Redis commands
	//Close() error
	GetConfig() *RedisConfig
	DelWithPrefix(ctx context.Context, prefix string) (int64, error)
	PrefixedKey(key string) string
	//Exist(ctx context.Context, key string) (bool, error)
	//GetKeyName(key string) string
	//GetChannelName(channel string) string

	//Get(ctx context.Context, key string) *redis.StringCmd
	//Set(ctx context.Context, key string, value interface{}, expireTime int64) error
	//Del(ctx context.Context, key string) (int64, error)
	//Expire(ctx context.Context, key string, expire int64) (bool, error)
	//Ping(ctx context.Context) error
	//Incr(ctx context.Context, key string) (int64, error)
	//TTL(ctx context.Context, key string) (int64, error)
	//Publish(ctx context.Context, channel string, value interface{}) error
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}

type RedisConfig struct {
	Host             *string
	Password         string
	SentinelPassword *string
	DB               int
	Port             *string
	Addrs            []string
	MasterName       *string
	KeyPrefix        *string
	ChannelPrefix    *string
	Username         *string
}
