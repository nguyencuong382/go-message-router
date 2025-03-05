package mredis

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

type BaseRedisClient struct {
	redis.Cmdable
	prefix string
}

// Helper function to apply the prefix
func (b *BaseRedisClient) prefixedKey(key string) string {
	return b.prefix + key
}

// Generic Set function
func (b *BaseRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return b.Cmdable.Set(ctx, b.prefixedKey(key), value, expiration)
}

// Generic Get function
func (b *BaseRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return b.Cmdable.Get(ctx, b.prefixedKey(key))
}

// Generic Del function
func (b *BaseRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	for i, key := range keys {
		keys[i] = b.prefixedKey(key)
	}
	return b.Cmdable.Del(ctx, keys...)
}

type IRedisClient interface {
	redis.Cmdable // Embeds all Redis commands
	//Close() error
	//DelWithPrefix(ctx context.Context, prefix string) (int64, error)
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
