package mredis

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type IRedisClient interface {
	redis.Cmdable // Embeds all Redis commands
	GetConfig() *RedisConfig
	PrefixedKey(key string) string
	MrDelWithPrefix(ctx context.Context, prefix string) (int64, error)
	MrExist(ctx context.Context, key string) (bool, error)
	MrGet(ctx context.Context, key string) *redis.StringCmd
	MrSet(ctx context.Context, key string, value interface{}, expireTime int64) *redis.StatusCmd
	MrDel(ctx context.Context, key string) *redis.IntCmd
	MrExpire(ctx context.Context, key string, expire int64) *redis.BoolCmd
	MrIncr(ctx context.Context, key string) (int64, error)
	MrTTL(ctx context.Context, key string) (int64, error)
	MrPublish(ctx context.Context, channel string, value interface{}) error
	MrSubscribe(ctx context.Context, channels ...string) *redis.PubSub
	MrClose() error
}
