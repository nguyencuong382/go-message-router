package mredis

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type IRedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expireTime int64) error
	Get(ctx context.Context, key string) (interface{}, error)
	Del(ctx context.Context, key string) (int64, error)
	Expire(ctx context.Context, key string, expire int64) (bool, error)
	Ping(ctx context.Context) error
	Exist(ctx context.Context, key string) (bool, error)
	Incr(ctx context.Context, key string) (int64, error)
	TTL(ctx context.Context, key string) (int64, error)
	Publish(ctx context.Context, channel string, value interface{}) error
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}

type RedisConfig struct {
	Host       *string
	Password   string
	DB         int
	Port       *string
	Addrs      []string
	MasterName *string
}
