package mredis

import (
	"context"
	"fmt"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"github.com/redis/go-redis/v9"
	"time"
)

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

func (_this *BaseRedisClient) MrSet(ctx context.Context, key string, value interface{}, expireTime int64) *redis.StatusCmd {
	return _this.Set(ctx, _this.PrefixedKey(key), value, time.Duration(expireTime)*time.Second)
}

func (_this *BaseRedisClient) MrGet(ctx context.Context, key string) *redis.StringCmd {
	return _this.Get(ctx, _this.PrefixedKey(key))
}

func (_this *BaseRedisClient) MrDel(ctx context.Context, key string) *redis.IntCmd {
	return _this.Del(ctx, _this.PrefixedKey(key))
}

func (_this *BaseRedisClient) MrExpire(ctx context.Context, key string, expire int64) *redis.BoolCmd {
	return _this.Expire(ctx, key, time.Duration(expire)*time.Second)
}

func (_this *BaseRedisClient) MrExist(ctx context.Context, key string) (bool, error) {
	value, err := _this.Exists(ctx, _this.PrefixedKey(key)).Result()
	if err != nil {
		return false, err
	}
	return value == 1, nil
}

func (_this *BaseRedisClient) MrIncr(ctx context.Context, key string) (int64, error) {
	return _this.Incr(ctx, key).Result()
}

func (_this *BaseRedisClient) MrTTL(ctx context.Context, key string) (int64, error) {
	result, err := _this.TTL(ctx, _this.PrefixedKey(key)).Result()
	if err != nil {
		return 0, err
	}
	return int64(result.Seconds()), nil
}

func (_this *BaseRedisClient) MrPublish(ctx context.Context, channel string, value interface{}) error {
	_channel := _this.PrefixedKey(channel)

	if _this.GetConfig().Debug {
		fmt.Printf("Publish chanel: %v\n", _channel)
	}

	return _this.Publish(ctx, _this.PrefixedKey(channel), value).Err()
}

func (_this *BaseRedisClient) MrDelWithPrefix(ctx context.Context, prefix string) (int64, error) {
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
