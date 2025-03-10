package mredis

import (
	"context"
	"github.com/redis/go-redis/v9"
)

var _ redis.Hook = (*BaseRedisClient)(nil)

// List of commands that should be prefixed
var commandsToPrefix = map[string]bool{
	"set":     true,
	"get":     true,
	"exists":  true,
	"expire":  true,
	"publish": true,
}

// DialHook is used to intercept and modify the behavior of the connection establishment process.
// When it's called: This hook is invoked whenever a new connection to the Redis server is established.
// No need in this case, just skip
func (h *BaseRedisClient) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook is used to intercept and modify the behavior of individual commands sent to the Redis server.
// When it's called: This hook is invoked before any command is processed.
func (h *BaseRedisClient) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.hookRedisCmder(cmd)
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook is used to intercept and modify the behavior of commands sent to the Redis server as part of a pipeline.
// When it's called: This hook is invoked before a set of commands in a pipeline is processed.
func (h *BaseRedisClient) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmd []redis.Cmder) error {
		for _, cmd := range cmd {
			h.hookRedisCmder(cmd)
		}
		return next(ctx, cmd)
	}
}

func (h *BaseRedisClient) hookRedisCmder(cmd redis.Cmder) {
	if len(cmd.Args()) > 1 {
		cmdName, ok := cmd.Args()[0].(string)
		if ok && commandsToPrefix[cmdName] {
			if key, ok := cmd.Args()[1].(string); ok {
				cmd.Args()[1] = h.PrefixedKey(key)
			}
		}
	}
}
