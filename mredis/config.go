package mredis

import "github.com/nguyencuong382/go-message-router/mrouter"

type RedisConfig struct {
	*mrouter.PubsubConfig
	Password         string
	Username         *string
	SentinelPassword *string
	DB               int
	MasterName       *string
}
