package mrouter

import (
	"context"
	"encoding/json"
)

type HandlerFunc func(ctx context.Context)
type MessageRoutingFn func(router *Engine)

type Engine struct {
	Handlers       map[string]HandlerFunc
	ChannelHandler map[string]map[string]HandlerFunc
}

func New() *Engine {
	return &Engine{
		Handlers:       make(map[string]HandlerFunc),
		ChannelHandler: make(map[string]map[string]HandlerFunc),
	}
}

func (_this *Engine) Register(_func string, handler HandlerFunc) {
	_this.Handlers[_func] = handler
}

func (_this *Engine) RegisterWithChannel(channel string, _func string, handler HandlerFunc) {
	if _, ok := _this.ChannelHandler[channel]; !ok {
		_this.ChannelHandler[channel] = make(map[string]HandlerFunc)
	}

	_this.ChannelHandler[channel][_func] = handler
}

func (_this *Engine) Route(message []byte) error {
	var req PublishReq
	err := json.Unmarshal(message, &req)
	if err != nil {
		return nil
	}

	if handler, ok := _this.Handlers[req.Func]; ok {
		ctx := context.WithValue(context.Background(), "data", req)
		handler(ctx)
	}

	return nil
}

func (_this *Engine) RouteChannel(_channel string, message []byte) error {
	var req PublishReq
	err := json.Unmarshal(message, &req)
	if err != nil {
		return nil
	}

	if channel, ok := _this.ChannelHandler[_channel]; ok {
		if handler, ok := channel[req.Func]; ok {
			ctx := context.WithValue(context.Background(), "data", req)
			handler(ctx)
		}
	}

	return nil
}
