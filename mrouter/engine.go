package mrouter

import (
	"encoding/json"
)

type HandlerFunc func(ctx *Context)
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

func (_this *Engine) Register(routeName string, handler HandlerFunc) {
	_this.Handlers[routeName] = handler
}

func (_this *Engine) RegisterChannelFunction(channel string, _func string, handler HandlerFunc) {
	if _, ok := _this.ChannelHandler[channel]; !ok {
		_this.ChannelHandler[channel] = make(map[string]HandlerFunc)
	}

	_this.ChannelHandler[channel][_func] = handler
}

func (_this *Engine) Route(channel string, message []byte) error {
	var req Message
	err := json.Unmarshal(message, &req)
	if err == nil {
		if handler, ok := _this.Handlers[req.Func]; ok {
			handler(WithValue(req))
		}
		if _channel, ok := _this.ChannelHandler[channel]; ok {
			if handler, ok := _channel[req.Func]; ok {
				handler(WithValue(req))
			}
		}
	}
	return _this.RouteChannel(channel, message)
}

func (_this *Engine) RouteChannel(_channel string, message []byte) error {
	if handler, ok := _this.Handlers[_channel]; ok {
		handler(WithValue(message))
	}
	return nil
}
