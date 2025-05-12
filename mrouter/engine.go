package mrouter

import (
	"encoding/json"
	"log"
)

type HandlerFunc func(ctx *Context) error
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
	var (
		req   Message
		value interface{}
	)
	err := json.Unmarshal(message, &req)
	if err == nil {
		value = req
	} else {
		value = message
	}

	log.Println("Routing:", channel, req.Func)

	if handler, ok := _this.Handlers[req.Func]; ok {
		return handler(WithValue(value))
	}
	if _channel, ok := _this.ChannelHandler[channel]; ok {
		if handler, ok := _channel[req.Func]; ok {
			return handler(WithValue(value))
		}
	}
	return _this.RouteChannel(channel, value)
}

func (_this *Engine) RouteChannel(_channel string, value interface{}) error {
	if handler, ok := _this.Handlers[_channel]; ok {
		return handler(WithValue(value))
	}
	return nil
}
