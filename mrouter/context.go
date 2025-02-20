package mrouter

import (
	"context"
	"encoding/json"
)

type Context struct {
	ctx context.Context
}

func WithValue(value interface{}) *Context {
	return &Context{
		ctx: context.WithValue(context.Background(), "data", value),
	}
}

func (_this *Context) Message() Message {
	return _this.ctx.Value("data").(Message)
}

func (_this *Context) Value() any {
	return _this.ctx.Value("data")
}

func (_this *Context) BindData(param interface{}) error {
	value := _this.Message()
	i, _ := json.Marshal(value.Data)
	return json.Unmarshal(i, param)
}

func (_this *Context) Context() context.Context {
	return _this.ctx
}
