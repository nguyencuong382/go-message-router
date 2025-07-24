package mrouter

import (
	"context"
	"encoding/json"
	"sync"
)

type Context struct {
	ctx          context.Context
	AppCtx       context.Context
	AppWaitGroup *sync.WaitGroup
	Offset       int64
}

func WithValue(value interface{}) *Context {
	return &Context{
		ctx: context.WithValue(context.Background(), "data", value),
	}
}

func WithAppContextValue(args *OpenServerArgs, value interface{}, offset int64) *Context {
	return &Context{
		AppCtx:       args.AppCtx,
		AppWaitGroup: args.AppWaitGroup,
		ctx:          context.WithValue(context.Background(), "data", value),
		Offset:       offset,
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
