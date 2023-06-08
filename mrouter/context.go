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
func (_this *Context) Value() PublishReq {
	return _this.ctx.Value("data").(PublishReq)
}

func (_this *Context) BindData(param interface{}) error {
	value := _this.Value()
	i, _ := json.Marshal(value.Data)
	return json.Unmarshal(i, param)
}
