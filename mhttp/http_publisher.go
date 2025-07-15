package mhttp

import (
	"crypto/tls"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/nguyencuong382/go-message-router/mrouter"
	"go.uber.org/dig"
	"time"
)

type HttpConfig struct {
	URL         string
	Username    *string
	Password    *string
	EnableTrace bool
}

type httpPub struct {
	Config *HttpConfig
}

type HttpPublishArgs struct {
	dig.In
	Config *HttpConfig
}

func NewHttpPublisher(args HttpPublishArgs) mrouter.IPublisher {
	return &httpPub{
		Config: args.Config,
	}
}

func (_this *httpPub) Publish(req *mrouter.PublishReq) (int64, error) {
	// Create a Resty Client
	client := resty.New()

	// Unique settings at Client level
	//--------------------------------
	// Enable debug mode
	if _this.Config.EnableTrace {
		client.SetDebug(true)
	}

	// or One can disable security check (https)
	client.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})

	// Set client timeout as per your need
	client.SetTimeout(10 * time.Second)

	if _this.Config.Username != nil && _this.Config.Password != nil {
		client = client.SetBasicAuth(*_this.Config.Username, *_this.Config.Password)
	}

	_, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(req.Value).
		Post(fmt.Sprintf("%s/%s", _this.Config.URL, req.URL))
	return 0, err
}
