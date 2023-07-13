package mrouter

// PublishReq is publishing/sending request
// Eg: a publishReq instance is a Publishing Data to Redis Pub/Sub, a Request to client via Websocket
type PublishReq struct {
	ID     string `json:"id"`
	Type   int    `json:"type"`
	Result int64  `json:"result"`
	Func   string `json:"function"`
	// Whom request refer to, like sub in jwt
	Sub  string      `json:"sub"`
	Data interface{} `json:"data"`
}

type SubscribeRes struct {
	ID string `json:"id"`
	// Type of request
	Type int    `json:"type"`
	Func string `json:"function"`
	// Result Code response
	// Eg: 200 - OK
	Result int64       `json:"result"`
	Data   interface{} `json:"data"`
}
