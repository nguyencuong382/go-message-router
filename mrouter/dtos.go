package mrouter

// Message is publishing/sending request
// Eg: a publishReq instance is a Publishing Data to Redis Pub/Sub, a Request to client via Websocket
type Message struct {
	ID     string `json:"id"`
	Type   int    `json:"type"`
	Result int64  `json:"result"`
	Func   string `json:"function"`
	// Whom request refer to, like sub in jwt
	Sub  string      `json:"sub"`
	Subs []string    `json:"subs"`
	Data interface{} `json:"data"`
}
