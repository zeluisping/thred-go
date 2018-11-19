package main

type Options struct {
	Reconnect bool
	// Address               string
	OnConnectHandler      func(c *Client)
	OnDisconnectHandler   func(c *Client, reason error)
	DefaultMessageHandler SubscriptionCallback
}
