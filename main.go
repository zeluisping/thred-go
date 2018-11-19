package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

var counter uint64

var options = Options{
	true,
	func(c *Client) {
		fmt.Println("Connected to node")

		c.Subscribe("something/testing/123", nil)
	},
	func(c *Client, reason error) {
		fmt.Println("Disconnected from node for", reason)
	},
	func(c *Client, topic string, payload []byte) {
		atomic.AddUint64(&counter, 1)
	},
}

func testing() {
	client := NewThredClient(options)

	client.Connect("127.0.0.1:5987")

	for {
		client.Publish("something/testing/123", []byte("empty"))
		client.Publish("something/testing/123", []byte("not empty"))
		//client.Publish("something/testing/123", []byte("or is it empty"))
		//client.Publish("something/testing/123", []byte("I am not sure"))
		//client.Publish("something/testing/123", []byte("maybe it is empty"))
		time.Sleep(time.Second)
	}
}

func main() {
	for i := 0; i < 500; i++ {
		go testing()
	}

	// c := make(chan os.Signal, 1)
	// signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	// <-c
	for {
		fmt.Printf("%v messages per second\n", atomic.LoadUint64(&counter))
		atomic.StoreUint64(&counter, 0)
		time.Sleep(time.Second)
	}
}
