package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// Message types.
const (
	SUBSCRIBE   = byte(1)
	UNSUBSCRIBE = byte(2)
	PUBLISH     = byte(3)
)

// SubscriptionCallback for handling messages.
type SubscriptionCallback func(c *Client, topic string, payload []byte)

// ErrNotConnected happens when trying to do something while not connected.
var ErrNotConnected = errors.New("Not connected")

// Client represents a connection to a node.
type Client struct {
	options   Options
	address   string
	conn      net.Conn
	subs      map[string]SubscriptionCallback
	connected bool
}

// NewThredClient creates a new client instance.
func NewThredClient(options Options) *Client {
	return &Client{
		options,
		"",
		nil,
		nil,
		false,
	}
}

// Connect to a node.
func (c *Client) Connect(address string) {
	if c.connected {
		return
	}

	c.address = address

	go c.reconnect(nil)
}

// Subscribe to a topic.
func (c *Client) Subscribe(topic string, callback SubscriptionCallback) error {
	if c.connected == false {
		return ErrNotConnected
	}

	buff := bytes.NewBuffer(make([]byte, 0, 1+4+len(topic)))

	err := buff.WriteByte(SUBSCRIBE)
	if err != nil {
		fmt.Println("error writing packet type")
		return err
	}

	err = binary.Write(buff, binary.LittleEndian, uint32(len(topic)))
	if err != nil {
		fmt.Println("error writing topic length")
		return err
	}

	_, err = buff.WriteString(topic)
	if err != nil {
		fmt.Println("error writing topic data")
		return err
	}

	if callback != nil {
		c.subs[topic] = callback
	}

	buff.WriteTo(c.conn)

	return nil
}

// Unsubscribe from a topic.
func (c *Client) Unsubscribe(topic string) error {
	if c.connected == false {
		return ErrNotConnected
	}

	buff := bytes.NewBuffer(make([]byte, 0, 1+4+len(topic)))

	err := buff.WriteByte(SUBSCRIBE)
	if err != nil {
		fmt.Println("error writing packet type", err)
		return err
	}

	err = binary.Write(buff, binary.LittleEndian, uint32(len(topic)))
	if err != nil {
		fmt.Println("error writing topic length", err)
		return err
	}

	_, err = buff.WriteString(topic)
	if err != nil {
		fmt.Println("error writing topic data", err)
		return err
	}

	_, err = buff.WriteTo(c.conn)
	if err != nil {
		fmt.Println("error writing data to connection", err)
		return err
	}

	delete(c.subs, topic)

	return nil
}

// Publish a message to a topic.
func (c *Client) Publish(topic string, payload []byte) error {
	if c.connected == false {
		return ErrNotConnected
	}

	buff := bytes.NewBuffer(make([]byte, 0, 1+8+len(topic)+len(payload)))

	err := buff.WriteByte(PUBLISH)
	if err != nil {
		fmt.Println("error writing packet type")
		return err
	}

	err = binary.Write(buff, binary.LittleEndian, uint32(len(topic)))
	if err != nil {
		fmt.Println("error writing topic length")
		return err
	}

	_, err = buff.WriteString(topic)
	if err != nil {
		fmt.Println("error writing topic data")
		return err
	}

	err = binary.Write(buff, binary.LittleEndian, uint32(len(payload)))
	if err != nil {
		fmt.Println("error writing payload length")
		return err
	}

	_, err = buff.Write(payload)
	if err != nil {
		fmt.Println("error writing payload data")
		return err
	}

	//c.conn.Write(buff.Bytes())
	buff.WriteTo(c.conn)

	return nil
}

func (c *Client) reconnect(reason error) {
	// make sure it is closed
	if c.conn != nil {
		c.conn.Close()
	}

	// if it was previously connected, call disconnect handler
	if c.connected == true && c.options.OnDisconnectHandler != nil {
		go c.options.OnDisconnectHandler(c, reason)
	}

	// if this is a reconnect and reconnect is disabled, abort
	if c.connected == true && c.options.Reconnect == false {
		// but first update the flag
		c.connected = false

		// now yes, abort
		return
	}

	// update flag before reconnecting
	c.connected = false

	// reconnect
	var conn net.Conn
	var err = io.EOF // dummy value

	for err != nil {
		conn, err = net.Dial("tcp4", c.address)
	}

	c.conn = conn
	c.connected = true

	if c.options.OnConnectHandler != nil {
		go c.options.OnConnectHandler(c)
	}

	// @note no need to create new goroutine, we are
	// already one that is about to expire, so reuse
	readHandler(c)
}

func whelele(conn *net.Conn, into *[]byte) error {
	var sure int

	intocap := cap(*into)

	for {
		tmp := make([]byte, intocap-sure)

		// read topic
		n, err := (*conn).Read(tmp)

		// make sure no error occurred
		if err != nil {
			return err
		}

		sure += n

		*into = append(*into, tmp...)

		if sure == intocap {
			break
		}
	}

	return nil
}

func readHandler(c *Client) {
	//reader := bufio.NewReader(c.conn)

	var (
		t   []byte
		err error
	)

	t = make([]byte, 0, 1)

	for {
		t = t[:0]

		err = whelele(&c.conn, &t)

		// make sure no error occurred
		if err != nil {
			break
		}

		// make sure it's a valid type
		if t[0] != PUBLISH {
			break
		}

		var (
			topiclen uint32
			topic    []byte
		)

		// retrieve topic length
		err = binary.Read(c.conn, binary.LittleEndian, &topiclen)
		if err != nil {
			break
		}

		// prepare topic data holder
		topic = make([]byte, topiclen)
		err = whelele(&c.conn, &topic)
		if err != nil {
			break
		}

		var (
			payloadlen uint32
			payload    []byte
		)

		// retrieve payload length
		err = binary.Read(c.conn, binary.LittleEndian, &payloadlen)

		// make sure no error occurred
		if err != nil {
			break
		}

		// prepare payload data holder
		payload = make([]byte, payloadlen)
		err = whelele(&c.conn, &payload)
		if err != nil {
			break
		}

		// check if there is a specific handler
		if c.subs[string(topic)] != nil {
			// yes there is, invoke it
			go c.subs[string(topic)](c, string(topic), payload)
		} else if c.options.DefaultMessageHandler != nil {
			go c.options.DefaultMessageHandler(c, string(topic), payload)
		}
	}

	c.reconnect(err)
}

func _readHandler(c *Client) {
	reader := bufio.NewReader(c.conn)

	var (
		t   byte
		n   int
		err error
	)

	for {
		// read packet type
		t, err = reader.ReadByte()

		// make sure no error occurred
		if err != nil {
			break
		}

		// make sure it's a valid type
		if t != PUBLISH {
			break
		}

		var (
			topiclen uint32
			topic    []byte
		)

		// retrieve topic length
		err = binary.Read(reader, binary.LittleEndian, &topiclen)
		if err != nil {
			break
		}

		// prepare topic data holder
		topic = make([]byte, topiclen)

		var sure uint32

		for {
			tmp := make([]byte, topiclen-sure)

			// read topic
			n, err = reader.Read(tmp)

			// make sure no error occurred
			if err != nil {
				break
			}

			sure += uint32(n)

			topic = append(topic, tmp...)

			if sure == uint32(topiclen) {
				break
			}
		}

		var (
			payloadlen uint32
			payload    []byte
		)

		// retrieve payload length
		err = binary.Read(reader, binary.LittleEndian, &payloadlen)

		// make sure no error occurred
		if err != nil {
			break
		}

		// prepare payload data holder
		payload = make([]byte, payloadlen)

		sure = 0
		for {
			tmp := make([]byte, payloadlen-sure)

			// read topic
			n, err = reader.Read(tmp)

			// make sure no error occurred
			if err != nil {
				break
			}

			sure += uint32(n)

			payload = append(payload, tmp...)

			if sure == uint32(payloadlen) {
				break
			}
		}

		// check if there is a specific handler
		if c.subs[string(topic)] != nil {
			// yes there is, invoke it
			go c.subs[string(topic)](c, string(topic), payload)
		} else if c.options.DefaultMessageHandler != nil {
			go c.options.DefaultMessageHandler(c, string(topic), payload)
		}
	}

	c.reconnect(err)
}
