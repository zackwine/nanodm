package collector

import (
	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
)

/*
 * Client:  This is the server side representation of the client
 */

type Client struct {
	log        *logrus.Entry
	sourceName string
	clientUrl  string
	pusher     *nanodm.Pusher
	pusherChan chan nanodm.Message

	objects []nanodm.Object
}

func NewClient(log *logrus.Entry, sourceName string, clientUrl string) *Client {
	return &Client{
		log:        log,
		sourceName: sourceName,
		clientUrl:  clientUrl,
		pusherChan: make(chan nanodm.Message),
	}
}

// Connect - connect to the client nanomsg pull socket
func (cl *Client) Connect() error {
	cl.pusher = nanodm.NewPusher(cl.log, cl.clientUrl, cl.pusherChan)
	return cl.pusher.Start()
}

func (cl *Client) Disconnect() error {
	return cl.pusher.Stop()
}

func (cl *Client) Send(message nanodm.Message) {
	cl.pusherChan <- message
}

func (cl *Client) GetMessage(msgType nanodm.MessageType) nanodm.Message {
	return nanodm.Message{
		Type:        msgType,
		SourceName:  cl.sourceName,
		Destination: cl.clientUrl,
	}
}
