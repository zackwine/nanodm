package nanodm

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol/push"

	// register transports
	_ "nanomsg.org/go/mangos/v2/transport/tcp"
)

const (
	RETRY_PERIOD     = 2 * time.Second
	MAX_RETRY_PERIOD = time.Minute
)

type Pusher struct {
	log         *logrus.Entry
	url         string
	messageChan chan Message

	pushSock  mangos.Socket
	closeChan chan struct{}
}

func NewPusher(log *logrus.Entry, url string, messageChan chan Message) *Pusher {

	return &Pusher{
		log:         log,
		url:         url,
		messageChan: messageChan,
		closeChan:   make(chan struct{}),
	}
}

func (pu *Pusher) Start() error {
	var err error

	if err = pu.connect(); err != nil {
		return err
	}

	go pu.pushTask()
	return err
}

func (pu *Pusher) Stop() error {
	close(pu.closeChan)
	err := pu.pushSock.Close()
	if err != nil {
		pu.log.Errorf("Failed to close push socket: %v", err)
	}

	return err
}

func (pu *Pusher) connect() error {
	var err error

	if pu.pushSock, err = push.NewSocket(); err != nil {
		logrus.Errorf("can't get new push socket: %v", err)
		return err
	}

	err = pu.pushSock.Dial(pu.url)
	retryWait := RETRY_PERIOD

	// Retry forever with progressive backoff
	for err != nil {

		if retryWait < MAX_RETRY_PERIOD {
			retryWait = retryWait * 2
		}
		logrus.Errorf("failed to dial (%s) push socket retry in (%v), error: %v", pu.url, retryWait, err)
		<-time.After(retryWait)
		err = pu.pushSock.Dial(pu.url)
	}

	return nil
}

func (pu *Pusher) pushTask() {
	for {
		select {
		case message := <-pu.messageChan:
			msgBytes, err := msgpack.Marshal(&message)
			if err != nil {
				pu.log.Errorf("[%s] Failed to Marshal message %+v: %v", pu.url, message, err)
				continue
			}
			err = pu.pushSock.Send(msgBytes)
			if err != nil {
				pu.log.Errorf("[%s] Failed to send message %+v: %v", pu.url, message, err)
			}
		case <-pu.closeChan:
			pu.log.Warnf("[%s] closing push task", pu.url)
			return
		}
	}
}
