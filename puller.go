package nanodm

import (
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
	"nanomsg.org/go/mangos/v2"
	"nanomsg.org/go/mangos/v2/protocol/pull"

	// register transports
	_ "nanomsg.org/go/mangos/v2/transport/tcp"
)

type Puller struct {
	log         *logrus.Entry
	url         string
	messageChan chan Message

	pullSock mangos.Socket
}

func NewPuller(log *logrus.Entry, url string, messageChan chan Message) *Puller {

	return &Puller{
		log:         log,
		url:         url,
		messageChan: messageChan,
	}
}

func (pu *Puller) Start() error {
	var err error

	if pu.pullSock, err = pull.NewSocket(); err != nil {
		pu.log.Errorf("Failed to open pull socket: %v", err)
		return err
	}

	if err = pu.pullSock.Listen(pu.url); err != nil {
		pu.log.Errorf("Failed to Listen for pull socket on %s: %v", pu.url, err)
		return err
	}
	pu.pullSock.SetPipeEventHook(func(event mangos.PipeEvent, pipe mangos.Pipe) {
		pu.log.Debugf("Pull socket event (%s) (%v) %+v", pu.url, event, pipe)
	})

	go pu.pullTask()

	return nil
}

func (pu *Puller) Stop() error {
	err := pu.pullSock.Close()
	if err != nil {
		pu.log.Errorf("Failed to close pull socket: %v", err)
	}

	return err
}

func (pu *Puller) pullTask() {
	defer pu.log.Infof("Exiting pullTask (%s)", pu.url)
	for {
		var message Message
		msgBytes, err := pu.pullSock.Recv()
		if err != nil {
			pu.log.Errorf("cannot receive from mangos Socket (%s): %v", pu.url, err)
			return
		}
		if len(msgBytes) == 0 {
			continue
		}
		err = msgpack.Unmarshal(msgBytes, &message)
		if err != nil {
			pu.log.Errorf("cannot Unmarshal (%s)(%v): %v", pu.url, msgBytes, err)
		}
		pu.messageChan <- message
	}
}
