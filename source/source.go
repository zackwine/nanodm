package source

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
)

type Source struct {
	log       *logrus.Entry
	name      string
	serverUrl string
	pullUrl   string
	callbacks SourceInterface

	pusher     *nanodm.Pusher
	pusherChan chan nanodm.Message

	puller      *nanodm.Puller
	pullerChan  chan nanodm.Message
	pullerClose chan struct{}
	ackMap      *nanodm.ConcurrentMessageMap
}

type SourceInterface interface {
	GetObjects(objectNames []string) (objects []nanodm.Object, err error)
	SetObjects(objects []nanodm.Object) error
}

// NewSource creates a new source where `name` should be unique to the
// server at `serverUrl`.
func NewSource(log *logrus.Entry, name string, serverUrl string, pullUrl string, callbacks SourceInterface) *Source {
	return &Source{
		log:         log,
		name:        name,
		serverUrl:   serverUrl,
		pullUrl:     pullUrl,
		callbacks:   callbacks,
		pusherChan:  make(chan nanodm.Message),
		pullerChan:  make(chan nanodm.Message),
		pullerClose: make(chan struct{}),
		ackMap:      nanodm.NewConcurrentMessageMap(),
	}
}

func (so *Source) newMessage(msgType nanodm.MessageType) nanodm.Message {
	return nanodm.Message{
		Type:           msgType,
		SourceName:     so.name,
		Source:         so.pullUrl,
		Destination:    so.serverUrl,
		TransactionUID: nanodm.GetTransactionUID(),
	}
}

func (so *Source) Connect() error {
	so.pusher = nanodm.NewPusher(so.log, so.serverUrl, so.pusherChan)
	err := so.pusher.Start()
	if err != nil {
		return err
	}

	so.puller = nanodm.NewPuller(so.log, so.pullUrl, so.pullerChan)
	err = so.puller.Start()
	if err != nil {
		so.pusher.Stop()
		return err
	}
	go so.pullerTask()

	return nil
}

func (so *Source) Disconnect() error {
	so.Deregister()
	return nil
}

func (so *Source) Register(objects []nanodm.Object) error {
	message := so.newMessage(nanodm.RegisterMessageType)
	message.Objects = objects
	so.pusherChan <- message

	// Wait for ack
	ackMessage, err := so.ackMap.WaitForKey(message.TransactionUID.String(), 10*time.Second)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("received registration error: %v", ackMessage.Error)
	} else {
		return fmt.Errorf("received unknown message type (%d)", ackMessage.Type)
	}
}

func (so *Source) Deregister() error {
	var err error
	deregMessage := so.newMessage(nanodm.DeregisterMessageType)
	so.pusherChan <- deregMessage

	ackMessage, err := so.ackMap.WaitForKey(deregMessage.TransactionUID.String(), 10*time.Second)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("received deregistration error: %v", ackMessage.Error)
	} else {
		return fmt.Errorf("received unknown message type (%d)", ackMessage.Type)
	}

}

func (so *Source) pullerTask() {
	for {
		select {
		case message := <-so.pullerChan:
			so.log.Infof("%+v", message)
			switch {
			case message.Type == nanodm.AckMessageType || message.Type == nanodm.NackMessageType:
				so.ackMap.Set(message.TransactionUID.String(), message)
			case message.Type == nanodm.SetMessageType:
				so.handleSet(message)
			case message.Type == nanodm.GetMessageType:
				so.handleGet(message)
			}
		case <-so.pullerClose:
			so.log.Info("exiting pullerTask")
			return
		}
	}

}

func (so *Source) handleSet(setMessage nanodm.Message) {
	err := so.callbacks.SetObjects(setMessage.Objects)
	if err != nil {
		nackMessasge := so.newMessage(nanodm.NackMessageType)
		nackMessasge.TransactionUID = setMessage.TransactionUID
		nackMessasge.Error = err.Error()
		so.pusherChan <- nackMessasge
	}

	ackMessage := so.newMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = setMessage.TransactionUID
	so.pusherChan <- ackMessage
}

func (so *Source) handleGet(getMessage nanodm.Message) {
	objectNames := make([]string, 0)

	for _, object := range getMessage.Objects {
		objectNames = append(objectNames, object.Name)
	}
	objects, err := so.callbacks.GetObjects(objectNames)
	if err != nil {
		nackMessasge := so.newMessage(nanodm.NackMessageType)
		nackMessasge.TransactionUID = getMessage.TransactionUID
		nackMessasge.Error = err.Error()
		so.pusherChan <- nackMessasge
	}

	ackMessage := so.newMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = getMessage.TransactionUID
	ackMessage.Objects = objects
	so.pusherChan <- ackMessage
}
