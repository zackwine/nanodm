package source

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
)

const (
	defaultAckTimeout      = 10 * time.Second
	defaultPingCheckPeriod = 15 * time.Second
	defaultPingTimeout     = 30 * time.Second
)

type Source struct {
	log       *logrus.Entry
	name      string
	serverUrl string
	pullUrl   string
	handler   SourceHandler

	pusher           *nanodm.Pusher
	pusherChan       chan nanodm.Message
	pusherAckTimeout time.Duration
	objects          []nanodm.Object

	puller        *nanodm.Puller
	pullerChan    chan nanodm.Message
	pullerClose   chan struct{}
	ackMap        *nanodm.ConcurrentMessageMap
	registered    bool
	lastPing      time.Time
	lastPingMutex sync.Mutex
}

type SourceHandler interface {
	GetObjects(objectNames []string) (objects []nanodm.Object, err error)
	SetObjects(objects []nanodm.Object) error
	AddRow(objects nanodm.Object) (row string, err error)
	DeleteRow(row nanodm.Object) error
}

// NewSource creates a new source where `name` should be unique to the
// server at `serverUrl`.
func NewSource(log *logrus.Entry, name string, serverUrl string, pullUrl string, handler SourceHandler) *Source {
	return &Source{
		log:              log,
		name:             name,
		serverUrl:        serverUrl,
		pullUrl:          pullUrl,
		handler:          handler,
		pusherAckTimeout: defaultAckTimeout,
		pusherChan:       make(chan nanodm.Message),
		pullerChan:       make(chan nanodm.Message),
		pullerClose:      make(chan struct{}),
		ackMap:           nanodm.NewConcurrentMessageMap(),
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

func (so *Source) SetHandler(handler SourceHandler) {
	so.handler = handler
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
	go so.pingTask()

	return nil
}

func (so *Source) Disconnect() error {
	if so.registered {
		so.Unregister()
	}
	return nil
}

func (so *Source) Register(objects []nanodm.Object) error {
	message := so.newMessage(nanodm.RegisterMessageType)
	so.objects = objects
	message.Objects = objects
	so.pusherChan <- message

	// Wait for ack
	ackMessage, err := so.ackMap.WaitForKey(message.TransactionUID.String(), so.pusherAckTimeout)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		so.registered = true
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("received registration error: %v", ackMessage.Error)
	} else {
		return fmt.Errorf("received unknown message type (%d)", ackMessage.Type)
	}
}

func (so *Source) Unregister() error {
	var err error
	unregMessage := so.newMessage(nanodm.UnregisterMessageType)
	so.pusherChan <- unregMessage
	so.registered = false

	ackMessage, err := so.ackMap.WaitForKey(unregMessage.TransactionUID.String(), so.pusherAckTimeout)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("received unregistration error: %v", ackMessage.Error)
	} else {
		return fmt.Errorf("received unknown message type (%d)", ackMessage.Type)
	}

}

func (so *Source) UpdateObjects(objects []nanodm.Object) error {
	var err error
	updateMessage := so.newMessage(nanodm.UpdateObjectsMessageType)
	so.objects = objects
	updateMessage.Objects = objects
	so.pusherChan <- updateMessage
	// Wait for ack
	ackMessage, err := so.ackMap.WaitForKey(updateMessage.TransactionUID.String(), so.pusherAckTimeout)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("received update error: %v", ackMessage.Error)
	} else {
		return fmt.Errorf("received unknown message type (%d)", ackMessage.Type)
	}
}

func (so *Source) GetObjects(objects []nanodm.Object) ([]nanodm.Object, error) {
	var err error
	getMessage := so.newMessage(nanodm.GetMessageType)
	getMessage.Objects = objects

	so.pusherChan <- getMessage
	// Wait for ack
	ackMessage, err := so.ackMap.WaitForKey(getMessage.TransactionUID.String(), so.pusherAckTimeout)
	if err != nil {
		return nil, err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return ackMessage.Objects, nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return ackMessage.Objects, fmt.Errorf("received get error: %v", ackMessage.Error)
	} else {
		return ackMessage.Objects, fmt.Errorf("received unknown message type (%d)", ackMessage.Type)
	}
}

func (so *Source) SetObject(object nanodm.Object) error {
	var err error
	setMessage := so.newMessage(nanodm.SetMessageType)
	setMessage.Objects = []nanodm.Object{object}

	so.pusherChan <- setMessage
	// Wait for ack
	ackMessage, err := so.ackMap.WaitForKey(setMessage.TransactionUID.String(), so.pusherAckTimeout)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("received set error: %v", ackMessage.Error)
	} else {
		return fmt.Errorf("received unknown message type (%d)", ackMessage.Type)
	}
}

func (so *Source) ListObjects(objects []nanodm.Object) ([]nanodm.Object, error) {
	var err error
	getMessage := so.newMessage(nanodm.ListMessagesType)
	getMessage.Objects = objects

	so.pusherChan <- getMessage
	// Wait for ack
	ackMessage, err := so.ackMap.WaitForKey(getMessage.TransactionUID.String(), so.pusherAckTimeout)
	if err != nil {
		return nil, err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return ackMessage.Objects, nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return ackMessage.Objects, fmt.Errorf("received get error: %v", ackMessage.Error)
	} else {
		return ackMessage.Objects, fmt.Errorf("received unknown message type (%d)", ackMessage.Type)
	}
}

func (so *Source) pullerTask() {
	for {
		select {
		case message := <-so.pullerChan:
			so.log.Infof("Received message: %+v", message)
			switch {
			case message.Type == nanodm.AckMessageType || message.Type == nanodm.NackMessageType:
				so.ackMap.Set(message.TransactionUID.String(), message)
			case message.Type == nanodm.SetMessageType:
				so.handleSet(message)
			case message.Type == nanodm.GetMessageType:
				so.handleGet(message)
			case message.Type == nanodm.AddRowMessageType:
				so.handleAddRow(message)
			case message.Type == nanodm.DeleteRowMessageType:
				so.handleDeleteRow(message)
			case message.Type == nanodm.PingMessageType:
				so.updatePing()
				so.pusherChan <- so.newMessage(nanodm.PingMessageType)
			}
		case <-so.pullerClose:
			so.log.Info("exiting pullerTask")
			return
		}
	}
}

func (so *Source) updatePing() {
	so.lastPingMutex.Lock()
	so.lastPing = time.Now()
	so.lastPingMutex.Unlock()
}

func (so *Source) pingTask() {

	for {
		select {
		case now := <-time.After(defaultPingCheckPeriod):
			so.lastPingMutex.Lock()
			if now.After(so.lastPing.Add(defaultPingTimeout)) {
				diff := now.Sub(so.lastPing)
				so.log.Warnf("re-registering client %s, last ping was %s ago", so.name, diff.String())
				err := so.Register(so.objects)
				if err != nil {
					so.log.Errorf("failed to re-register: %v", err)
				}
			}
			so.lastPingMutex.Unlock()
		case <-so.pullerClose:
			so.log.Info("exiting pingTask")
			return
		}
	}
}

func (so *Source) respondNack(msg nanodm.Message, errStr string) {
	nackMessasge := so.newMessage(nanodm.NackMessageType)
	nackMessasge.TransactionUID = msg.TransactionUID
	nackMessasge.Error = errStr
	so.pusherChan <- nackMessasge
}

func (so *Source) handleSet(setMessage nanodm.Message) {
	if so.handler == nil {
		so.respondNack(setMessage, "source handler not set")
		return
	}

	err := so.handler.SetObjects(setMessage.Objects)
	if err != nil {
		so.respondNack(setMessage, err.Error())
		return
	}

	ackMessage := so.newMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = setMessage.TransactionUID
	so.pusherChan <- ackMessage
}

func (so *Source) handleGet(getMessage nanodm.Message) {

	if so.handler == nil {
		so.respondNack(getMessage, "source handler not set")
		return
	}

	objectNames := make([]string, 0)

	for _, object := range getMessage.Objects {
		objectNames = append(objectNames, object.Name)
	}
	objects, err := so.handler.GetObjects(objectNames)
	if err != nil {
		so.respondNack(getMessage, err.Error())
		return
	}

	ackMessage := so.newMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = getMessage.TransactionUID
	ackMessage.Objects = objects
	so.pusherChan <- ackMessage
}

func (so *Source) handleAddRow(addRowMessage nanodm.Message) {
	if so.handler == nil {
		so.respondNack(addRowMessage, "source handler not set")
		return
	}

	if len(addRowMessage.Objects) != 1 {
		so.respondNack(addRowMessage, fmt.Sprintf("Invalid number of objects (%d) in add row", len(addRowMessage.Objects)))
		return
	}

	row, err := so.handler.AddRow(addRowMessage.Objects[0])
	if err != nil {
		so.respondNack(addRowMessage, err.Error())
		return
	}

	ackMessage := so.newMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = addRowMessage.TransactionUID
	ackMessage.Objects = append(ackMessage.Objects, nanodm.Object{Name: row})
	so.pusherChan <- ackMessage
}

func (so *Source) handleDeleteRow(deleteRowMessage nanodm.Message) {
	if so.handler == nil {
		so.respondNack(deleteRowMessage, "source handler not set")
		return
	}

	if len(deleteRowMessage.Objects) != 1 {
		so.respondNack(deleteRowMessage, fmt.Sprintf("Invalid number of objects (%d) in delete row", len(deleteRowMessage.Objects)))
		return
	}

	err := so.handler.DeleteRow(deleteRowMessage.Objects[0])
	if err != nil {
		so.respondNack(deleteRowMessage, err.Error())
		return
	}

	ackMessage := so.newMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = deleteRowMessage.TransactionUID
	so.pusherChan <- ackMessage
}
