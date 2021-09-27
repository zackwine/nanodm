package collector

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
)

type Server struct {
	log        *logrus.Entry
	url        string
	puller     *nanodm.Puller
	pullerChan chan nanodm.Message
	closeChan  chan struct{}

	clients map[string]*Client
	objects map[string]*CollectorObject
	ackMap  *nanodm.ConcurrentMessageMap
}

type CollectorObject struct {
	object *nanodm.Object
	client *Client
}

func NewServer(log *logrus.Entry, url string) *Server {
	return &Server{
		log:        log,
		url:        url,
		pullerChan: make(chan nanodm.Message),
		closeChan:  make(chan struct{}),
		clients:    make(map[string]*Client),
		objects:    make(map[string]*CollectorObject),
		ackMap:     nanodm.NewConcurrentMessageMap(),
	}
}
func (se *Server) Start() error {
	var err error
	se.puller = nanodm.NewPuller(se.log, se.url, se.pullerChan)

	go se.pullerTask()

	err = se.puller.Start()
	if err != nil {
		se.log.Errorf("Failed to start server on %s: %v", se.url, err)
		return err
	}

	return nil
}

func (se *Server) Stop() error {
	close(se.closeChan)
	return se.puller.Stop()
}

func (se *Server) Set(object nanodm.Object) error {
	var setMessage nanodm.Message
	if cobject, ok := se.objects[object.Name]; ok {
		setMessage = cobject.client.GetMessage(nanodm.SetMessageType)
		setMessage.Source = se.url
		setMessage.Objects = []nanodm.Object{object}
		cobject.client.Send(setMessage)
	} else {
		return fmt.Errorf("the object %s doesn't exist", object.Name)
	}

	ackMessage, err := se.ackMap.WaitForKey(setMessage.TransactionUID.String(), 10*time.Second)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("failed to set object %s: %v", object.Name, ackMessage.Error)
	} else {
		return fmt.Errorf("set received unknown message response type (%d)", ackMessage.Type)
	}

}

func (se *Server) Get(objectNames []string) (objects []nanodm.Object, errs []error) {
	clientToObject := make(map[string][]nanodm.Object)

	// Build a list for each client
	for _, objName := range objectNames {
		if cobject, ok := se.objects[objName]; ok {
			clientToObject[cobject.client.sourceName] = append(clientToObject[cobject.client.sourceName], *cobject.object)
		}
	}

	for sourceName, getObjects := range clientToObject {
		retObjects, err := se.getSource(sourceName, getObjects)
		if err != nil {
			errs = append(errs, err)
		}

		objects = append(objects, retObjects...)
	}

	return objects, errs
}

func (se *Server) getSource(sourceName string, getObjects []nanodm.Object) (objects []nanodm.Object, err error) {
	var getMessage nanodm.Message
	if client, ok := se.clients[sourceName]; ok {

		getMessage = client.GetMessage(nanodm.GetMessageType)
		getMessage.Source = se.url
		getMessage.Objects = getObjects
		client.Send(getMessage)

		ackMessage, err := se.ackMap.WaitForKey(getMessage.TransactionUID.String(), 10*time.Second)
		if err != nil {
			return ackMessage.Objects, err
		}
		if ackMessage.Type == nanodm.AckMessageType {
			return ackMessage.Objects, nil
		} else if ackMessage.Type == nanodm.NackMessageType {
			return ackMessage.Objects, fmt.Errorf("failed to get objects: %v", ackMessage.Error)
		} else {
			return ackMessage.Objects, fmt.Errorf("get received unknown message response type (%d)", ackMessage.Type)
		}

	}

	return nil, fmt.Errorf("failed to find source %s", sourceName)
}

func (se *Server) handleMessage(message nanodm.Message) {
	switch {
	case message.Type == nanodm.RegisterMessageType:
		if client, ok := se.clients[message.SourceName]; ok {
			// TODO: Client already exists update registered objects?
			client.objects = message.Objects
		} else {
			se.log.Infof("Registering new client (%s)", message.SourceName)
			se.registerClient(message)
		}
	case message.Type == nanodm.DeregisterMessageType:
		se.log.Infof("Deregistering client (%s)", message.Source)
	case message.Type == nanodm.GetMessageType:
		se.log.Infof("Get message from client (%s)", message.Source)
	case message.Type == nanodm.SetMessageType:
		se.log.Infof("Set message from client (%s)", message.Source)
	case message.Type == nanodm.AckMessageType || message.Type == nanodm.NackMessageType:
		se.ackMap.Set(message.TransactionUID.String(), message)
	}
}

func (se *Server) pullerTask() {
	defer se.log.Warnf("Exiting server pullerTask (%s)", se.url)
	for {
		select {
		case message := <-se.pullerChan:
			se.handleMessage(message)
		case <-se.closeChan:
			return
		}
	}
}

func (se *Server) registerClient(message nanodm.Message) {

	newClient := NewClient(se.log, message.SourceName, message.Source)
	err := newClient.Connect()
	if err != nil {
		se.log.Errorf("Failed to connect to source %s at %s.", message.SourceName, message.Source)
		return
	}

	err = se.addObjects(newClient, message.Objects)
	if err != nil {
		se.log.Errorf("Failed to add objects for %s: %v", message.SourceName, err)
		return
	}

	se.log.Infof("Registered new client (%s)", message.SourceName)
	se.clients[message.SourceName] = newClient

	ackMessage := newClient.GetMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = message.TransactionUID
	ackMessage.Source = se.url
	newClient.Send(ackMessage)
}

func (se *Server) isObjectRegistered(objectName string) bool {
	if _, ok := se.objects[objectName]; ok {
		return true
	}
	return false
}

func (se *Server) addObjects(client *Client, objects []nanodm.Object) error {
	// Check if objects are registered
	for _, object := range objects {
		if se.isObjectRegistered(object.Name) {
			return fmt.Errorf("failed to add objects object (%s) already exists", object.Name)
		}
	}

	client.objects = objects

	for _, object := range objects {
		se.objects[object.Name] = &CollectorObject{
			object: &object,
			client: client,
		}
	}

	return nil
}
