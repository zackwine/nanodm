package coordinator

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
)

type Server struct {
	log     *logrus.Entry
	url     string
	handler CoordinatorHandler

	puller     *nanodm.Puller
	pullerChan chan nanodm.Message
	closeChan  chan struct{}

	clients map[string]*Client
	objects map[string]*CoordinatorObject
	ackMap  *nanodm.ConcurrentMessageMap
}

type CoordinatorObject struct {
	object nanodm.Object
	client *Client
}

func NewServer(log *logrus.Entry, url string, handler CoordinatorHandler) *Server {
	return &Server{
		log:        log,
		url:        url,
		handler:    handler,
		pullerChan: make(chan nanodm.Message),
		closeChan:  make(chan struct{}),
		clients:    make(map[string]*Client),
		objects:    make(map[string]*CoordinatorObject),
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
		if cobject.object.Access != nanodm.AccessRW {
			return fmt.Errorf("the object %s isn't read-write", object.Name)
		}
		se.log.Infof("Calling Set on object (%+v) %+v", object, cobject.object)
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
			clientToObject[cobject.client.sourceName] = append(clientToObject[cobject.client.sourceName], cobject.object)
		} else {
			errs = append(errs, fmt.Errorf("object (%s) doesn't exist", objName))
		}
	}
	if len(errs) > 0 {
		return objects, errs
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

// PrintObjectMap: For debugging purposes
func (se *Server) PrintObjectMap() {

	se.log.Infof("Printing (%d) objects", len(se.objects))
	for name, object := range se.objects {
		se.log.Infof("- %s", name)
		se.log.Infof("     %+v", object)
	}
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
		se.log.Infof("Registering new client (%s)", message.SourceName)
		se.registerClient(message)
	case message.Type == nanodm.UnregisterMessageType:
		se.log.Infof("Unregistering client (%s)", message.SourceName)
		se.unregisterClient(message)
	case message.Type == nanodm.UpdateObjectsMessageType:
		se.log.Infof("Updating client (%s)", message.SourceName)
		se.updateObjects(message)
	case message.Type == nanodm.GetMessageType:
		se.log.Infof("Get message from client (%s)", message.SourceName)
		//TODO: allow client to get other clients
	case message.Type == nanodm.SetMessageType:
		se.log.Infof("Set message from client (%s)", message.SourceName)
		//TODO: allow client to set other clients
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

func (se *Server) respondNack(client *Client, request nanodm.Message, errMsg string) {
	nackMessage := client.GetMessage(nanodm.NackMessageType)
	nackMessage.TransactionUID = request.TransactionUID
	nackMessage.Source = se.url
	nackMessage.Error = errMsg
	client.Send(nackMessage)
}

func (se *Server) registerClient(message nanodm.Message) {

	newClient := NewClient(se.log, message.SourceName, message.Source)
	err := newClient.Connect()
	if err != nil {
		se.log.Errorf("Failed to connect to source %s at %s.", message.SourceName, message.Source)
		return
	}

	if _, ok := se.clients[message.SourceName]; ok {
		errStr := fmt.Sprintf("error source name (%s) already exists", message.SourceName)
		se.log.Error(errStr)
		se.respondNack(newClient, message, errStr)
		go func() {
			// TODO: Can this be event driven?
			// Give time for nack message to send, then disconnect
			<-time.After(2 * time.Second)
			newClient.Disconnect()
		}()

		return
	}

	err = se.addObjects(newClient, message.Objects)
	if err != nil {
		errStr := fmt.Sprintf("failed to add objects for %s: %v", message.SourceName, err)
		se.log.Error(errStr)
		se.respondNack(newClient, message, errStr)
		go func() {
			// TODO: Can this be event driven?
			// Give time for nack message to send, then disconnect
			<-time.After(2 * time.Second)
			newClient.Disconnect()
		}()
		return
	}

	se.log.Infof("Registered new client (%s)", message.SourceName)
	se.clients[message.SourceName] = newClient

	ackMessage := newClient.GetMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = message.TransactionUID
	ackMessage.Source = se.url
	newClient.Send(ackMessage)

	se.handler.Registered(se, message.SourceName, message.Objects)
}

func (se *Server) isObjectRegistered(objectName string) bool {
	if _, ok := se.objects[objectName]; ok {
		return true
	}
	return false
}

func (se *Server) addObjects(client *Client, objects []nanodm.Object) error {
	// Are these objects already registered
	for _, object := range objects {
		if se.isObjectRegistered(object.Name) {
			return fmt.Errorf("failed to add objects object (%s) already exists", object.Name)
		}
	}

	client.objects = objects

	for _, object := range objects {
		//se.log.Debugf("Registering object (%+v)", object)
		se.objects[object.Name] = &CoordinatorObject{
			object: object,
			client: client,
		}
	}

	return nil
}

func (se *Server) unregisterClient(message nanodm.Message) {
	if client, exists := se.clients[message.SourceName]; exists {
		err := se.handler.Unregistered(se, client.sourceName, client.objects)
		if err != nil {
			se.log.Errorf("failed in unregister callback: %v", err)
		}
		se.removeObjects(client)
		delete(se.clients, message.SourceName)

		// Notify the client they have been unregistered
		ackMessage := client.GetMessage(nanodm.AckMessageType)
		ackMessage.TransactionUID = message.TransactionUID
		ackMessage.Source = se.url
		client.Send(ackMessage)

	} else {
		se.log.Errorf("Error unregistering client (%s) it isn't registered? %+v", message.SourceName, message)
	}
}

func (se *Server) removeObjects(client *Client) {
	// remove all objects owned by this client
	for _, object := range client.objects {
		delete(se.objects, object.Name)
	}
}

func (se *Server) updateObjects(message nanodm.Message) {

	// Does the client exist
	client, clientExists := se.clients[message.SourceName]
	if !clientExists {
		se.log.Errorf("error received update to non-existent client")
		return
	}

	// Create maps for sorting objects
	existingMap := make(map[string]nanodm.Object)
	newMap := make(map[string]nanodm.Object)
	for _, updatedObject := range message.Objects {
		se.log.Infof("Checking updated object (%s)", updatedObject.Name)
		if existingObject, ok := se.objects[updatedObject.Name]; ok {
			// Are these updated objects registed with another client
			if existingObject.client.sourceName != message.SourceName {
				errStr := fmt.Sprintf("failed to add objects object (%s) already exists and is owned by %s", updatedObject.Name, existingObject.client.sourceName)
				se.log.Error(errStr)
				se.respondNack(client, message, errStr)
				return
			}
			existingMap[updatedObject.Name] = updatedObject
		} else {
			newMap[updatedObject.Name] = updatedObject
		}
	}

	// Update existing objects, and remove missing objects
	for _, oldObject := range client.objects {
		if _, exists := existingMap[oldObject.Name]; exists {
			// Update existing objects
			se.objects[oldObject.Name].object = existingMap[oldObject.Name]
		} else {
			// Delete objects missing from the new updated list
			delete(se.objects, oldObject.Name)
		}
	}

	// Add new objects
	for objectName, newObject := range newMap {
		se.objects[objectName] = &CoordinatorObject{
			object: newObject,
			client: client,
		}
	}
	client.objects = message.Objects

	ackMessage := client.GetMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = message.TransactionUID
	ackMessage.Source = se.url
	client.Send(ackMessage)

	se.handler.UpdateObjects(se, client.sourceName, client.objects)
}
