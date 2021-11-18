package coordinator

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
)

const (
	PING_PERIOD = 15 * time.Second
)

type Server struct {
	log     *logrus.Entry
	url     string
	handler CoordinatorHandler

	puller     *nanodm.Puller
	pullerChan chan nanodm.Message
	closeChan  chan struct{}

	clients           map[string]*Client
	objects           map[string]*CoordinatorObject
	dynamicLists      map[string]*CoordinatorObject
	ackMap            *nanodm.ConcurrentMessageMap
	registrationMutex sync.Mutex
}

type CoordinatorObject struct {
	object nanodm.Object
	client *Client
}

func NewServer(log *logrus.Entry, url string, handler CoordinatorHandler) *Server {
	return &Server{
		log:          log,
		url:          url,
		handler:      handler,
		pullerChan:   make(chan nanodm.Message),
		closeChan:    make(chan struct{}),
		clients:      make(map[string]*Client),
		objects:      make(map[string]*CoordinatorObject),
		dynamicLists: make(map[string]*CoordinatorObject),
		ackMap:       nanodm.NewConcurrentMessageMap(),
	}
}
func (se *Server) SetHandler(handler CoordinatorHandler) {
	se.handler = handler
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

	go se.pingTask()

	return nil
}

func (se *Server) Stop() error {
	close(se.closeChan)
	return se.puller.Stop()
}

func (se *Server) Set(object nanodm.Object) error {
	var setMessage nanodm.Message
	if cobject, ok := se.objects[object.Name]; ok {
		se.log.Infof("Calling Set on object (%+v) %+v", object, cobject.object)
		setMessage = cobject.client.GetMessage(nanodm.SetMessageType)
		setMessage.Source = se.url
		setMessage.Objects = []nanodm.Object{object}
		cobject.client.Send(setMessage)
	} else if dynObject := se.isObjectHandledByDynamicList(object.Name); dynObject != nil {
		se.log.Infof("Calling Set on object on dynamic list object (%+v) %+v", object, dynObject.object)
		setMessage = dynObject.client.GetMessage(nanodm.SetMessageType)
		setMessage.Source = se.url
		setMessage.Objects = []nanodm.Object{object}
		dynObject.client.Send(setMessage)
	} else {
		return fmt.Errorf("the object %s isn't registered", object.Name)
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
		} else if dynamicObject, exists := se.dynamicLists[objName]; exists {
			se.log.Infof("dynamicObject: %+v", dynamicObject)
			se.log.Infof("clientToObject: %+v", clientToObject)
			clientToObject[dynamicObject.client.sourceName] = append(clientToObject[dynamicObject.client.sourceName], dynamicObject.object)
		} else if dynamicObject := se.isObjectHandledByDynamicList(objName); dynamicObject != nil {
			clientToObject[dynamicObject.client.sourceName] = append(clientToObject[dynamicObject.client.sourceName], nanodm.Object{Name: objName})
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
		if retObjects != nil {
			objects = append(objects, retObjects...)
		}
	}

	return objects, errs
}

func (se *Server) AddRow(object nanodm.Object) error {
	var addRowMessage nanodm.Message

	if dynObject := se.isObjectHandledByDynamicList(object.Name); dynObject != nil {
		se.log.Infof("Calling AddRow on object on dynamic list (%+v) %+v", object, dynObject.object)
		addRowMessage = dynObject.client.GetMessage(nanodm.AddRowMessageType)
		addRowMessage.Source = se.url
		addRowMessage.Objects = []nanodm.Object{object}
		dynObject.client.Send(addRowMessage)
	} else {
		return fmt.Errorf("the object %s isn't handled", object.Name)
	}

	ackMessage, err := se.ackMap.WaitForKey(addRowMessage.TransactionUID.String(), 10*time.Second)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("failed to add row %s: %v", object.Name, ackMessage.Error)
	} else {
		return fmt.Errorf("AddRow received unknown message response type (%d)", ackMessage.Type)
	}

}

func (se *Server) DeleteRow(object nanodm.Object) error {
	var deleteRowMessage nanodm.Message

	if dynObject := se.isObjectHandledByDynamicList(object.Name); dynObject != nil {
		se.log.Infof("Calling DeleteRow on object on dynamic list (%+v) %+v", object, dynObject.object)
		deleteRowMessage = dynObject.client.GetMessage(nanodm.DeleteRowMessageType)
		deleteRowMessage.Source = se.url
		deleteRowMessage.Objects = []nanodm.Object{object}
		dynObject.client.Send(deleteRowMessage)
	} else {
		return fmt.Errorf("the object %s isn't handled", object.Name)
	}

	ackMessage, err := se.ackMap.WaitForKey(deleteRowMessage.TransactionUID.String(), 10*time.Second)
	if err != nil {
		return err
	}
	if ackMessage.Type == nanodm.AckMessageType {
		return nil
	} else if ackMessage.Type == nanodm.NackMessageType {
		return fmt.Errorf("failed to delete row %s: %v", object.Name, ackMessage.Error)
	} else {
		return fmt.Errorf("DeleteRow received unknown message response type (%d)", ackMessage.Type)
	}

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
			return nil, err
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
		se.registrationMutex.Lock()
		se.registerClient(message)
		se.registrationMutex.Unlock()
	case message.Type == nanodm.UnregisterMessageType:
		se.log.Infof("Unregistering client (%s)", message.SourceName)
		se.registrationMutex.Lock()
		se.unregisterClient(message)
		se.registrationMutex.Unlock()
	case message.Type == nanodm.UpdateObjectsMessageType:
		se.log.Infof("Updating client (%s)", message.SourceName)
		se.updateObjects(message)
	case message.Type == nanodm.GetMessageType:
		se.log.Infof("Get message from client (%s)", message.SourceName)
		go se.handleClientGet(message)
	case message.Type == nanodm.SetMessageType:
		se.log.Infof("Set message from client (%s)", message.SourceName)
		go se.handleClientSet(message)
	case message.Type == nanodm.AckMessageType || message.Type == nanodm.NackMessageType:
		se.ackMap.Set(message.TransactionUID.String(), message)
	case message.Type == nanodm.ListMessagesType:
		se.handleClientList(message)
	case message.Type == nanodm.PingMessageType:
		se.handleClientPing(message)
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

func (se *Server) pingTask() {
	defer se.log.Warnf("Exiting server pingTask (%s)", se.url)
	for {
		select {
		case now := <-time.After(PING_PERIOD):
			// Given this handler runs as a goroutine, block modifications caused by registration
			se.registrationMutex.Lock()
			for _, client := range se.clients {
				if now.After(client.lastPing.Add(5 * PING_PERIOD)) {
					diff := now.Sub(client.lastPing)
					se.log.Warnf("removing client %s, last ping was %s ago", client.sourceName, diff.String())
					se.removeClient(client)
				}
				message := client.GetMessage(nanodm.PingMessageType)
				message.Source = se.url
				client.Send(message)
			}
			se.registrationMutex.Unlock()
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

	existingClient, clientExists := se.clients[message.SourceName]
	if clientExists && existingClient.clientUrl != newClient.clientUrl {

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

	if clientExists {
		se.log.Infof("Reregistering client (%s)", message.SourceName)
		se.removeObjects(existingClient)
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

	se.log.Infof("Registered client (%s)", message.SourceName)
	newClient.lastPing = time.Now()
	se.clients[message.SourceName] = newClient

	ackMessage := newClient.GetMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = message.TransactionUID
	ackMessage.Source = se.url
	newClient.Send(ackMessage)

	if se.handler != nil {
		se.handler.Registered(se, message.SourceName, message.Objects)
	}

}

func (se *Server) isObjectRegistered(objectName string) bool {
	if _, ok := se.objects[objectName]; ok {
		return true
	}
	if _, ok := se.dynamicLists[objectName]; ok {
		return true
	}
	return false
}

func (se *Server) isDynamicListConflicting(dynamicListPrefix string) bool {

	for objName := range se.objects {
		if strings.Contains(objName, dynamicListPrefix) {
			return true
		}
	}

	for prefixName := range se.dynamicLists {
		if strings.Contains(prefixName, dynamicListPrefix) {
			return true
		}
	}

	return false
}

func (se *Server) addObjects(client *Client, objects []nanodm.Object) error {
	// Are these objects already registered
	for _, object := range objects {
		if se.isObjectRegistered(object.Name) {
			return fmt.Errorf("failed to add objects object (%s) already exists", object.Name)
		}
		if object.Type == nanodm.TypeDynamicList {
			se.isDynamicListConflicting(object.Name)
		}
	}

	client.objects = objects

	for _, object := range objects {
		if object.Type == nanodm.TypeDynamicList {
			//se.log.Debugf("Registering object (%+v)", object)
			se.dynamicLists[object.Name] = &CoordinatorObject{
				object: object,
				client: client,
			}
		} else {
			//se.log.Debugf("Registering object (%+v)", object)
			se.objects[object.Name] = &CoordinatorObject{
				object: object,
				client: client,
			}
		}
	}

	return nil
}

func (se *Server) removeClient(client *Client) {

	if se.handler != nil {
		err := se.handler.Unregistered(se, client.sourceName, client.objects)
		if err != nil {
			se.log.Errorf("failed in unregister callback: %v", err)
		}
	}
	se.removeObjects(client)
	delete(se.clients, client.sourceName)
}

func (se *Server) unregisterClient(message nanodm.Message) {
	if client, exists := se.clients[message.SourceName]; exists {
		se.removeClient(client)

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
		if object.Type == nanodm.TypeDynamicList {
			delete(se.dynamicLists, object.Name)
		} else {
			delete(se.objects, object.Name)
		}

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
	deletedMap := make(map[string]nanodm.Object)

	for _, updatedObject := range message.Objects {
		se.log.Infof("Checking updated object (%s)", updatedObject.Name)
		if updatedObject.Type == nanodm.TypeDynamicList {
			if existingDynamic, exists := se.dynamicLists[updatedObject.Name]; exists {
				// Are these updated objects registed with another client
				if existingDynamic.client.sourceName != message.SourceName {
					errStr := fmt.Sprintf("failed to add objects object (%s) already exists and is owned by %s", updatedObject.Name, existingDynamic.client.sourceName)
					se.log.Error(errStr)
					se.respondNack(client, message, errStr)
					return
				}
				existingMap[updatedObject.Name] = updatedObject
			} else {
				newMap[updatedObject.Name] = updatedObject
			}

		} else if existingObject, ok := se.objects[updatedObject.Name]; ok {
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
			if oldObject.Type == nanodm.TypeDynamicList {
				// Update existing objects
				se.dynamicLists[oldObject.Name].object = existingMap[oldObject.Name]
			} else {
				// Update existing objects
				se.objects[oldObject.Name].object = existingMap[oldObject.Name]
			}

		} else {
			// Delete objects missing from the new updated list
			deletedMap[oldObject.Name] = oldObject
			if oldObject.Type == nanodm.TypeDynamicList {
				delete(se.dynamicLists, oldObject.Name)
			} else {
				delete(se.objects, oldObject.Name)
			}
		}
	}

	// Add new objects
	for objectName, newObject := range newMap {
		if newObject.Type == nanodm.TypeDynamicList {
			se.dynamicLists[objectName] = &CoordinatorObject{
				object: newObject,
				client: client,
			}
		} else {
			se.objects[objectName] = &CoordinatorObject{
				object: newObject,
				client: client,
			}
		}

	}
	client.objects = message.Objects

	ackMessage := client.GetMessage(nanodm.AckMessageType)
	ackMessage.TransactionUID = message.TransactionUID
	ackMessage.Source = se.url
	client.Send(ackMessage)

	if se.handler != nil {
		se.handler.UpdateObjects(se, client.sourceName, client.objects, deletedMap)
	}
}

func (se *Server) handleClientPing(message nanodm.Message) {
	if client, exists := se.clients[message.SourceName]; exists {
		client.lastPing = time.Now()
	}
}

func (se *Server) handleClientGet(message nanodm.Message) {
	var objNames []string

	// Given this handler runs as a goroutine, block modifications caused by registration
	se.registrationMutex.Lock()
	defer se.registrationMutex.Unlock()

	if client, exists := se.clients[message.SourceName]; exists {
		if message.Objects == nil || len(message.Objects) == 0 {
			se.log.Errorf("Invalid get request with empty objects list")
			se.respondNack(client, message, "Invalid get request with empty objects list")
			return
		}

		for _, obj := range message.Objects {
			objNames = append(objNames, obj.Name)
		}
		objects, err := se.Get(objNames)
		if err != nil {
			errStr := fmt.Sprintf("Failed to get objects with %v", err)
			se.log.Errorf(errStr)
			se.respondNack(client, message, errStr)
			return
		}
		ackMessage := client.GetMessage(nanodm.AckMessageType)
		ackMessage.TransactionUID = message.TransactionUID
		ackMessage.Source = se.url
		ackMessage.Objects = objects
		client.Send(ackMessage)
	} else {
		se.log.Errorf("Error get client (%s) it isn't a registered client? %+v", message.SourceName, message)
	}
}

func (se *Server) handleClientSet(message nanodm.Message) {
	var err error
	var errStr string
	var failedObjects []nanodm.Object

	// Given this handler runs as a goroutine, block modifications caused by registration
	se.registrationMutex.Lock()
	defer se.registrationMutex.Unlock()

	if client, exists := se.clients[message.SourceName]; exists {
		if message.Objects == nil || len(message.Objects) == 0 {
			se.log.Errorf("Invalid set request with empty objects list")
			se.respondNack(client, message, "Invalid set request with empty objects list")
			return
		}

		for _, object := range message.Objects {
			err = se.Set(object)
			if err != nil {
				errStr = fmt.Sprintf("%s %s;", errStr, err.Error())
				failedObjects = append(failedObjects, object)
			}
		}

		if errStr != "" {
			se.log.Errorf("Failed to set objects: %s", errStr)
			nackMessage := client.GetMessage(nanodm.NackMessageType)
			nackMessage.TransactionUID = message.TransactionUID
			nackMessage.Source = se.url
			nackMessage.Objects = failedObjects
			client.Send(nackMessage)
		}

		ackMessage := client.GetMessage(nanodm.AckMessageType)
		ackMessage.TransactionUID = message.TransactionUID
		ackMessage.Source = se.url
		ackMessage.Destination = message.Source
		client.Send(ackMessage)

	} else {
		se.log.Errorf("Error set client (%s) it isn't a registered client? %+v", message.SourceName, message)
	}
}

func (se *Server) List(path string) (objects []nanodm.Object, err error) {

	if strings.HasSuffix(path, ".") {
		for objName, regObject := range se.objects {
			if strings.HasPrefix(objName, path) {
				objects = append(objects, regObject.object)
			}
		}
	} else if regObject, exists := se.objects[path]; exists {
		objects = append(objects, regObject.object)
	} else {
		err = fmt.Errorf("failed to find object at path %s", path)
	}

	return
}

func (se *Server) handleClientList(message nanodm.Message) {

	var retObjects []nanodm.Object

	if client, exists := se.clients[message.SourceName]; exists {
		if message.Objects == nil || len(message.Objects) == 0 {
			se.log.Errorf("Invalid get request with empty objects list")
			se.respondNack(client, message, "Invalid get request with empty objects list")
			return
		}

		for _, obj := range message.Objects {
			objects, err := se.List(obj.Name)
			if err != nil {
				errStr := fmt.Sprintf("Failed to get objects with %v", err)
				se.log.Errorf(errStr)
				se.respondNack(client, message, errStr)
				return
			}
			retObjects = append(retObjects, objects...)
		}

		ackMessage := client.GetMessage(nanodm.AckMessageType)
		ackMessage.TransactionUID = message.TransactionUID
		ackMessage.Source = se.url
		ackMessage.Objects = retObjects
		client.Send(ackMessage)
	} else {
		se.log.Errorf("Error get client (%s) it isn't a registered client? %+v", message.SourceName, message)
	}
}

// isObjectHandledByDynamicList searches the dynamic object list of prefixes to
// see if `objectName` is handled by a dynamic list.  Returns the dynamic object
// if found, and nil otherwise
func (se *Server) isObjectHandledByDynamicList(objectName string) *CoordinatorObject {
	for dynObjName, dynObject := range se.dynamicLists {
		if strings.HasPrefix(objectName, dynObjName) {
			// if objectName has the prefix dynObjName, then return
			return dynObject
		}
	}
	return nil
}

//
// Exported version of isObjectHandledByDynamicList(), but only returns a boolean
//
func (se *Server) IsObjectHandledByDynamicList(objectName string) bool {
	for dynObjName, _ := range se.dynamicLists {
		if strings.HasPrefix(objectName, dynObjName) {
			// if objectName has the prefix dynObjName, then return
			return true
		}
	}
	return false
}
