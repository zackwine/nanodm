package nanodm

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type MessageType uint

const (
	RegisterMessageType MessageType = iota
	UnregisterMessageType
	UpdateObjectsMessageType
	SetMessageType
	GetMessageType
	AckMessageType
	NackMessageType
	ListMessagesType
	PingMessageType
)

type ObjectType uint

const (
	TypeString ObjectType = iota
	TypeInt
	TypeUnsignedInt
	TypeBool
	TypeDateTime
	TypeBase64
	TypeLong
	TypeUnsignedLong
	TypeFloat
	TypeDouble
	TypeByte
	TypeDynamicList ObjectType = 100000000
)

type ObjectAccess uint

const (
	AccessRW ObjectAccess = iota
	AccessRO
)

type Object struct {
	Name          string       `json:"name"`
	Access        ObjectAccess `json:"access"`
	Type          ObjectType   `json:"type"`
	IndexableFrom string       `json:"indexablefrom,omitempty"`
	Value         interface{}  `json:"value,omitempty"`
}

type Message struct {
	Type           MessageType `json:"type"`
	TransactionUID uuid.UUID   `json:"transactionUID,omitempty"`
	SourceName     string      `json:"sourceName,omitempty"`
	Source         string      `json:"source,omitempty"`
	Destination    string      `json:"destination,omitempty"`
	Objects        []Object    `json:"object,omitempty"`
	Error          string      `json:"error,omitempty"`
}

func GetTransactionUID() uuid.UUID {
	return uuid.New()
}

// A thread save map for tracking transaction IDs
type ConcurrentMessageMap struct {
	messages map[string]Message
	lock     sync.RWMutex
}

func NewConcurrentMessageMap() *ConcurrentMessageMap {
	return &ConcurrentMessageMap{
		messages: make(map[string]Message),
	}
}
func (cm *ConcurrentMessageMap) Get(key string) *Message {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	if message, ok := cm.messages[key]; ok {
		return &message
	}
	return nil
}

func (cm *ConcurrentMessageMap) Set(key string, message Message) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	cm.messages[key] = message
}

func (cm *ConcurrentMessageMap) Delete(key string) {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	delete(cm.messages, key)
}

func (cm *ConcurrentMessageMap) WaitForKey(key string, timeout time.Duration) (*Message, error) {
	defer cm.Delete(key)
	if message := cm.Get(key); message != nil {
		return message, nil
	}
	loopCnt := 0
	for {
		loopCnt++
		select {
		case <-time.After(time.Duration(loopCnt*500) * time.Millisecond):
			if message := cm.Get(key); message != nil {
				return message, nil
			}
		case <-time.After(timeout):
			return nil, fmt.Errorf("timeout waiting for (%s)", key)
		}
	}
}

func GetObjectsFromMap(objMap map[string]Object) (objects []Object) {
	for _, object := range objMap {
		objects = append(objects, object)
	}
	return objects
}
