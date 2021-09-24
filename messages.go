package nanodm

import (
	"sync"

	"github.com/google/uuid"
)

type MessageType uint

const (
	RegisterMessageType MessageType = iota
	DeregisterMessageType
	SetMessageType
	GetMessageType
	AckMessageType
	NackMessageType
)

type ObjectType uint

const (
	TypeString ObjectType = iota
	TypeInt
	TypeUnsignedInt
	TypeBool
	TypeNil
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

type ConcurrentMessageMap struct {
	messages map[string]Message
	lock     sync.RWMutex
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
