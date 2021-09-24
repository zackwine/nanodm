package nanodm

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPusherPuller(t *testing.T) {
	logrus.SetFormatter(&logrus.TextFormatter{
		PadLevelText:  true,
		FullTimestamp: true,
		ForceQuote:    true,
	})
	logrus.SetLevel(logrus.DebugLevel)
	log := logrus.NewEntry(logrus.New())

	uri := "tcp://127.0.0.1:4500"

	pullerChan := make(chan Message)
	puller := NewPuller(log, uri, pullerChan)
	err := puller.Start()
	assert.Nil(t, err)

	pusherChan := make(chan Message)
	pusher := NewPusher(log, uri, pusherChan)
	err = pusher.Start()
	assert.Nil(t, err)

	message := Message{
		Type:        RegisterMessageType,
		Source:      uri,
		Destination: "test",
	}
	pusherChan <- message

	select {
	case readMessage := <-pullerChan:
		log.Infof("Received message: %+v", readMessage)
		assert.Equal(t, message.Type, readMessage.Type)
		assert.Equal(t, message.Source, readMessage.Source)
		assert.Equal(t, message.Destination, readMessage.Destination)
	case <-time.After(3 * time.Second):
		t.Error("Timeout waiting for message")
	}

	message2 := Message{
		Type:        GetMessageType,
		Source:      uri,
		Destination: "test",
	}
	pusherChan <- message2

	select {
	case readMessage2 := <-pullerChan:
		log.Infof("Received message2: %+v", readMessage2)
		assert.Equal(t, message2.Type, readMessage2.Type)
		assert.Equal(t, message2.Source, readMessage2.Source)
		assert.Equal(t, message2.Destination, readMessage2.Destination)
	case <-time.After(3 * time.Second):
		t.Error("Timeout waiting for message")
	}

	err = pusher.Stop()
	assert.Nil(t, err)
	<-time.After(2 * time.Second)
	err = puller.Stop()
	assert.Nil(t, err)
}
