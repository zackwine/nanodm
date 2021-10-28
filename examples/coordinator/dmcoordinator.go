package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zackwine/nanodm"
	"github.com/zackwine/nanodm/coordinator"
)

type ExampleCoordinator struct {
	log *logrus.Entry
}

func (ch *ExampleCoordinator) Registered(server *coordinator.Server, sourceName string, objects []nanodm.Object) error {
	ch.log.Infof("Registered source %s", sourceName)
	if sourceName == "testSource2" {
		go ch.testGetAndSet(server)
		go ch.printObjects(server)
	}

	return nil
}

func (ch *ExampleCoordinator) Unregistered(server *coordinator.Server, sourceName string, objects []nanodm.Object) error {
	ch.log.Infof("Unregistered source %s", sourceName)
	if sourceName == "testSource2" {
		go ch.printObjects(server)
	}
	return nil
}

func (ch *ExampleCoordinator) UpdateObjects(server *coordinator.Server, sourceName string, objects []nanodm.Object, deletedObjects map[string]nanodm.Object) error {
	ch.log.Infof("UpdateObjects called for source %s", sourceName)
	go ch.printObjects(server)
	return nil
}

func (ch *ExampleCoordinator) printObjects(server *coordinator.Server) {
	<-time.After(3 * time.Second)
	server.PrintObjectMap()
}

func (ch *ExampleCoordinator) testGetAndSet(server *coordinator.Server) {

	// Get some of the example objects
	objects, errs := server.Get([]string{"Device.DeviceInfo.Version", "Device.DeviceInfo.Serial", "Device.Custom.Setting1", "Device.Custom.Setting2", "Device.Custom.Version"})
	if len(errs) > 0 {
		ch.log.Errorf("Failed to get object: %v", errs)
	}
	ch.log.Infof("%+v", objects)

	// Set one of the read-write objects
	err := server.Set(nanodm.Object{
		Name:  "Device.Custom.Setting1",
		Value: "3.3.3.3",
	})
	if err != nil {
		ch.log.Errorf("Failed to set object: %v", err)
	}

	// Get the object that was just set
	objects, errs = server.Get([]string{"Device.Custom.Setting1"})
	if len(errs) > 0 {
		ch.log.Errorf("Failed to get object: %v", errs)
	}
	ch.log.Infof("%+v", objects)
}

func main() {

	url := "tcp://127.0.0.1:4800"

	logrus.SetFormatter(&logrus.TextFormatter{
		PadLevelText:  true,
		FullTimestamp: true,
		ForceQuote:    true,
	})
	logrus.SetLevel(logrus.DebugLevel)
	log := logrus.NewEntry(logrus.New())

	exCoordinator := &ExampleCoordinator{
		log: log.WithField("handler", "ExampleCoordinator"),
	}

	log.Info("Starting dmcoordinator example...")
	server := coordinator.NewServer(log, url, exCoordinator)
	err := server.Start()
	if err != nil {
		log.Fatalf("Failed to start server with %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	logrus.Infof("Killed with sig %v", sig)

}
